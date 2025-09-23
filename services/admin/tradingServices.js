import mongoose from "mongoose";
import LPPosition from "../../models/LPPositionSchema.js";
import Order from "../../models/OrderSchema.js";
import Ledger from "../../models/LedgerSchema.js";
import Account from "../../models/AccountSchema.js";
import mt5Service from "../../services/Trading/mt5Service.js";
import LPProfit from "../../models/LPProfit.js";

const TROY_OUNCE_GRAMS = 31.103;
const TTB_FACTOR = 116.64;

const GRAMS_PER_BAR = {
  TTBAR: 117,
  KGBAR: 1000,
};

// Conversion factors for TTBAR and KGBAR
const CONVERSION_FACTORS = {
  TTBAR: 13.7628,
  KGBAR: 32.1507 * 3.674, // 32.1507 * 3.6740 = 118.00167218
};

// Symbol mapping for CRM to MT5
const SYMBOL_MAPPING = {
  TTBAR: process.env.MT5_SYMBOL || "XAUUSD.gm",
  KGBAR: process.env.MT5_SYMBOL || "XAUUSD.gm",
};

// Generate unique entry ID for ledger
const generateEntryId = (prefix) => {
  const timestamp = Date.now().toString();
  const randomStr = Math.random().toString(36).substring(2, 5).toUpperCase();
  return `${prefix}-${timestamp.substring(timestamp.length - 5)}-${randomStr}`;
};

export const createTrade = async (
  adminId,
  userId,
  tradeData,
  session = null
) => {
  const mongoSession = session || (await mongoose.startSession());
  let committed = false;
  let sessionEnded = false;
  console.log(tradeData);
  try {
    if (!session) mongoSession.startTransaction();

    // Validate user account
    const userAccount = await Account.findById(userId).session(mongoSession);
    if (!userAccount) {
      throw new Error("User account not found");
    }

    const currentCashBalance = parseFloat(userAccount.reservedAmount);
    const currentMetalBalance = parseFloat(userAccount.METAL_WT);
    let currentPrice = parseFloat(tradeData.openingPrice); // This is the client price (with spread)
    const volume = parseFloat(tradeData.volume);
    const symbol = tradeData.symbol;

    // Validate symbol
    if (!GRAMS_PER_BAR[symbol]) {
      throw new Error(`Invalid symbol: ${symbol}`);
    }

    // Check sufficient balances
    const requiredMargin = parseFloat(tradeData.requiredMargin || 0);
    if (currentCashBalance < requiredMargin) {
      throw new Error("Insufficient cash balance");
    }

    // Calculate grams for the trade based on symbol
    const grams = volume / GRAMS_PER_BAR[symbol];
    currentPrice = currentPrice / grams;

    // Calculate base MT5 price (without spread) from client price
    let mt5BasePrice;
    if (symbol === "TTBAR") {
      mt5BasePrice = currentPrice / CONVERSION_FACTORS.TTBAR;
    } else {
      mt5BasePrice = currentPrice / CONVERSION_FACTORS.KGBAR;
    }

    // Calculate client order price with spread
    let clientOrderPrice;
    if (tradeData.type === "BUY") {
      clientOrderPrice = currentPrice; // Already includes askSpread from frontend
    } else {
      clientOrderPrice = currentPrice; // Already includes bidSpread from frontend
    }

    // Calculate LP price (MT5 price with spread adjustment)
    let lpCurrentPrice;
    if (tradeData.type === "BUY") {
      lpCurrentPrice = mt5BasePrice - (userAccount.askSpread || 0); // Subtract askSpread for BUY
    } else {
      lpCurrentPrice = mt5BasePrice - (userAccount.bidSpread || 0); // Subtract bidSpread for SELL
    }

    // Calculate gold weight value (client perspective)
    let goldWeightValue;
    if (symbol === "TTBAR") {
      goldWeightValue = clientOrderPrice * CONVERSION_FACTORS.TTBAR * grams;
    } else {
      goldWeightValue = clientOrderPrice * CONVERSION_FACTORS.KGBAR * grams;
    }

    // Calculate LP gold weight value (LP perspective)
    let lpGoldWeightValue;
    if (symbol === "TTBAR") {
      lpGoldWeightValue = lpCurrentPrice * CONVERSION_FACTORS.TTBAR * grams;
    } else {
      lpGoldWeightValue = lpCurrentPrice * CONVERSION_FACTORS.KGBAR * grams;
    }

    // Calculate LP Profit
    const spread =
      tradeData.type === "BUY"
        ? userAccount.askSpread || 0
        : userAccount.bidSpread || 0;
    const gramValue =
      symbol === "TTBAR" ? CONVERSION_FACTORS.TTBAR : CONVERSION_FACTORS.KGBAR;
    const lpProfitValue = (gramValue * grams * spread).toFixed(2);

    // Create order
    const newOrder = new Order({
      orderNo: tradeData.orderNo,
      type: tradeData.type,
      volume: tradeData.volume,
      symbol: tradeData.symbol,
      requiredMargin: requiredMargin,
      price: tradeData.openingPrice.toFixed(2),
      openingPrice: tradeData.openingPrice.toFixed(2),
      profit: 0,
      user: userId,
      adminId: adminId,
      orderStatus: "PROCESSING",
      openingDate: tradeData.openingDate,
      storedTime: new Date(),
      comment: tradeData.comment,
      ticket: tradeData.ticket || null,
      lpPositionId: null,
      stopLoss: tradeData.stopLoss || 0,
      takeProfit: tradeData.takeProfit || 0,
      isTradeSafe: tradeData.takeProfit || tradeData.stopLoss ? true : false,
    });
    const savedOrder = await newOrder.save({ session: mongoSession });

    // Create LP position
    const lpPosition = new LPPosition({
      positionId: tradeData.orderNo,
      type: tradeData.type,
      profit: 0,
      volume: tradeData.volume,
      adminId: adminId,
      symbol: tradeData.symbol,
      entryPrice: lpCurrentPrice.toFixed(2),
      openDate: tradeData.openingDate,
      currentPrice: lpCurrentPrice.toFixed(2),
      clientOrders: savedOrder._id,
      status: "OPEN",
    });
    const savedLPPosition = await lpPosition.save({ session: mongoSession });

    // Create LP Profit entry
    const lpProfit = new LPProfit({
      orderNo: tradeData.orderNo,
      orderType: tradeData.type,
      status: "OPEN",
      volume: tradeData.volume,
      value: lpProfitValue,
      user: userId,
      datetime: new Date(tradeData.openingDate),
    });
    const savedLPProfit = await lpProfit.save({ session: mongoSession });

    // Update order with lpPositionId
    savedOrder.lpPositionId = savedLPPosition._id;
    await savedOrder.save({ session: mongoSession });

    // Update account balances
    let newCashBalance = currentCashBalance - requiredMargin;
    let newMetalBalance = currentMetalBalance;

    if (tradeData.type === "BUY") {
      newMetalBalance = currentMetalBalance + grams;
    } else if (tradeData.type === "SELL") {
      newMetalBalance = currentMetalBalance - grams;
    }

    await Account.findByIdAndUpdate(
      userId,
      {
        reservedAmount: newCashBalance.toFixed(2),
        METAL_WT: newMetalBalance.toFixed(2),
      },
      { session: mongoSession, new: true }
    );

    // Create ledger entries
    const orderLedgerEntry = new Ledger({
      entryId: generateEntryId("ORD"),
      entryType: "ORDER",
      referenceNumber: tradeData.orderNo,
      description: `Margin for ${tradeData.type} ${tradeData.volume} ${
        tradeData.symbol
      } @ ${clientOrderPrice.toFixed(2)} (AED ${goldWeightValue.toFixed(2)})`,
      amount: requiredMargin.toFixed(2),
      entryNature: "DEBIT",
      runningBalance: newCashBalance.toFixed(2),
      orderDetails: {
        type: tradeData.type,
        symbol: tradeData.symbol,
        volume: tradeData.volume,
        entryPrice: clientOrderPrice,
        profit: 0,
        status: "PROCESSING",
      },
      user: userId,
      adminId: adminId,
      date: new Date(tradeData.openingDate),
    });
    const savedOrderLedger = await orderLedgerEntry.save({
      session: mongoSession,
    });

    const lpLedgerEntry = new Ledger({
      entryId: generateEntryId("LP"),
      entryType: "LP_POSITION",
      referenceNumber: tradeData.orderNo,
      description: `LP Position opened for ${tradeData.type} ${
        tradeData.volume
      } ${tradeData.symbol} @ ${lpCurrentPrice.toFixed(
        2
      )} (AED ${lpGoldWeightValue.toFixed(2)})`,
      amount: lpGoldWeightValue.toFixed(2),
      entryNature: "CREDIT",
      runningBalance: newCashBalance.toFixed(2),
      lpDetails: {
        positionId: tradeData.orderNo,
        type: tradeData.type,
        symbol: tradeData.symbol,
        volume: tradeData.volume,
        entryPrice: lpCurrentPrice,
        profit: 0,
        status: "OPEN",
      },
      user: userId,
      adminId: adminId,
      date: new Date(tradeData.openingDate),
    });
    const savedLPLedger = await lpLedgerEntry.save({ session: mongoSession });

    const cashTransactionLedgerEntry = new Ledger({
      entryId: generateEntryId("TRX"),
      entryType: "TRANSACTION",
      referenceNumber: tradeData.orderNo,
      description: `Margin allocation for trade ${tradeData.orderNo}`,
      amount: requiredMargin.toFixed(2),
      entryNature: "DEBIT",
      runningBalance: newCashBalance.toFixed(2),
      transactionDetails: {
        type: null,
        asset: "CASH",
        previousBalance: currentCashBalance,
      },
      user: userId,
      adminId: adminId,
      date: new Date(tradeData.openingDate),
      notes: `Cash margin allocated for ${tradeData.type} order on ${tradeData.symbol}`,
    });
    const savedCashLedger = await cashTransactionLedgerEntry.save({
      session: mongoSession,
    });

    // Validate and place MT5 trade
    const mt5Symbol = SYMBOL_MAPPING[tradeData.symbol];
    if (!mt5Symbol) {
      throw new Error(
        `Invalid symbol: ${tradeData.symbol}. No MT5 mapping found.`
      );
    }

    const validatedSymbol = await mt5Service.validateSymbol(mt5Symbol);
    console.log(`Validated MT5 Symbol: ${validatedSymbol}`);

    const priceData = await mt5Service.getPrice(validatedSymbol);
    console.log(
      `Market Price for ${validatedSymbol}: ${JSON.stringify(priceData)}`
    );
    if (!priceData || !priceData.bid || !priceData.ask) {
      throw new Error(`No valid price quote available for ${validatedSymbol}`);
    }

    const mt5TradeData = {
      symbol: validatedSymbol,
      volume: tradeData.volume,
      type: tradeData.type,
      slDistance: null,
      tpDistance: null,
      comment: tradeData.comment,
      magic: 123456,
    };
    console.log(
      `MT5 Trade Parameters: ${JSON.stringify(mt5TradeData, null, 2)}`
    );

    const mt5Result = await mt5Service.placeTrade(mt5TradeData);
    console.log(`MT5 trade placed successfully: Order ID ${mt5Result.ticket}`);

    if (!mt5Result.success) {
      await updateTradeStatus(
        adminId,
        savedOrder._id.toString(),
        {
          orderStatus: "FAILED",
          notificationError: mt5Result.error || "Unknown MT5 error",
        },
        mongoSession
      );
      throw new Error(
        `MT5 trade failed: ${mt5Result.error || "Unknown error"}`
      );
    }

    // Update with MT5 results
    const mt5Price = parseFloat(mt5Result.price);
    let finalClientOpeningPrice;
    let finalLPPrice;

    if (tradeData.type === "BUY") {
      finalClientOpeningPrice = mt5Price + (userAccount.askSpread || 0);
      finalLPPrice = mt5Price - (userAccount.askSpread || 0); // Subtract askSpread for BUY
    } else {
      finalClientOpeningPrice = mt5Price - (userAccount.bidSpread || 0);
      finalLPPrice = mt5Price - (userAccount.bidSpread || 0); // Subtract bidSpread for SELL
    }

    // Recalculate values with MT5 price
    const updatedGoldWeightValue =
      tradeData.symbol === "TTBAR"
        ? finalClientOpeningPrice * CONVERSION_FACTORS.TTBAR * grams
        : finalClientOpeningPrice * CONVERSION_FACTORS.KGBAR * grams;

    finalClientOpeningPrice = updatedGoldWeightValue;
    const updatedLPGoldWeightValue =
      tradeData.symbol === "TTBAR"
        ? finalLPPrice * CONVERSION_FACTORS.TTBAR * grams
        : finalLPPrice * CONVERSION_FACTORS.KGBAR * grams;
    finalLPPrice = updatedLPGoldWeightValue;

    const updatedLPProfitValue = (gramValue * grams * spread).toFixed(2);
    console.log("+++++++++++++++++++++++++");
    console.log(finalClientOpeningPrice);
    console.log(finalLPPrice);
    console.log("+++++++++++++++++++++++++");

    // Update Order
    await Order.findByIdAndUpdate(
      savedOrder._id,
      {
        price: finalClientOpeningPrice.toFixed(2),
        openingPrice: finalClientOpeningPrice.toFixed(2),
        orderStatus: "OPEN",
        ticket: mt5Result.ticket.toString(),
        volume: mt5Result.volume,
        symbol: tradeData.symbol,
      },
      { session: mongoSession }
    );

    // Update LP Position
    await LPPosition.findByIdAndUpdate(
      savedLPPosition._id,
      {
        entryPrice: finalLPPrice.toFixed(2),
        currentPrice: finalLPPrice.toFixed(2),
      },
      { session: mongoSession }
    );

    // Update LP Profit
    await LPProfit.findByIdAndUpdate(
      savedLPProfit._id,
      {
        value: updatedLPProfitValue,
      },
      { session: mongoSession }
    );

    // Update Ledger Entries
    await Ledger.findByIdAndUpdate(
      savedOrderLedger._id,
      {
        description: `Margin for ${tradeData.type} ${tradeData.volume} ${
          tradeData.symbol
        } @ ${finalClientOpeningPrice.toFixed(
          2
        )} (AED ${updatedGoldWeightValue.toFixed(2)})`,
        "orderDetails.entryPrice": finalClientOpeningPrice,
        "orderDetails.status": "OPEN",
      },
      { session: mongoSession }
    );

    await Ledger.findByIdAndUpdate(
      savedLPLedger._id,
      {
        description: `LP Position opened for ${tradeData.type} ${
          tradeData.volume
        } ${tradeData.symbol} @ ${finalLPPrice.toFixed(
          2
        )} (AED ${updatedLPGoldWeightValue.toFixed(2)})`,
        amount: updatedLPGoldWeightValue.toFixed(2),
        "lpDetails.entryPrice": finalLPPrice,
        "lpDetails.status": "OPEN",
      },
      { session: mongoSession }
    );

    if (!session) {
      await mongoSession.commitTransaction();
      committed = true;
      mongoSession.endSession();
      sessionEnded = true;
    }

    return {
      clientOrder: {
        ...savedOrder.toObject(),
        price: finalClientOpeningPrice.toFixed(2),
        openingPrice: finalClientOpeningPrice.toFixed(2),
        orderStatus: "OPEN",
        ticket: mt5Result.ticket.toString(),
      },
      lpPosition: {
        ...savedLPPosition.toObject(),
        entryPrice: finalLPPrice.toFixed(2),
        currentPrice: finalLPPrice.toFixed(2),
      },
      lpProfit: {
        ...savedLPProfit.toObject(),
        value: updatedLPProfitValue,
      },
      mt5Trade: {
        ticket: mt5Result.ticket,
        volume: mt5Result.volume,
        price: mt5Result.price,
        symbol: mt5Result.symbol,
        type: mt5Result.type,
      },
      balances: {
        cash: newCashBalance,
        gold: newMetalBalance,
      },
      requiredMargin,
      goldWeightValue: updatedGoldWeightValue,
      lpProfitValue: updatedLPProfitValue,
      convertedPrice: {
        client: finalClientOpeningPrice.toFixed(2),
        lp: finalLPPrice.toFixed(2),
      },
      priceDetails: {
        mt5ExecutionPrice: mt5Price,
        clientOpeningPrice: finalClientOpeningPrice.toFixed(2),
        lpPrice: finalLPPrice.toFixed(2),
        spreadApplied:
          tradeData.type === "BUY"
            ? userAccount.askSpread
            : userAccount.bidSpread,
      },
      ledgerEntries: {
        order: {
          ...orderLedgerEntry,
          description: `Margin for ${tradeData.type} ${tradeData.volume} ${
            tradeData.symbol
          } @ ${finalClientOpeningPrice.toFixed(
            2
          )} (AED ${updatedGoldWeightValue.toFixed(2)})`,
        },
        lp: {
          ...lpLedgerEntry,
          description: `LP Position opened for ${tradeData.type} ${
            tradeData.volume
          } ${tradeData.symbol} @ ${finalLPPrice.toFixed(
            2
          )} (AED ${updatedLPGoldWeightValue.toFixed(2)})`,
          amount: updatedLPGoldWeightValue.toFixed(2),
        },
        cashTransaction: cashTransactionLedgerEntry,
      },
    };
  } catch (error) {
    if (!committed && !session) {
      try {
        await mongoSession.abortTransaction();
      } catch (abortError) {
        console.error(`Failed to abort transaction: ${abortError.message}`);
      }
    }
    console.error(
      `Trade creation error: ${error.message}, Stack: ${error.stack}`
    );
    throw new Error(`Error creating trade: ${error.message}`);
  } finally {
    if (!session && !sessionEnded) {
      try {
        mongoSession.endSession();
      } catch (endError) {
        console.error(`Failed to end session: ${endError.message}`);
      }
    }
  }
};

export const updateTradeStatus = async (
  adminId,
  orderId,
  updateData,
  session = null
) => {
  const mongoSession = session || (await mongoose.startSession());
  let committed = false;
  let sessionEnded = false;
  console.log(updateData);

  const GRAMS_PER_BAR = {
    TTBAR: 117,
    KGBAR: 1000,
  };

  const CONVERSION_FACTORS = {
    TTBAR: 13.7628,
    KGBAR: 32.1507 * 3.674,
  };

  try {
    if (!session) mongoSession.startTransaction();

    const allowedUpdates = [
      "orderStatus",
      "closingPrice",
      "closingDate",
      "profit",
      "comment",
      "price",
      "ticket",
      "volume",
      "symbol",
      "notificationError",
      "AMOUNTFC",
    ];
    const sanitizedData = {};
    Object.keys(updateData).forEach((key) => {
      if (allowedUpdates.includes(key)) {
        sanitizedData[key] = updateData[key];
      }
    });

    if (sanitizedData.orderStatus === "CLOSED" && !sanitizedData.closingDate) {
      sanitizedData.closingDate = new Date();
    }
    if (sanitizedData.closingPrice) {
      sanitizedData.price = sanitizedData.closingPrice;
    }

    console.log(
      `Updating trade with orderId: ${orderId}, adminId: ${adminId}, updateData: ${JSON.stringify(
        sanitizedData
      )}`
    );
    const order = await Order.findOne({ _id: orderId, adminId }).session(
      mongoSession
    );
    if (!order) {
      console.error(
        `Order not found for orderId: ${orderId}, adminId: ${adminId}`
      );
      throw new Error("Order not found or unauthorized");
    }

    const userAccount = await Account.findById(order.user).session(
      mongoSession
    );
    if (!userAccount) {
      console.error(`User account not found for userId: ${order.user}`);
      throw new Error("User account not found");
    }

    let mt5CloseResult = null;
    let mt5ClosingPrice = null; // Actual MT5 closing price in USD per oz
     let clientClosingPrice = null
    if (
      sanitizedData.orderStatus === "CLOSED" &&
      order.orderStatus !== "CLOSED"
    ) {
      try {
        console.log(`Fetching MT5 positions for ticket ${order.ticket}`);
        const mt5CloseData = {
          ticket: order.ticket,
          symbol: SYMBOL_MAPPING[order.symbol] || order.symbol,
          volume: parseFloat(order.volume),
          type: order.type === "BUY" ? "SELL" : "BUY", // Opposite type for closing
          openingPrice: parseFloat(order.openingPrice),
        };
        console.log(
          `MT5 close trade request: ${JSON.stringify(mt5CloseData, null, 2)}`
        );

        mt5CloseResult = await mt5Service.closeTrade(mt5CloseData);
        console.log(
          `MT5 close trade result: ${JSON.stringify(mt5CloseResult, null, 2)}`
        );

        if (!mt5CloseResult.success) {
          if (
            mt5CloseResult.error.includes("Position not found") ||
            mt5CloseResult.likelyClosed ||
            mt5CloseResult.error.includes("Request failed with status code 400")
          ) {
            console.warn(
              `Position ${order.ticket} not found in MT5. Assuming already closed.`
            );
            const priceData = await mt5Service.getPrice(
              SYMBOL_MAPPING[order.symbol] || order.symbol
            );
            console.log(`Price data received: ${JSON.stringify(priceData)}`);

            if (!priceData || (!priceData.bid && !priceData.ask)) {
              throw new Error(
                `No valid price quote available for ${order.symbol}`
              );
            }

            mt5ClosingPrice =
              order.type === "BUY" ? priceData.bid : priceData.ask;
          } else {
            throw new Error(
              `MT5 trade closure failed: ${
                mt5CloseResult.error || "Unknown error"
              }`
            );
          }
        } else {
          mt5ClosingPrice = parseFloat(
            mt5CloseResult.closePrice || mt5CloseResult.data?.price
          );
        }
      } catch (mt5Error) {
        console.error(
          `Failed to close MT5 trade for ticket ${order.ticket}: ${mt5Error.message}, Stack: ${mt5Error.stack}`
        );
        if (
          mt5Error.message.includes("Position not found") ||
          mt5Error.message.includes("Request failed with status code 400")
        ) {
          console.warn(
            `Position ${order.ticket} not found in MT5. Assuming already closed.`
          );
          try {
            const priceData = await mt5Service.getPrice(
              SYMBOL_MAPPING[order.symbol] || order.symbol
            );
            console.log(
              `Fallback price data received: ${JSON.stringify(priceData)}`
            );

            if (!priceData || (!priceData.bid && !priceData.ask)) {
              console.warn(
                `No current price available, using opening price as fallback`
              );
              mt5ClosingPrice = parseFloat(order.openingPrice);
            } else {
              mt5ClosingPrice =
                order.type === "BUY" ? priceData.bid : priceData.ask;
            }
          } catch (priceError) {
            console.error(`Failed to get price data: ${priceError.message}`);
            mt5ClosingPrice = parseFloat(order.openingPrice);
          }
        } else {
          throw new Error(`Failed to close MT5 trade: ${mt5Error.message}`);
        }
      }


      if (mt5ClosingPrice !== null) {
        let spreadApplied;
        let clientUSDPerOz;
        if (order.type === "BUY") {
          spreadApplied = userAccount.bidSpread || 0;
          clientUSDPerOz = mt5ClosingPrice - spreadApplied;
        } else {
          spreadApplied = userAccount.askSpread || 0;
          clientUSDPerOz = mt5ClosingPrice + spreadApplied;
        }

        const numUnits = order.volume / GRAMS_PER_BAR[order.symbol];
        const conversion = CONVERSION_FACTORS[order.symbol];

        clientClosingPrice = clientUSDPerOz * conversion * numUnits;

        console.log(
          `MT5 Closing Price: ${mt5ClosingPrice}, Client Closing Price: ${clientClosingPrice}, Spread Applied: ${spreadApplied}`
        );

        sanitizedData.closingPrice = clientClosingPrice.toFixed(2);
      }
    }

    if (!clientClosingPrice && sanitizedData.closingPrice) {
      clientClosingPrice = parseFloat(sanitizedData.closingPrice);
    }

    let clientProfit = 0;
    if (clientClosingPrice) {
      const entryGoldWeightValue = parseFloat(order.openingPrice);
      const closingGoldWeightValue = clientClosingPrice;

      if (order.type === "BUY") {
        clientProfit = closingGoldWeightValue - entryGoldWeightValue;
      } else {
        clientProfit = entryGoldWeightValue - closingGoldWeightValue;
      }
    }

    let newCashBalance = parseFloat(userAccount.reservedAmount);
    let newMetalBalance = parseFloat(userAccount.METAL_WT);
    let newAMOUNTFC = parseFloat(userAccount.AMOUNTFC || 0);
    const currentCashBalance = newCashBalance;
    const currentMetalBalance = newMetalBalance;
    const currentAMOUNTFC = newAMOUNTFC;

    Object.keys(sanitizedData).forEach((key) => {
      order[key] = sanitizedData[key];
    });

    if (sanitizedData.orderStatus === "CLOSED") {
      order.profit = clientProfit.toFixed(2);
    }

    await order.save({ session: mongoSession });

    let lpProfit = 0;
    const lpPosition = await LPPosition.findOne({
      positionId: order.orderNo,
    }).session(mongoSession);

    if (lpPosition) {
      let lpClosingPrice = null;
      if (mt5ClosingPrice !== null) {
        const numUnits = order.volume / GRAMS_PER_BAR[order.symbol];
        const conversion = CONVERSION_FACTORS[order.symbol];
        lpClosingPrice = mt5ClosingPrice * conversion * numUnits;
        lpPosition.closingPrice = lpClosingPrice.toFixed(2);
        lpPosition.currentPrice = lpClosingPrice.toFixed(2);
      } else if (sanitizedData.closingPrice) {
        lpPosition.closingPrice = sanitizedData.closingPrice;
        lpPosition.currentPrice = sanitizedData.closingPrice;
      }

      if (sanitizedData.closingDate) {
        lpPosition.closeDate = sanitizedData.closingDate;
      }

      if (sanitizedData.orderStatus === "CLOSED") {
        lpPosition.status = "CLOSED";

        const lpEntryGoldWeightValue = parseFloat(lpPosition.entryPrice);
        const lpClosingGoldWeightValue =
          lpClosingPrice || parseFloat(sanitizedData.closingPrice);

        const entryGoldWeightValue = parseFloat(order.openingPrice);
        const closingGoldWeightValue = clientClosingPrice;

        const openingDifference = Math.abs(
          lpEntryGoldWeightValue - entryGoldWeightValue
        );
        const closingDifference = Math.abs(
          lpClosingGoldWeightValue - closingGoldWeightValue
        );
        lpProfit = openingDifference + closingDifference;

        lpPosition.profit = lpProfit.toFixed(2);
      } else if (mt5ClosingPrice !== null) {
        lpPosition.currentPrice = lpClosingPrice.toFixed(2);
      } else if (sanitizedData.price) {
        lpPosition.currentPrice = sanitizedData.price;
      }

      await lpPosition.save({ session: mongoSession });
      order.lpPositionId = lpPosition._id;
      await order.save({ session: mongoSession });
    } else {
      console.warn(`LPPosition not found for positionId: ${order.orderNo}`);
    }

    if (sanitizedData.orderStatus === "CLOSED") {
      const lpProfitRecord = await LPProfit.findOne({
        orderNo: order.orderNo,
        status: "OPEN",
      }).session(mongoSession);

      if (lpProfitRecord) {
        const gramValue = CONVERSION_FACTORS[order.symbol];
        const numUnits = order.volume / GRAMS_PER_BAR[order.symbol];
        let closingLPProfitValue;

        if (order.type === "BUY") {
          closingLPProfitValue =
            gramValue * numUnits * (userAccount.bidSpread || 0);
        } else {
          closingLPProfitValue =
            gramValue * numUnits * (userAccount.askSpread || 0);
        }

        const totalLPProfit =
          parseFloat(lpProfitRecord.value) + closingLPProfitValue;
        lpProfitRecord.status = "CLOSED";
        lpProfitRecord.value = totalLPProfit.toFixed(2);
        lpProfitRecord.datetime = new Date(sanitizedData.closingDate);

        await lpProfitRecord.save({ session: mongoSession });
      } else {
        console.warn(
          `LP Profit record not found for orderNo: ${order.orderNo}`
        );
      }
    }

    if (sanitizedData.orderStatus === "CLOSED") {
      const settlementAmount = order.requiredMargin
        ? parseFloat(order.requiredMargin)
        : 0;

      const userProfit = clientProfit > 0 ? clientProfit : 0;

      if (order.type === "BUY") {
        newCashBalance = currentCashBalance + settlementAmount + clientProfit;
        newAMOUNTFC = currentAMOUNTFC + clientProfit;
        newMetalBalance = currentMetalBalance - order.volume;
      } else if (order.type === "SELL") {
        newCashBalance = currentCashBalance + settlementAmount + clientProfit;
        newAMOUNTFC = currentAMOUNTFC + clientProfit;
        newMetalBalance = currentMetalBalance + order.volume;
      }

      await Account.findByIdAndUpdate(
        order.user,
        {
          reservedAmount: newCashBalance.toFixed(2),
          METAL_WT: newMetalBalance.toFixed(2),
          AMOUNTFC: newAMOUNTFC.toFixed(2),
        },
        { session: mongoSession, new: true }
      );

      const orderLedgerEntry = new Ledger({
        entryId: generateEntryId("ORD"),
        entryType: "ORDER",
        referenceNumber: order.orderNo,
        description: `Closing ${order.type} ${order.volume} ${
          order.symbol
        } @ ${clientClosingPrice.toFixed(2)}${
          userProfit > 0 ? " with profit" : ""
        }`,
        amount: (settlementAmount + userProfit).toFixed(2),
        entryNature: "CREDIT",
        runningBalance: newCashBalance.toFixed(2),
        orderDetails: {
          type: order.type,
          symbol: order.symbol,
          volume: order.volume,
          entryPrice: order.openingPrice,
          closingPrice: clientClosingPrice.toFixed(2),
          profit: clientProfit.toFixed(2),
          status: "CLOSED",
        },
        user: order.user,
        adminId: adminId,
        date: new Date(sanitizedData.closingDate),
      });
      await orderLedgerEntry.save({ session: mongoSession });

      if (lpPosition) {
        const lpClosingPrice = mt5ClosingPrice
          ? mt5ClosingPrice *
            CONVERSION_FACTORS[order.symbol] *
            (order.volume / GRAMS_PER_BAR[order.symbol])
          : clientClosingPrice;
        const lpLedgerEntry = new Ledger({
          entryId: generateEntryId("LP"),
          entryType: "LP_POSITION",
          referenceNumber: order.orderNo,
          description: `LP Position closed for ${order.type} ${order.volume} ${
            order.symbol
          } @ ${lpClosingPrice.toFixed(2)}`,
          amount: settlementAmount.toFixed(2),
          entryNature: "DEBIT",
          runningBalance: newCashBalance.toFixed(2),
          lpDetails: {
            positionId: order.orderNo,
            type: order.type,
            symbol: order.symbol,
            volume: order.volume,
            entryPrice: lpPosition.entryPrice,
            closingPrice: lpClosingPrice,
            profit: lpProfit.toFixed(2),
            status: "CLOSED",
          },
          user: order.user,
          adminId: adminId,
          date: new Date(sanitizedData.closingDate),
        });
        await lpLedgerEntry.save({ session: mongoSession });
      }

      const cashTransactionLedgerEntry = new Ledger({
        entryId: generateEntryId("TRX"),
        entryType: "TRANSACTION",
        referenceNumber: order.orderNo,
        description: `Cash settlement for closing trade ${order.orderNo}`,
        amount: settlementAmount.toFixed(2),
        entryNature: "CREDIT",
        runningBalance: newCashBalance.toFixed(2),
        transactionDetails: {
          type: null,
          asset: "CASH",
          previousBalance: currentCashBalance,
        },
        user: order.user,
        adminId: adminId,
        date: new Date(sanitizedData.closingDate),
        notes: `Cash settlement for closed ${order.type} order on ${order.symbol}`,
      });
      await cashTransactionLedgerEntry.save({ session: mongoSession });

      const amountFCLedgerEntry = new Ledger({
        entryId: generateEntryId("TRX"),
        entryType: "TRANSACTION",
        referenceNumber: order.orderNo,
        description: `AMOUNTFC ${
          clientProfit >= 0 ? "profit" : "loss"
        } for closing trade ${order.orderNo}`,
        amount: Math.abs(clientProfit).toFixed(2),
        entryNature: clientProfit >= 0 ? "CREDIT" : "DEBIT",
        runningBalance: newAMOUNTFC.toFixed(2),
        transactionDetails: {
          type: null,
          asset: "CASH",
          previousBalance: currentAMOUNTFC,
        },
        user: order.user,
        adminId: adminId,
        date: new Date(sanitizedData.closingDate),
        notes: `AMOUNTFC updated with ${
          clientProfit >= 0 ? "profit" : "loss"
        } from closed ${order.type} order`,
      });
      await amountFCLedgerEntry.save({ session: mongoSession });
    }

    if (!session) {
      await mongoSession.commitTransaction();
      committed = true;
      mongoSession.endSession();
      sessionEnded = true;
    }

    console.log(
      `Trade ${order.orderNo} successfully updated to ${
        sanitizedData.orderStatus || "current"
      } status`
    );

    return {
      order,
      balances: {
        cash: newCashBalance,
        gold: newMetalBalance,
        AMOUNTFC: newAMOUNTFC,
      },
      profit: {
        client: clientProfit,
        lp: lpPosition ? parseFloat(lpPosition.profit) : 0,
      },
      closingPrices: {
        mt5ClosingPrice: mt5ClosingPrice,
        clientClosingPrice: clientClosingPrice,
        spreadApplied:
          order.type === "BUY" ? userAccount.bidSpread : userAccount.askSpread,
      },
    };
  } catch (error) {
    if (!committed && !session) {
      try {
        await mongoSession.abortTransaction();
      } catch (abortError) {
        console.error(
          `Failed to abort transaction for orderId ${orderId}: ${abortError.message}`
        );
      }
    }
    console.error(
      `Trade update error for orderId ${orderId}: ${error.message}, Stack: ${error.stack}`
    );
    throw new Error(`Error updating trade: ${error.message}`);
  } finally {
    if (!session && !sessionEnded) {
      try {
        mongoSession.endSession();
      } catch (endError) {
        console.error(
          `Failed to end session for orderId ${orderId}: ${endError.message}`
        );
      }
    }
  }
};

export const getLPProfitOrders = async () => {
  try {
    const LPProfitInfo = await LPProfit.find({})
      .populate("user", "_id firstName lastName ACCOUNT_HEAD")
      .sort({ datetime: -1 });

    return LPProfitInfo;
  } catch (error) {
    throw new Error(`Error fetching trades: ${error.message}`);
  }
};

export const getTradesByUser = async (adminId, userId) => {
  try {
    const trades = await Order.find({
      adminId: adminId,
    })
      .populate(
        "user",
        "_id firstName lastName ACCOUNT_HEAD email phoneNumber bidSpread askSpread accountStatus"
      )
      .sort({ createdAt: -1 });

    return trades;
  } catch (error) {
    throw new Error(`Error fetching trades: ${error.message}`);
  }
};

export const getOrdersByUser = async (adminId, userId) => {
  try {
    const orders = await Order.find({
      adminId: adminId,
      user: userId,
    })
      .populate(
        "user",
        "firstName lastName ACCOUNT_HEAD email phoneNumber userSpread accountStatus"
      )
      .sort({ createdAt: -1 });

    return orders;
  } catch (error) {
    throw new Error(`Error fetching user orders: ${error.message}`);
  }
};

export const getTradesByLP = async (adminId, userId) => {
  try {
    const trades = await LPPosition.find({
      adminId: adminId,
    }).sort({ createdAt: -1 });

    return trades;
  } catch (error) {
    throw new Error(`Error fetching trades: ${error.message}`);
  }
};

export const getTradeById = async (adminId, tradeId) => {
  try {
    const trade = await Order.findOne({
      _id: tradeId,
      adminId: adminId,
    });

    return trade;
  } catch (error) {
    throw new Error(`Error fetching trade: ${error.message}`);
  }
};
