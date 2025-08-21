import mongoose from "mongoose";
import LPPosition from "../../models/LPPositionSchema.js";
import Order from "../../models/OrderSchema.js";
import Ledger from "../../models/LedgerSchema.js";
import Account from "../../models/AccountSchema.js";
import mt5Service from "../../services/Trading/mt5Service.js";
import LPProfit from "../../models/LPProfit.js";

const TROY_OUNCE_GRAMS = 31.103;
const TTB_FACTOR = 116.64;

// Symbol mapping for CRM to MT5
const SYMBOL_MAPPING = {
  GOLD: "XAUUSD.#",
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
    const currentPrice = parseFloat(tradeData.price);
    const volume = parseFloat(tradeData.volume);

    // Check sufficient balances
    const requiredMargin = parseFloat(tradeData.requiredMargin || 0);
    if (currentCashBalance < requiredMargin) {
      throw new Error("Insufficient cash balance");
    }

    // Calculate client order price with spread
    let clientOrderPrice = currentPrice;

    const goldWeightValue =
      (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

    const lpCurrentPrice =
      (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

    // Calculate LP Profit
    const gramValue = TTB_FACTOR / TROY_OUNCE_GRAMS;

    let lpProfitValue;

    if (tradeData.type === "BUY") {
      // For BUY orders, LP profit is calculated using askSpread
      lpProfitValue = gramValue * volume * (userAccount.askSpread || 0);
    } else {
      // For SELL orders, LP profit is calculated using bidSpread
      lpProfitValue = gramValue * volume * (userAccount.bidSpread || 0);
    }

    // Create order
    const newOrder = new Order({
      orderNo: tradeData.orderNo,
      type: tradeData.type,
      volume: tradeData.volume,
      symbol: tradeData.symbol,
      requiredMargin: requiredMargin,
      price: currentPrice,
      openingPrice: clientOrderPrice.toFixed(2),
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
      isTradeSafe: (tradeData.takeProfit || tradeData.stopLoss) ? true : false
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
      entryPrice: currentPrice,
      openDate: tradeData.openingDate,
      currentPrice: currentPrice,
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
      value: lpProfitValue.toFixed(2),
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
      newMetalBalance = currentMetalBalance + tradeData.volume;
    } else if (tradeData.type === "SELL") {
      newMetalBalance = currentMetalBalance - tradeData.volume;
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
    await orderLedgerEntry.save({ session: mongoSession });

    const lpLedgerEntry = new Ledger({
      entryId: generateEntryId("LP"),
      entryType: "LP_POSITION",
      referenceNumber: tradeData.orderNo,
      description: `LP Position opened for ${tradeData.type} ${
        tradeData.volume
      } ${tradeData.symbol} @ ${currentPrice.toFixed(
        2
      )} (AED ${lpCurrentPrice.toFixed(2)})`,
      amount: lpCurrentPrice.toFixed(2),
      entryNature: "CREDIT",
      runningBalance: newCashBalance.toFixed(2),
      lpDetails: {
        positionId: tradeData.orderNo,
        type: tradeData.type,
        symbol: tradeData.symbol,
        volume: tradeData.volume,
        entryPrice: currentPrice,
        profit: 0,
        status: "OPEN",
      },
      user: userId,
      adminId: adminId,
      date: new Date(tradeData.openingDate),
    });
    await lpLedgerEntry.save({ session: mongoSession });

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
    await cashTransactionLedgerEntry.save({ session: mongoSession });

    const goldTransactionLedgerEntry = new Ledger({
      entryId: generateEntryId("TRX"),
      entryType: "TRANSACTION",
      referenceNumber: tradeData.orderNo,
      description: `Gold ${
        tradeData.type === "BUY" ? "credit" : "debit"
      } for trade ${tradeData.orderNo}`,
      amount: tradeData.volume,
      entryNature: tradeData.type === "BUY" ? "CREDIT" : "DEBIT",
      runningBalance: newMetalBalance.toFixed(2),
      transactionDetails: {
        type: null,
        asset: "GOLD",
        previousBalance: currentMetalBalance,
      },
      user: userId,
      adminId: adminId,
      date: new Date(tradeData.openingDate),
      notes: `Gold weight (${tradeData.volume}) ${
        tradeData.type === "BUY" ? "added to" : "subtracted from"
      } account for ${
        tradeData.type
      } order (Value: AED ${requiredMargin.toFixed(2)})`,
    });
    await goldTransactionLedgerEntry.save({ session: mongoSession });

    // Validate and place MT5 trade
   const mt5Symbol = SYMBOL_MAPPING[tradeData.symbol];
  if (!mt5Symbol) {
    throw new Error(`Invalid symbol: ${tradeData.symbol}. No MT5 mapping found.`);
  }

  const validatedSymbol = await mt5Service.validateSymbol(mt5Symbol);
  console.log(`Validated MT5 Symbol: ${validatedSymbol}`);

  const priceData = await mt5Service.getPrice(validatedSymbol);
  console.log(`Market Price for ${validatedSymbol}: ${JSON.stringify(priceData)}`);
  if (!priceData || !priceData.bid || !priceData.ask) {
    throw new Error(`No valid price quote available for ${validatedSymbol}`);
  }

  const mt5TradeData = {
    symbol: validatedSymbol, // Use validated symbol
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

    // Update CRM trade with MT5 details
    await updateTradeStatus(
      adminId,
      savedOrder._id.toString(),
      {
        orderStatus: "OPEN",
        ticket: mt5Result.ticket.toString(),
        openingPrice: mt5Result.price,
        volume: mt5Result.volume,
        symbol: mt5Result.symbol,
      },
      mongoSession
    );

    if (!session) {
      await mongoSession.commitTransaction();
      committed = true;
      mongoSession.endSession();
      sessionEnded = true;
    }

    return {
      clientOrder: savedOrder,
      lpPosition: savedLPPosition,
      lpProfit: savedLPProfit,
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
      goldWeightValue,
      lpProfitValue,
      convertedPrice: {
        client: (goldWeightValue / volume).toFixed(2),
        lp: (lpCurrentPrice / volume).toFixed(2),
      },
      ledgerEntries: {
        order: orderLedgerEntry,
        lp: lpLedgerEntry,
        cashTransaction: cashTransactionLedgerEntry,
        goldTransaction: goldTransactionLedgerEntry,
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
            // Fetch current market price to calculate closing price
            const priceData = await mt5Service.getPrice(
              SYMBOL_MAPPING[order.symbol] || order.symbol
            );
            if (!priceData || !priceData.bid || !priceData.ask) {
              throw new Error(
                `No valid price quote available for ${order.symbol}`
              );
            }
            sanitizedData.closingPrice =
              order.type === "BUY" ? priceData.bid : priceData.ask;
          } else {
            throw new Error(
              `MT5 trade closure failed: ${
                mt5CloseResult.error || "Unknown error"
              }`
            );
          }
        } else {
          sanitizedData.closingPrice =
            mt5CloseResult.closePrice || mt5CloseResult.data.price;
        }
      } catch (mt5Error) {
        console.error(
          `Failed to close MT5 trade for ticket ${order.ticket}: ${mt5Error.message}, Stack: ${mt5Error.stack}`
        );
        // Handle specific MT5 errors indicating the position is likely closed
        if (
          mt5Error.message.includes("Position not found") ||
          mt5Error.message.includes("Request failed with status code 400")
        ) {
          console.warn(
            `Position ${order.ticket} not found in MT5. Assuming already closed.`
          );
          const priceData = await mt5Service.getPrice(
            SYMBOL_MAPPING[order.symbol] || order.symbol
          );
          if (!priceData || !priceData.bid || !priceData.ask) {
            throw new Error(`No valid price quote available for ${order.symbol}`);
          }
          sanitizedData.closingPrice =
            order.type === "BUY" ? priceData.bid : priceData.ask;
        } else {
          throw new Error(`Failed to close MT5 trade: ${mt5Error.message}`);
        }
      }
    }

    const currentPrice = parseFloat(sanitizedData.closingPrice);
    let clientClosingPrice = currentPrice;
    if (order.type === "BUY") {
      clientClosingPrice = currentPrice - (userAccount.bidSpread || 0);
    } else {
      clientClosingPrice = currentPrice + (userAccount.askSpread || 0);
    }

    if (sanitizedData.orderStatus === "CLOSED") {
      sanitizedData.closingPrice = clientClosingPrice.toFixed(2);
    }

    const entryPrice = parseFloat(order.openingPrice);
    const volume = parseFloat(order.volume);

    const entryGoldWeightValue =
      (entryPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
    const closingGoldWeightValue =
      (clientClosingPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

    let clientProfit;
    if (order.type === "BUY") {
      clientProfit = closingGoldWeightValue - entryGoldWeightValue;
    } else {
      clientProfit = entryGoldWeightValue - closingGoldWeightValue;
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
      if (sanitizedData.closingPrice) {
        lpPosition.closingPrice = currentPrice;
        lpPosition.currentPrice = currentPrice;
      }
      if (sanitizedData.closingDate) {
        lpPosition.closeDate = sanitizedData.closingDate;
      }
      if (sanitizedData.orderStatus === "CLOSED") {
        lpPosition.status = "CLOSED";

        const lpEntryPrice = parseFloat(lpPosition.entryPrice);
        const lpEntryGoldWeightValue =
          (lpEntryPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
        const lpClosingGoldWeightValue =
          (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

        const openingDifference = Math.abs(
          lpEntryGoldWeightValue - entryGoldWeightValue
        );
        const closingDifference = Math.abs(
          lpClosingGoldWeightValue - closingGoldWeightValue
        );
        lpProfit = openingDifference + closingDifference;

        lpPosition.profit = lpProfit.toFixed(2);
      } else if (sanitizedData.price) {
        lpPosition.currentPrice = currentPrice;
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
        status: "OPEN", // Only update the open LP profit record
      }).session(mongoSession);
      if (lpProfitRecord) {
        const gramValue = TTB_FACTOR / TROY_OUNCE_GRAMS;
        let closingLPProfitValue;

        if (order.type === "BUY") {
          closingLPProfitValue =
            gramValue * volume * (userAccount.bidSpread || 0);
        } else {
          closingLPProfitValue =
            gramValue * volume * (userAccount.askSpread || 0);
        }

        // Update existing LP Profit record
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
        : order.type === "BUY"
        ? closingGoldWeightValue
        : entryGoldWeightValue;

      const userProfit = clientProfit > 0 ? clientProfit : 0;

      if (order.type === "BUY") {
        newCashBalance = currentCashBalance + settlementAmount + clientProfit;
        newAMOUNTFC = currentAMOUNTFC + clientProfit;
        newMetalBalance = currentMetalBalance - volume;
      } else if (order.type === "SELL") {
        newCashBalance = currentCashBalance + settlementAmount + clientProfit;
        newAMOUNTFC = currentAMOUNTFC + clientProfit;
        newMetalBalance = currentMetalBalance + volume;
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
        description: `Closing ${order.type} ${volume} ${
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
          volume: volume,
          entryPrice: entryPrice,
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
        const lpLedgerEntry = new Ledger({
          entryId: generateEntryId("LP"),
          entryType: "LP_POSITION",
          referenceNumber: order.orderNo,
          description: `LP Position closed for ${order.type} ${volume} ${
            order.symbol
          } @ ${currentPrice.toFixed(2)}`,
          amount: settlementAmount.toFixed(2),
          entryNature: "DEBIT",
          runningBalance: newCashBalance.toFixed(2),
          lpDetails: {
            positionId: order.orderNo,
            type: order.type,
            symbol: order.symbol,
            volume: volume,
            entryPrice: parseFloat(lpPosition.entryPrice),
            closingPrice: currentPrice,
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

      const goldTransactionLedgerEntry = new Ledger({
        entryId: generateEntryId("TRX"),
        entryType: "TRANSACTION",
        referenceNumber: order.orderNo,
        description: `Gold ${
          order.type === "BUY" ? "debit" : "credit"
        } for closing trade ${order.orderNo}`,
        amount: volume,
        entryNature: order.type === "BUY" ? "DEBIT" : "CREDIT",
        runningBalance: newMetalBalance.toFixed(2),
        transactionDetails: {
          type: null,
          asset: "GOLD",
          previousBalance: currentMetalBalance,
        },
        user: order.user,
        adminId: adminId,
        date: new Date(sanitizedData.closingDate),
        notes: `Gold ${
          order.type === "BUY" ? "removed from" : "added to"
        } account for closing ${order.type} order`,
      });
      await goldTransactionLedgerEntry.save({ session: mongoSession });

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
// Other service functions (unchanged)
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
