import mongoose from "mongoose";
import LPPosition from "../../models/LPPositionSchema.js";
import Order from "../../models/OrderSchema.js";
import Ledger from "../../models/LedgerSchema.js";
import Account from "../../models/AccountSchema.js";
import mt5Service from "../../services/Trading/mt5Service.js";
import LPProfit from "../../models/LPProfit.js";

const TROY_OUNCE_GRAMS = 31.103;
const TTB_FACTOR = 116.64;


const SYMBOL_ALIASES = {
  XAUUSD: 'GOLD',
  // Add other aliases if needed, e.g., 'GOLDUSD': 'GOLD'
};

// Symbol mapping for CRM to MT5
const SYMBOL_MAPPING = {
  GOLD: process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
};

// Helper to generate unique entry ID
const generateEntryId = (prefix) => {
  const timestamp = Date.now().toString();
  const randomStr = Math.random().toString(36).substring(2, 5).toUpperCase();
  return `${prefix}-${timestamp.substring(timestamp.length - 5)}-${randomStr}`;
};

const validateOrderData = (tradeData, userId, adminId) => {
  const errors = [];

  if (!tradeData.orderNo || typeof tradeData.orderNo !== "string") {
    errors.push("Invalid or missing orderNo");
  }
  if (!["BUY", "SELL"].includes(tradeData.type)) {
    errors.push("Invalid type: must be BUY or SELL");
  }
  if (isNaN(tradeData.volume) || tradeData.volume < 0.01) {
    errors.push("Invalid volume: must be a number >= 0.01");
  }

  // Map symbol to CRM symbol if it's an alias
  const crmSymbol = SYMBOL_ALIASES[tradeData.symbol] || tradeData.symbol;
  if (!crmSymbol || !SYMBOL_MAPPING[crmSymbol]) {
    errors.push(
      `Invalid symbol: ${tradeData.symbol}. Supported: ${Object.keys(
        SYMBOL_MAPPING
      ).join(", ")}`
    );
  }
  // Update tradeData.symbol to CRM symbol
  tradeData.symbol = crmSymbol;

  const price = tradeData.openingPrice ?? tradeData.price;
  if (isNaN(price) || price <= 0) {
    errors.push("Invalid price or openingPrice: must be a positive number");
  }
  if (isNaN(tradeData.requiredMargin) || tradeData.requiredMargin <= 0) {
    errors.push("Invalid requiredMargin: must be a positive number");
  }
  if (!mongoose.Types.ObjectId.isValid(userId)) {
    errors.push("Invalid userId");
  }
  if (!mongoose.Types.ObjectId.isValid(adminId)) {
    errors.push("Invalid adminId");
  }
  if (
    !tradeData.openingDate ||
    isNaN(new Date(tradeData.openingDate).getTime())
  ) {
    errors.push("Invalid or missing openingDate");
  }

  return errors.length ? errors.join("; ") : null;
};

export const createTrade = async (
  adminId,
  userId,
  tradeData,
  session = null
) => {
  console.log("createTrade", { adminId, userId, tradeData });
  const mongoSession = session || (await mongoose.startSession());
  let committed = false;
  let sessionEnded = false;

  try {
    if (!session) mongoSession.startTransaction();

    // Validate trade data
    const validationError = validateOrderData(tradeData, userId, adminId);
    if (validationError) {
      throw new Error(`Order validation failed: ${validationError}`);
    }

    // Validate user account
    const userAccount = await Account.findById(userId).session(mongoSession);
    if (!userAccount) {
      throw new Error("User account not found");
    }

    const currentCashBalance = parseFloat(userAccount.reservedAmount);
    const currentMetalBalance = parseFloat(userAccount.METAL_WT);
    const volume = parseFloat(tradeData.volume);
    const requiredMargin = parseFloat(tradeData.requiredMargin || 0);

    // Check sufficient balances
    if (currentCashBalance < requiredMargin) {
      throw new Error("Insufficient cash balance");
    }

    // Get user spreads
    const askSpread = parseFloat(userAccount.askSpread) || 0;
    const bidSpread = parseFloat(userAccount.bidSpread) || 0;

    console.log("User spreads", { askSpread, bidSpread });
    // Initial prices (before MT5 execution)
    let currentPrice = parseFloat(tradeData.openingPrice ?? tradeData.price); // Market price (will be updated with MT5 result)
    let openingPrice; // Client price with spread (will be set after MT5 result)

    // Create new order with initial prices
    const newOrder = new Order({
      orderNo: tradeData.orderNo,
      type: tradeData.type,
      volume: tradeData.volume,
      symbol: tradeData.symbol,
      requiredMargin: requiredMargin,
      price: currentPrice.toFixed(2),
      openingPrice: currentPrice.toFixed(2), // Temporary, will be updated
      user: userId,
      adminId: adminId,
      orderStatus: "PROCESSING",
      profit: 0,
      openingDate: new Date(tradeData.openingDate),
      storedTime: new Date(),
      comment: tradeData.comment || `Ord-${tradeData.orderNo}`,
      ticket: tradeData.ticket || null,
      stopLoss: tradeData.stopLoss || 0,
      takeProfit: tradeData.takeProfit || 0,
      isTradeSafe: tradeData.takeProfit || tradeData.stopLoss ? true : false,
    });

    // Save order
    let savedOrder;
    try {
      const existingOrder = await Order.findOne({
        orderNo: tradeData.orderNo,
        adminId,
      }).session(mongoSession);
      if (existingOrder) {
        throw new Error(`Duplicate orderNo: ${tradeData.orderNo}`);
      }
      savedOrder = await newOrder.save({ session: mongoSession });
    } catch (saveError) {
      if (saveError.code === 11000) {
        throw new Error(`Duplicate orderNo: ${tradeData.orderNo}`);
      }
      throw new Error(`Failed to save order: ${saveError.message}`);
    }

    // Create LP position with initial prices
    const lpPosition = new LPPosition({
      positionId: tradeData.orderNo,
      type: tradeData.type,
      profit: 0,
      volume: tradeData.volume,
      adminId: adminId,
      symbol: tradeData.symbol,
      entryPrice: currentPrice.toFixed(2),
      openDate: new Date(tradeData.openingDate),
      currentPrice: currentPrice.toFixed(2),
      clientOrders: savedOrder._id,
      status: "OPEN",
    });
    const savedLPPosition = await lpPosition.save({ session: mongoSession });

    // Calculate initial LP Profit
    const gramValue = TTB_FACTOR / TROY_OUNCE_GRAMS;
    let lpProfitValue =
      tradeData.type === "BUY"
        ? gramValue * volume * askSpread
        : gramValue * volume * bidSpread;

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

    savedOrder.lpPositionId = savedLPPosition._id;
    await savedOrder.save({ session: mongoSession });

    // Update balances
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

    // Calculate initial values for ledgers
    let goldWeightValue =
      (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
    let lpCurrentPrice =
      (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

    // Create initial ledger entries
    const orderLedgerEntry = new Ledger({
      entryId: generateEntryId("ORD"),
      entryType: "ORDER",
      referenceNumber: tradeData.orderNo,
      description: `Margin for ${tradeData.type} ${tradeData.volume} ${
        tradeData.symbol
      } @ ${currentPrice.toFixed(2)} (AED ${goldWeightValue.toFixed(2)})`,
      amount: requiredMargin.toFixed(2),
      entryNature: "DEBIT",
      runningBalance: newCashBalance.toFixed(2),
      orderDetails: {
        type: tradeData.type,
        symbol: tradeData.symbol,
        volume: tradeData.volume,
        entryPrice: currentPrice,
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
      } account for ${tradeData.type} order`,
    });
    await goldTransactionLedgerEntry.save({ session: mongoSession });

    // MT5 Integration
    let mt5Result = null;

    if (!tradeData.ticket) {
      try {
        const mt5Symbol = SYMBOL_MAPPING[tradeData.symbol];
        if (!mt5Symbol) {
          throw new Error(
            `Invalid symbol: ${tradeData.symbol}. No MT5 mapping found.`
          );
        }

        const validatedSymbol = await mt5Service.validateSymbol(mt5Symbol);
        const priceData = await mt5Service.getPrice(validatedSymbol);

        if (!priceData || !priceData.bid || !priceData.ask) {
          throw new Error(
            `No valid price quote available for ${validatedSymbol}`
          );
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

        mt5Result = await mt5Service.placeTrade(mt5TradeData);
        console.log("MT5 Result", mt5Result);

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

        // Update prices based on MT5 response
        currentPrice = parseFloat(mt5Result.data.price);
        console.log("Current price", currentPrice);
        openingPrice =
          tradeData.type === "BUY"
            ? currentPrice + askSpread
            : currentPrice - bidSpread;

            console.log("Opening price", openingPrice);
        // Recalculate values with updated prices
        const updatedGoldWeightValue =
          (openingPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
        const updatedLPCurrentPrice =
          (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

        // Recalculate LP Profit
        lpProfitValue =
          tradeData.type === "BUY"
            ? gramValue * volume * askSpread
            : gramValue * volume * bidSpread;

        // Update Order
        await Order.findByIdAndUpdate(
          savedOrder._id,
          {
            price: currentPrice.toFixed(2),
            openingPrice: openingPrice.toFixed(2),
            orderStatus: "OPEN",
            ticket: mt5Result.data.order.toString(),
            volume: mt5Result.data.volume,
            symbol: mt5Result.data.symbol || tradeData.symbol,
          },
          { session: mongoSession }
        );

        // Update LP Position
        await LPPosition.findByIdAndUpdate(
          savedLPPosition._id,
          {
            entryPrice: currentPrice.toFixed(2),
            currentPrice: currentPrice.toFixed(2),
          },
          { session: mongoSession }
        );

        // Update LP Profit
        await LPProfit.findByIdAndUpdate(
          savedLPProfit._id,
          {
            value: lpProfitValue.toFixed(2),
          },
          { session: mongoSession }
        );

        // Update Ledger Entries
        await Ledger.findByIdAndUpdate(
          savedOrderLedger._id,
          {
            description: `Margin for ${tradeData.type} ${tradeData.volume} ${
              tradeData.symbol
            } @ ${openingPrice.toFixed(
              2
            )} (AED ${updatedGoldWeightValue.toFixed(
              2
            )}) [MT5: ${currentPrice.toFixed(2)}]`,
            "orderDetails.entryPrice": openingPrice,
            "orderDetails.status": "OPEN",
          },
          { session: mongoSession }
        );

        await Ledger.findByIdAndUpdate(
          savedLPLedger._id,
          {
            description: `LP Position opened for ${tradeData.type} ${
              tradeData.volume
            } ${tradeData.symbol} @ ${currentPrice.toFixed(
              2
            )} (AED ${updatedLPCurrentPrice.toFixed(2)})`,
            amount: updatedLPCurrentPrice.toFixed(2),
            "lpDetails.entryPrice": currentPrice,
            "lpDetails.status": "OPEN",
          },
          { session: mongoSession }
        );

        await Ledger.findByIdAndUpdate(
          savedCashLedger._id,
          {
            notes: `Cash margin allocated for ${tradeData.type} order on ${
              tradeData.symbol
            } (Client: ${openingPrice.toFixed(2)}, MT5: ${currentPrice.toFixed(
              2
            )})`,
          },
          { session: mongoSession }
        );

        await Ledger.findByIdAndUpdate(
          goldTransactionLedgerEntry._id,
          {
            notes: `Gold weight (${tradeData.volume}) ${
              tradeData.type === "BUY" ? "added to" : "subtracted from"
            } account for ${
              tradeData.type
            } order (Client Value: AED ${updatedGoldWeightValue.toFixed(
              2
            )}, MT5: ${currentPrice.toFixed(2)})`,
          },
          { session: mongoSession }
        );
      } catch (mt5Error) {
        console.error(`MT5 trade failed: ${mt5Error.message}`);
        await updateTradeStatus(
          adminId,
          savedOrder._id.toString(),
          {
            orderStatus: "FAILED",
            notificationError: mt5Error.message,
          },
          mongoSession
        );
        throw mt5Error;
      }
    } else {
      // Handle provided ticket case
      try {
        const mt5Symbol = SYMBOL_MAPPING[tradeData.symbol] || tradeData.symbol;
        const validatedSymbol = await mt5Service.validateSymbol(mt5Symbol);

        // Use tradeData.price as MT5 price if provided, else fetch current market price
        if (tradeData.price && !isNaN(parseFloat(tradeData.price))) {
          currentPrice = parseFloat(tradeData.price);
        } else {
          const priceData = await mt5Service.getPrice(validatedSymbol);
          if (priceData && priceData.bid && priceData.ask) {
            currentPrice =
              tradeData.type === "BUY"
                ? parseFloat(priceData.ask)
                : parseFloat(priceData.bid);
          } else {
            throw new Error(
              `No valid price quote available for ${validatedSymbol}`
            );
          }
        }

        // Calculate openingPrice based on MT5 price and spread
        openingPrice =
          tradeData.type === "BUY"
            ? currentPrice + askSpread
            : currentPrice - bidSpread;

        // Recalculate values with updated prices
        const updatedGoldWeightValue =
          (openingPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
        const updatedLPCurrentPrice =
          (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

        // Recalculate LP Profit
        lpProfitValue =
          tradeData.type === "BUY"
            ? gramValue * volume * askSpread
            : gramValue * volume * bidSpread;

        // Update Order
        await Order.findByIdAndUpdate(
          savedOrder._id,
          {
            price: currentPrice.toFixed(2),
            openingPrice: openingPrice.toFixed(2),
            orderStatus: "OPEN",
            ticket: tradeData.ticket.toString(),
            volume: tradeData.volume,
            symbol: tradeData.symbol,
          },
          { session: mongoSession }
        );

        // Update LP Position
        await LPPosition.findByIdAndUpdate(
          savedLPPosition._id,
          {
            entryPrice: currentPrice.toFixed(2),
            currentPrice: currentPrice.toFixed(2),
          },
          { session: mongoSession }
        );

        // Update LP Profit
        await LPProfit.findByIdAndUpdate(
          savedLPProfit._id,
          {
            value: lpProfitValue.toFixed(2),
          },
          { session: mongoSession }
        );

        // Update Ledger Entries
        await Ledger.findByIdAndUpdate(
          savedOrderLedger._id,
          {
            description: `Margin for ${tradeData.type} ${tradeData.volume} ${
              tradeData.symbol
            } @ ${openingPrice.toFixed(
              2
            )} (AED ${updatedGoldWeightValue.toFixed(
              2
            )}) [MT5: ${currentPrice.toFixed(2)}]`,
            "orderDetails.entryPrice": openingPrice,
            "orderDetails.status": "OPEN",
          },
          { session: mongoSession }
        );

        await Ledger.findByIdAndUpdate(
          savedLPLedger._id,
          {
            description: `LP Position opened for ${tradeData.type} ${
              tradeData.volume
            } ${tradeData.symbol} @ ${currentPrice.toFixed(
              2
            )} (AED ${updatedLPCurrentPrice.toFixed(2)})`,
            amount: updatedLPCurrentPrice.toFixed(2),
            "lpDetails.entryPrice": currentPrice,
            "lpDetails.status": "OPEN",
          },
          { session: mongoSession }
        );

        await Ledger.findByIdAndUpdate(
          savedCashLedger._id,
          {
            notes: `Cash margin allocated for ${tradeData.type} order on ${
              tradeData.symbol
            } (Client: ${openingPrice.toFixed(2)}, MT5: ${currentPrice.toFixed(
              2
            )})`,
          },
          { session: mongoSession }
        );

        await Ledger.findByIdAndUpdate(
          goldTransactionLedgerEntry._id,
          {
            notes: `Gold weight (${tradeData.volume}) ${
              tradeData.type === "BUY" ? "added to" : "subtracted from"
            } account for ${
              tradeData.type
            } order (Client Value: AED ${updatedGoldWeightValue.toFixed(
              2
            )}, MT5: ${currentPrice.toFixed(2)})`,
          },
          { session: mongoSession }
        );
      } catch (error) {
        console.error(`Error processing provided ticket: ${error.message}`);
        throw error;
      }
    }

    if (!session) {
      await mongoSession.commitTransaction();
      committed = true;
      mongoSession.endSession();
      sessionEnded = true;
    }

    return {
      clientOrder: {
        ...savedOrder.toObject(),
        price: currentPrice.toFixed(2),
        openingPrice: openingPrice.toFixed(2),
        orderStatus: mt5Result
          ? "OPEN"
          : tradeData.ticket
          ? "OPEN"
          : "PROCESSING",
        ticket:
          mt5Result?.data?.order?.toString() ||
          tradeData.ticket?.toString() ||
          null,
      },
      lpPosition: {
        ...savedLPPosition.toObject(),
        entryPrice: currentPrice.toFixed(2),
        currentPrice: currentPrice.toFixed(2),
      },
      lpProfit: {
        ...savedLPProfit.toObject(),
        value: lpProfitValue.toFixed(2),
      },
      mt5Trade: mt5Result
        ? {
            ticket: mt5Result.data.order,
            volume: mt5Result.data.volume,
            price: mt5Result.data.price,
            symbol: mt5Result.data.symbol || tradeData.symbol,
            type: tradeData.type,
          }
        : null,
      balances: {
        cash: newCashBalance,
        gold: newMetalBalance,
      },
      priceDetails: {
        initialPrice: parseFloat(tradeData.openingPrice ?? tradeData.price),
        currentPrice: currentPrice,
        openingPrice: openingPrice,
        spreadApplied: tradeData.type === "BUY" ? askSpread : bidSpread,
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

  try {
    if (!session) mongoSession.startTransaction();

    const allowedUpdates = [
      "orderStatus",
      "closingPrice",
      "closingDate",
      "profit",
      "comment",
      "price",
      "openingPrice",
      "ticket",
      "volume",
      "symbol",
      "notificationError",
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

    const order = await Order.findOne({ _id: orderId, adminId }).session(
      mongoSession
    );
    if (!order) {
      throw new Error("Order not found or unauthorized");
    }

    const userAccount = await Account.findById(order.user).session(
      mongoSession
    );
    if (!userAccount) {
      throw new Error("User account not found");
    }

    // Get user spreads
    const askSpread = parseFloat(userAccount.askSpread) || 0;
    const bidSpread = parseFloat(userAccount.bidSpread) || 0;

    let mt5ClosingPrice = null; // Market closing price from MT5
    let clientClosingPrice = null; // Client closing price with spread

    if (
      sanitizedData.orderStatus === "CLOSED" &&
      order.orderStatus !== "CLOSED"
    ) {
      try {
        const mt5Symbol = SYMBOL_MAPPING[order.symbol] || order.symbol;
        const validatedSymbol = await mt5Service.validateSymbol(mt5Symbol);

        const mt5CloseData = {
          ticket: order.ticket,
          symbol: validatedSymbol,
          volume: parseFloat(order.volume),
          type: order.type === "BUY" ? "SELL" : "BUY",
          openingPrice: parseFloat(order.price),
        };

        const mt5CloseResult = await mt5Service.closeTrade(mt5CloseData);

        if (!mt5CloseResult.success) {
          if (mt5CloseResult.error.includes("Position not found")) {
            const priceData = await mt5Service.getPrice(validatedSymbol);
            if (priceData && priceData.bid && priceData.ask) {
              mt5ClosingPrice =
                order.type === "BUY"
                  ? parseFloat(priceData.bid)
                  : parseFloat(priceData.ask);
            } else {
              mt5ClosingPrice = parseFloat(order.price);
            }
          } else {
            throw new Error(
              `MT5 trade closure failed: ${mt5CloseResult.error}`
            );
          }
        } else {
          mt5ClosingPrice = parseFloat(
            mt5CloseResult.closePrice || mt5CloseResult.data?.price
          );
        }

        // Calculate client closing price with spread
        clientClosingPrice =
          order.type === "BUY"
            ? mt5ClosingPrice - bidSpread
            : mt5ClosingPrice + askSpread;

        sanitizedData.closingPrice = clientClosingPrice.toFixed(2);
        sanitizedData.price = mt5ClosingPrice.toFixed(2);
      } catch (mt5Error) {
        console.error(`Failed to close MT5 trade: ${mt5Error.message}`);

        if (mt5Error.message.includes("Position not found")) {
          try {
            const priceData = await mt5Service.getPrice(
              SYMBOL_MAPPING[order.symbol] || order.symbol
            );
            if (priceData && priceData.bid && priceData.ask) {
              mt5ClosingPrice =
                order.type === "BUY"
                  ? parseFloat(priceData.bid)
                  : parseFloat(priceData.ask);
              clientClosingPrice =
                order.type === "BUY"
                  ? mt5ClosingPrice - bidSpread
                  : mt5ClosingPrice + askSpread;
              sanitizedData.closingPrice = clientClosingPrice.toFixed(2);
              sanitizedData.price = mt5ClosingPrice.toFixed(2);
            }
          } catch (priceError) {
            console.error(`Failed to get price data: ${priceError.message}`);
          }
        } else {
          throw mt5Error;
        }
      }
    }

    // Use provided closing price if no MT5 execution
    if (!clientClosingPrice && sanitizedData.closingPrice) {
      clientClosingPrice = parseFloat(sanitizedData.closingPrice);
    }

    // Calculate profit
    const entryPrice = parseFloat(order.openingPrice); // Client entry price with spread
    const volume = parseFloat(order.volume);
    let clientProfit = 0;

    if (clientClosingPrice && sanitizedData.orderStatus === "CLOSED") {
      const entryGoldValue =
        (entryPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
      const closingGoldValue =
        (clientClosingPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

      if (order.type === "BUY") {
        clientProfit = closingGoldValue - entryGoldValue;
      } else {
        clientProfit = entryGoldValue - closingGoldValue;
      }
    }

    // Update account balances
    let newCashBalance = parseFloat(userAccount.reservedAmount);
    let newMetalBalance = parseFloat(userAccount.METAL_WT);
    let newAMOUNTFC = parseFloat(userAccount.AMOUNTFC || 0);
    const currentCashBalance = newCashBalance;
    const currentMetalBalance = newMetalBalance;
    const currentAMOUNTFC = newAMOUNTFC;

    // Update order
    Object.keys(sanitizedData).forEach((key) => {
      order[key] = sanitizedData[key];
    });

    if (sanitizedData.orderStatus === "CLOSED") {
      order.profit = clientProfit.toFixed(2);
    }

    await order.save({ session: mongoSession });

    // Update LP Position
    const lpPosition = await LPPosition.findOne({
      positionId: order.orderNo,
    }).session(mongoSession);

    if (lpPosition) {
      if (mt5ClosingPrice !== null && sanitizedData.orderStatus === "CLOSED") {
        lpPosition.closingPrice = mt5ClosingPrice.toFixed(2);
        lpPosition.currentPrice = mt5ClosingPrice.toFixed(2);
        lpPosition.status = "CLOSED";

        // Calculate LP profit (spread profit)
        const lpEntryPrice = parseFloat(lpPosition.entryPrice);
        const clientEntryPrice = parseFloat(order.openingPrice);

        const lpEntryGoldValue =
          (lpEntryPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
        const clientEntryGoldValue =
          (clientEntryPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
        const lpClosingGoldValue =
          (mt5ClosingPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
        const clientClosingGoldValue =
          (clientClosingPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

        const openingSpreadProfit = Math.abs(
          clientEntryGoldValue - lpEntryGoldValue
        );
        const closingSpreadProfit = Math.abs(
          clientClosingGoldValue - lpClosingGoldValue
        );
        const lpProfit = openingSpreadProfit + closingSpreadProfit;

        lpPosition.profit = lpProfit.toFixed(2);
      } else if (mt5ClosingPrice !== null) {
        lpPosition.currentPrice = mt5ClosingPrice.toFixed(2);
      }

      if (sanitizedData.closingDate) {
        lpPosition.closeDate = sanitizedData.closingDate;
      }

      await lpPosition.save({ session: mongoSession });
    }

    // Update LP Profit record
    if (sanitizedData.orderStatus === "CLOSED") {
      const lpProfitRecord = await LPProfit.findOne({
        orderNo: order.orderNo,
        status: "OPEN",
      }).session(mongoSession);

      if (lpProfitRecord) {
        const gramValue = TTB_FACTOR / TROY_OUNCE_GRAMS;
        let closingLPProfitValue =
          order.type === "BUY"
            ? gramValue * volume * bidSpread
            : gramValue * volume * askSpread;

        const totalLPProfit =
          parseFloat(lpProfitRecord.value) + closingLPProfitValue;
        lpProfitRecord.status = "CLOSED";
        lpProfitRecord.value = totalLPProfit.toFixed(2);
        lpProfitRecord.datetime = new Date(sanitizedData.closingDate);

        await lpProfitRecord.save({ session: mongoSession });
      }
    }

    // Handle account balance updates for closed trades
    if (sanitizedData.orderStatus === "CLOSED") {
      const settlementAmount = parseFloat(order.requiredMargin || 0);

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

      // Create ledger entries
      const orderLedgerEntry = new Ledger({
        entryId: generateEntryId("ORD"),
        entryType: "ORDER",
        referenceNumber: order.orderNo,
        description: `Closing ${order.type} ${volume} ${
          order.symbol
        } @ ${clientClosingPrice.toFixed(2)} [MT5: ${
          mt5ClosingPrice ? mt5ClosingPrice.toFixed(2) : "N/A"
        }]${clientProfit > 0 ? " with profit" : ""}`,
        amount: (
          settlementAmount + (clientProfit > 0 ? clientProfit : 0)
        ).toFixed(2),
        entryNature: "CREDIT",
        runningBalance: newCashBalance.toFixed(2),
        orderDetails: {
          type: order.type,
          symbol: order.symbol,
          volume: volume,
          entryPrice: entryPrice,
          closingPrice: clientClosingPrice,
          profit: clientProfit.toFixed(2),
          status: "CLOSED",
        },
        user: order.user,
        adminId: adminId,
        date: new Date(sanitizedData.closingDate),
      });
      await orderLedgerEntry.save({ session: mongoSession });

      if (lpPosition) {
        const lpClosingPrice = mt5ClosingPrice || clientClosingPrice;
        const lpLedgerEntry = new Ledger({
          entryId: generateEntryId("LP"),
          entryType: "LP_POSITION",
          referenceNumber: order.orderNo,
          description: `LP Position closed for ${order.type} ${volume} ${
            order.symbol
          } @ ${lpClosingPrice.toFixed(
            2
          )} [Client: ${clientClosingPrice.toFixed(2)}]`,
          amount: settlementAmount.toFixed(2),
          entryNature: "DEBIT",
          runningBalance: newCashBalance.toFixed(2),
          lpDetails: {
            positionId: order.orderNo,
            type: order.type,
            symbol: order.symbol,
            volume: volume,
            entryPrice: parseFloat(lpPosition.entryPrice),
            closingPrice: lpClosingPrice,
            profit: parseFloat(lpPosition.profit || 0).toFixed(2),
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
        notes: `Cash settlement for closed ${order.type} order on ${
          order.symbol
        } (Client: ${clientClosingPrice.toFixed(2)}, MT5: ${
          mt5ClosingPrice ? mt5ClosingPrice.toFixed(2) : "N/A"
        })`,
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
        } from closed ${order.type} order (Entry: ${entryPrice.toFixed(
          2
        )}, Exit: ${clientClosingPrice.toFixed(2)})`,
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
        lp: lpPosition ? parseFloat(lpPosition.profit || 0) : 0,
      },
      closingPrices: {
        mt5ClosingPrice: mt5ClosingPrice,
        clientClosingPrice: clientClosingPrice,
        spreadApplied: order.type === "BUY" ? bidSpread : askSpread,
      },
      priceDetails: {
        entryPrice: entryPrice,
        closingPrice: clientClosingPrice,
        mt5ClosingPrice: mt5ClosingPrice,
        spreadOnClosing: order.type === "BUY" ? bidSpread : askSpread,
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
