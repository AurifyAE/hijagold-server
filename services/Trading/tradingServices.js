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

// Helper to generate unique entry ID
const generateEntryId = (prefix) => {
  const timestamp = Date.now().toString();
  const randomStr = Math.random().toString(36).substring(2, 5).toUpperCase();
  return `${prefix}-${timestamp.substring(timestamp.length - 5)}-${randomStr}`;
};

// Validate trade data against OrderSchema
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
  if (!tradeData.symbol || !SYMBOL_MAPPING[tradeData.symbol]) {
    errors.push(
      `Invalid symbol: ${tradeData.symbol}. Supported: ${Object.keys(
        SYMBOL_MAPPING
      ).join(", ")}`
    );
  }
  if (isNaN(tradeData.price) || tradeData.price <= 0) {
    errors.push("Invalid price: must be a positive number");
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
  if (!session) mongoSession.startTransaction();

  try {
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
    const currentPrice = parseFloat(tradeData.price);
    const volume = parseFloat(tradeData.volume);

    // Check sufficient balances
    const requiredMargin = parseFloat(tradeData.requiredMargin || 0);
    if (currentCashBalance < requiredMargin) {
      throw new Error("Insufficient cash balance");
    }
    // if (tradeData.type === "SELL" && currentMetalBalance < volume) {
    //   throw new Error("Insufficient gold balance for SELL order");
    // }

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

    // Create new order
    const newOrder = new Order({
      orderNo: tradeData.orderNo,
      type: tradeData.type,
      volume: tradeData.volume,
      symbol: tradeData.symbol,
      requiredMargin: requiredMargin,
      price: currentPrice,
      openingPrice: clientOrderPrice,
      user: userId,
      adminId: adminId,
      orderStatus: "PROCESSING",
      profit: 0,
      openingDate: new Date(tradeData.openingDate),
      storedTime: new Date(),
      comment: tradeData.comment || `Ord-${tradeData.orderNo}`,
      ticket: tradeData.ticket || null, // Use ticket from crmTradeData if provided
    });

    let savedOrder;
    try {
      // Check for duplicate orderNo
      const existingOrder = await Order.findOne({
        orderNo: tradeData.orderNo,
        adminId,
      }).session(mongoSession);
      if (existingOrder) {
        throw new Error(`Duplicate orderNo: ${tradeData.orderNo}`);
      }
      savedOrder = await newOrder.save({ session: mongoSession });
      console.log(
        `Trade created in CRM: ${savedOrder._id}, orderNo: ${savedOrder.orderNo}, storedTime: ${savedOrder.storedTime}, ticket: ${savedOrder.ticket}`
      );
    } catch (saveError) {
      console.error(
        `Failed to save order: ${saveError.message}, Stack: ${saveError.stack}`
      );
      if (saveError.code === 11000) {
        throw new Error(`Duplicate orderNo: ${tradeData.orderNo}`);
      }
      throw new Error(`Failed to save order: ${saveError.message}`);
    }

    // Create LP position
    const lpPosition = new LPPosition({
      positionId: tradeData.orderNo,
      type: tradeData.type,
      profit: 0,
      volume: tradeData.volume,
      adminId: adminId,
      symbol: tradeData.symbol,
      entryPrice: currentPrice,
      openDate: new Date(tradeData.openingDate),
      currentPrice: currentPrice,
      clientOrders: savedOrder._id,
      status: "OPEN",
    });
    const savedLPPosition = await lpPosition.save({ session: mongoSession });

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

    // Create ledger entries
    // In createTrade, update ledger descriptions to use tradeData.price
    const orderLedgerEntry = new Ledger({
      entryId: generateEntryId("ORD"),
      entryType: "ORDER",
      referenceNumber: tradeData.orderNo,
      description: `Margin for ${tradeData.type} ${tradeData.volume} ${
        tradeData.symbol
      } @ ${tradeData.price.toFixed(2)} (AED ${goldWeightValue.toFixed(2)})`,
      amount: requiredMargin.toFixed(2),
      entryNature: "DEBIT",
      runningBalance: newCashBalance.toFixed(2),
      orderDetails: {
        type: tradeData.type,
        symbol: tradeData.symbol,
        volume: tradeData.volume,
        entryPrice: tradeData.price, // Use adjusted price
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

    // Update CRM trade with MT5 details (only if ticket is provided and session is managed externally)
    if (tradeData.ticket && session) {
      await updateTradeStatus(
        adminId,
        savedOrder._id.toString(),
        {
          orderStatus: "OPEN",
          ticket: tradeData.ticket.toString(),
          openingPrice: tradeData.price,
          volume: tradeData.volume,
          symbol: tradeData.symbol,
        },
        mongoSession
      );
    }

    if (!session) await mongoSession.commitTransaction();

    return {
      clientOrder: savedOrder,
      lpPosition: savedLPPosition,
      mt5Trade: tradeData.ticket
        ? {
            ticket: tradeData.ticket,
            volume: tradeData.volume,
            price: tradeData.price,
            symbol: tradeData.symbol,
            type: tradeData.type,
          }
        : null,
      balances: {
        cash: newCashBalance,
        gold: newMetalBalance,
      },
      requiredMargin,
      goldWeightValue,
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
    if (!session) await mongoSession.abortTransaction();
    console.error(
      `Trade creation error: ${error.message}, Stack: ${error.stack}`
    );
    throw new Error(`Error creating trade: ${error.message}`);
  } finally {
    if (!session) mongoSession.endSession();
  }
};

export const updateTradeStatus = async (
  adminId,
  ticket,
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
    if (sanitizedData.closingPrice) {
      sanitizedData.price = sanitizedData.closingPrice;
    }

    console.log(
      `Updating CRM trade for ticket: ${ticket}, adminId: ${adminId}, updateData: ${JSON.stringify(
        sanitizedData
      )}`
    );
    const order = await Order.findOne({ _id: ticket, adminId }).session(
      mongoSession
    );
    if (!order) {
      console.error(`Order not found for _id: ${ticket}, adminId: ${adminId}`);
      throw new Error("Order not found or unauthorized");
    }

    const userAccount = await Account.findById(order.user).session(
      mongoSession
    );
    if (!userAccount) {
      console.error(`User account not found for userId: ${order.user}`);
      throw new Error("User account not found");
    }

    if (
      sanitizedData.orderStatus === "CLOSED" &&
      order.orderStatus !== "CLOSED"
    ) {
      try {
        // Fetch live market data
        const priceData = await mt5Service.getPrice(
          SYMBOL_MAPPING[order.symbol] || order.symbol
        );
        if (!priceData || !priceData.ask || !priceData.bid) {
          throw new Error("Failed to fetch live market data");
        }

        // Calculate adjusted closing prices
        const adjustedAskPrice = (
          parseFloat(priceData.ask) + (parseFloat(userAccount.askSpread) || 0)
        ).toFixed(2);
        const adjustedBidPrice = (
          parseFloat(priceData.bid) - (parseFloat(userAccount.bidSpread) || 0)
        ).toFixed(2);

        // Use adjusted price for closing (opposite of opening type)
        const clientClosingPrice =
          order.type === "BUY" ? adjustedBidPrice : adjustedAskPrice;
        sanitizedData.closingPrice = parseFloat(clientClosingPrice).toFixed(2);

        console.log(`Fetching MT5 positions for ticket ${order.ticket}`);
        const positions = await mt5Service.getPositions();
        const position =
          positions && Array.isArray(positions)
            ? positions.find((p) => p.ticket === order.ticket)
            : null;

        let mt5CloseResult = null;
        if (position) {
          console.log(
            `Position found for ticket ${order.ticket}, volume: ${position.volume}`
          );
          mt5CloseResult = await mt5Service.closeTrade({
            ticket: order.ticket,
            symbol: SYMBOL_MAPPING[order.symbol] || order.symbol,
            volume: position.volume,
            type: order.type === "BUY" ? "SELL" : "BUY",
            openingPrice: order.openingPrice,
          });
          console.log(
            `MT5 close trade result: ${JSON.stringify(mt5CloseResult, null, 2)}`
          );
          if (!mt5CloseResult.success) {
            if (mt5CloseResult.error.includes("Position not found")) {
              console.warn(
                `Position ${order.ticket} not found in MT5. Assuming already closed.`
              );
            } else if (mt5CloseResult.error.includes("10021")) {
              console.warn(
                `Retcode 10021 for ticket ${order.ticket}. Checking deal history.`
              );
            } else {
              throw new Error(
                `MT5 trade closure failed: ${
                  mt5CloseResult.error || "Unknown error"
                }`
              );
            }
          }
        } else {
          console.warn(
            `Position ${order.ticket} not found in MT5 positions. Attempting MT5 closure to confirm.`
          );
          mt5CloseResult = await mt5Service.closeTrade({
            ticket: order.ticket,
            symbol: SYMBOL_MAPPING[order.symbol] || order.symbol,
            volume: order.volume,
            type: order.type === "BUY" ? "SELL" : "BUY",
            openingPrice: order.openingPrice,
          });
          console.log(
            `MT5 close trade result: ${JSON.stringify(mt5CloseResult, null, 2)}`
          );
          if (
            !mt5CloseResult.success &&
            mt5CloseResult.error.includes("Position not found")
          ) {
            console.warn(
              `Position ${order.ticket} confirmed not found in MT5. Assuming already closed.`
            );
          } else if (mt5CloseResult.success) {
            console.log(
              `MT5 trade closed successfully: Ticket ${order.ticket}`
            );
          } else if (mt5CloseResult.error.includes("10021")) {
            console.warn(
              `Retcode 10021 for ticket ${order.ticket}. Assuming already closed or invalid parameters.`
            );
          } else {
            throw new Error(
              `MT5 trade closure failed: ${
                mt5CloseResult.error || "Unknown error"
              }`
            );
          }
        }
      } catch (mt5Error) {
        if (
          mt5Error.message.includes("Position not found") ||
          mt5Error.message.includes("10021")
        ) {
          console.warn(
            `Position ${order.ticket} not found or invalid request in MT5. Proceeding with CRM update using latest price.`
          );
          const priceData = await mt5Service.getPrice(
            SYMBOL_MAPPING[order.symbol] || order.symbol
          );
          const adjustedAskPrice = (
            parseFloat(priceData.ask) + (parseFloat(userAccount.askSpread) || 0)
          ).toFixed(2);
          const adjustedBidPrice = (
            parseFloat(priceData.bid) - (parseFloat(userAccount.bidSpread) || 0)
          ).toFixed(2);
          sanitizedData.closingPrice =
            order.type === "BUY" ? adjustedBidPrice : adjustedAskPrice;
        } else {
          console.error(
            `Failed to close MT5 trade for ticket ${order.ticket}: ${mt5Error.message}`
          );
          throw new Error(`Failed to close MT5 trade: ${mt5Error.message}`);
        }
      }
    }

    const currentPrice = parseFloat(sanitizedData.closingPrice);
    const entryPrice = parseFloat(order.openingPrice);
    const volume = parseFloat(order.volume);

    const entryGoldWeightValue =
      (entryPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;
    const closingGoldWeightValue =
      (currentPrice / TROY_OUNCE_GRAMS) * TTB_FACTOR * volume;

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

        const lpProfit = new LPProfit({
          orderNo: order.orderNo,
          orderType: order.type === "BUY" ? "SELL" : "BUY",
          status: "CLOSED",
          volume: volume,
          value: closingLPProfitValue.toFixed(2),
          user: order.user,
          datetime: new Date(sanitizedData.closingDate),
        });
        await lpProfit.save({ session: mongoSession });
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
        } @ ${currentPrice.toFixed(2)}${
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
          closingPrice: currentPrice.toFixed(2),
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
        console.error(`Failed to abort transaction: ${abortError.message}`);
      }
    }
    console.error(
      `Trade update error for ticket ${ticket}: ${error.message}, Stack: ${error.stack}`
    );
    throw new Error(`Error updating trade: ${error.message}`);
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