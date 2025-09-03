import mt5Service from "./mt5Service.js";
import mt5MarketDataService from "./mt5MarketDataService.js";
import Order from "../../models/OrderSchema.js";
import {
  createTrade,
  updateTradeStatus,
} from "../../services/admin/tradingServices.js";
import { getAdminUser, isAuthorizedUser } from "../market/userService.js";
import { checkSufficientBalance } from "../market/balanceService.js";

// Constants for symbol alternatives
const SYMBOL_ALTERNATIVES = [
  "XAUUSD_TTBAR.Fix",
  "XAUUSD",
  "GOLD",
  "XAUUSD.Fix",
  "XAU/USD",
];

const DEFAULT_SYMBOL = process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix";

// Error types for consistent error handling
export const ERROR_TYPES = {
  USER_NOT_AUTHORIZED: "USER_NOT_AUTHORIZED",
  ADMIN_NOT_FOUND: "ADMIN_NOT_FOUND",
  INSUFFICIENT_BALANCE: "INSUFFICIENT_BALANCE",
  MARKET_DATA_UNAVAILABLE: "MARKET_DATA_UNAVAILABLE",
  INVALID_MARKET_DATA: "INVALID_MARKET_DATA",
  DATABASE_ERROR: "DATABASE_ERROR",
  MT5_EXECUTION_FAILED: "MT5_EXECUTION_FAILED",
  MT5_CONNECTION_FAILED: "MT5_CONNECTION_FAILED",
  SYSTEM_ERROR: "SYSTEM_ERROR",
  INVALID_INPUT: "INVALID_INPUT",
  INVALID_ORDER_TYPE: "INVALID_ORDER_TYPE",
  INVALID_VOLUME: "INVALID_VOLUME",
  ORDER_NOT_FOUND: "ORDER_NOT_FOUND",
  HANDLER_ERROR: "HANDLER_ERROR",
};

/**
 * Get market data with fallback to alternative symbols
 * @param {string} primarySymbol - Primary symbol to try first
 * @returns {Promise<{marketData: Object, symbol: string}>}
 */
// const getMarketDataWithFallback = async (primarySymbol = DEFAULT_SYMBOL) => {
//     let marketData;
//     let workingSymbol = primarySymbol;

//     try {
//         marketData = await mt5MarketDataService.getMarketData(primarySymbol);

//         // If the primary symbol fails, try alternatives
//         if (!marketData || !marketData.bid || !marketData.ask) {
//             console.log(`Failed to get data for ${primarySymbol}, trying alternatives...`);

//             for (const altSymbol of SYMBOL_ALTERNATIVES) {
//                 if (altSymbol === primarySymbol) continue; // Skip already tried symbol

//                 try {
//                     marketData = await mt5MarketDataService.getMarketData(altSymbol);
//                     if (marketData && marketData.bid && marketData.ask) {
//                         workingSymbol = altSymbol;
//                         console.log(`Successfully got data for ${altSymbol}`);
//                         break;
//                     }
//                 } catch (altError) {
//                     console.log(`Alternative symbol ${altSymbol} also failed:`, altError.message);
//                 }
//             }
//         }
//     } catch (priceError) {
//         console.error('Failed to get market data:', priceError);
//         throw new Error(`Market data unavailable: ${priceError.message}`);
//     }

//     // Final validation
//     if (!marketData || !marketData.bid || !marketData.ask) {
//         throw new Error('No valid market data available from any symbol');
//     }

//     return { marketData, symbol: workingSymbol };
// };

/**
 * Generate unique order number
 * @returns {string}
 */
const generateOrderNumber = () => {
  return `MT5-${Date.now()}-${Math.random()
    .toString(36)
    .substring(2, 7)
    .toUpperCase()}`;
};

/**
 * Update order status in database with error handling
 * @param {string} orderId - Order ID to update
 * @param {Object} updateData - Data to update
 * @returns {Promise<boolean>}
 */
const updateOrderStatus = async (orderId, updateData) => {
  try {
    await Order.findByIdAndUpdate(orderId, updateData);
    return true;
  } catch (updateError) {
    console.error("Failed to update order status:", updateError);
    return false;
  }
};

/**
 * Process order placement for MT5 trading
 * @param {Object} pendingOrder - Order details from session
 * @param {string} userPhoneNumber - User's phone number
 * @returns {Promise<Object>}
 */
export const processOrderPlacementMT5 = async (
  pendingOrder,
  userPhoneNumber
) => {
  let tradeResult = null;
  const orderNo = generateOrderNumber();

  try {
    // Extract parameters - handle both old and new calling patterns
    const phoneNumber = userPhoneNumber || pendingOrder?.phoneNumber;
    const volume = pendingOrder?.volume || pendingOrder;
    const type = pendingOrder?.type || arguments[2]; // fallback for old pattern

    if (!phoneNumber || !volume || !type) {
      return {
        success: false,
        message: "Missing required parameters for order placement",
        error: ERROR_TYPES.INVALID_INPUT,
      };
    }

    console.log(
      `Processing ${type} order for ${volume} grams for phone: ${phoneNumber}`
    );

    // Step 1: Check if user is authorized
    const authResult = await isAuthorizedUser(phoneNumber);
    if (!authResult.isAuthorized) {
      return {
        success: false,
        message: "Access denied. Your number is not registered for trading.",
        error: ERROR_TYPES.USER_NOT_AUTHORIZED,
      };
    }

    const userAccountId = authResult.accountId;

    // Step 2: Get admin user
    const admin = await getAdminUser();
    if (!admin) {
      return {
        success: false,
        message: "System error: No admin user found",
        error: ERROR_TYPES.ADMIN_NOT_FOUND,
      };
    }

    // Step 3: Check sufficient balance
    const balanceCheck = await checkSufficientBalance(userAccountId, volume);
    if (!balanceCheck.success) {
      return {
        success: false,
        message: balanceCheck.message,
        error: ERROR_TYPES.INSUFFICIENT_BALANCE,
      };
    }

    // Step 4: Ensure MT5 connection
    if (!mt5Service.isConnected) {
      try {
        console.log("MT5 not connected, attempting to connect...");
        await mt5Service.connect();
      } catch (connectionError) {
        console.error("Failed to connect to MT5:", connectionError);
        return {
          success: false,
          message:
            "Unable to connect to trading platform. Please try again later.",
          error: ERROR_TYPES.MT5_CONNECTION_FAILED,
          details: connectionError.message,
        };
      }
    }

    // Step 5: Get market data with fallback
    let marketData, symbol;
    try {
      const marketResult = await getMarketDataWithFallback();
      marketData = marketResult.marketData;
      symbol = marketResult.symbol;
    } catch (marketError) {
      return {
        success: false,
        message: "Unable to get current market price. Please try again.",
        error: ERROR_TYPES.MARKET_DATA_UNAVAILABLE,
        details: marketError.message,
      };
    }

    // Step 6: Calculate trade parameters
    const goldPrice = type === "BUY" ? marketData.ask : marketData.bid;
    const requiredAmount = parseFloat(balanceCheck.requiredAmount);

    const tradeData = {
      orderNo,
      type,
      volume,
      symbol,
      price: goldPrice,
      requiredMargin: requiredAmount,
      openingPrice: goldPrice,
      openingDate: new Date(),
      marketDataTimestamp: marketData.timestamp,
      comment: `WhatsApp ${type} ${volume} ${symbol}`,
      phoneNumber: phoneNumber, // Add phone number to trade data
    };

    // Step 7: Create trade in database
    try {
      tradeResult = await createTrade(admin._id, userAccountId, tradeData);
      if (!tradeResult || !tradeResult.clientOrder) {
        throw new Error("Failed to create trade record in database");
      }
    } catch (dbError) {
      console.error("Database trade creation failed:", dbError);
      return {
        success: false,
        message: "Failed to create trade record. Please try again.",
        error: ERROR_TYPES.DATABASE_ERROR,
        details: dbError.message,
      };
    }

    // Step 8: Execute trade on MT5 - UPDATED TO MATCH PYTHON CONNECTOR
    let mt5Result = null;
    try {
      // Convert volume from grams to lots if necessary
      // Assuming 1 lot = 100 ounces = ~3110 grams (approximate)
      const volumeInLots = volume / 3110; // Convert grams to lots
      const minVolume = 0.01; // Minimum lot size
      const finalVolume = Math.max(volumeInLots, minVolume);

      const mt5TradeData = {
        symbol: symbol,
        volume: finalVolume,
        type: type.toUpperCase(), // 'BUY' or 'SELL'
        slDistance: 10.0, // $10 stop loss distance
        tpDistance: 10.0, // $10 take profit distance
        comment: `DB-${tradeResult.clientOrder._id}`,
        orderNo: orderNo,
        magic: 123456,
      };

      console.log("Executing MT5 trade with data:", mt5TradeData);

      // Use the corrected placeTrade method
      mt5Result = await mt5Service.placeTrade(mt5TradeData);

      if (!mt5Result || !mt5Result.success) {
        throw new Error(mt5Result?.error || "MT5 trade execution failed");
      }

      console.log("MT5 trade result:", mt5Result);

      // Update order with MT5 ticket number
      await updateOrderStatus(tradeResult.clientOrder._id, {
        comment: mt5Result.ticket
          ? `MT5-${mt5Result.ticket}`
          : tradeData.comment,
        orderStatus: "EXECUTED",
        mt5Ticket: mt5Result.ticket,
        mt5Deal: mt5Result.deal,
        executionPrice: mt5Result.price,
        executionTime: new Date(),
      });

      return {
        success: true,
        message: `${type} order executed successfully`,
        orderNo,
        orderId: tradeResult.clientOrder._id,
        symbol,
        volume: finalVolume,
        volumeInGrams: volume,
        price: mt5Result.price || goldPrice,
        requiredAmount: requiredAmount.toFixed(2),
        userBalance: balanceCheck.userBalance,
        remainingBalance: balanceCheck.remainingBalance,
        mt5Ticket: mt5Result.ticket || null,
        mt5Deal: mt5Result.deal || null,
        executionTime: new Date().toISOString(),
      };
    } catch (mt5Error) {
      console.error("MT5 execution failed:", mt5Error);

      // Update order status to failed but keep database record
      await updateOrderStatus(tradeResult.clientOrder._id, {
        orderStatus: "FAILED",
        comment: `MT5 Error: ${mt5Error.message}`,
        failureReason: mt5Error.message,
        failureTime: new Date(),
      });

      return {
        success: false,
        message: `Order was recorded but execution failed: ${mt5Error.message}`,
        error: ERROR_TYPES.MT5_EXECUTION_FAILED,
        orderNo,
        orderId: tradeResult.clientOrder._id,
        details: mt5Error.message,
        symbol,
        volume,
        requestedPrice: goldPrice,
      };
    }
  } catch (error) {
    console.error("Unexpected error in order placement:", error);

    // If we have a database record, update it to failed
    if (tradeResult && tradeResult.clientOrder) {
      await updateOrderStatus(tradeResult.clientOrder._id, {
        orderStatus: "FAILED",
        comment: `System Error: ${error.message}`,
        failureReason: error.message,
        failureTime: new Date(),
      });
    }

    return {
      success: false,
      message:
        "An unexpected error occurred while processing your order. Please try again.",
      error: ERROR_TYPES.SYSTEM_ERROR,
      details: error.message,
      orderId: tradeResult?.clientOrder?._id || null,
    };
  }
};

// Helper function to get market data with multiple symbol fallbacks
async function getMarketDataWithFallback() {
  const goldSymbols = ["XAUUSD", "XAUUSD.Fix", process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix", "GOLD", "GOLDm"];

  let lastError;

  for (const symbol of goldSymbols) {
    try {
      console.log(`Trying to get market data for: ${symbol}`);
      const marketData = await mt5Service.getPrice(symbol);

      if (marketData && marketData.bid && marketData.ask) {
        console.log(`Successfully got market data for: ${symbol}`);
        return {
          marketData: {
            bid: marketData.bid,
            ask: marketData.ask,
            spread: marketData.spread,
            timestamp: new Date(),
          },
          symbol: symbol,
        };
      }
    } catch (error) {
      console.log(`Failed to get market data for ${symbol}:`, error.message);
      lastError = error;
      continue;
    }
  }

  throw new Error(
    `Failed to get market data for any gold symbol. Last error: ${lastError?.message}`
  );
}

/**
 * Close MT5 order/position
 * @param {string} orderId - Order ID to close
 * @param {string} userPhoneNumber - User's phone number for authentication
 * @returns {Promise<Object>}
 */
export const processOrderCloseMT5 = async (orderId, userPhoneNumber) => {
  try {
    // Get user authorization
    const authResult = await isAuthorizedUser(userPhoneNumber);
    if (!authResult.isAuthorized) {
      return {
        success: false,
        message: "Access denied. Your number is not registered for trading.",
        error: ERROR_TYPES.USER_NOT_AUTHORIZED,
      };
    }

    const admin = await getAdminUser();
    if (!admin) {
      return {
        success: false,
        message: "System error: No admin user found",
        error: ERROR_TYPES.ADMIN_NOT_FOUND,
      };
    }

    // Find the order
    const order = await Order.findOne({
      _id: orderId,
      user: authResult.accountId,
      orderStatus: { $in: ["PROCESSING", "EXECUTED"] },
    });

    if (!order) {
      return {
        success: false,
        message: "Order not found or already closed",
        error: ERROR_TYPES.ORDER_NOT_FOUND,
      };
    }

    // Get current market data
    let marketData;
    try {
      const result = await getMarketDataWithFallback(order.symbol);
      marketData = result.marketData;
    } catch (marketError) {
      return {
        success: false,
        message: "Unable to get current market price for closing order.",
        error: ERROR_TYPES.MARKET_DATA_UNAVAILABLE,
        details: marketError.message,
      };
    }

    const closeType = order.type === "BUY" ? "SELL" : "BUY";
    const currentPrice = closeType === "BUY" ? marketData.ask : marketData.bid;

    // Close position on MT5 if we have a ticket number
    let mt5CloseResult = null;
    if (order.mt5Ticket) {
      try {
        mt5CloseResult = await mt5Service.closeTrade(
          order.mt5Ticket,
          order.volume
        );
      } catch (mt5Error) {
        console.error("MT5 close failed:", mt5Error);
        // Continue with database closure even if MT5 fails
      }
    } else if (order.comment && order.comment.includes("MT5-")) {
      try {
        const mt5Ticket = order.comment.split("MT5-")[1];
        mt5CloseResult = await mt5Service.closeTrade(mt5Ticket, order.volume);
      } catch (mt5Error) {
        console.error("MT5 close failed:", mt5Error);
        // Continue with database closure even if MT5 fails
      }
    }

    const updateData = {
      orderStatus: "CLOSED",
      closingPrice: currentPrice,
      closingDate: new Date(),
      marketDataTimestamp: marketData.timestamp,
    };

    const result = await updateTradeStatus(admin._id, orderId, updateData);

    return {
      success: true,
      orderNo: order.orderNo,
      orderId: orderId,
      symbol: order.symbol,
      volume: order.volume,
      openPrice: order.openingPrice,
      closePrice: currentPrice,
      profit: result.profit?.client?.toFixed(2) || "0.00",
      newCashBalance: result.balances?.cash?.toFixed(2) || "0.00",
      newGoldBalance: result.balances?.gold?.toFixed(2) || "0.00",
      mt5Closed: !!mt5CloseResult,
    };
  } catch (error) {
    console.error("Order closing error:", error);
    return {
      success: false,
      message: "An error occurred while closing the order. Please try again.",
      error: ERROR_TYPES.SYSTEM_ERROR,
      details: error.message,
    };
  }
};

/**
 * Validate MT5 connection before processing orders
 * @returns {Promise<Object>}
 */
export const validateMT5Connection = async () => {
  try {
    if (!mt5Service.isConnected) {
      console.log("MT5 not connected, attempting to connect...");
      await mt5Service.connect();
    }

    // Test with a simple price request using fallback symbols
    let testPrice;
    for (const symbol of SYMBOL_ALTERNATIVES) {
      try {
        testPrice = await mt5Service.getPrice(symbol);
        if (testPrice) break;
      } catch (err) {
        console.log(`Failed to get test price for ${symbol}`);
      }
    }

    return {
      success: true,
      connected: true,
      lastPrice: testPrice,
    };
  } catch (error) {
    console.error("MT5 connection validation failed:", error);
    return {
      success: false,
      connected: false,
      error: error.message,
    };
  }
};

/**
 * Enhanced order placement with pre-validation
 * @param {Object} pendingOrder - Order details from session
 * @param {string} userPhoneNumber - User's phone number
 * @returns {Promise<Object>}
 */
export const processOrderPlacementMT5Enhanced = async (
  pendingOrder,
  userPhoneNumber
) => {
  // Pre-check MT5 connection
  const connectionCheck = await validateMT5Connection();
  if (!connectionCheck.success) {
    return {
      success: false,
      message:
        "Trading platform is currently unavailable. Please try again in a few minutes.",
      error: ERROR_TYPES.MT5_CONNECTION_FAILED,
      details: connectionCheck.error,
    };
  }

  // Proceed with order placement
  return await processOrderPlacementMT5(pendingOrder, userPhoneNumber);
};

/**
 * Input validation helper
 * @param {Object} params - Parameters to validate
 * @returns {Object} Validation result
 */
const validateInputs = (params) => {
  const { session, volume, type } = params;

  if (!session || volume === undefined || !type) {
    return {
      isValid: false,
      error: ERROR_TYPES.INVALID_INPUT,
      message: "Missing required parameters",
    };
  }

  if (!["BUY", "SELL"].includes(type.toUpperCase())) {
    return {
      isValid: false,
      error: ERROR_TYPES.INVALID_ORDER_TYPE,
      message: "Invalid order type. Must be BUY or SELL",
    };
  }

  if (volume <= 0) {
    return {
      isValid: false,
      error: ERROR_TYPES.INVALID_VOLUME,
      message: "Volume must be greater than 0",
    };
  }

  return { isValid: true };
};

/**
 * HTTP handler for order placement
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 * @returns {Promise<void>}
 */
export const handleOrderPlacement = async (req, res) => {
  try {
    const { session, volume, type } = req.body;

    // Validate input
    const validation = validateInputs({ session, volume, type });
    if (!validation.isValid) {
      return res.status(400).json({
        success: false,
        message: validation.message,
        error: validation.error,
      });
    }

    const result = await processOrderPlacementMT5Enhanced(
      { volume, type: type.toUpperCase() },
      session
    );

    // Return appropriate status code
    const statusCode = result.success ? 200 : 400;
    res.status(statusCode).json(result);
  } catch (error) {
    console.error("Order placement handler error:", error);
    res.status(500).json({
      success: false,
      message: "Internal server error",
      error: ERROR_TYPES.HANDLER_ERROR,
      details: error.message,
    });
  }
};

/**
 * Get user's open positions for MT5
 * @param {string} userPhoneNumber - User's phone number
 * @returns {Promise<Array>}
 */
export const getUserOpenPositions = async (userPhoneNumber) => {
  try {
    const authResult = await isAuthorizedUser(userPhoneNumber);
    if (!authResult.isAuthorized) {
      return [];
    }

    const orders = await Order.find({
      user: authResult.accountId,
      orderStatus: { $in: ["PROCESSING", "EXECUTED"] },
    }).sort({ openingDate: -1 });

    return orders.map((order, index) => ({
      orderId: order._id,
      orderNo: order.orderNo,
      type: order.type === "BUY" ? 0 : 1, // MT5 convention: 0 = BUY, 1 = SELL
      volume: order.volume,
      openPrice: order.openingPrice,
      symbol: order.symbol,
      openTime: order.openingDate,
      profit: 0, // This would need to be calculated based on current prices
      comment: order.comment,
    }));
  } catch (error) {
    console.error("Error fetching user positions:", error);
    return [];
  }
};
