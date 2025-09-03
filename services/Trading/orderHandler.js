// Complete Order Handler - MT5 to CRM Integration
import { createTrade, updateTradeStatus } from "./tradingServices.js";
import mt5MarketDataService from "./mt5MarketDataService.js";
import mt5Service from "./mt5Service.js";

/**
 * Complete Order Handler for BUY/SELL/CLOSE operations
 */
export class OrderHandler {
  constructor() {
    this.pendingOrders = new Map();
    this.orderTimeout = 30000; // 30 seconds
  }

  /**
   * Handle BUY order - Place in MT5 then store in CRM
   */
  async handleBuyOrder(userSession, orderData) {
    const {
      volume,
      symbol = process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
      phoneNumber,
    } = orderData;

    try {
      console.log(
        `Processing BUY order for ${phoneNumber}: ${volume} ${symbol}`
      );

      // Step 1: Get current market price
      const marketData = await mt5MarketDataService.getMarketData(
        symbol,
        phoneNumber
      );
      if (!marketData) {
        throw new Error("Unable to get market price");
      }

      // Step 2: Place BUY order in MT5
      const mt5TradeData = {
        symbol: symbol,
        volume: parseFloat(volume),
        type: "BUY", // Market BUY
        comment: `BUY-${phoneNumber.slice(-4)}-${Date.now()
          .toString()
          .slice(-6)}`,
        magic: 123456,
      };

      const mt5Result = await mt5Service.placeTrade(mt5TradeData);

      if (!mt5Result.success) {
        throw new Error(
          `MT5 BUY order failed: ${mt5Result.error || "Unknown error"}`
        );
      }

      console.log("MT5 BUY order successful:", mt5Result);

      // Step 3: Store in CRM database
      const crmTradeData = {
        orderNo: mt5Result.ticket.toString(),
        type: "BUY",
        symbol: symbol,
        volume: mt5Result.volume,
        price: mt5Result.price.toString(),
        openingPrice: mt5Result.price.toString(),
        openingDate: new Date(),
        requiredMargin: (mt5Result.price * mt5Result.volume * 0.1).toFixed(2), // 10% margin
        comment: mt5Result.comment,
        mt5Ticket: mt5Result.ticket,
        sl: mt5Result.sl,
        tp: mt5Result.tp,
      };

      const crmResult = await createTrade(
        userSession.adminId || "default_admin",
        userSession.accountId,
        crmTradeData
      );

      console.log(
        "CRM BUY order stored successfully:",
        crmResult.clientOrder._id
      );

      return {
        success: true,
        message:
          `✅ BUY Order Executed Successfully!\n\n` +
          `📊 Symbol: ${symbol}\n` +
          `📈 Type: BUY\n` +
          `⚖️ Volume: ${mt5Result.volume}\n` +
          `💰 Price: ${mt5Result.price}\n` +
          `🎫 Ticket: ${mt5Result.ticket}\n` +
          `💵 Margin Used: ${crmTradeData.requiredMargin}\n\n` +
          `Your position is now OPEN! 🚀`,
        mt5Data: mt5Result,
        crmData: crmResult,
        orderType: "BUY",
      };
    } catch (error) {
      console.error("BUY order error:", error);
      return {
        success: false,
        message: `❌ BUY Order Failed!\n\nError: ${error.message}\n\nPlease try again or contact support.`,
        error: error.message,
      };
    }
  }

  /**
   * Handle SELL order - Place in MT5 then store in CRM
   */
  async handleSellOrder(userSession, orderData) {
    const {
      volume,
      symbol = process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
      phoneNumber,
    } = orderData;

    try {
      console.log(
        `Processing SELL order for ${phoneNumber}: ${volume} ${symbol}`
      );

      // Step 1: Get current market price
      const marketData = await mt5MarketDataService.getMarketData(
        symbol,
        phoneNumber
      );
      if (!marketData) {
        throw new Error("Unable to get market price");
      }

      // Step 2: Place SELL order in MT5
      const mt5TradeData = {
        symbol: symbol,
        volume: parseFloat(volume),
        type: "SELL", // Market SELL
        comment: `SELL-${phoneNumber.slice(-4)}-${Date.now()
          .toString()
          .slice(-6)}`,
        magic: 123456,
      };

      const mt5Result = await mt5Service.placeTrade(mt5TradeData);

      if (!mt5Result.success) {
        throw new Error(
          `MT5 SELL order failed: ${mt5Result.error || "Unknown error"}`
        );
      }

      console.log("MT5 SELL order successful:", mt5Result);

      // Step 3: Store in CRM database
      const crmTradeData = {
        orderNo: mt5Result.ticket.toString(),
        type: "SELL",
        symbol: symbol,
        volume: mt5Result.volume,
        price: mt5Result.price.toString(),
        openingPrice: mt5Result.price.toString(),
        openingDate: new Date(),
        requiredMargin: (mt5Result.price * mt5Result.volume * 0.1).toFixed(2), // 10% margin
        comment: mt5Result.comment,
        mt5Ticket: mt5Result.ticket,
        sl: mt5Result.sl,
        tp: mt5Result.tp,
      };

      const crmResult = await createTrade(
        userSession.adminId || "default_admin",
        userSession.accountId,
        crmTradeData
      );

      console.log(
        "CRM SELL order stored successfully:",
        crmResult.clientOrder._id
      );

      return {
        success: true,
        message:
          `✅ SELL Order Executed Successfully!\n\n` +
          `📊 Symbol: ${symbol}\n` +
          `📉 Type: SELL\n` +
          `⚖️ Volume: ${mt5Result.volume}\n` +
          `💰 Price: ${mt5Result.price}\n` +
          `🎫 Ticket: ${mt5Result.ticket}\n` +
          `💵 Margin Used: ${crmTradeData.requiredMargin}\n\n` +
          `Your position is now OPEN! 📉`,
        mt5Data: mt5Result,
        crmData: crmResult,
        orderType: "SELL",
      };
    } catch (error) {
      console.error("SELL order error:", error);
      return {
        success: false,
        message: `❌ SELL Order Failed!\n\nError: ${error.message}\n\nPlease try again or contact support.`,
        error: error.message,
      };
    }
  }

  /**
   * Handle CLOSE order - Close in MT5 then update CRM
   */
  async handleCloseOrder(userSession, orderData) {
    const { ticket, volume, phoneNumber } = orderData;

    try {
      console.log(
        `Processing CLOSE order for ${phoneNumber}: Ticket ${ticket}`
      );

      // Step 1: Get current market price for profit calculation
      const marketData = await mt5MarketDataService.getMarketData(
        process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
        phoneNumber
      );
      if (!marketData) {
        throw new Error("Unable to get current market price");
      }

      // Step 2: Close position in MT5
      const mt5CloseResult = await mt5Service.closeTrade(ticket, volume);

      if (!mt5CloseResult.success) {
        throw new Error(
          `MT5 close failed: ${mt5CloseResult.error || "Unknown error"}`
        );
      }

      console.log("MT5 position closed successfully:", mt5CloseResult);

      // Step 3: Update CRM database - mark as CLOSED
      const crmUpdateData = {
        orderStatus: "CLOSED",
        closingPrice: mt5CloseResult.closePrice.toString(),
        closingDate: new Date(),
        profit: mt5CloseResult.profit.toString(),
        comment: `Position closed via MT5 - Ticket: ${ticket}`,
      };

      // Find the order in CRM by ticket number
      const crmResult = await updateTradeStatus(
        userSession.adminId || "default_admin",
        ticket, // Assuming you store MT5 ticket as order ID in CRM
        crmUpdateData
      );

      console.log("CRM order updated as CLOSED:", ticket);

      const profitEmoji = mt5CloseResult.profit >= 0 ? "📈" : "📉";
      const profitText = mt5CloseResult.profit >= 0 ? "PROFIT" : "LOSS";

      return {
        success: true,
        message:
          `✅ Position Closed Successfully!\n\n` +
          `🎫 Ticket: ${ticket}\n` +
          `💰 Closing Price: ${mt5CloseResult.closePrice}\n` +
          `${profitEmoji} ${profitText}: ${Math.abs(
            mt5CloseResult.profit
          ).toFixed(2)}\n` +
          `📅 Closed: ${new Date().toLocaleString()}\n\n` +
          `Position has been closed in both MT5 and your account! ✨`,
        mt5Data: mt5CloseResult,
        crmData: crmResult,
        orderType: "CLOSE",
        profit: mt5CloseResult.profit,
      };
    } catch (error) {
      console.error("CLOSE order error:", error);
      return {
        success: false,
        message: `❌ Close Order Failed!\n\nTicket: ${ticket}\nError: ${error.message}\n\nPlease try again or contact support.`,
        error: error.message,
      };
    }
  }

  /**
   * Process order based on type (BUY/SELL/CLOSE)
   */
  async processOrder(userSession, orderData) {
    const { orderType, phoneNumber } = orderData;

    // Validate required data
    if (!phoneNumber) {
      return {
        success: false,
        message: "❌ Phone number is required for order processing",
      };
    }

    if (!userSession.accountId) {
      return {
        success: false,
        message: "❌ User account not found. Please contact support.",
      };
    }

    try {
      switch (orderType.toUpperCase()) {
        case "BUY":
          return await this.handleBuyOrder(userSession, orderData);

        case "SELL":
          return await this.handleSellOrder(userSession, orderData);

        case "CLOSE":
          return await this.handleCloseOrder(userSession, orderData);

        default:
          return {
            success: false,
            message: `❌ Invalid order type: ${orderType}\n\nSupported types: BUY, SELL, CLOSE`,
          };
      }
    } catch (error) {
      console.error("Order processing error:", error);
      return {
        success: false,
        message: `❌ Order processing failed: ${error.message}`,
        error: error.message,
      };
    }
  }

  /**
   * Get user's open positions from both MT5 and CRM
   */
  async getUserPositions(userSession, phoneNumber) {
    try {
      // Get positions from MT5
      const mt5Positions = await mt5MarketDataService.getOpenPositions(
        phoneNumber,
        phoneNumber
      );

      return {
        success: true,
        positions: mt5Positions,
        count: mt5Positions.length,
      };
    } catch (error) {
      console.error("Error fetching positions:", error);
      return {
        success: false,
        message: "❌ Unable to fetch positions",
        error: error.message,
      };
    }
  }

  /**
   * Format positions for WhatsApp display
   */
  formatPositionsMessage(positions) {
    if (!positions || positions.length === 0) {
      return "📊 *Your Open Positions*\n\n❌ No open positions found.\n\nReady to start trading? 🚀";
    }

    let message = `📊 *Your Open Positions* (${positions.length})\n\n`;

    positions.forEach((pos, index) => {
      const profitEmoji = pos.profit >= 0 ? "📈" : "📉";
      message += `${index + 1}. 🎫 *${pos.orderId}*\n`;
      message += `   📊 ${pos.symbol} | ${pos.type}\n`;
      message += `   ⚖️ Volume: ${pos.volume}\n`;
      message += `   💰 Open: ${pos.openPrice}\n`;
      message += `   💰 Current: ${pos.currentPrice}\n`;
      message += `   ${profitEmoji} P&L: ${pos.profit.toFixed(2)}\n\n`;
    });

    message += "💡 *Tip:* Type ticket number to close position";

    return message;
  }
}

// Export singleton instance
export const orderHandler = new OrderHandler();

/**
 * Integration function for WhatsApp webhook
 * Add this to your existing processUserInputMT5 function
 */
export const integrateWithWhatsApp = async (body, userSession, phoneNumber) => {
  const trimmedBody = body.trim().toLowerCase();

  // Parse order commands
  if (trimmedBody.startsWith("buy ")) {
    const volume = parseFloat(trimmedBody.split(" ")[1]);
    if (isNaN(volume) || volume <= 0) {
      return "❌ Invalid volume. Example: 'BUY 0.1'";
    }

    const result = await orderHandler.processOrder(userSession, {
      orderType: "BUY",
      volume: volume,
      phoneNumber: phoneNumber,
      symbol: process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
    });

    return result.message;
  }

  if (trimmedBody.startsWith("sell ")) {
    const volume = parseFloat(trimmedBody.split(" ")[1]);
    if (isNaN(volume) || volume <= 0) {
      return "❌ Invalid volume. Example: 'SELL 0.1'";
    }

    const result = await orderHandler.processOrder(userSession, {
      orderType: "SELL",
      volume: volume,
      phoneNumber: phoneNumber,
      symbol: process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
    });

    return result.message;
  }

  if (trimmedBody.startsWith("close ")) {
    const ticket = trimmedBody.split(" ")[1];
    if (!ticket) {
      return "❌ Invalid ticket. Example: 'CLOSE 123456'";
    }

    const result = await orderHandler.processOrder(userSession, {
      orderType: "CLOSE",
      ticket: ticket,
      phoneNumber: phoneNumber,
    });

    return result.message;
  }

  // Show positions
  if (trimmedBody === "positions" || trimmedBody === "pos") {
    const result = await orderHandler.getUserPositions(
      userSession,
      phoneNumber
    );
    if (result.success) {
      return orderHandler.formatPositionsMessage(result.positions);
    } else {
      return result.message;
    }
  }

  return null; // No order command found
};
