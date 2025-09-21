import * as tradingServices from "../../services/admin/tradingServices.js";
import { getMainMenuMT5 } from "../../services/market/messageService.js";
import Account from "../../models/AccountSchema.js";
import pkg from "twilio";
const { Twilio } = pkg;
import mongoose from "mongoose";
import dotenv from "dotenv";
import Order from "../../models/OrderSchema.js";

dotenv.config();

// Initialize Twilio client
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
let twilioPhoneNumber = process.env.TWILIO_WHATSAPP_NUMBER;
const client = new Twilio(accountSid, authToken);

// Ensure twilioPhoneNumber is WhatsApp-formatted
if (!twilioPhoneNumber.startsWith("whatsapp:")) {
  twilioPhoneNumber = `whatsapp:+${twilioPhoneNumber.replace(
    /^(whatsapp:)?[\+\s\-()]/g,
    ""
  )}`;
}

// Symbol mapping for CRM to MT5
const SYMBOL_MAPPING = {
  TTBAR: process.env.MT5_SYMBOL || "XAUUSD.gm",
  KGBAR: process.env.MT5_SYMBOL || "XAUUSD.gm",
};

// Validate trade data
const validateTradeData = (tradeData) => {
  const { userId, orderNo, type, volume, symbol, price, requiredMargin } =
    tradeData;
  const errors = [];

  if (!userId || !mongoose.Types.ObjectId.isValid(userId)) {
    errors.push("Invalid or missing userId");
  }
  if (!orderNo || typeof orderNo !== "string") {
    errors.push("Invalid or missing orderNo");
  }
  if (!["BUY", "SELL"].includes(type?.toUpperCase())) {
    errors.push("Invalid type: must be BUY or SELL");
  }
  if (isNaN(parseFloat(volume)) || parseFloat(volume) <= 0) {
    errors.push("Invalid volume: must be a positive number");
  }
  if (!symbol || !SYMBOL_MAPPING[symbol]) {
    errors.push(
      `Invalid symbol: ${symbol}. Supported: ${Object.keys(SYMBOL_MAPPING).join(
        ", "
      )}`
    );
  }
  if (isNaN(parseFloat(price)) || parseFloat(price) <= 0) {
    errors.push("Invalid price: must be a positive number");
  }
  if (isNaN(parseFloat(requiredMargin)) || parseFloat(requiredMargin) <= 0) {
    errors.push("Invalid requiredMargin: must be a positive number");
  }

  return errors.length ? errors.join("; ") : null;
};

export const createTrade = async (req, res, next) => {
  let phoneNumber;

  try {
    const { adminId } = req.params;
    const {
      userId,
      orderNo,
      type,
      volume,
      symbol,
      price,
      openingPrice,
      requiredMargin,
      comment,
      stopLoss,
      takeProfit
    } = req.body;

    console.log(`Received trade request: ${JSON.stringify(req.body, null, 2)}`);
    // Validate trade data
    const validationError = validateTradeData(req.body);
    if (validationError) {
      throw new Error(`Validation failed: ${validationError}`);
    }

    // console.log(`Received trade request: ${JSON.stringify(req.body, null, 2)}`);

    // Fetch user's phone number for WhatsApp messaging
    const account = await Account.findOne({ _id: userId });
    if (!account || !account.phoneNumber) {
      throw new Error("User account or phone number not found");
    }
    phoneNumber = account.phoneNumber;
    if (!phoneNumber.startsWith("whatsapp:")) {
      phoneNumber = `whatsapp:+${phoneNumber.replace(
        /^(whatsapp:)?[\+\s\-()]/g,
        ""
      )}`;
    }
    // console.log(`User phone number: ${phoneNumber}`);

    // Pass trade data to service layer
    const tradeResult = await tradingServices.createTrade(adminId, userId, {
      orderNo,
      type: type.toUpperCase(),
      volume: parseFloat(volume),
      symbol,
      price: parseFloat(price),
      openingPrice: parseFloat(openingPrice),
      requiredMargin: parseFloat(requiredMargin),
      comment: comment || `Ord-${orderNo}-${phoneNumber.slice(-4)}`,
      openingDate: new Date(),
      stopLoss: stopLoss ? parseFloat(stopLoss) : 0,
      takeProfit: takeProfit ? parseFloat(takeProfit) : 0,
    });

    // Send WhatsApp confirmation message
    const confirmationMessage = `✅ Order Placed!\n📋 Type: ${
      tradeResult.mt5Trade.type
    }\n💰 Volume: ${tradeResult.mt5Trade.volume} TTBAR\n💵 Price: $${tradeResult.clientOrder.openingPrice}\n📊 Order ID: ${tradeResult.mt5Trade.ticket}\n📡 Symbol: ${
      tradeResult.mt5Trade.symbol
    }\n🕒 ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}\n\n${await getMainMenuMT5()}`;

    try {
      await client.messages.create({
        body: confirmationMessage,
        from: twilioPhoneNumber,
        to: phoneNumber,
      });
      console.log(`WhatsApp confirmation sent to ${phoneNumber}`);
    } catch (whatsappError) {
      console.error(
        `Failed to send WhatsApp message: ${whatsappError.message}, Code: ${
          whatsappError.code
        }, Details: ${JSON.stringify(whatsappError)}`
      );
      await tradingServices.updateTradeStatus(
        adminId,
        tradeResult.clientOrder._id.toString(),
        {
          notificationError: `WhatsApp notification failed: ${whatsappError.message} (Code: ${whatsappError.code})`,
        }
      );
    }

    res.status(201).json({
      status: 201,
      success: true,
      message: "Trade created and placed successfully",
      data: {
        crmTrade: tradeResult.clientOrder,
        mt5Trade: tradeResult.mt5Trade,
      },
    });
  } catch (error) {
    console.error(`Error creating trade: ${error.message}`);

    // Send WhatsApp error message
    const errorMessage = `❌ Order Failed!\n\nError: ${
      error.message
    }\n\n${await getMainMenuMT5()}`;
    try {
      await client.messages.create({
        body: errorMessage,
        from: twilioPhoneNumber,
        to: phoneNumber || "whatsapp:+918138823410", // Fallback phone number
      });
      console.log(
        `WhatsApp error message sent to ${
          phoneNumber || "whatsapp:+918138823410"
        }`
      );
    } catch (whatsappError) {
      console.error(
        `Failed to send WhatsApp error message: ${
          whatsappError.message
        }, Code: ${whatsappError.code}, Details: ${JSON.stringify(
          whatsappError
        )}`
      );
    }

    res.status(500).json({
      status: 500,
      success: false,
      message: `Error creating trade: ${error.message}`,
    });
  }
};

export const updateTrade = async (req, res, next) => {
  let phoneNumber;
  try {
    const { adminId, orderId } = req.params;
    const { orderStatus, profit, closingPrice, forceClose } = req.body;

    // Validate input
    if (!mongoose.Types.ObjectId.isValid(orderId)) {
      throw new Error("Invalid orderId");
    }
    if (!orderStatus || orderStatus !== "CLOSED") {
      throw new Error("Invalid or missing orderStatus: must be CLOSED");
    }

    // Fetch order to get details
    const order = await Order.findOne({ _id: orderId, adminId });
    if (!order) {
      throw new Error(
        `Order not found for orderId: ${orderId}, adminId: ${adminId}`
      );
    }

    // Check if already closed unless force close is requested
    if (order.orderStatus === "CLOSED" && !forceClose) {
      throw new Error(`Order ${orderId} is already closed`);
    }

    // Fetch user's phone number for WhatsApp messaging
    const account = await Account.findOne({ _id: order.user });
    if (!account || !account.phoneNumber) {
      throw new Error("User account or phone number not found");
    }
    phoneNumber = account.phoneNumber;
    if (!phoneNumber.startsWith("whatsapp:")) {
      phoneNumber = `whatsapp:+${phoneNumber.replace(
        /^(whatsapp:)?[\+\s\-()]/g,
        ""
      )}`;
    }
    console.log(`User phone number: ${phoneNumber}`);

    // Prepare update data for trade closure
    const updateData = {
      orderStatus: "CLOSED",
      ticket: order.ticket,
      symbol: order.symbol,
      volume: order.volume,
      type: order.type,
      openingPrice: order.openingPrice,
      profit: profit || 0,
      closingPrice: closingPrice,
      forceClose: forceClose || false,
    };

    console.log('Sending update data:', updateData);

    // Update trade status (includes MT5 closure)
    const updatedTrade = await tradingServices.updateTradeStatus(
      adminId,
      orderId,
      updateData
    );

    // Send WhatsApp confirmation message
    const mt5StatusText = updatedTrade.mt5Status?.closed 
      ? "✅ MT5 Closed" 
      : "⚠️ MT5 Status Unknown";
    
    const successMessage = `✅ Position Closed Successfully!\n📊 Ticket: ${
      order.ticket
    }\n💰 Close Price: $${updatedTrade.order.closingPrice}\n📈 P&L: $ ${updatedTrade.order.profit}\n🔄 ${mt5StatusText}\n🕒 ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}\n\n${await getMainMenuMT5()}`;

    try {
      await client.messages.create({
        body: successMessage,
        from: twilioPhoneNumber,
        to: phoneNumber,
      });
      console.log(`WhatsApp confirmation sent to ${phoneNumber}`);
    } catch (whatsappError) {
      console.error(
        `Failed to send WhatsApp message: ${whatsappError.message}, Code: ${
          whatsappError.code
        }, Details: ${JSON.stringify(whatsappError)}`
      );
      await tradingServices.updateTradeStatus(adminId, orderId, {
        notificationError: `WhatsApp notification failed: ${whatsappError.message} (Code: ${whatsappError.code})`,
      });
    }

    res.json({
      status: 200,
      success: true,
      message: "Trade updated successfully",
      data: {
        ...updatedTrade,
        mt5Synchronized: updatedTrade.mt5Status?.closed || false,
      },
    });
  } catch (error) {
    console.error(
      `Error updating trade for orderId ${req.params.orderId}: ${error.message}`
    );

    // Send WhatsApp error message
    const errorMessage = `❌ Error Closing Position\n📊 Order ID: ${
      req.params.orderId
    }\n📝 Error: ${error.message}\n\n${await getMainMenuMT5()}`;
    
    try {
      await client.messages.create({
        body: errorMessage,
        from: twilioPhoneNumber,
        to: phoneNumber || "whatsapp:+918138823410",
      });
      console.log(
        `WhatsApp error message sent to ${
          phoneNumber || "whatsapp:+918138823410"
        }`
      );
    } catch (whatsappError) {
      console.error(
        `Failed to send WhatsApp error message: ${
          whatsappError.message
        }, Code: ${whatsappError.code}, Details: ${JSON.stringify(
          whatsappError
        )}`
      );
    }

    res.status(500).json({
      status: 500,
      success: false,
      message: `Error updating trade: ${error.message}`,
    });
  }
};


// Other controller functions (unchanged)
export const getUserTrades = async (req, res, next) => {
  try {
    const { adminId } = req.params;
    const trades = await tradingServices.getTradesByUser(adminId);

    res.json({
      status: 200,
      success: true,
      message: "User trades retrieved successfully",
      data: trades,
    });
  } catch (error) {
    next(error);
  }
};

export const getLPProfitOrdersByAdmin = async (req, res, next) => {
  try {
    const LPProfitInfo = await tradingServices.getLPProfitOrders();

    res.json({
      status: 200,
      success: true,
      message: "fetch LPProfit successfully",
      data: LPProfitInfo,
    });
  } catch (error) {
    next(error);
  }
};
export const getUserOrdersByAdmin = async (req, res, next) => {
  try {
    const { adminId, userId } = req.params;
    const orders = await tradingServices.getOrdersByUser(adminId, userId);

    res.json({
      status: 200,
      success: true,
      message: "User orders retrieved successfully",
      data: orders,
    });
  } catch (error) {
    next(error);
  }
};

export const getLPTrades = async (req, res, next) => {
  try {
    const { adminId } = req.params;
    const trades = await tradingServices.getTradesByLP(adminId);

    res.json({
      status: 200,
      success: true,
      message: "LP trades retrieved successfully",
      data: trades,
    });
  } catch (error) {
    next(error);
  }
};

export const getTrade = async (req, res, next) => {
  try {
    const { adminId, tradeId } = req.params;
    const trade = await tradingServices.getTradeById(adminId, tradeId);

    if (!trade) {
      return res.status(404).json({
        status: 404,
        success: false,
        message:
          "Trade not found or you don't have permission to view this trade",
      });
    }

    res.json({
      status: 200,
      success: true,
      message: "Trade retrieved successfully",
      data: trade,
    });
  } catch (error) {
    next(error);
  }
};
