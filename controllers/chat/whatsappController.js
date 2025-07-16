import pkg from "twilio";
const { Twilio, twiml } = pkg;
const { MessagingResponse } = twiml;
import dotenv from "dotenv";
import {
  getUserSession,
  resetSession,
} from "../../services/market/sessionService.js";
import { isAuthorizedUser } from "../../services/market/userService.js";
import {
  getPriceMessageMT5,
  processUserInputMT5,
  getMainMenuMT5,
  getPositionsMessageMT5,
} from "../../services/market/messageService.js";
import { getUserBalance } from "../../services/market/balanceService.js";
import mt5MarketDataService from "../../services/Trading/mt5MarketDataService.js";
import mt5Service from "../../services/Trading/mt5Service.js";
import {
  createTrade,
  updateTradeStatus,
} from "../../services/Trading/tradingServices.js";
import mongoose from "mongoose";
import Account from "../../models/AccountSchema.js";
import Order from "../../models/OrderSchema.js";

// Initialize environment variables
dotenv.config();

// Initialize Twilio client
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilioPhoneNumber = process.env.TWILIO_WHATSAPP_NUMBER;
const client = new Twilio(accountSid, authToken);

// Constants
const SYMBOL_MAPPING = { GOLD: "XAUUSD_TTBAR.Fix" };
const UNAUTHORIZED_MESSAGE = `🚫 *Access Denied*\n\nYour number is not registered.\nContact support:\n📞 *Ajmal TK* – Aurify Technologies\n📱 +971 58 502 3411\n\nWe're here to help! 💬`;
const ERROR_MESSAGE =
  "❌ An error occurred. Please try again or contact support.";

// Deduplication cache
const processedMessages = new Map();
const MESSAGE_CACHE_TTL = 300000; // 5 minutes

// Helper to generate unique entry ID
const generateEntryId = (prefix) => {
  const timestamp = Date.now().toString();
  const randomStr = Math.random().toString(36).substring(2, 5).toUpperCase();
  return `${prefix}-${timestamp.substring(timestamp.length - 5)}-${randomStr}`;
};

// Helper to get userId and adminId from phoneNumber
const getUserIdFromPhoneNumber = async (phoneNumber) => {
  try {
    let cleanPhoneNumber = phoneNumber.replace(/^(whatsapp:)?[\+\s\-()]/g, "");
    const queryVariations = [
      cleanPhoneNumber,
      `+${cleanPhoneNumber}`,
      cleanPhoneNumber.startsWith("91")
        ? cleanPhoneNumber.slice(2)
        : `91${cleanPhoneNumber}`,
      `whatsapp:${cleanPhoneNumber}`,
      `whatsapp:+${cleanPhoneNumber}`,
    ];
    const account = await Account.findOne({
      phoneNumber: { $in: queryVariations },
    }).lean();
    if (!account)
      return { userId: null, adminId: null, error: "Account not found" };
    const userId = account._id.toString();
    const adminId = account.addedBy ? account.addedBy.toString() : null;
    return !adminId
      ? { userId, adminId: null, error: "Admin ID not found" }
      : { userId, adminId, error: null };
  } catch (error) {
    console.error(`Error fetching userId: ${error.message}`);
    return { userId: null, adminId: null, error: error.message };
  }
};

// Webhook handler with async processing
export const handleWhatsAppWebhook = async (req, res) => {
  const { Body, From, ProfileName, MessageSid } = req.body;
  if (!Body || !From || !MessageSid) {
    console.log("Missing parameters:", req.body);
    return res.status(400).send("Missing required parameters");
  }

  // Deduplicate using MessageSid and fallback to composite key
  const dedupKey = MessageSid || `${From}:${Body}:${Date.now()}`;
  if (processedMessages.has(dedupKey)) {
    console.log(`Duplicate request detected: ${dedupKey}`);
    return res.status(200).send(new MessagingResponse().toString());
  }
  processedMessages.set(dedupKey, Date.now());
  setTimeout(() => processedMessages.delete(dedupKey), MESSAGE_CACHE_TTL);

  console.log(`Processing message from ${From}: ${Body}, SID: ${MessageSid}`);
  res.status(200).send(new MessagingResponse().toString()); // Immediate 200 response

  try {
    await refreshMarketData(From);
    const authResult = await isAuthorizedUser(From);
    if (!authResult.isAuthorized) {
      await sendMessage(From, UNAUTHORIZED_MESSAGE);
      return;
    }

    const userSession = initializeUserSession(
      From,
      authResult.accountId,
      ProfileName
    );
    const responseMessage = await processMessage(Body, userSession, From);
    if (responseMessage) await sendMessage(From, responseMessage);
  } catch (error) {
    console.error(`Webhook error: ${error.message}`);
    await sendMessage(From, ERROR_MESSAGE);
  }
};

// Refresh market data with caching
const marketDataCache = new Map();
const MARKET_DATA_TTL = 30000; // 30 seconds
const refreshMarketData = async (clientId) => {
  const now = Date.now();
  const cached = marketDataCache.get(clientId);
  if (cached && now - cached.timestamp < MARKET_DATA_TTL) return;
  await mt5MarketDataService.getMarketData("XAUUSD_TTBAR.Fix", clientId);
  marketDataCache.set(clientId, { timestamp: now });
};

// Initialize user session
const initializeUserSession = (from, accountId, profileName) => {
  const userSession = getUserSession(from);
  userSession.accountId = accountId;
  userSession.phoneNumber = from;
  userSession.tradingMode = "mt5";
  if (profileName && !userSession.userName) userSession.userName = profileName;
  return userSession;
};

// Process incoming message
const processMessage = async (body, userSession, from) => {
  const trimmedBody = body.trim().toLowerCase();
  const specialCommandResponse = await handleSpecialCommands(
    trimmedBody,
    userSession,
    from
  );
  if (specialCommandResponse !== null) return specialCommandResponse;
  try {
    return await processUserInputMT5(
      body,
      userSession,
      client,
      from,
      twilioPhoneNumber,
      from
    );
  } catch (error) {
    console.error(`Input processing error: ${error.message}`);
    userSession.state = "MAIN_MENU";
    return `${ERROR_MESSAGE}\n\n${await getMainMenuMT5()}`;
  }
};

// Handle special commands
const handleSpecialCommands = async (trimmedBody, userSession, from) => {
  const commands = {
    reset: async () => {
      resetSession(from);
      const newSession = getUserSession(from);
      newSession.tradingMode = "mt5";
      return await getMainMenuMT5();
    },
    hi: async () => await handleGreeting(userSession),
    hello: async () => await handleGreeting(userSession),
    start: async () => await handleGreeting(userSession),
    balance: async () => await handleBalanceCommand(userSession),
    5: async () => await handleBalanceCommand(userSession),
    cancel: async () => await handleCancelCommand(userSession),
    price: async () => await getPriceMessageMT5(),
    prices: async () => await getPriceMessageMT5(),
    orders: async () => await getPositionsMessageMT5(userSession, from),
    positions: async () => await getPositionsMessageMT5(userSession, from),
    4: async () => await getPositionsMessageMT5(userSession, from),
    refresh: async () => {
      await refreshMarketData(from);
      return "🔄 Refreshing market data... Type 'PRICE' to check updated prices.";
    },
    menu: async () => {
      userSession.state = "MAIN_MENU";
      return await getMainMenuMT5();
    },
    help: async () => {
      userSession.state = "MAIN_MENU";
      const mainMenu = await getMainMenuMT5();
      return `📖 *Help & Commands*\n\n📊 *Quick Commands:*\n• PRICE - View live prices\n• BALANCE - Check account balance\n• POSITIONS - View open positions\n• MENU - Return to main menu\n• RESET - Reset session\n\n${mainMenu}`;
    },
  };

  const commandHandler = commands[trimmedBody];
  if (commandHandler) {
    try {
      return await commandHandler();
    } catch (error) {
      console.error(
        `Error handling command '${trimmedBody}': ${error.message}`
      );
      return ERROR_MESSAGE;
    }
  }
  return null;
};

// Handle greeting commands
const handleGreeting = async (userSession) => {
  userSession.state = "MAIN_MENU";
  userSession.tradingMode = "mt5";
  return `Hello ${
    userSession.userName || "there"
  }! 👋\n\n${await getMainMenuMT5()}`;
};

// Handle balance command
const handleBalanceCommand = async (userSession) => {
  try {
    const balance = await getUserBalance(
      userSession.accountId,
      userSession.phoneNumber
    );
    const goldPrice = await getCurrentGoldPrice();
    const portfolioValue = balance.cash + balance.gold * goldPrice;
    return `💰 *Your Current Balance*\n\n💵 Cash: ${balance.cash.toFixed(
      2
    )}\n🥇 Gold: ${balance.gold.toFixed(2)} TTB\n📊 Gold Value: ${(
      balance.gold * goldPrice
    ).toFixed(2)}\n💎 Total Portfolio: ${portfolioValue.toFixed(2)}`;
  } catch (error) {
    console.error(`Balance error: ${error.message}`);
    return "❌ Unable to fetch balance. Please try again.";
  }
};

// Handle cancel command
const handleCancelCommand = async (userSession) => {
  userSession.state = "MAIN_MENU";
  userSession.pendingOrder = null;
  return `❌ ${
    userSession.state === "CONFIRM_ORDER"
      ? "Order cancelled"
      : "No active order to cancel"
  }.\n\n${await getMainMenuMT5()}`;
};

// Get current gold price
const getCurrentGoldPrice = async () => {
  try {
    const marketData = await mt5MarketDataService.getMarketData(
      "XAUUSD_TTBAR.Fix"
    );
    return marketData?.bid || 0;
  } catch (error) {
    console.error(`Gold price error: ${error.message}`);
    return 0;
  }
};

// Centralized message sending
const sendMessage = async (to, message) => {
  try {
    const formattedTo = to.startsWith("whatsapp:") ? to : `whatsapp:${to}`;
    const formattedFrom = twilioPhoneNumber.startsWith("whatsapp:")
      ? twilioPhoneNumber
      : `whatsapp:${twilioPhoneNumber}`;
    const twilioMessage = await client.messages.create({
      body: message,
      from: formattedFrom,
      to: formattedTo,
    });
    console.log(`Message sent to ${to}, SID: ${twilioMessage.sid}`);
  } catch (error) {
    console.error(
      `Twilio error to ${to}: ${error.message}, code: ${error.code}`
    );
    const { userId, adminId } = (await getUserIdFromPhoneNumber(to)) || {};
    if (userId && adminId) {
      await Order.updateOne(
        { user: userId, adminId },
        { $set: { notificationError: `Twilio error: ${error.message}` } }
      );
    }
  }
};

// Health check endpoint
export const healthCheck = (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    service: "WhatsApp Webhook Handler",
  });
};

// Handle main menu for MT5
export const handleMainMenuMT5 = async (input, session, phoneNumber) => {
  console.log(`handleMainMenuMT5: ${input}, ${session.state}`);
  switch (input.toLowerCase()) {
    case "1":
    case "buy":
      session.state = "AWAITING_VOLUME";
      session.pendingOrder = { type: "BUY", symbol: "GOLD" };
      return `📈 Buy Gold\nEnter volume in grams (e.g., 0.01):\nType MENU to cancel.`;
    case "2":
    case "sell":
      session.state = "AWAITING_VOLUME";
      session.pendingOrder = { type: "SELL", symbol: "GOLD" };
      return `📉 Sell Gold\nEnter volume in grams (e.g., 0.01):\nType MENU to cancel.`;
    case "3":
    case "price":
    case "prices":
      return await getPriceMessageMT5();
    case "4":
    case "positions":
      return await getPositionsMessageMT5(session, phoneNumber);
    case "5":
    case "close":
      return await getPositionsMessageMT5(session, phoneNumber);
    default:
      return await getMainMenuMT5();
  }
};

// Handle volume input for MT5
export const handleVolumeInputMT5 = async (input, session, phoneNumber) => {
  console.log(
    `handleVolumeInputMT5: ${input}, ${session.state}, ${phoneNumber}`
  );
  if (input.toLowerCase() === "menu") {
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return await getMainMenuMT5();
  }
  const volume = parseFloat(input);
  if (isNaN(volume) || volume <= 0)
    return "❌ Invalid volume. Enter a number (e.g., 0.01) or MENU to cancel.";
  try {
    const marketData = await mt5MarketDataService.getMarketData(
      "XAUUSD_TTBAR.Fix"
    );
    if (!marketData) {
      session.state = "MAIN_MENU";
      return `⚠️ No market data. Try again.\n\n${await getMainMenuMT5()}`;
    }
    const price =
      session.pendingOrder.type === "BUY" ? marketData.ask : marketData.bid;
    const totalCost = volume * price;
    session.pendingOrder.volume = volume;
    session.pendingOrder.price = price;
    session.pendingOrder.totalCost = totalCost;
    session.state = "CONFIRM_ORDER";
    return `📊 Order Confirmation\n📋 Type: ${
      session.pendingOrder.type
    }\n💰 Volume: ${volume} grams\n💵 Price: $${price.toFixed(
      2
    )}\n💸 Total: $${totalCost.toFixed(
      2
    )}\n📡 Symbol: GOLD\n🔧 Type YES to confirm, NO or MENU to cancel.`;
  } catch (error) {
    console.error(`Volume error: ${error.message}`);
    session.state = "MAIN_MENU";
    return `❌ Error processing volume. Try again.\n\n${await getMainMenuMT5()}`;
  }
};

// Handle order confirmation for MT5
export const handleOrderConfirmationMT5 = async (
  input,
  session,
  phoneNumber
) => {
  console.log(
    `handleOrderConfirmationMT5: ${input}, ${session.state}, ${phoneNumber}`
  );
  if (input.toLowerCase() === "menu" || input.toLowerCase() === "no") {
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return `❌ Order cancelled.\n\n${await getMainMenuMT5()}`;
  }
  if (input.toLowerCase() !== "yes")
    return `ℹ️ Type YES to confirm or NO/MENU to cancel.\n\n${await getMainMenuMT5()}`;
  const mongoSession = await mongoose.startSession();
  mongoSession.startTransaction();
  try {
    const { userId, adminId, error } = await getUserIdFromPhoneNumber(
      phoneNumber
    );
    if (!userId || !adminId)
      throw new Error(error || "User account or admin not found");
    const orderNo = generateEntryId("OR");
    const tradeData = {
      symbol: session.pendingOrder.symbol,
      volume: session.pendingOrder.volume,
      type: session.pendingOrder.type,
      slDistance: 10.0,
      tpDistance: 10.0,
      comment: `Ord-${orderNo}`,
      magic: 123456,
    };
    const result = await mt5Service.placeTrade({
      ...tradeData,
      symbol: SYMBOL_MAPPING[session.pendingOrder.symbol],
    });
    if (!result.success) throw new Error(result.error || "MT5 trade failed");
    const crmTradeData = {
      orderNo,
      type: result.type,
      volume: result.volume,
      ticket: result.ticket,
      symbol: session.pendingOrder.symbol,
      price: result.price,
      openingDate: new Date(),
      requiredMargin: (result.volume * result.price).toFixed(2),
      comment: tradeData.comment,
    };
    await createTrade(adminId, userId, crmTradeData, mongoSession);
    await mongoSession.commitTransaction();
    const successMessage = `✅ Order Placed!\n📋 Type: ${
      result.type
    }\n💰 Volume: ${result.volume} grams\n💵 Price: $${result.price.toFixed(
      2
    )}\n📊 Order ID: ${result.ticket}\n📡 Symbol: ${
      session.pendingOrder.symbol
    }\n🕒 ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}\n\n${await getMainMenuMT5()}`;
    await sendMessage(phoneNumber, successMessage); // Ensure message is sent
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return successMessage;
  } catch (error) {
    await mongoSession.abortTransaction();
    console.error(
      `Order error: ${error.message}, Stack: ${error.stack}, Transaction State: ${mongoSession.transaction.state}`
    );
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    const errorMessage = `❌ Error placing order: ${
      error.message
    }\n\n${await getMainMenuMT5()}`;
    await sendMessage(phoneNumber, errorMessage); // Ensure error message is sent
    return errorMessage;
  } finally {
    mongoSession.endSession();
  }
};

// Handle position selection for MT5
export const handlePositionSelectionMT5 = async (input, session, phoneNumber) => {
  console.log(`handlePositionSelectionMT5: ${input}, ${session.state}, ${phoneNumber}`);
  if (input.toLowerCase() === "menu") {
    session.state = "MAIN_MENU";
    session.openPositions = null;
    return await getMainMenuMT5();
  }

  const positionIndex = parseInt(input) - 1;
  if (!session.openPositions || positionIndex < 0 || positionIndex >= session.openPositions.length) {
    return "❌ Invalid position number. Select a valid number or type MENU.";
  }

  const selectedPosition = session.openPositions[positionIndex];
  console.log("Selected position:", JSON.stringify(selectedPosition, null, 2));

  if (!selectedPosition.volume || isNaN(selectedPosition.volume) || selectedPosition.volume <= 0) {
    console.error(`Invalid volume for ticket ${selectedPosition.ticket}: ${selectedPosition.volume}`);
    return "❌ Invalid volume for the selected position.";
  }

  const mongoSession = await mongoose.startSession();
  let committed = false;
  let sessionEnded = false;

  try {
    mongoSession.startTransaction();

    const { userId, adminId, error } = await getUserIdFromPhoneNumber(phoneNumber);
    if (!userId || !adminId) {
      throw new Error(error || "User account or admin not found");
    }

    const order = await Order.findOne({ ticket: selectedPosition.ticket, adminId })
      .session(mongoSession)
      .lean();
    console.log("Order found:", JSON.stringify(order, null, 2));
    if (!order) {
      throw new Error(`CRM order not found for ticket: ${selectedPosition.ticket}`);
    }
    if (order.orderStatus === "CLOSED") {
      throw new Error(`Order ${selectedPosition.ticket} is already closed`);
    }

    // Update trade status (includes MT5 closure)
    const updateData = {
      orderStatus: "CLOSED",
    };
    const updatedOrder = await updateTradeStatus(adminId, order._id.toString(), updateData, mongoSession);
    console.log(`Updated order: ${JSON.stringify(updatedOrder, null, 2)}`);

    await mongoSession.commitTransaction();
    committed = true;

    session.state = "MAIN_MENU";
    session.openPositions = null;

    const successMessage = `✅ Position Closed Successfully!\n📊 Ticket: ${
      selectedPosition.ticket
    }\n💰 Close Price: $${updatedOrder.order.closingPrice.toFixed(2)}\n📈 P&L: $${updatedOrder.order.profit.toFixed(
      2
    )}\n🕒 ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}\n\n${await getMainMenuMT5()}`;
    
    try {
      await sendMessage(phoneNumber, successMessage);
    } catch (sendError) {
      console.error(`Failed to send WhatsApp message for ticket ${selectedPosition.ticket}: ${sendError.message}, Stack: ${sendError.stack}`);
      // Log the error but don't throw to avoid affecting the committed transaction
    }

    return successMessage;
  } catch (error) {
    if (!committed) {
      try {
        await mongoSession.abortTransaction();
      } catch (abortError) {
        console.error(`Failed to abort transaction for ticket ${selectedPosition?.ticket || "unknown"}: ${abortError.message}`);
      }
    }
    console.error(
      `Position close error for ticket ${
        selectedPosition?.ticket || "unknown"
      }: ${error.message}, Stack: ${error.stack}, Transaction State: ${
        mongoSession.transaction?.state || "unknown"
      }`
    );
    session.state = "MAIN_MENU";
    session.openPositions = null;
    
    let errorMessage = `❌ Error Closing Position\n📊 Ticket: ${
      selectedPosition?.ticket || "unknown"
    }\n📝 Error: ${error.message}\n\n${await getMainMenuMT5()}`;
    if (error.message.includes("Position not found")) {
      errorMessage = `❌ Position ${selectedPosition?.ticket || "unknown"} not found in MT5. It may already be closed.\n\n${await getMainMenuMT5()}`;
    }
    
    try {
      await sendMessage(phoneNumber, errorMessage);
    } catch (sendError) {
      console.error(`Failed to send WhatsApp error message for ticket ${selectedPosition?.ticket || "unknown"}: ${sendError.message}, Stack: ${sendError.stack}`);
    }
    
    return errorMessage;
  } finally {
    if (!sessionEnded) {
      try {
        await mongoSession.endSession();
      } catch (endError) {
        console.error(`Failed to end session for ticket ${selectedPosition?.ticket || "unknown"}: ${endError.message}`);
      }
    }
  }
};
