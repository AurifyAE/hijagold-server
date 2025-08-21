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
const SYMBOL_MAPPING = { GOLD: "XAUUSD.#" };
const UNAUTHORIZED_MESSAGE = `🚫 *Access Denied*\n\nYour number is not registered.\nContact support:\n📞 *Ajmal TK* – Aurify Technologies\n📱 +971 58 502 3411\n\nWe're here to help! 💬`;
const ERROR_MESSAGE =
  "❌ An error occurred. Please try again or contact support.";
const MINIMUM_BALANCE_PERCENTAGE = 2;
const TROY_OUNCE_GRAMS = 31.103;
const TTB_FACTOR = 116.64;

// Enhanced deduplication with processing state tracking
const messageProcessingState = new Map();
const MESSAGE_CACHE_TTL = 300000; // 5 minutes
const PROCESSING_TIMEOUT = 30000; // 30 seconds max processing time

// Processing states
const PROCESSING_STATES = {
  PENDING: "PENDING",
  PROCESSING: "PROCESSING",
  COMPLETED: "COMPLETED",
  FAILED: "FAILED",
};

// Helper to generate unique entry ID
const generateEntryId = (prefix) => {
  const timestamp = Date.now().toString();
  const randomStr = Math.random().toString(36).substring(2, 5).toUpperCase();
  return `${prefix}-${timestamp.substring(timestamp.length - 5)}-${randomStr}`;
};

// Enhanced deduplication check
const isDuplicateMessage = (messageSid, from, body) => {
  const primaryKey = messageSid;
  const fallbackKey = `${from}:${body}:${Math.floor(Date.now() / 1000)}`; // 1-second window

  const existingState =
    messageProcessingState.get(primaryKey) ||
    messageProcessingState.get(fallbackKey);

  if (existingState) {
    const timeDiff = Date.now() - existingState.timestamp;

    // If still processing or recently completed, it's a duplicate
    if (
      existingState.state === PROCESSING_STATES.PROCESSING ||
      (existingState.state === PROCESSING_STATES.COMPLETED && timeDiff < 5000)
    ) {
      return true;
    }

    // Clean up old failed attempts
    if (
      existingState.state === PROCESSING_STATES.FAILED &&
      timeDiff > PROCESSING_TIMEOUT
    ) {
      messageProcessingState.delete(primaryKey);
      messageProcessingState.delete(fallbackKey);
    }
  }

  return false;
};

// Mark message as processing
const markMessageProcessing = (messageSid, from, body) => {
  const primaryKey = messageSid;
  const fallbackKey = `${from}:${body}:${Math.floor(Date.now() / 1000)}`;

  const processingData = {
    state: PROCESSING_STATES.PROCESSING,
    timestamp: Date.now(),
  };

  messageProcessingState.set(primaryKey, processingData);
  messageProcessingState.set(fallbackKey, processingData);

  // Auto-cleanup after timeout
  setTimeout(() => {
    const current = messageProcessingState.get(primaryKey);
    if (current && current.state === PROCESSING_STATES.PROCESSING) {
      messageProcessingState.set(primaryKey, {
        ...current,
        state: PROCESSING_STATES.FAILED,
      });
    }
  }, PROCESSING_TIMEOUT);

  return { primaryKey, fallbackKey };
};

// Mark message as completed/failed
const markMessageComplete = (keys, success = true) => {
  const state = success
    ? PROCESSING_STATES.COMPLETED
    : PROCESSING_STATES.FAILED;
  const timestamp = Date.now();

  keys.forEach((key) => {
    messageProcessingState.set(key, { state, timestamp });
  });

  // Cleanup after TTL
  setTimeout(() => {
    keys.forEach((key) => messageProcessingState.delete(key));
  }, MESSAGE_CACHE_TTL);
};

// Enhanced transaction wrapper for safe operations
const executeInTransaction = async (operation, maxRetries = 3) => {
  let attempt = 0;
  let lastError;

  while (attempt < maxRetries) {
    const mongoSession = await mongoose.startSession();
    let transactionStarted = false;
    let transactionCommitted = false;

    try {
      mongoSession.startTransaction();
      transactionStarted = true;

      const result = await operation(mongoSession);

      await mongoSession.commitTransaction();
      transactionCommitted = true;

      return { success: true, result };
    } catch (error) {
      lastError = error;

      // Only abort if transaction was started but not committed
      if (transactionStarted && !transactionCommitted) {
        try {
          await mongoSession.abortTransaction();
        } catch (abortError) {
          console.error(
            `Failed to abort transaction (attempt ${attempt + 1}): ${
              abortError.message
            }`
          );
        }
      }

      console.error(
        `Transaction failed (attempt ${attempt + 1}/${maxRetries}): ${
          error.message
        }`
      );

      // Don't retry on certain errors
      if (
        error.message.includes("already closed") ||
        error.message.includes("not found") ||
        error.message.includes("Account not found")
      ) {
        break;
      }

      attempt++;

      // Wait before retry
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
      }
    } finally {
      try {
        await mongoSession.endSession();
      } catch (endError) {
        console.error(`Failed to end session: ${endError.message}`);
      }
    }
  }

  return { success: false, error: lastError };
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

// Calculate trade cost
const calculateTradeCost = (price, volume) => {
  const volumeValue = parseFloat(volume) || 0;
  const tradeValue = (price / TROY_OUNCE_GRAMS) * TTB_FACTOR * volumeValue;
  return tradeValue;
};

// Check sufficient balance
const checkSufficientBalance = async (price, volumeInput, phoneNumber) => {
  try {
    const { userId } = await getUserIdFromPhoneNumber(phoneNumber);
    if (!userId) {
      return { isSufficient: false, errorMessage: "User account not found" };
    }

    const account = await Account.findById(userId).lean();
    if (!account || account.reservedAmount === undefined) {
      return {
        isSufficient: false,
        errorMessage: "User account information not available",
      };
    }

    const volume = parseFloat(volumeInput) || 0;
    if (volume <= 0) {
      return {
        isSufficient: false,
        errorMessage: "Volume must be at least 0.1",
      };
    }

    const availableBalance = parseFloat(account.reservedAmount) || 0;
    const tradeCost = calculateTradeCost(price, volume);
    const marginRequirement = tradeCost * (MINIMUM_BALANCE_PERCENTAGE / 100);

    if (marginRequirement > availableBalance) {
      return {
        isSufficient: false,
        errorMessage: `Insufficient available balance for this trade.\nRequired Margin: $${marginRequirement.toFixed(
          2
        )}\nAvailable Balance: $${availableBalance.toFixed(2)}`,
      };
    }

    return { isSufficient: true, errorMessage: null };
  } catch (error) {
    console.error(`Balance check error: ${error.message}`);
    return {
      isSufficient: false,
      errorMessage: "Error checking account balance. Please try again.",
    };
  }
};

// Centralized message sending with retry logic
const sendMessage = async (to, message, retries = 2) => {
  let lastError;

  for (let attempt = 0; attempt <= retries; attempt++) {
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

      console.log(
        `Message sent to ${to}, SID: ${twilioMessage.sid}, Attempt: ${
          attempt + 1
        }`
      );
      return { success: true, sid: twilioMessage.sid };
    } catch (error) {
      lastError = error;
      console.error(
        `Twilio error to ${to} (Attempt ${attempt + 1}): ${
          error.message
        }, code: ${error.code}`
      );

      // Don't retry on certain errors
      if (error.code === 63016 || error.code === 21211) break;

      // Wait before retry
      if (attempt < retries) {
        await new Promise((resolve) =>
          setTimeout(resolve, 1000 * (attempt + 1))
        );
      }
    }
  }

  // Log error to database
  try {
    const { userId, adminId } = (await getUserIdFromPhoneNumber(to)) || {};
    if (userId && adminId) {
      await Order.updateOne(
        { user: userId, adminId },
        { $set: { notificationError: `Twilio error: ${lastError.message}` } }
      );
    }
  } catch (dbError) {
    console.error(`Failed to log Twilio error to DB: ${dbError.message}`);
  }

  return { success: false, error: lastError };
};

// Main webhook handler with improved error handling and deduplication
export const handleWhatsAppWebhook = async (req, res) => {
  const { Body, From, ProfileName, MessageSid } = req.body;

  // Validate required parameters
  if (!Body || !From || !MessageSid) {
    console.log("Missing parameters:", req.body);
    return res.status(400).send("Missing required parameters");
  }

  // Enhanced duplicate check
  if (isDuplicateMessage(MessageSid, From, Body)) {
    console.log(`Duplicate request detected: ${MessageSid} from ${From}`);
    return res.status(200).send(new MessagingResponse().toString());
  }

  // Mark message as processing
  const processingKeys = markMessageProcessing(MessageSid, From, Body);

  console.log(`Processing message from ${From}: ${Body}, SID: ${MessageSid}`);

  // Send immediate 200 response to Twilio
  res.status(200).send(new MessagingResponse().toString());

  let success = false;
  try {
    // Refresh market data with caching
    await refreshMarketData(From);

    // Check authorization
    const authResult = await isAuthorizedUser(From);
    if (!authResult.isAuthorized) {
      await sendMessage(From, UNAUTHORIZED_MESSAGE);
      success = true;
      return;
    }

    // Initialize user session
    const userSession = initializeUserSession(
      From,
      authResult.accountId,
      ProfileName
    );

    // Process message and get response
    const responseMessage = await processMessage(Body, userSession, From);

    // Send response only once
    if (responseMessage) {
      const sendResult = await sendMessage(From, responseMessage);
      success = sendResult.success;
    } else {
      success = true; // No message to send is also success
    }
  } catch (error) {
    console.error(`Webhook error for ${MessageSid}: ${error.message}`);
    await sendMessage(From, ERROR_MESSAGE);
    success = false;
  } finally {
    // Mark message as completed or failed
    markMessageComplete(
      [processingKeys.primaryKey, processingKeys.fallbackKey],
      success
    );
  }
};

// Refresh market data with caching
const marketDataCache = new Map();
const MARKET_DATA_TTL = 30000; // 30 seconds

const refreshMarketData = async (clientId) => {
  const now = Date.now();
  const cached = marketDataCache.get(clientId);
  if (cached && now - cached.timestamp < MARKET_DATA_TTL) return;

  try {
    await mt5MarketDataService.getMarketData("XAUUSD.#", clientId);
    marketDataCache.set(clientId, { timestamp: now });
  } catch (error) {
    console.error(`Market data refresh error: ${error.message}`);
  }
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

// Process incoming message - returns message to send (no automatic sending)
const processMessage = async (body, userSession, from) => {
  const trimmedBody = body.trim().toLowerCase();

  // Handle special commands first
  const specialCommandResponse = await handleSpecialCommands(
    trimmedBody,
    userSession,
    from
  );
  if (specialCommandResponse !== null) return specialCommandResponse;

  try {
    // Use existing processUserInputMT5 but ensure it doesn't send messages
    return await processUserInputMT5(body, userSession, null, from, null, from);
  } catch (error) {
    console.error(`Input processing error: ${error.message}`);
    userSession.state = "MAIN_MENU";
    return `${ERROR_MESSAGE}\n\n${await getMainMenuMT5()}`;
  }
};

// Handle special commands - returns response message
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
  const wasConfirming = userSession.state === "CONFIRM_ORDER";
  userSession.state = "MAIN_MENU";
  userSession.pendingOrder = null;

  return `❌ ${
    wasConfirming ? "Order cancelled" : "No active order to cancel"
  }.\n\n${await getMainMenuMT5()}`;
};

// Get current gold price
const getCurrentGoldPrice = async () => {
  try {
    const marketData = await mt5MarketDataService.getMarketData(
      "XAUUSD.#"
    );
    return marketData?.bid || 0;
  } catch (error) {
    console.error(`Gold price error: ${error.message}`);
    return 0;
  }
};

// Health check endpoint
export const healthCheck = (req, res) => {
  res.status(200).json({
    status: "healthy",
    timestamp: new Date().toISOString(),
    service: "WhatsApp Webhook Handler",
    processingMessages: messageProcessingState.size,
  });
};

// Updated handlers that return messages instead of sending them directly

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
  if (isNaN(volume) || volume <= 0) {
    return "❌ Invalid volume. Enter a number (e.g., 0.01) or MENU to cancel.";
  }

  try {
    const marketData = await mt5MarketDataService.getMarketData(
      "XAUUSD.#"
    );
    if (!marketData) {
      session.state = "MAIN_MENU";
      return `⚠️ No market data. Try again.\n\n${await getMainMenuMT5()}`;
    }

    const price =
      session.pendingOrder.type === "BUY" ? marketData.ask : marketData.bid;

    // Check sufficient balance
    const balanceCheck = await checkSufficientBalance(
      price,
      volume,
      phoneNumber
    );
    if (!balanceCheck.isSufficient) {
      session.state = "MAIN_MENU";
      session.pendingOrder = null;
      return `${balanceCheck.errorMessage}\n\n${await getMainMenuMT5()}`;
    }

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

  if (input.toLowerCase() !== "yes") {
    return `ℹ️ Type YES to confirm or NO/MENU to cancel.`;
  }

  const mongoSession = await mongoose.startSession();
  let transactionStarted = false;
  let transactionCommitted = false;

  try {
    mongoSession.startTransaction();
    transactionStarted = true;

    const { userId, adminId, error } = await getUserIdFromPhoneNumber(
      phoneNumber
    );
    if (!userId || !adminId) {
      throw new Error(error || "User account or admin not found");
    }

    const account = await Account.findById(userId).session(mongoSession).lean();
    if (!account) {
      throw new Error("User account not found");
    }

    const marketData = await mt5MarketDataService.getMarketData(
      "XAUUSD.#"
    );
    if (!marketData || !marketData.ask || !marketData.bid) {
      throw new Error("Failed to fetch live market data");
    }

    const adjustedAskPrice = (
      parseFloat(marketData.ask) + (parseFloat(account.askSpread) || 0)
    ).toFixed(2);
    const adjustedBidPrice = (
      parseFloat(marketData.bid) - (parseFloat(account.bidSpread) || 0)
    ).toFixed(2);

    const price =
      session.pendingOrder.type === "BUY" ? adjustedAskPrice : adjustedBidPrice;
    const volume = parseFloat(session.pendingOrder.volume);

    const tradeCost = calculateTradeCost(price, volume);
    const requiredMargin = tradeCost * (MINIMUM_BALANCE_PERCENTAGE / 100);

    const balanceCheck = await checkSufficientBalance(
      price,
      volume,
      phoneNumber
    );
    if (!balanceCheck.isSufficient) {
      throw new Error(balanceCheck.errorMessage);
    }

    const orderNo = generateEntryId("OR");

    const tradeData = {
      symbol: session.pendingOrder.symbol,
      volume: volume,
      type: session.pendingOrder.type,
      slDistance: null,
      tpDistance: null,
      comment: `Ord-${orderNo}`,
      magic: 123456,
    };

    const result = await mt5Service.placeTrade({
      ...tradeData,
      symbol: SYMBOL_MAPPING[session.pendingOrder.symbol],
    });

    if (!result.success) {
      throw new Error(result.error || "MT5 trade failed");
    }

    const crmTradeData = {
      orderNo,
      type: result.type,
      volume: result.volume,
      ticket: result.ticket,
      symbol: session.pendingOrder.symbol,
      price: parseFloat(price),
      openingDate: new Date(),
      requiredMargin: requiredMargin,
      comment: tradeData.comment,
    };

    await createTrade(adminId, userId, crmTradeData, mongoSession);

    await mongoSession.commitTransaction();
    transactionCommitted = true;

    console.log(
      `Trade successfully created and committed for ticket: ${result.ticket}`
    );

    // Construct response message *before* resetting session
    const responseMessage = `✅ Order Placed!\n📋 Type: ${
      result.type
    }\n💰 Volume: ${result.volume} grams\n💵 Price: ${parseFloat(price).toFixed(
      2
    )}\n📊 Order ID: ${result.ticket}\n📡 Symbol: ${
      session.pendingOrder.symbol
    }\n🕒 ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}\n\n${await getMainMenuMT5()}`;

    // Reset session state *after* constructing response
    session.state = "MAIN_MENU";
    session.pendingOrder = null;

    return responseMessage;
  } catch (error) {
    if (transactionStarted && !transactionCommitted) {
      try {
        await mongoSession.abortTransaction();
        console.log("Transaction aborted successfully");
      } catch (abortError) {
        console.error(`Failed to abort transaction: ${abortError.message}`);
      }
    }

    console.error(`Order placement error: ${error.message}`);

    // Reset session state
    session.state = "MAIN_MENU";
    session.pendingOrder = null;

    return `❌ Error placing order: ${
      error.message
    }\n\n${await getMainMenuMT5()}`;
  } finally {
    try {
      await mongoSession.endSession();
    } catch (endError) {
      console.error(`Failed to end session: ${endError.message}`);
    }
  }
};

export const handlePositionSelectionMT5 = async (
  input,
  session,
  phoneNumber
) => {
  console.log(
    `handlePositionSelectionMT5: ${input}, ${session.state}, ${phoneNumber}`
  );

  if (input.toLowerCase() === "menu") {
    session.state = "MAIN_MENU";
    session.openPositions = null;
    return await getMainMenuMT5();
  }

  const positionIndex = parseInt(input) - 1;
  if (
    !session.openPositions ||
    positionIndex < 0 ||
    positionIndex >= session.openPositions.length
  ) {
    return "❌ Invalid position number. Select a valid number or type MENU.";
  }

  const selectedPosition = session.openPositions[positionIndex];
  console.log("Selected position:", JSON.stringify(selectedPosition, null, 2));

  if (
    !selectedPosition.volume ||
    isNaN(selectedPosition.volume) ||
    selectedPosition.volume <= 0
  ) {
    console.error(
      `Invalid volume for ticket ${selectedPosition.ticket}: ${selectedPosition.volume}`
    );
    return "❌ Invalid volume for the selected position.";
  }

  const mongoSession = await mongoose.startSession();
  let transactionStarted = false;
  let transactionCommitted = false;

  try {
    mongoSession.startTransaction();
    transactionStarted = true;

    const { userId, adminId, error } = await getUserIdFromPhoneNumber(
      phoneNumber
    );
    if (!userId || !adminId) {
      throw new Error(error || "User account or admin not found");
    }

    const order = await Order.findOne({
      ticket: selectedPosition.ticket,
      adminId,
    })
      .session(mongoSession)
      .lean();

    console.log("Order found:", JSON.stringify(order, null, 2));

    if (!order) {
      throw new Error(
        `CRM order not found for ticket: ${selectedPosition.ticket}`
      );
    }

    if (order.orderStatus === "CLOSED") {
      throw new Error(`Order ${selectedPosition.ticket} is already closed`);
    }

    // Update trade status (includes MT5 closure)
    const updateData = { orderStatus: "CLOSED" };
    const updatedOrder = await updateTradeStatus(
      adminId,
      order._id.toString(),
      updateData,
      mongoSession
    );

    console.log(`Updated order: ${JSON.stringify(updatedOrder, null, 2)}`);

    // Commit the transaction
    await mongoSession.commitTransaction();
    transactionCommitted = true;

    console.log(
      `Position successfully closed and committed for ticket: ${selectedPosition.ticket}`
    );

    // Reset session state
    session.state = "MAIN_MENU";
    session.openPositions = null;

    return `✅ Position Closed Successfully!\n📊 Ticket: ${
      selectedPosition.ticket
    }\n💰 Close Price: ${updatedOrder.order.closingPrice.toFixed(
      2
    )}\n📈 P&L: ${updatedOrder.order.profit.toFixed(
      2
    )}\n🕒 ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}\n\n${await getMainMenuMT5()}`;
  } catch (error) {
    // Only abort if transaction was started but not committed
    if (transactionStarted && !transactionCommitted) {
      try {
        await mongoSession.abortTransaction();
        console.log("Transaction aborted successfully");
      } catch (abortError) {
        console.error(`Failed to abort transaction: ${abortError.message}`);
      }
    }

    console.error(
      `Position close error for ticket ${
        selectedPosition?.ticket || "unknown"
      }: ${error.message}`
    );

    // Reset session state
    session.state = "MAIN_MENU";
    session.openPositions = null;

    let errorMessage = `❌ Error Closing Position\n📊 Ticket: ${
      selectedPosition?.ticket || "unknown"
    }\n📝 Error: ${error.message}\n\n${await getMainMenuMT5()}`;

    if (error.message.includes("Position not found")) {
      errorMessage = `❌ Position ${
        selectedPosition?.ticket || "unknown"
      } not found in MT5. It may already be closed.\n\n${await getMainMenuMT5()}`;
    }

    return errorMessage;
  } finally {
    try {
      await mongoSession.endSession();
    } catch (endError) {
      console.error(`Failed to end session: ${endError.message}`);
    }
  }
};
