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
const UNAUTHORIZED_MESSAGE = `🚫 *Access Denied*

┌─────────────────────────┐
│    🔐 *UNAUTHORIZED*    │
└─────────────────────────┘

Your number is not registered.

📞 *Contact Support:*
Ajmal TK – Aurify Technologies
📱 +971 58 502 3411

💬 *We're here to help!*`;
const ERROR_MESSAGE = `❌ *ERROR OCCURRED*

┌─────────────────────────┐
│   🔧 *SYSTEM ERROR*     │
└─────────────────────────┘

Something went wrong. Please try again.

🔙 Type *MENU* to return`;
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
  const fallbackKey = `${from}:${body}:${Math.floor(Date.now() / 1000)}`;

  const existingState =
    messageProcessingState.get(primaryKey) ||
    messageProcessingState.get(fallbackKey);

  if (existingState) {
    const timeDiff = Date.now() - existingState.timestamp;
    if (
      existingState.state === PROCESSING_STATES.PROCESSING ||
      (existingState.state === PROCESSING_STATES.COMPLETED && timeDiff < 5000)
    ) {
      return true;
    }
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

  setTimeout(() => {
    keys.forEach((key) => messageProcessingState.delete(key));
  }, MESSAGE_CACHE_TTL);
};

// Utility function to format currency
const formatCurrency = (amount) => {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount || 0);
};

// Time-based greeting function
const getTimeBasedGreeting = () => {
  const hour = new Date().getHours();
  if (hour < 12) return "Good Morning";
  if (hour < 17) return "Good Afternoon";
  if (hour < 21) return "Good Evening";
  return "Good Night";
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
      if (
        error.message.includes("already closed") ||
        error.message.includes("not found") ||
        error.message.includes("Account not found")
      ) {
        break;
      }
      attempt++;
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
      if (error.code === 63016 || error.code === 21211) break;
      if (attempt < retries) {
        await new Promise((resolve) =>
          setTimeout(resolve, 1000 * (attempt + 1))
        );
      }
    }
  }

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

// Enhanced Message Templates
// Welcome Message Template with Balance Display
const createWelcomeMessage = async (
  userName,
  equity,
  availableBalance,
  goldPrice
) => {
  const totalPortfolioValue = equity + availableBalance;
  const greeting = getTimeBasedGreeting();

  return `🌟 *${greeting}, ${userName || "Valued Client"}!* 🌟

┌─────────────────────────┐
│  🏦 *ACCOUNT OVERVIEW*  │
└─────────────────────────┘

💰 *Equity Balance:* $${formatCurrency(equity)}
💵 *Available Balance:* $${formatCurrency(availableBalance)}
📊 *Total Portfolio:* $${formatCurrency(totalPortfolioValue)}

┌─────────────────────────┐
│  📈 *LIVE MARKET DATA*  │
└─────────────────────────┘
🥇 *Gold (XAU/USD):* $${goldPrice?.toFixed(2) || "Loading..."}

━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚀 *Ready to start trading?* 
Choose an option below:

${await getMainMenuMT5()}`;
};

// Enhanced Main Menu Template
const getEnhancedMainMenuMT5 = async () => {
  return `┌─────────────────────────┐
│    📱 *TRADING MENU*    │
└─────────────────────────┘

🟢 *1* │ 📈 *BUY GOLD*
🔴 *2* │ 📉 *SELL GOLD*  
📊 *3* │ 💹 *LIVE PRICES*
📋 *4* │ 🔍 *MY POSITIONS*
💰 *5* │ 💳 *BALANCE*

━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *Quick Commands:*
• Type *PRICE* for instant quotes
• Type *HELP* for assistance
• Type *REFRESH* to update data

💬 *Simply type a number to continue...*`;
};

// Enhanced Balance Display Template
const createBalanceMessage = async (
  equity,
  availableBalance,
  goldPrice,
  goldHolding = 0
) => {
  const goldValue = goldHolding * goldPrice;
  const totalPortfolio = equity + availableBalance + goldValue;
  const profitLoss = equity - availableBalance; // Assuming equity includes unrealized P&L
  const profitColor = profitLoss >= 0 ? "🟢" : "🔴";
  const profitSign = profitLoss >= 0 ? "+" : "";

  return `┌─────────────────────────┐
│   💰 *ACCOUNT BALANCE*  │
└─────────────────────────┘

💎 *Equity:* $${formatCurrency(equity)}
💵 *Available:* $${formatCurrency(availableBalance)}
${goldHolding > 0 ? `🥇 *Gold Holdings:* ${goldHolding.toFixed(2)} oz` : ""}
${goldHolding > 0 ? `📈 *Gold Value:* $${formatCurrency(goldValue)}` : ""}

━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 *Total Portfolio:* $${formatCurrency(totalPortfolio)}
${profitColor} *Unrealized P&L:* ${profitSign}$${Math.abs(profitLoss).toFixed(
    2
  )}

🕒 *Last Updated:* ${new Date().toLocaleString("en-US", {
    timeZone: "Asia/Dubai",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })} UAE

━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 *Ready to trade? Type MENU*`;
};

// Enhanced Price Display Template
const createPriceMessage = async (marketData, spread) => {
  const timestamp = new Date().toLocaleString("en-US", {
    timeZone: "Asia/Dubai",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });

  return `┌─────────────────────────┐
│   📈 *LIVE GOLD PRICES* │
└─────────────────────────┘

🥇 *XAUUSD (Gold/USD)*

🟢 *BID:* $${marketData?.bid?.toFixed(2) || "N/A"}
🔴 *ASK:* $${marketData?.ask?.toFixed(2) || "N/A"}
📊 *SPREAD:* ${spread?.toFixed(1) || "N/A"} pips

━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 *Updated:* ${timestamp} UAE
🔄 *Auto-refresh every 30 seconds*

━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *Quick Actions:*
• Type *1* to BUY at $${marketData?.ask?.toFixed(2) || "N/A"}
• Type *2* to SELL at $${marketData?.bid?.toFixed(2) || "N/A"}
• Type *REFRESH* for latest prices`;
};

// Enhanced Order Confirmation Template
const createOrderConfirmation = (
  orderType,
  volume,
  price,
  totalCost,
  symbol
) => {
  const typeEmoji = orderType === "BUY" ? "📈🟢" : "📉🔴";
  const actionWord = orderType === "BUY" ? "Purchase" : "Sale";

  return `${typeEmoji} *ORDER CONFIRMATION*

┌─────────────────────────┐
│     📋 *ORDER DETAILS*  │
└─────────────────────────┘

🎯 *Action:* ${orderType} ${symbol}
⚖️ *Volume:* ${volume} grams
💰 *Price:* $${price.toFixed(2)}
💸 *Total Cost:* $${totalCost.toFixed(2)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ *Please confirm your ${actionWord.toLowerCase()}:*

✅ Type *YES* to execute order
❌ Type *NO* to cancel
🔙 Type *MENU* to return

⏰ *This quote expires in 30 seconds*
🔐 *Secure trading powered by MT5*`;
};

// Enhanced Order Success Template
const createOrderSuccessMessage = async (
  result,
  orderType,
  volume,
  price,
  symbol,
  ticket
) => {
  const typeEmoji = orderType === "BUY" ? "📈✅" : "📉✅";
  const timestamp = new Date().toLocaleString("en-US", {
    timeZone: "Asia/Dubai",
    dateStyle: "medium",
    timeStyle: "medium",
  });

  return `${typeEmoji} *ORDER EXECUTED!*

┌─────────────────────────┐
│    🎉 *TRADE SUCCESS*   │
└─────────────────────────┘

🎫 *Ticket:* #${ticket}
🎯 *Type:* ${orderType} ${symbol}
⚖️ *Volume:* ${volume} grams
💰 *Price:* $${price.toFixed(2)}
💸 *Total:* $${(volume * price).toFixed(2)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━
🕒 *Executed:* ${timestamp}
🏦 *Status:* ACTIVE
📊 *Platform:* MetaTrader 5

━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 *What's next?*
• Type *4* to view positions
• Type *PRICE* for current rates
• Type *MENU* for main menu`;
};

// Enhanced Positions List Template
const createPositionsMessage = async (positions, totalPL) => {
  if (!positions || positions.length === 0) {
    return `┌─────────────────────────┐
│   📋 *MY POSITIONS*     │
└─────────────────────────┘

📭 *No open positions found*

━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 *Ready to start trading?*
• Type *1* to BUY Gold
• Type *2* to SELL Gold  
• Type *MENU* to return

🚀 *Start your first trade today!*`;
  }

  let positionsText = `┌─────────────────────────┐
│   📋 *MY POSITIONS*     │
└─────────────────────────┘

`;

  positions.forEach((pos, index) => {
    const plColor = pos.profit >= 0 ? "🟢" : "🔴";
    const plSign = pos.profit >= 0 ? "+" : "";

    positionsText += `${pos.type === "BUY" ? "📈" : "📉"} *${index + 1}.* ${
      pos.symbol
    }
🎫 Ticket: #${pos.ticket}
⚖️ Volume: ${pos.volume} grams
💰 Open: $${pos.openPrice?.toFixed(2)}
📊 Current: $${pos.currentPrice?.toFixed(2)}
${plColor} P&L: ${plSign}$${pos.profit?.toFixed(2)}

`;
  });

  const totalColor = totalPL >= 0 ? "🟢" : "🔴";
  const totalSign = totalPL >= 0 ? "+" : "";

  positionsText += `━━━━━━━━━━━━━━━━━━━━━━━━━━━
${totalColor} *Total P&L: ${totalSign}$${Math.abs(totalPL).toFixed(2)}*

━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔧 *To close a position:*
Type the position number (1, 2, 3...)

🔙 Type *MENU* to return`;

  return positionsText;
};

// Enhanced Error Message Template
const createErrorMessage = async (errorType, details = "") => {
  const errorTemplates = {
    INSUFFICIENT_BALANCE: `❌ *INSUFFICIENT BALANCE*

┌─────────────────────────┐
│   💰 *BALANCE ERROR*    │
└─────────────────────────┘

🚫 Your available balance is too low for this trade.

${details}

💡 *Solutions:*
• Reduce your trade volume
• Check your available balance
• Contact support for assistance

💬 Type *BALANCE* to check funds
🔙 Type *MENU* to return`,

    MARKET_CLOSED: `⏰ *MARKET CLOSED*

┌─────────────────────────┐
│   🌙 *TRADING HOURS*    │
└─────────────────────────┘

🚫 Gold market is currently closed.

📅 *Trading Hours (UAE Time):*
Monday - Friday: 06:00 - 05:00
Weekend: CLOSED

⏰ *Current Time:* ${new Date().toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
    })}

🔙 Type *MENU* to return`,

    NETWORK_ERROR: `🌐 *CONNECTION ERROR*

┌─────────────────────────┐
│   ⚠️ *NETWORK ISSUE*    │
└─────────────────────────┘

🚫 Unable to connect to trading server.

💡 *Please try:*
• Checking your internet connection
• Waiting a moment and trying again
• Typing *REFRESH* to retry

🔙 Type *MENU* to return`,

    GENERAL: `❌ *ERROR OCCURRED*

┌─────────────────────────┐
│   🔧 *SYSTEM ERROR*     │
└─────────────────────────┘

🚫 Something went wrong.

${details ? `📝 *Details:* ${details}` : ""}

💡 *Please try again or contact support*

🔙 Type *MENU* to return`,
  };

  return errorTemplates[errorType] || errorTemplates["GENERAL"];
};

// Enhanced Help Message
const createHelpMessage = async () => {
  return `┌─────────────────────────┐
│     📖 *HELP & GUIDE*   │
└─────────────────────────┘

🚀 *Quick Commands:*
• *MENU* - Main trading menu
• *PRICE* - Live gold prices  
• *BALANCE* - Account balance
• *POSITIONS* - Open trades
• *REFRESH* - Update data
• *RESET* - Restart session

━━━━━━━━━━━━━━━━━━━━━━━━━━━
📱 *How to Trade:*

1️⃣ Check live prices with *PRICE*
2️⃣ Choose *1* for BUY or *2* for SELL
3️⃣ Enter your volume in grams
4️⃣ Confirm with *YES*
5️⃣ Monitor with *POSITIONS*

━━━━━━━━━━━━━━━━━━━━━━━━━━━
🛡️ *Support:*
📞 *Ajmal TK* - Aurify Technologies
📱 +971 58 502 3411

🔙 Type *MENU* to start trading`;
};

// Status indicators
const STATUS_INDICATORS = {
  ONLINE: "🟢 ONLINE",
  OFFLINE: "🔴 OFFLINE",
  UPDATING: "🟡 UPDATING",
  ERROR: "❌ ERROR",
};

// Market status template
const createMarketStatusMessage = (status, nextOpen = null) => {
  const statusEmoji = status === "OPEN" ? "🟢" : "🔴";
  const statusText = status === "OPEN" ? "MARKET OPEN" : "MARKET CLOSED";

  return `${statusEmoji} *${statusText}*

🕒 *Current Time:* ${new Date().toLocaleString("en-US", {
    timeZone: "Asia/Dubai",
    dateStyle: "full",
    timeStyle: "short",
  })} UAE

${nextOpen ? `⏰ *Next Opening:* ${nextOpen}` : ""}

${status === "OPEN" ? "✅ Trading is available" : "⏸️ Trading is suspended"}`;
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

// Get current gold price
const getCurrentGoldPrice = async () => {
  try {
    const marketData = await mt5MarketDataService.getMarketData("XAUUSD.#");
    return marketData?.bid || 0;
  } catch (error) {
    console.error(`Gold price error: ${error.message}`);
    return 0;
  }
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

    // Fetch account details for balance
    const { userId } = await getUserIdFromPhoneNumber(From);
    const account = await Account.findById(userId).lean();
    const goldPrice = await getCurrentGoldPrice();

    // Process message and get response
    const responseMessage = await processMessage(
      Body,
      userSession,
      From,
      account,
      goldPrice
    );

    // Send response only once
    if (responseMessage) {
      const sendResult = await sendMessage(From, responseMessage);
      success = sendResult.success;
    } else {
      success = true; // No message to send is also success
    }
  } catch (error) {
    console.error(`Webhook error for ${MessageSid}: ${error.message}`);
    await sendMessage(From, await createErrorMessage("GENERAL", error.message));
    success = false;
  } finally {
    markMessageComplete(
      [processingKeys.primaryKey, processingKeys.fallbackKey],
      success
    );
  }
};

// Process incoming message - returns message to send
const processMessage = async (body, userSession, from, account, goldPrice) => {
  const trimmedBody = body.trim().toLowerCase();

  // Handle special commands first
  const specialCommandResponse = await handleSpecialCommands(
    trimmedBody,
    userSession,
    from,
    account,
    goldPrice
  );
  if (specialCommandResponse !== null) return specialCommandResponse;

  try {
    return await processUserInputMT5(body, userSession, null, from, null, from);
  } catch (error) {
    console.error(`Input processing error: ${error.message}`);
    userSession.state = "MAIN_MENU";
    return `${await createErrorMessage(
      "GENERAL",
      error.message
    )}\n\n${await getEnhancedMainMenuMT5()}`;
  }
};

// Handle special commands - returns response message
const handleSpecialCommands = async (
  trimmedBody,
  userSession,
  from,
  account,
  goldPrice
) => {
  const commands = {
    reset: async () => {
      resetSession(from);
      const newSession = getUserSession(from);
      newSession.tradingMode = "mt5";
      return await createWelcomeMessage(
        userSession.userName,
        account?.AMOUNTFC || 0,
        account?.reservedAmount || 0,
        goldPrice
      );
    },
    hi: async () => await handleGreeting(userSession, account, goldPrice),
    hello: async () => await handleGreeting(userSession, account, goldPrice),
    start: async () => await handleGreeting(userSession, account, goldPrice),
    balance: async () =>
      await handleBalanceCommand(userSession, account, goldPrice),
    5: async () => await handleBalanceCommand(userSession, account, goldPrice),
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
      return await getEnhancedMainMenuMT5();
    },
    help: async () => {
      userSession.state = "MAIN_MENU";
      return await createHelpMessage();
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
      return await createErrorMessage("GENERAL", error.message);
    }
  }
  return null;
};

// Handle greeting commands
const handleGreeting = async (userSession, account, goldPrice) => {
  userSession.state = "MAIN_MENU";
  userSession.tradingMode = "mt5";
  return await createWelcomeMessage(
    userSession.userName,
    account?.AMOUNTFC || 0,
    account?.reservedAmount || 0,
    goldPrice
  );
};

// Handle balance command
const handleBalanceCommand = async (userSession, account, goldPrice) => {
  try {
    const balance = await getUserBalance(
      userSession.accountId,
      userSession.phoneNumber
    );
    return await createBalanceMessage(
      account?.AMOUNTFC || balance.cash,
      account?.reservedAmount || balance.cash,
      goldPrice,
      balance.gold || 0
    );
  } catch (error) {
    console.error(`Balance error: ${error.message}`);
    return await createErrorMessage(
      "GENERAL",
      "Unable to fetch balance. Please try again."
    );
  }
};

// Handle cancel command
const handleCancelCommand = async (userSession) => {
  const wasConfirming = userSession.state === "CONFIRM_ORDER";
  userSession.state = "MAIN_MENU";
  userSession.pendingOrder = null;

  return `❌ ${
    wasConfirming ? "Order cancelled" : "No active order to cancel"
  }.\n\n${await getEnhancedMainMenuMT5()}`;
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
      const marketData = await mt5MarketDataService.getMarketData("XAUUSD.#");
      const spread = marketData ? (marketData.ask - marketData.bid) * 10 : null;
      return await createPriceMessage(marketData, spread);
    case "4":
    case "positions":
      return await getPositionsMessageMT5(session, phoneNumber);
    case "5":
    case "close":
      return await getPositionsMessageMT5(session, phoneNumber);
    default:
      return await getEnhancedMainMenuMT5();
  }
};

export const handleVolumeInputMT5 = async (input, session, phoneNumber) => {
  console.log(
    `handleVolumeInputMT5: ${input}, ${session.state}, ${phoneNumber}`
  );

  if (input.toLowerCase() === "menu") {
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return await getEnhancedMainMenuMT5();
  }

  const volume = parseFloat(input);
  if (isNaN(volume) || volume <= 0) {
    return await createErrorMessage(
      "GENERAL",
      "Invalid volume. Enter a number (e.g., 0.01) or MENU to cancel."
    );
  }

  try {
    const marketData = await mt5MarketDataService.getMarketData("XAUUSD.#");
    if (!marketData) {
      session.state = "MAIN_MENU";
      return `${await createErrorMessage(
        "NETWORK_ERROR"
      )}\n\n${await getEnhancedMainMenuMT5()}`;
    }

    const price =
      session.pendingOrder.type === "BUY" ? marketData.ask : marketData.bid;

    const balanceCheck = await checkSufficientBalance(
      price,
      volume,
      phoneNumber
    );
    if (!balanceCheck.isSufficient) {
      session.state = "MAIN_MENU";
      session.pendingOrder = null;
      return `${await createErrorMessage(
        "INSUFFICIENT_BALANCE",
        balanceCheck.errorMessage
      )}\n\n${await getEnhancedMainMenuMT5()}`;
    }

    const totalCost = volume * price;
    session.pendingOrder.volume = volume;
    session.pendingOrder.price = price;
    session.pendingOrder.totalCost = totalCost;
    session.state = "CONFIRM_ORDER";

    return await createOrderConfirmation(
      session.pendingOrder.type,
      volume,
      price,
      totalCost,
      session.pendingOrder.symbol
    );
  } catch (error) {
    console.error(`Volume error: ${error.message}`);
    session.state = "MAIN_MENU";
    return `${await createErrorMessage(
      "GENERAL",
      "Error processing volume. Try again."
    )}\n\n${await getEnhancedMainMenuMT5()}`;
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
    return `❌ Order cancelled.\n\n${await getEnhancedMainMenuMT5()}`;
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

    const marketData = await mt5MarketDataService.getMarketData("XAUUSD.#");
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

    const responseMessage = await createOrderSuccessMessage(
      result,
      result.type,
      result.volume,
      parseFloat(price),
      session.pendingOrder.symbol,
      result.ticket
    );

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
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return `${await createErrorMessage(
      "GENERAL",
      error.message
    )}\n\n${await getEnhancedMainMenuMT5()}`;
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
    return await getEnhancedMainMenuMT5();
  }

  const positionIndex = parseInt(input) - 1;
  if (
    !session.openPositions ||
    positionIndex < 0 ||
    positionIndex >= session.openPositions.length
  ) {
    return await createErrorMessage(
      "GENERAL",
      "Invalid position number. Select a valid number or type MENU."
    );
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
    return await createErrorMessage(
      "GENERAL",
      "Invalid volume for the selected position."
    );
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

    const updateData = { orderStatus: "CLOSED" };
    const updatedOrder = await updateTradeStatus(
      adminId,
      order._id.toString(),
      updateData,
      mongoSession
    );
    console.log(`Updated order: ${JSON.stringify(updatedOrder, null, 2)}`);

    await mongoSession.commitTransaction();
    transactionCommitted = true;

    console.log(
      `Position successfully closed and committed for ticket: ${selectedPosition.ticket}`
    );

    session.state = "MAIN_MENU";
    session.openPositions = null;

    return `✅ Position Closed Successfully!
┌─────────────────────────┐
│    🎉 *CLOSURE SUCCESS* │
└─────────────────────────┘

🎫 *Ticket:* #${selectedPosition.ticket}
💰 *Close Price:* $${updatedOrder.order.closingPrice.toFixed(2)}
📈 *P&L:* $${updatedOrder.order.profit.toFixed(2)}
🕒 *Closed:* ${new Date().toLocaleString("en-US", { timeZone: "Asia/Dubai" })}

━━━━━━━━━━━━━━━━━━━━━━━━━━━
💡 *What's next?*
• Type *4* to view positions
• Type *MENU* to return`;
  } catch (error) {
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
    session.state = "MAIN_MENU";
    session.openPositions = null;

    let errorMessage = await createErrorMessage(
      "GENERAL",
      `Error Closing Position\n📊 Ticket: ${
        selectedPosition?.ticket || "unknown"
      }\n📝 Error: ${error.message}`
    );
    if (error.message.includes("Position not found")) {
      errorMessage = await createErrorMessage(
        "GENERAL",
        `Position ${
          selectedPosition?.ticket || "unknown"
        } not found in MT5. It may already be closed.`
      );
    }
    return `${errorMessage}\n\n${await getEnhancedMainMenuMT5()}`;
  } finally {
    try {
      await mongoSession.endSession();
    } catch (endError) {
      console.error(`Failed to end session: ${endError.message}`);
    }
  }
};
