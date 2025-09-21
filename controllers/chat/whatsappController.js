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
const SYMBOL_MAPPING = { GOLD: process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix" };
const SYMBOL_FACTORS = {
  TTBAR: 116.64,
  KGBAR: 1000,
};
const UNAUTHORIZED_MESSAGE = `🚫 *Access Denied*
Your number is not registered.

📞 *Contact Support:*
Ajmal TK – Aurify Technologies
📱 +971 58 502 3411`;
const ERROR_MESSAGE = `❌ *Error*
Something went wrong. Type *MENU* to continue.`;
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
const calculateTradeCost = (price, volume, symbol = "TTBAR") => {
  const volumeValue = parseFloat(volume) || 0;
  const factor = SYMBOL_FACTORS[symbol] || TTB_FACTOR;
  const tradeValue = (price / TROY_OUNCE_GRAMS) * factor * volumeValue;
  return tradeValue;
};

// Helper to get adjusted prices based on user symbol
const getAdjustedPrices = (marketData, symbol = "TTBAR") => {
  if (!marketData || !marketData.bid || !marketData.ask) {
    return { adjustedBid: 0, adjustedAsk: 0, factor: SYMBOL_FACTORS[symbol] || TTB_FACTOR };
  }

  const factor = SYMBOL_FACTORS[symbol] || TTB_FACTOR;
  const baseBid = (marketData.bid / TROY_OUNCE_GRAMS) * factor;
  const baseAsk = (marketData.ask / TROY_OUNCE_GRAMS) * factor;

  return {
    adjustedBid: baseBid,
    adjustedAsk: baseAsk,
    factor,
    symbol,
  };
};

// Check sufficient balance
const checkSufficientBalance = async (price, volumeInput, phoneNumber, symbol = "TTBAR") => {
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
    const tradeCost = calculateTradeCost(price, volume, symbol);
    const marginRequirement = tradeCost * (MINIMUM_BALANCE_PERCENTAGE / 100);

    if (marginRequirement > availableBalance) {
      return {
        isSufficient: false,
        errorMessage: `Insufficient balance.\nRequired: $${marginRequirement.toFixed(2)}\nAvailable: $${availableBalance.toFixed(2)}`,
      };
    }

    return { isSufficient: true, errorMessage: null };
  } catch (error) {
    console.error(`Balance check error: ${error.message}`);
    return {
      isSufficient: false,
      errorMessage: "Error checking balance. Try again.",
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
// Welcome Message Template - Short and User-Friendly
const createWelcomeMessage = async (
  userName,
  equity,
  availableBalance,
  goldPrice,
  marketData,
  symbol = "TTBAR"
) => {
  const adjusted = getAdjustedPrices(marketData, symbol);
  const unit = symbol === "KGBAR" ? "KG" : "GRAM";

  return `👋 *${userName || "Valued Client"}*

🥇 *${symbol} Prices:*
🟢 Buy: $${adjusted.adjustedAsk.toFixed(2)}/${unit}
🔴 Sell: $${adjusted.adjustedBid.toFixed(2)}/${unit}

📋 Options:
1️⃣ Buy
2️⃣ Sell
3️⃣ Balance
4️⃣ Positions
5️⃣ Prices

💬 Type a number (e.g., 1) or 'buy <volume>' (e.g., buy 1)`;
};

// Enhanced Main Menu Template - Short and User-Friendly
const getEnhancedMainMenuMT5 = async (marketData, symbol = "TTBAR", userName = "") => {
  if (!marketData) {
    return `👋 *${userName || "Valued Client"}*

🥇 *${symbol} Prices:* Not available

📋 Options:
1️⃣ Buy
2️⃣ Sell
3️⃣ Balance
4️⃣ Positions
5️⃣ Prices

💬 Type a number (e.g., 1) or 'buy <volume>' (e.g., buy 1)`;
  }

  const adjusted = getAdjustedPrices(marketData, symbol);
  const unit = symbol === "KGBAR" ? "KG" : "GRAM";

  return `👋 *${userName || "Valued Client"}*

🥇 *${symbol} Prices:*
🟢 Buy: $${adjusted.adjustedAsk.toFixed(2)}/${unit}
🔴 Sell: $${adjusted.adjustedBid.toFixed(2)}/${unit}

📋 Options:
1️⃣ Buy
2️⃣ Sell
3️⃣ Balance
4️⃣ Positions
5️⃣ Prices

💬 Type a number (e.g., 1) or 'buy <volume>' (e.g., buy 1)`;
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
  const profitLoss = equity - availableBalance;
  const profitColor = profitLoss >= 0 ? "🟢" : "🔴";
  const profitSign = profitLoss >= 0 ? "+" : "";

  return `💰 *Balance*
Equity: $${formatCurrency(equity)}
Available: $${formatCurrency(availableBalance)}
${goldHolding > 0 ? `Gold Holdings: ${goldHolding.toFixed(2)} oz` : ""}
${goldHolding > 0 ? `Gold Value: $${formatCurrency(goldValue)}` : ""}
Total: $${formatCurrency(totalPortfolio)}
${profitColor} P&L: ${profitSign}$${Math.abs(profitLoss).toFixed(2)}

💬 Type *MENU*`;
};

// Enhanced Price Display Template
const createPriceMessage = async (marketData, spread, symbol = "TTBAR", userName = "") => {
  const adjusted = getAdjustedPrices(marketData, symbol);
  const unit = symbol === "KGBAR" ? "KG" : "GRAM";
  const timestamp = new Date().toLocaleString("en-US", {
    timeZone: "Asia/Dubai",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });

  return `📈 *${symbol} Prices*
🟢 Buy: $${adjusted.adjustedAsk.toFixed(2)}/${unit}
🔴 Sell: $${adjusted.adjustedBid.toFixed(2)}/${unit}
Spread: ${spread?.toFixed(1) || "N/A"} pips

Updated: ${timestamp} UAE
💬 Type *1* (Buy), *2* (Sell), or *MENU*`;
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
  const typePrefix = orderType === "BUY" ? "Buy" : "Sell";
  const unit = symbol === "KGBAR" ? "KG" : "GRAM";

  return `✅ *${typePrefix} ${symbol} order executed successfully*
Volume: ${volume} ${unit}
Price: $${price.toFixed(2)}
Total: $${(volume * price).toFixed(2)}
Ticket: #${ticket}

💬 Type *4* (Positions) or *MENU*`;
};

// Enhanced Positions List Template
const createPositionsMessage = async (positions, totalPL) => {
  if (!positions || positions.length === 0) {
    return `📋 *Positions*
No open positions.

💬 Type *1* (Buy), *2* (Sell), or *MENU*`;
  }

  let positionsText = `📋 *Positions*\n\n`;
  positions.forEach((pos, index) => {
    const plColor = pos.profit >= 0 ? "🟢" : "🔴";
    const plSign = pos.profit >= 0 ? "+" : "";

    positionsText += `${pos.type === "BUY" ? "📈" : "📉"} *${index + 1}.* ${
      pos.symbol
    }
Ticket: #${pos.ticket}
Volume: ${pos.volume} TTBAR
Open: $${pos.openPrice?.toFixed(2)}
Current: $${pos.currentPrice?.toFixed(2)}
${plColor} P&L: ${plSign}$${pos.profit?.toFixed(2)}\n\n`;
  });

  const totalColor = totalPL >= 0 ? "🟢" : "🔴";
  const totalSign = totalPL >= 0 ? "+" : "";

  positionsText += `${totalColor} Total P&L: ${totalSign}$${Math.abs(totalPL).toFixed(2)}\n\n💬 Type a number to close or *MENU*`;

  return positionsText;
};

// Enhanced Error Message Template
const createErrorMessage = async (errorType, details = "") => {
  const errorTemplates = {
    INSUFFICIENT_BALANCE: `❌ *Insufficient Balance*
${details}
💬 Type *3* (Balance) or *MENU*`,
    MARKET_CLOSED: `⏰ *Market Closed*
Trading Hours (UAE): Mon-Fri 06:00-05:00
💬 Type *MENU*`,
    NETWORK_ERROR: `🌐 *Connection Error*
Try again or type *REFRESH*.
💬 Type *MENU*`,
    GENERAL: `❌ *Error*
${details || "Something went wrong."}
💬 Type *MENU*`,
  };

  return errorTemplates[errorType] || errorTemplates["GENERAL"];
};

// Enhanced Help Message
const createHelpMessage = async () => {
  return `📖 *Help*
Commands:
• *MENU*: Main menu
• *PRICE*: Live prices
• *BALANCE*: Check balance
• *POSITIONS*: View trades
• *REFRESH*: Update data
• *RESET*: Restart session

How to Trade:
1️⃣ Type 'buy <volume>' or 'sell <volume>' (e.g., buy 1)
2️⃣ Order executes immediately
3️⃣ Type *4* to monitor positions

Support: Ajmal TK, +971 58 502 3411
💬 Type *MENU*`;
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
  const statusText = status === "OPEN" ? "Market Open" : "Market Closed";

  return `${statusEmoji} *${statusText}*
Time: ${new Date().toLocaleString("en-US", {
    timeZone: "Asia/Dubai",
    dateStyle: "short",
    timeStyle: "short",
  })} UAE
${nextOpen ? `Next Open: ${nextOpen}` : ""}
${status === "OPEN" ? "✅ Trading available" : "⏸️ Trading paused"}`;
};

// Refresh market data with caching
const marketDataCache = new Map();
const MARKET_DATA_TTL = 30000; // 30 seconds

const refreshMarketData = async (clientId) => {
  const now = Date.now();
  const cached = marketDataCache.get(clientId);
  if (cached && now - cached.timestamp < MARKET_DATA_TTL) return;

  try {
    await mt5MarketDataService.getMarketData(
      process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
      clientId
    );
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
    const marketData = await mt5MarketDataService.getMarketData(
      process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"
    );
    return marketData?.bid || 0;
  } catch (error) {
    console.error(`Gold price error: ${error.message}`);
    return 0;
  }
};

// Main webhook handler
export const handleWhatsAppWebhook = async (req, res) => {
  const { Body, From, ProfileName, MessageSid } = req.body;

  if (!Body || !From || !MessageSid) {
    console.log("Missing parameters:", req.body);
    return res.status(400).send("Missing required parameters");
  }

  if (isDuplicateMessage(MessageSid, From, Body)) {
    console.log(`Duplicate request detected: ${MessageSid} from ${From}`);
    return res.status(200).send(new MessagingResponse().toString());
  }

  const processingKeys = markMessageProcessing(MessageSid, From, Body);
  console.log(`Processing message from ${From}: ${Body}, SID: ${MessageSid}`);

  res.status(200).send(new MessagingResponse().toString());

  let success = false;
  try {
    await refreshMarketData(From);
    const authResult = await isAuthorizedUser(From);
    if (!authResult.isAuthorized) {
      await sendMessage(From, UNAUTHORIZED_MESSAGE);
      success = true;
      return;
    }

    const userSession = initializeUserSession(
      From,
      authResult.accountId,
      ProfileName
    );

    const { userId } = await getUserIdFromPhoneNumber(From);
    const account = await Account.findById(userId).lean();
    const goldPrice = await getCurrentGoldPrice();
    const marketData = await mt5MarketDataService.getMarketData(
      process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"
    );

    const responseMessage = await processMessage(
      Body,
      userSession,
      From,
      account,
      goldPrice,
      marketData
    );

    if (responseMessage) {
      const sendResult = await sendMessage(From, responseMessage);
      success = sendResult.success;
    } else {
      success = true;
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

// Process incoming message
const processMessage = async (body, userSession, from, account, goldPrice, marketData) => {
  const trimmedBody = body.trim().toLowerCase();
  const specialCommandResponse = await handleSpecialCommands(
    trimmedBody,
    userSession,
    from,
    account,
    goldPrice,
    marketData
  );
  if (specialCommandResponse !== null) return specialCommandResponse;

  const symbol = account?.symbol || "TTBAR";
  switch (userSession.state) {
    case "MAIN_MENU":
      return await handleMainMenuMT5(body, userSession, from, account, marketData, symbol);
    case "AWAITING_VOLUME":
      return await handleVolumeInputMT5(body, userSession, from, account, marketData, symbol);
    case "SELECT_POSITION":
      return await handlePositionSelectionMT5(body, userSession, from);
    default:
      userSession.state = "MAIN_MENU";
      return await getEnhancedMainMenuMT5(marketData, symbol, userSession.userName || "");
  }
};

// Handle special commands
const handleSpecialCommands = async (
  trimmedBody,
  userSession,
  from,
  account,
  goldPrice,
  marketData
) => {
  const symbol = account?.symbol || "TTBAR";
  const commands = {
    reset: async () => {
      resetSession(from);
      const newSession = getUserSession(from);
      newSession.tradingMode = "mt5";
      return await createWelcomeMessage(
        userSession.userName,
        account?.AMOUNTFC || 0,
        account?.reservedAmount || 0,
        goldPrice,
        marketData,
        symbol
      );
    },
    hi: async () => await handleGreeting(userSession, account, goldPrice, marketData, symbol),
    hello: async () => await handleGreeting(userSession, account, goldPrice, marketData, symbol),
    start: async () => await handleGreeting(userSession, account, goldPrice, marketData, symbol),
    balance: async () => await handleBalanceCommand(userSession, account, marketData),
    3: async () => await handleBalanceCommand(userSession, account, marketData),
    cancel: async () => await handleCancelCommand(userSession, marketData, symbol),
    price: async () => {
      const spread = marketData ? (marketData.ask - marketData.bid) * 10 : null;
      return await createPriceMessage(marketData, spread, symbol, userSession.userName || "");
    },
    prices: async () => {
      const spread = marketData ? (marketData.ask - marketData.bid) * 10 : null;
      return await createPriceMessage(marketData, spread, symbol, userSession.userName || "");
    },
    orders: async () => await getPositionsMessageMT5(userSession, from),
    positions: async () => await getPositionsMessageMT5(userSession, from),
    4: async () => await getPositionsMessageMT5(userSession, from),
    closing: async () => await getPositionsMessageMT5(userSession, from),
    5: async () => {
      const spread = marketData ? (marketData.ask - marketData.bid) * 10 : null;
      return await createPriceMessage(marketData, spread, symbol, userSession.userName || "");
    },
    live: async () => {
      const spread = marketData ? (marketData.ask - marketData.bid) * 10 : null;
      return await createPriceMessage(marketData, spread, symbol, userSession.userName || "");
    },
    refresh: async () => {
      await refreshMarketData(from);
      return "🔄 Refreshing data... Type '5' or 'PRICE'";
    },
    menu: async () => {
      userSession.state = "MAIN_MENU";
      return await getEnhancedMainMenuMT5(marketData, symbol, userSession.userName || "");
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
      console.error(`Error handling command '${trimmedBody}': ${error.message}`);
      return await createErrorMessage("GENERAL", error.message);
    }
  }
  return null;
};

// Handle greeting commands
const handleGreeting = async (userSession, account, goldPrice, marketData, symbol) => {
  userSession.state = "MAIN_MENU";
  userSession.tradingMode = "mt5";
  return await createWelcomeMessage(
    userSession.userName,
    account?.AMOUNTFC || 0,
    account?.reservedAmount || 0,
    goldPrice,
    marketData,
    symbol
  );
};

// Handle balance command
const handleBalanceCommand = async (userSession, account, marketData) => {
  try {
    const balance = await getUserBalance(
      userSession.accountId,
      userSession.phoneNumber
    );
    return await createBalanceMessage(
      account?.AMOUNTFC || balance.cash,
      account?.reservedAmount || balance.cash,
      marketData?.bid || 0,
      balance.gold || 0
    );
  } catch (error) {
    console.error(`Balance error: ${error.message}`);
    return await createErrorMessage(
      "GENERAL",
      "Unable to fetch balance. Try again."
    );
  }
};

// Handle cancel command
const handleCancelCommand = async (userSession, marketData, symbol) => {
  const wasConfirming = userSession.state === "CONFIRM_ORDER";
  userSession.state = "MAIN_MENU";
  userSession.pendingOrder = null;

  return `❌ ${
    wasConfirming ? "Order cancelled" : "No active order to cancel"
  }\n\n${await getEnhancedMainMenuMT5(marketData, symbol)}`;
};

// Handle main menu
export const handleMainMenuMT5 = async (input, session, phoneNumber, account, marketData, symbol) => {
  console.log(`handleMainMenuMT5: ${input}, ${session.state}`);
  const unit = symbol === "KGBAR" ? "KG" : "GRAM";

  // Parse input for "buy <volume>" or "sell <volume>"
  const inputParts = input.toLowerCase().trim().split(/\s+/);
  const command = inputParts[0];
  const volumeInput = inputParts[1];

  if (["buy", "sell"].includes(command) && volumeInput) {
    const orderType = command === "buy" ? "BUY" : "SELL";
    session.state = "AWAITING_VOLUME";
    session.pendingOrder = { type: orderType, symbol: symbol };
    return await handleVolumeInputMT5(volumeInput, session, phoneNumber, account, marketData, symbol);
  }

  switch (input.toLowerCase()) {
    case "1":
    case "buy":
      session.state = "AWAITING_VOLUME";
      session.pendingOrder = { type: "BUY", symbol: symbol };
      const adjustedBuy = getAdjustedPrices(marketData, symbol).adjustedAsk;
      return `📈 Buy ${symbol} at $${adjustedBuy.toFixed(2)}/${unit}\nEnter volume in ${unit.toLowerCase()} (e.g., 1):`;
    case "2":
    case "sell":
      session.state = "AWAITING_VOLUME";
      session.pendingOrder = { type: "SELL", symbol: symbol };
      const adjustedSell = getAdjustedPrices(marketData, symbol).adjustedBid;
      return `📉 Sell ${symbol} at $${adjustedSell.toFixed(2)}/${unit}\nEnter volume in ${unit.toLowerCase()} (e.g., 1):`;
    case "3":
    case "balance":
      const balance = await getUserBalance(session.accountId, phoneNumber);
      return await createBalanceMessage(
        account?.AMOUNTFC || balance.cash,
        account?.reservedAmount || balance.cash,
        marketData?.bid || 0,
        balance.gold || 0
      );
    case "4":
    case "closing":
    case "positions":
      return await getPositionsMessageMT5(session, phoneNumber);
    case "5":
    case "live":
    case "price":
    case "prices":
      const spread = marketData ? (marketData.ask - marketData.bid) * 10 : null;
      return await createPriceMessage(marketData, spread, symbol, session.userName || "");
    default:
      return await getEnhancedMainMenuMT5(marketData, symbol, session.userName || "");
  }
};

// Handle volume input
export const handleVolumeInputMT5 = async (input, session, phoneNumber, account, marketData, symbol) => {
  console.log(`handleVolumeInputMT5: ${input}, ${session.state}, ${phoneNumber}`);

  const unit = symbol === "KGBAR" ? "KG" : "GRAM";
  const typePrefix = session.pendingOrder.type === "BUY" ? "Buy" : "Sell";

  if (input.toLowerCase() === "menu") {
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return await getEnhancedMainMenuMT5(marketData, symbol, session.userName || "");
  }

  const volume = parseFloat(input);
  if (isNaN(volume) || volume <= 0) {
    const adjustedPrices = getAdjustedPrices(marketData, symbol);
    const price = session.pendingOrder.type === "BUY" ? adjustedPrices.adjustedAsk : adjustedPrices.adjustedBid;
    return await createErrorMessage(
      "GENERAL",
      `Invalid volume. Enter a number >0 (e.g., 0.01 ${unit.toLowerCase()})\n\n📈 ${typePrefix} ${symbol} at $${price.toFixed(2)}/${unit}\nEnter volume in ${unit.toLowerCase()}:`
    );
  }

  try {
    if (!marketData) {
      session.state = "MAIN_MENU";
      session.pendingOrder = null;
      return `${await createErrorMessage(
        "NETWORK_ERROR"
      )}\n\n${await getEnhancedMainMenuMT5(null, symbol)}`;
    }

    const adjustedPrices = getAdjustedPrices(marketData, symbol);
    const price = session.pendingOrder.type === "BUY" ? adjustedPrices.adjustedAsk : adjustedPrices.adjustedBid;

    const balanceCheck = await checkSufficientBalance(price, volume, phoneNumber, symbol);
    if (!balanceCheck.isSufficient) {
      session.state = "MAIN_MENU";
      session.pendingOrder = null;
      return `${await createErrorMessage(
        "INSUFFICIENT_BALANCE",
        balanceCheck.errorMessage
      )}\n\n${await getEnhancedMainMenuMT5(marketData, symbol)}`;
    }

    // Directly execute the order without confirmation
    const totalCost = volume * price;
    session.pendingOrder.volume = volume;
    session.pendingOrder.price = price;
    session.pendingOrder.totalCost = totalCost;

    // Execute the order
    const mongoSession = await mongoose.startSession();
    let transactionStarted = false;
    let transactionCommitted = false;

    try {
      mongoSession.startTransaction();
      transactionStarted = true;

      const { userId, adminId, error } = await getUserIdFromPhoneNumber(phoneNumber);
      if (!userId || !adminId) {
        throw new Error(error || "User account or admin not found");
      }

      const accountDoc = await Account.findById(userId).session(mongoSession).lean();
      if (!accountDoc) {
        throw new Error("User account not found");
      }

      if (!marketData || !marketData.ask || !marketData.bid) {
        throw new Error("Failed to fetch live market data");
      }

      const orderNo = generateEntryId("OR");

      const tradeData = {
        symbol: SYMBOL_MAPPING[session.pendingOrder.symbol] || process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
        volume: volume,
        type: session.pendingOrder.type,
        slDistance: null,
        tpDistance: null,
        comment: `Ord-${orderNo}`,
        magic: 123456,
      };

      console.log("Placing MT5 trade with data:", tradeData);

      const mt5Result = await mt5Service.placeTrade(tradeData);
      console.log("MT5 trade result:", JSON.stringify(mt5Result, null, 2));

      if (!mt5Result.success) {
        throw new Error(mt5Result.error || "MT5 trade failed");
      }

      if (!mt5Result.price) {
        throw new Error("MT5 response missing execution price");
      }
      if (!mt5Result.ticket) {
        throw new Error("MT5 response missing ticket number");
      }

      const actualExecutionPrice = parseFloat(mt5Result.price);
      const actualVolume = parseFloat(mt5Result.volume);
      const actualTicket = mt5Result.ticket.toString();
      const symbolMt5 =  mt5Result.symbol;

      console.log("MT5 execution details:", {
        actualExecutionPrice,
        estimatedPrice: price,
        actualVolume,
        actualTicket,
        symbol: mt5Result.symbol,
      });

      const actualTradeCost = calculateTradeCost(actualExecutionPrice, actualVolume, symbol);
      const actualRequiredMargin = actualTradeCost * (MINIMUM_BALANCE_PERCENTAGE / 100);

      const crmTradeData = {
        orderNo,
        type: session.pendingOrder.type,
        volume: actualVolume,
        ticket: actualTicket,
        symbol: symbolMt5,
        price: actualExecutionPrice,
        openingDate: new Date(),
        requiredMargin: actualRequiredMargin,
        comment: tradeData.comment,
        stopLoss: session.pendingOrder.stopLoss || 0,
        takeProfit: session.pendingOrder.takeProfit || 0,
      };

      console.log("Creating CRM trade with data:", crmTradeData);

      const tradeResult = await createTrade(adminId, userId, crmTradeData, mongoSession);
      console.log("CRM trade created successfully:", {
        orderId: tradeResult.clientOrder._id,
        ticket: tradeResult.clientOrder.ticket,
        status: tradeResult.clientOrder.orderStatus,
        mt5Price: tradeResult.priceDetails.currentPrice,
        clientPrice: tradeResult.priceDetails.openingPrice,
      });

      await mongoSession.commitTransaction();
      transactionCommitted = true;

      console.log(`Trade successfully created and committed for ticket: ${actualTicket}`);

      const responseMessage = await createOrderSuccessMessage(
        {
          success: true,
          ticket: actualTicket,
          price: tradeResult.priceDetails.openingPrice,
          volume: actualVolume,
          type: session.pendingOrder.type,
        },
        session.pendingOrder.type,
        actualVolume,
        tradeResult.priceDetails.openingPrice,
        symbol,
        actualTicket
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
      console.error(`Error stack: ${error.stack}`);
      session.state = "MAIN_MENU";
      session.pendingOrder = null;
      return `${await createErrorMessage(
        "GENERAL",
        error.message
      )}\n\n${await getEnhancedMainMenuMT5(marketData, symbol)}`;
    } finally {
      try {
        await mongoSession.endSession();
      } catch (endError) {
        console.error(`Failed to end session: ${endError.message}`);
      }
    }
  } catch (error) {
    console.error(`Volume error: ${error.message}`);
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return `${await createErrorMessage(
      "GENERAL",
      "Error processing volume. Try again."
    )}\n\n${await getEnhancedMainMenuMT5(marketData, symbol)}`;
  }
};

// Handle order confirmation (retained for backward compatibility)
export const handleOrderConfirmationMT5 = async (
  input,
  session,
  phoneNumber,
  account
) => {
  console.log(
    `handleOrderConfirmationMT5: ${input}, ${session.state}, ${phoneNumber}`
  );

  const symbol = account?.symbol || "TTBAR";
  const marketData = await mt5MarketDataService.getMarketData(
    process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"
  );

  if (input.toLowerCase() === "menu" || input.toLowerCase() === "no") {
    session.state = "MAIN_MENU";
    session.pendingOrder = null;
    return `❌ Order cancelled\n\n${await getEnhancedMainMenuMT5(marketData, symbol)}`;
  }

  return `ℹ️ Type YES to confirm or NO/MENU to cancel`;
};

// Handle position selection
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
    const marketData = await mt5MarketDataService.getMarketData(
      process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"
    );
    return await getEnhancedMainMenuMT5(marketData, "TTBAR");
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

    return `✅ Position Closed!
Ticket: #${selectedPosition.ticket}
Close Price: $${updatedOrder.order.closingPrice.toFixed(2)}
P&L: $${updatedOrder.order.profit.toFixed(2)}

💬 Type *4* (Positions) or *MENU*`;
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
      `Error closing position #${selectedPosition?.ticket || "unknown"}: ${error.message}`
    );
    if (error.message.includes("Position not found")) {
      errorMessage = await createErrorMessage(
        "GENERAL",
        `Position ${selectedPosition?.ticket || "unknown"} not found. It may already be closed.`
      );
    }
    return `${errorMessage}\n\n${await getEnhancedMainMenuMT5(null, "TTBAR")}`;
  } finally {
    try {
      await mongoSession.endSession();
    } catch (endError) {
      console.error(`Failed to end session: ${endError.message}`);
    }
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