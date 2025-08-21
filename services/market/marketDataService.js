// services/market/marketDataService.js
import { io } from "socket.io-client";

// Configuration constants
const CONFIG = {
  SOCKET_SERVER_URL: "https://capital-server-gnsu.onrender.com",
  SECRET_KEY: "aurify@123",
  PRICE_FRESHNESS_TIMEOUT: 60000, // 1 minute
  TROY_OUNCE_GRAMS: 31.103,
  GOLD_CONVERSION_FACTOR: 3.674,
  TTB_FACTOR: 116.64,
  BACKUP_GOLD_PRICE: 2450.0,
  RECONNECT_BASE_DELAY: 5000,
  MAX_RECONNECT_ATTEMPTS: 5,
};

class MarketDataService {
  constructor() {
    this.socket = null;
    this.marketData = new Map();
    this.lastUpdated = new Map();
    this.subscribers = new Set();
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.reconnectDelay = CONFIG.RECONNECT_BASE_DELAY;
    this.pendingSymbolRequests = new Set(["GOLD"]); // Always request GOLD by default

    // Initialize with backup values
    this.initializeBackupData();

    // Start connection
    this.connect();

    // Set up periodic data staleness check
    this.startFreshnessChecks();
  }

  // Initialize with backup data
  initializeBackupData() {
    const now = Date.now();
    const backupGoldData = {
      symbol: "GOLD",
      epic: "GOLD",
      offer: CONFIG.BACKUP_GOLD_PRICE,
      bid: CONFIG.BACKUP_GOLD_PRICE * 0.99, // 1% spread for bid/ask
      high: CONFIG.BACKUP_GOLD_PRICE * 1.01,
      low: CONFIG.BACKUP_GOLD_PRICE * 0.98,
      timestamp: now,
      marketStatus: "TRADEABLE",
    };

    this.marketData.set("GOLD", backupGoldData);
    this.lastUpdated.set("GOLD", now);
    console.log(
      "Market Data Service: Initialized with backup gold price:",
      CONFIG.BACKUP_GOLD_PRICE
    );
  }

  // Periodically check data freshness and request updates for stale data
  startFreshnessChecks() {
    const checkInterval = Math.floor(CONFIG.PRICE_FRESHNESS_TIMEOUT / 2);

    setInterval(() => {
      const staleSymbols = [];

      // Check all symbols for staleness
      for (const [symbol, timestamp] of this.lastUpdated.entries()) {
        if (Date.now() - timestamp > CONFIG.PRICE_FRESHNESS_TIMEOUT * 0.8) {
          staleSymbols.push(symbol);
        }
      }

      // Request fresh data for stale symbols
      if (staleSymbols.length > 0 && this.isConnected) {
        this.requestSymbols(staleSymbols);
      }
    }, checkInterval);
  }

  connect() {
    if (this.socket) {
      // Clean up existing socket if any
      this.socket.off();
      this.socket.disconnect();
    }

    console.log("📡 Market Data Service: Connecting to WebSocket...");

    this.socket = io(CONFIG.SOCKET_SERVER_URL, {
      query: { secret: CONFIG.SECRET_KEY },
      transports: ["websocket"],
      withCredentials: true,
      reconnection: false, // We'll handle reconnection manually
    });

    this.setupSocketListeners();
  }

  setupSocketListeners() {
    this.socket.on("connect", () => {
      console.log("✅ Market Data Service: Connected to WebSocket server");
      this.isConnected = true;
      this.reconnectAttempts = 0;
      this.reconnectDelay = CONFIG.RECONNECT_BASE_DELAY;

      // Process pending requests
      if (this.pendingSymbolRequests.size > 0) {
        this.requestSymbols([...this.pendingSymbolRequests]);
        this.pendingSymbolRequests.clear();
      }
    });

    this.socket.on("market-data", this.handleMarketData.bind(this));

    this.socket.on("error", (error) => {
      console.error("❌ Market Data Service: WebSocket error:", error);
    });

    this.socket.on("disconnect", () => {
      console.log("🔌 Market Data Service: Disconnected from WebSocket server");
      this.isConnected = false;
      this.handleDisconnection();
    });
  }

  handleMarketData(data) {
    if (!data || !data.symbol) return;

    // Handle both symbol or epic format
    const symbol = (data.symbol || data.epic || "").toUpperCase();
    if (!symbol) return;

    // Validate the data
    if (this.isValidMarketData(data)) {
      // Map the incoming data to our standardized format
      // Support both askPrice/bidPrice and offer/bid formats
      const marketData = {
        symbol: symbol,
        epic: data.epic || symbol,
        // Map incoming data fields to our internal format
        offer: data.offer !== undefined ? data.offer : data.askPrice,
        bid: data.bid !== undefined ? data.bid : data.bidPrice,
        high: data.high || null,
        low: data.low || null,
        timestamp: data.timestamp || Date.now(),
        marketStatus: data.marketStatus || "TRADEABLE",
        marketOpenTimestamp: data.marketOpenTimestamp || null,
        nextMarketOpen: data.nextMarketOpen || null,
        initialBid: data.initialBid || null,
        highLowChanges: data.highLowChanges || []
      };

      // For backward compatibility, also map askPrice and bidPrice
      marketData.askPrice = marketData.offer;
      marketData.bidPrice = marketData.bid;

      this.marketData.set(symbol, marketData);
      this.lastUpdated.set(symbol, Date.now());

      // Notify subscribers
      this.notifySubscribers(symbol, marketData);
    }
  }

  handleDisconnection() {
    // Implement exponential backoff for reconnection
    if (this.reconnectAttempts < CONFIG.MAX_RECONNECT_ATTEMPTS) {
      const timeout =
        this.reconnectDelay * Math.pow(1.5, this.reconnectAttempts);
      console.log(
        `Market Data Service: Attempting to reconnect in ${
          timeout / 1000
        } seconds...`
      );

      setTimeout(() => {
        if (!this.isConnected) {
          this.reconnectAttempts++;
          this.connect();
        }
      }, timeout);
    } else {
      console.error(
        "Market Data Service: Maximum reconnection attempts reached"
      );
      // Reset for potential future manual reconnection attempt
      setTimeout(() => {
        this.reconnectAttempts = 0;
      }, 60000); // Wait a minute before resetting the counter
    }
  }

  // Validate market data to ensure it has required properties
  isValidMarketData(data) {
    // Support both askPrice/bidPrice and offer/bid formats
    const hasAskBid = typeof data.askPrice === "number" && !isNaN(data.askPrice) && 
                      typeof data.bidPrice === "number" && !isNaN(data.bidPrice);
    
    const hasOfferBid = typeof data.offer === "number" && !isNaN(data.offer) && 
                       typeof data.bid === "number" && !isNaN(data.bid);
    
    // Check if either format exists
    return data && 
           (typeof data.symbol === "string" || typeof data.epic === "string") && 
           (hasAskBid || hasOfferBid);
  }

  disconnect() {
    if (this.socket) {
      this.socket.disconnect();
      this.socket = null;
      this.isConnected = false;
      console.log("🔌 Market Data Service: Manually disconnected");
    }
  }

  requestSymbols(symbols = ["GOLD"]) {
    // Ensure symbols is an array
    const symbolsArray = Array.isArray(symbols) ? symbols : [symbols];

    if (this.socket && this.isConnected) {
      console.log(
        `Market Data Service: Requesting data for symbols: ${symbolsArray.join(
          ", "
        )}`
      );
      this.socket.emit("request-data", symbolsArray);
    } else {
      console.warn(
        "Market Data Service: Cannot request symbols - socket not connected"
      );
      // Queue up this request for when we reconnect
      symbolsArray.forEach((symbol) => this.pendingSymbolRequests.add(symbol));
    }
  }

  // Check if price data is fresh
  isPriceFresh(symbol = "Gold") {
    const upperSymbol = symbol.toUpperCase();
    const lastUpdate = this.lastUpdated.get(upperSymbol);

    if (!lastUpdate) return false;

    return Date.now() - lastUpdate < CONFIG.PRICE_FRESHNESS_TIMEOUT;
  }

  // Get latest price data for a symbol
  getMarketData(symbol = "Gold") {
    const upperSymbol = symbol.toUpperCase();
    return this.marketData.get(upperSymbol) || null;
  }

  // Get all market data as an object (for backward compatibility)
  getCurrentPrices() {
    const pricesObj = {};
    for (const [symbol, data] of this.marketData.entries()) {
      pricesObj[symbol] = { ...data };
    }
    return pricesObj;
  }

  // Get live price for a specific symbol with optional type (offer/bid)
  getLivePrice(symbol = "GOLD", type = "offer") {
    const upperSymbol = symbol.toUpperCase();
    const data = this.marketData.get(upperSymbol);

    // Map old type names to new ones for backward compatibility
    const typeMap = {
      askPrice: "offer",
      bidPrice: "bid"
    };
    
    const actualType = typeMap[type] || type;

    if (data && data[actualType] !== undefined) {
      return data[actualType];
    }

    // Fallback values based on symbol
    if (upperSymbol === "GOLD") {
      return actualType === "offer" || actualType === "askPrice"
        ? CONFIG.BACKUP_GOLD_PRICE
        : CONFIG.BACKUP_GOLD_PRICE * 0.99;
    }

    return null;
  }

  // Calculate TTB price from gold price - using optimized calculation
  calculateTTBPrice(goldPrice) {
    if (!goldPrice || isNaN(goldPrice)) return null;

    // Pre-calculate combined conversion factor for better performance
    const TTB_CONVERSION_FACTOR =
      (CONFIG.GOLD_CONVERSION_FACTOR * CONFIG.TTB_FACTOR) / CONFIG.TROY_OUNCE_GRAMS;
    return goldPrice * TTB_CONVERSION_FACTOR;
  }

  // Get current TTB prices based on gold prices
  getTTBPrices() {
    // Try to get gold data
    const goldData = this.getMarketData("GOLD");
    const now = Date.now();

    // If we have gold data and it's fresh, calculate TTB from it
    if (goldData && this.isPriceFresh("GOLD")) {
      // Use offer/bid if available, fall back to askPrice/bidPrice
      const offerPrice = goldData.offer || goldData.askPrice;
      const bidPrice = goldData.bid || goldData.bidPrice;
      
      const ttbAskPrice = this.calculateTTBPrice(offerPrice);
      const ttbBidPrice = this.calculateTTBPrice(bidPrice);

      return {
        askPrice: ttbAskPrice,
        bidPrice: ttbBidPrice,
        offer: ttbAskPrice,  // For new naming convention
        bid: ttbBidPrice,    // For new naming convention
        timestamp: this.lastUpdated.get("GOLD"),
      };
    }

    // If we don't have fresh gold data, use backup gold price
    const backupTTBAskPrice = this.calculateTTBPrice(CONFIG.BACKUP_GOLD_PRICE);
    const backupTTBBidPrice = this.calculateTTBPrice(
      CONFIG.BACKUP_GOLD_PRICE * 0.99
    );

    return {
      askPrice: backupTTBAskPrice,
      bidPrice: backupTTBBidPrice,
      offer: backupTTBAskPrice,  // For new naming convention
      bid: backupTTBBidPrice,    // For new naming convention
      timestamp: now - CONFIG.PRICE_FRESHNESS_TIMEOUT - 1000, // Mark as stale
    };
  }

  // Generate a unique order ID using more performant methods
  generateEntryId = () => {
     const timestamp = Date.now().toString();
    const randomStr = Math.random().toString(36).substring(2, 7).toUpperCase();
    return `OR-${timestamp.substring(timestamp.length - 7)}`
  };
  
  // Optimized subscription management
  subscribe(callback) {
    if (typeof callback !== "function") {
      console.warn(
        "Market Data Service: Attempted to subscribe with non-function callback"
      );
      return null;
    }

    this.subscribers.add(callback);

    // Return unsubscribe function
    return () => this.unsubscribe(callback);
  }

  unsubscribe(callback) {
    this.subscribers.delete(callback);
  }

  notifySubscribers(symbol, data) {
    for (const callback of this.subscribers) {
      try {
        callback(symbol, data);
      } catch (error) {
        console.error("Error in market data subscriber callback:", error);
      }
    }
  }
}

// Create and export singleton instance
const marketDataService = new MarketDataService();

// Export service instance and utility functions
export default marketDataService;

// Export utility functions for easy import elsewhere
export const isPriceFresh = (symbol) => marketDataService.isPriceFresh(symbol);
export const getCurrentPrices = () => marketDataService.getCurrentPrices();
export const getLivePrice = (symbol, type) =>
  marketDataService.getLivePrice(symbol, type);
export const generateOrderId = (prefix) =>
  marketDataService.generateEntryId(prefix);