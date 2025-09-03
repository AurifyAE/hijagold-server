import mt5Service from "./mt5Service.js";
import dotenv from "dotenv";
dotenv.config();
class OptimizedMT5MarketDataService {
  constructor() {
    this.symbols = [process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"];
    this.symbolMapping = { GOLD: process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix" };
    this.priceUpdateInterval = null;
    this.isUpdating = false;
    this.activeSubscribers = new Set();
    this.lastActivity = Date.now();

    this.UPDATE_INTERVAL = 10000;
    this.INACTIVE_TIMEOUT = 300000;
    this.BATCH_SIZE = 1;
    this.PRICE_CACHE_TTL = 15000;
    this.CONNECTION_RETRY_DELAY = 30000;

    this.autoScaleMode = process.env.AWS_AUTO_SCALE === "true";
    this.region = process.env.AWS_REGION || "us-east-1";
    this.instanceType = process.env.AWS_INSTANCE_TYPE || "t3.micro";

    this.initializeMT5();
    this.setupInactivityMonitor();
  }

  async initializeMT5() {
    try {
      console.log("Initializing MT5 connection...");
      await mt5Service.connect();
      console.log("MT5 connected successfully");

      // Validate the symbol exists and get available symbols for debugging
      try {
        const availableSymbols = await mt5Service.getSymbols();
        console.log(
          "Available symbols containing XAU:",
          availableSymbols.filter((s) => s.includes("XAU"))
        );

        // Try to validate our symbol - now with proper URL encoding support
        const validatedSymbol = await mt5Service.validateSymbol(
          process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"
        );
        console.log("Validated symbol:", validatedSymbol);

        // Update our symbols array with the validated symbol
        this.symbols = [validatedSymbol];
        this.symbolMapping = { GOLD: validatedSymbol };
      } catch (symbolError) {
        console.warn("Symbol validation warning:", symbolError.message);
        // Keep original symbols if validation fails - they should work now with URL encoding
      }

      this.startSmartPriceUpdates();
    } catch (error) {
      console.error("MT5 initialization failed:", error);
      const delay = Math.min(
        this.CONNECTION_RETRY_DELAY *
          Math.pow(1.5, this.connectionRetries || 0),
        300000
      );
      this.connectionRetries = (this.connectionRetries || 0) + 1;
      setTimeout(() => this.initializeMT5(), delay);
    }
  }

  startSmartPriceUpdates() {
    if (this.priceUpdateInterval) return;

    console.log(
      `Smart price updates started (${this.UPDATE_INTERVAL}ms interval)`
    );

    this.priceUpdateInterval = setInterval(async () => {
      if (this.isUpdating) return;

      this.isUpdating = true;
      try {
        const symbolsToUpdate = this.symbols.filter((symbol) => {
          const mapped = this.symbolMapping[symbol] || symbol;
          return !mt5Service.isPriceFresh(mapped, this.PRICE_CACHE_TTL);
        });

        if (symbolsToUpdate.length === 0) {
          console.log("All prices are fresh, skipping update");
          return;
        }

        console.log(`Updating ${symbolsToUpdate.length} symbols`);
        await this.processMinimalUpdates(symbolsToUpdate);
      } catch (error) {
        console.error("Price update failed:", error);
        // Don't increase interval too aggressively on errors now that URL encoding is fixed
        this.UPDATE_INTERVAL = Math.min(this.UPDATE_INTERVAL * 1.1, 30000);
      } finally {
        this.isUpdating = false;
      }
    }, this.UPDATE_INTERVAL);
  }

  async forcePriceUpdate(symbol) {
    const mapped = this.symbolMapping[symbol] || symbol;
    try {
      console.log(`Forcing price update for symbol: ${mapped}`);
      const priceData = await mt5Service.getPrice(mapped);
      console.log(
        `Forced price update for ${mapped}: bid=${priceData.bid}, ask=${priceData.ask}`
      );
      return priceData;
    } catch (error) {
      console.error(`Forced price update failed for ${mapped}:`, error.message);
      throw error;
    }
  }

  async processMinimalUpdates(symbols) {
    for (const symbol of symbols) {
      const mapped = this.symbolMapping[symbol] || symbol;
      try {
        console.log(`Processing price update for: ${mapped}`);
        const priceData = await mt5Service.getPrice(mapped);
        console.log(
          `Price updated for ${mapped}: ${priceData.bid}/${priceData.ask} (spread: ${priceData.spread})`
        );
        // Small delay between requests to avoid overwhelming the API
        await new Promise((resolve) => setTimeout(resolve, 50));
      } catch (error) {
        console.error(`Price update failed for ${mapped}:`, error.message);
        // Continue with other symbols even if one fails
      }
    }
  }

  addSubscriber(clientId) {
    this.activeSubscribers.add(clientId);
    this.lastActivity = Date.now();
    console.log(
      `Added subscriber ${clientId}. Total: ${this.activeSubscribers.size}`
    );

    if (this.activeSubscribers.size === 1 && this.autoScaleMode) {
      this.scaleUp();
    }
  }

  removeSubscriber(clientId) {
    this.activeSubscribers.delete(clientId);
    this.lastActivity = Date.now();
    console.log(
      `Removed subscriber ${clientId}. Total: ${this.activeSubscribers.size}`
    );
  }

  setupInactivityMonitor() {
    if (!this.autoScaleMode) return;

    setInterval(() => {
      const inactiveTime = Date.now() - this.lastActivity;

      if (
        this.activeSubscribers.size === 0 &&
        inactiveTime > this.INACTIVE_TIMEOUT
      ) {
        console.log("No activity detected, preparing for scale-down");
        this.prepareForScaleDown();
      }
    }, 60000);
  }

  scaleUp() {
    console.log("Scaling up: Increasing update frequency");
    this.UPDATE_INTERVAL = Math.max(this.UPDATE_INTERVAL * 0.8, 5000);
    this.restartPriceUpdates();
  }

  prepareForScaleDown() {
    console.log("Preparing for scale-down: Reducing resource usage");
    this.UPDATE_INTERVAL = 30000;
    this.restartPriceUpdates();

    if (process.env.AWS_ASG_NAME) {
      this.signalScaleDown();
    }
  }

  restartPriceUpdates() {
    if (this.priceUpdateInterval) {
      clearInterval(this.priceUpdateInterval);
      this.priceUpdateInterval = null;
    }
    this.startSmartPriceUpdates();
  }

  async signalScaleDown() {
    try {
      console.log("Signaling AWS Auto Scaling for scale-down");
      // AWS scaling logic would go here
    } catch (error) {
      console.error("Failed to signal scale-down:", error);
    }
  }

  async getMarketData(
    symbol = process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix",
    clientId = null
  ) {
    if (clientId) {
      this.addSubscriber(clientId);
    }

    const mapped = this.symbolMapping[symbol] || symbol;
    console.log(`Getting market data for symbol: ${mapped}`);

    let data = mt5Service.getCachedPrice(mapped);

    if (!data || !mt5Service.isPriceFresh(mapped, this.PRICE_CACHE_TTL)) {
      try {
        console.log(
          `Cache miss or stale data for ${mapped}, fetching fresh data`
        );
        data = await this.forcePriceUpdate(mapped);
      } catch (error) {
        console.error(`Fresh data fetch failed for ${mapped}:`, error.message);
        if (data) {
          console.warn(`Using stale cached data for ${mapped}`);
        } else {
          console.error(`No data available for ${mapped}`);
          return null;
        }
      }
    } else {
      console.log(`Using fresh cached data for ${mapped}`);
    }

    return data
      ? {
          symbol: mapped,
          bid: data.bid,
          ask: data.ask,
          spread: data.spread,
          timestamp: data.time,
          isFresh: mt5Service.isPriceFresh(mapped, this.PRICE_CACHE_TTL),
          source:
            data === mt5Service.getCachedPrice(mapped) ? "cache" : "fresh",
        }
      : null;
  }

  async getOpenPositions(phoneNumber, clientId = null) {
    if (clientId) {
      this.addSubscriber(clientId);
    }

    try {
      console.log(`Fetching positions for phone number: ${phoneNumber}`);
      const positions = await mt5Service.getPositions();
      if (!positions || !Array.isArray(positions)) {
        console.warn("No positions returned or invalid format");
        return [];
      }

      const filteredPositions = positions
        .filter((p) => p.comment && p.comment.includes(phoneNumber))
        .map((p) => ({
          orderId: p.ticket,
          type: p.type,
          volume: p.volume,
          openPrice: p.price_open,
          currentPrice: p.price_current,
          profit: p.profit,
          symbol: p.symbol,
        }));

      console.log(
        `Found ${filteredPositions.length} positions for ${phoneNumber}`
      );
      return filteredPositions;
    } catch (error) {
      console.error("Positions fetch failed:", error.message);
      return [];
    }
  }

  getResourceUsage() {
    const uptime = process.uptime();
    const memUsage = process.memoryUsage();

    return {
      uptime: uptime,
      memory: {
        used: Math.round(memUsage.heapUsed / 1024 / 1024),
        total: Math.round(memUsage.heapTotal / 1024 / 1024),
      },
      activeSubscribers: this.activeSubscribers.size,
      updateInterval: this.UPDATE_INTERVAL,
      connectionRetries: this.connectionRetries || 0,
      lastActivity: new Date(this.lastActivity).toISOString(),
      autoScaleMode: this.autoScaleMode,
      currentSymbols: this.symbols,
      symbolMapping: this.symbolMapping,
    };
  }

  getHealthStatus() {
    const isHealthy =
      mt5Service.isConnected && Date.now() - this.lastActivity < 600000;

    return {
      status: isHealthy ? "healthy" : "unhealthy",
      connected: mt5Service.isConnected,
      subscribers: this.activeSubscribers.size,
      lastActivity: this.lastActivity,
      uptime: process.uptime(),
      symbols: this.symbols,
      symbolMapping: this.symbolMapping,
    };
  }

  async shutdown() {
    console.log("Initiating graceful shutdown...");

    if (this.priceUpdateInterval) {
      clearInterval(this.priceUpdateInterval);
      this.priceUpdateInterval = null;
    }

    this.activeSubscribers.clear();

    try {
      await mt5Service.disconnect();
      console.log("MT5 disconnected");
    } catch (error) {
      console.error("Error during MT5 disconnect:", error);
    }

    console.log("Market data service shut down gracefully");
  }
}

const optimizedMT5MarketDataService = new OptimizedMT5MarketDataService();

// Graceful shutdown handlers
process.on("SIGTERM", async () => {
  console.log("Received SIGTERM, shutting down gracefully");
  await optimizedMT5MarketDataService.shutdown();
  process.exit(0);
});

process.on("SIGINT", async () => {
  console.log("Received SIGINT, shutting down gracefully");
  await optimizedMT5MarketDataService.shutdown();
  process.exit(0);
});

export default optimizedMT5MarketDataService;
