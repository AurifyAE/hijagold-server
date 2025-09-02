import mt5Service from "./mt5Service.js";

class OptimizedMT5MarketDataService {
  constructor() {
    this.accountKeys = Array.from(mt5Service.connections.keys());
    this.symbolMappings = new Map();
    for (const [key, conn] of mt5Service.connections.entries()) {
      this.symbolMappings.set(key, { GOLD: conn.symbol });
    }
    this.priceUpdateIntervals = new Map();
    this.isUpdating = new Map();
    this.activeSubscribers = new Set();
    this.lastActivity = Date.now();
    this.connectionRetries = new Map();

    this.UPDATE_INTERVAL = 10000;
    this.INACTIVE_TIMEOUT = 300000;
    this.PRICE_CACHE_TTL = 15000;
    this.CONNECTION_RETRY_DELAY = 30000;
    this.MAX_CONNECTION_RETRIES = 5;

    this.autoScaleMode = process.env.AWS_AUTO_SCALE === "true";
    this.region = process.env.AWS_REGION || "us-east-1";
    this.instanceType = process.env.AWS_INSTANCE_TYPE || "t3.micro";

    this.initializeMT5();
    this.setupInactivityMonitor();
  }

  async initializeMT5() {
    console.log(
      `Initializing MT5 connections for ${this.accountKeys.length} accounts...`
    );
    for (const accKey of this.accountKeys) {
      try {
        await mt5Service.connect(accKey);
        const conn = mt5Service.connections.get(accKey);
        const sym = conn.symbol;
        const validated = await mt5Service.validateSymbol(accKey, sym);
        conn.symbol = validated; // Update the default symbol to the validated one to prevent mismatches
        this.symbolMappings.get(accKey).GOLD = validated;
        console.log(`Validated symbol for ${accKey}: ${validated}`);
        this.startSmartPriceUpdates(accKey);
      } catch (error) {
        console.error(`MT5 initialization failed for ${accKey}:`, error);
        const retries = (this.connectionRetries.get(accKey) || 0) + 1;
        if (retries > this.MAX_CONNECTION_RETRIES) {
          console.error(`Max retries reached for ${accKey}`);
          continue;
        }
        const delay = Math.min(
          this.CONNECTION_RETRY_DELAY * Math.pow(1.5, retries),
          300000
        );
        this.connectionRetries.set(accKey, retries);
        setTimeout(() => this.initializeMT5(), delay);
      }
    }
  }

  startSmartPriceUpdates(accKey) {
    if (this.priceUpdateIntervals.get(accKey)) return;

    console.log(
      `Smart price updates started for ${accKey} (${this.UPDATE_INTERVAL}ms interval)`
    );
    this.isUpdating.set(accKey, false);

    const interval = setInterval(async () => {
      if (this.isUpdating.get(accKey)) return;

      this.isUpdating.set(accKey, true);
      try {
        const symbol = this.symbolMappings.get(accKey).GOLD;
        if (!mt5Service.isPriceFresh(accKey, symbol, this.PRICE_CACHE_TTL)) {
          console.log(`Updating symbol ${symbol} for account ${accKey}`);
          await mt5Service.getPrice(accKey, symbol);
        } else {
          console.log(`Price fresh for ${symbol} on ${accKey}, skipping`);
        }
      } catch (error) {
        console.error(`Price update failed for ${accKey}:`, error);
        this.UPDATE_INTERVAL = Math.min(this.UPDATE_INTERVAL * 1.1, 30000);
      } finally {
        this.isUpdating.set(accKey, false);
      }
    }, this.UPDATE_INTERVAL);

    this.priceUpdateIntervals.set(accKey, interval);
  }

  async forcePriceUpdate(accKey, symbol) {
    const mapped = this.symbolMappings.get(accKey).GOLD;
    try {
      console.log(`Forcing price update for symbol: ${mapped} on ${accKey}`);
      const priceData = await mt5Service.getPrice(accKey, mapped);
      console.log(
        `Forced price update for ${mapped} on ${accKey}: bid=${priceData.bid}, ask=${priceData.ask}`
      );
      return priceData;
    } catch (error) {
      console.error(
        `Forced price update failed for ${mapped} on ${accKey}:`,
        error.message
      );
      throw error;
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
    for (const accKey of this.accountKeys) {
      this.restartPriceUpdates(accKey);
    }
  }

  prepareForScaleDown() {
    console.log("Preparing for scale-down: Reducing resource usage");
    this.UPDATE_INTERVAL = 30000;
    for (const accKey of this.accountKeys) {
      this.restartPriceUpdates(accKey);
    }

    if (process.env.AWS_ASG_NAME) {
      this.signalScaleDown();
    }
  }

  async signalScaleDown() {
    try {
      console.log("Signaling AWS Auto Scaling for scale-down");
      // Implement AWS scaling logic here
    } catch (error) {
      console.error("Failed to signal scale-down:", error);
    }
  }

  restartPriceUpdates(accKey) {
    const interval = this.priceUpdateIntervals.get(accKey);
    if (interval) {
      clearInterval(interval);
      this.priceUpdateIntervals.delete(accKey);
    }
    this.startSmartPriceUpdates(accKey);
  }

  async getMarketData(accKey, symbol = "GOLD", clientId = null) {
    if (!this.accountKeys.includes(accKey)) {
      throw new Error(`Unknown account ${accKey}`);
    }
    if (clientId) {
      this.addSubscriber(clientId);
    }

    const mapped = this.symbolMappings.get(accKey)[symbol];
    console.log(`Getting market data for symbol: ${mapped} on ${accKey}`);

    let data = mt5Service.getCachedPrice(accKey, mapped);

    if (
      !data ||
      !mt5Service.isPriceFresh(accKey, mapped, this.PRICE_CACHE_TTL)
    ) {
      try {
        console.log(
          `Cache miss or stale data for ${mapped} on ${accKey}, fetching fresh data`
        );
        data = await this.forcePriceUpdate(accKey, mapped);
      } catch (error) {
        console.error(
          `Fresh data fetch failed for ${mapped} on ${accKey}:`,
          error.message
        );
        if (data) {
          console.warn(`Using stale cached data for ${mapped} on ${accKey}`);
        } else {
          console.error(`No data available for ${mapped} on ${accKey}`);
          return null;
        }
      }
    } else {
      console.log(`Using fresh cached data for ${mapped} on ${accKey}`);
    }

    return data
      ? {
          symbol: mapped,
          bid: data.bid,
          ask: data.ask,
          spread: data.spread,
          timestamp: data.time,
          high: data.high,
          low: data.low,
          marketStatus: data.marketStatus,
          isFresh: mt5Service.isPriceFresh(
            accKey,
            mapped,
            this.PRICE_CACHE_TTL
          ),
          source:
            data === mt5Service.getCachedPrice(accKey, mapped)
              ? "cache"
              : "fresh",
          account: accKey,
        }
      : null;
  }

  async getOpenPositions(phoneNumber, clientId = null) {
    if (clientId) {
      this.addSubscriber(clientId);
    }

    try {
      console.log(
        `Fetching positions for phone number: ${phoneNumber} across all accounts`
      );
      const allPositions = [];
      for (const accKey of this.accountKeys) {
        try {
          const positions = await mt5Service.getPositions(accKey);
          if (!positions || !Array.isArray(positions)) {
            console.warn(
              `No positions returned or invalid format for ${accKey}`
            );
            continue;
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
              account: accKey,
            }));

          console.log(
            `Found ${filteredPositions.length} positions for ${phoneNumber} on ${accKey}`
          );
          allPositions.push(...filteredPositions);
        } catch (error) {
          console.error(`Positions fetch failed for ${accKey}:`, error.message);
        }
      }
      return allPositions;
    } catch (error) {
      console.error("Positions fetch failed across accounts:", error.message);
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
      connectionRetries: Object.fromEntries(this.connectionRetries),
      lastActivity: new Date(this.lastActivity).toISOString(),
      autoScaleMode: this.autoScaleMode,
      accountKeys: this.accountKeys,
      symbolMappings: Object.fromEntries(this.symbolMappings),
    };
  }

  getHealthStatus() {
    const connectedAccounts = this.accountKeys.filter(
      (key) => mt5Service.connections.get(key)?.isConnected
    ).length;
    const isHealthy =
      connectedAccounts > 0 && Date.now() - this.lastActivity < 600000;

    return {
      status: isHealthy ? "healthy" : "unhealthy",
      connectedAccounts: connectedAccounts,
      totalAccounts: this.accountKeys.length,
      subscribers: this.activeSubscribers.size,
      lastActivity: this.lastActivity,
      uptime: process.uptime(),
      symbolMappings: Object.fromEntries(this.symbolMappings),
    };
  }

  async shutdown() {
    console.log("Initiating graceful shutdown...");

    for (const [accKey, interval] of this.priceUpdateIntervals.entries()) {
      clearInterval(interval);
      this.priceUpdateIntervals.delete(accKey);
    }

    this.activeSubscribers.clear();

    for (const accKey of this.accountKeys) {
      try {
        await mt5Service.disconnect(accKey);
        console.log(`Disconnected ${accKey}`);
      } catch (error) {
        console.error(`Error during disconnect for ${accKey}:`, error);
      }
    }

    console.log("Market data service shut down gracefully");
  }
}

const optimizedMT5MarketDataService = new OptimizedMT5MarketDataService();

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
