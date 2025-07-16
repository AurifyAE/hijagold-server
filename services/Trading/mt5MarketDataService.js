import mt5Service from './mt5Service.js';

class OptimizedMT5MarketDataService {
  constructor() {
    this.symbols = ['XAUUSD_TTBAR.Fix'];
    this.symbolMapping = { 'GOLD': 'XAUUSD_TTBAR.Fix' };
    this.priceUpdateInterval = null;
    this.isUpdating = false;
    this.activeSubscribers = new Set();
    this.lastActivity = Date.now();
    
    this.UPDATE_INTERVAL = 10000;
    this.INACTIVE_TIMEOUT = 300000;
    this.BATCH_SIZE = 1;
    this.PRICE_CACHE_TTL = 15000;
    this.CONNECTION_RETRY_DELAY = 30000;
    
    this.autoScaleMode = process.env.AWS_AUTO_SCALE === 'true';
    this.region = process.env.AWS_REGION || 'us-east-1';
    this.instanceType = process.env.AWS_INSTANCE_TYPE || 't3.micro';
    
    this.initializeMT5();
    this.setupInactivityMonitor();
  }

  async initializeMT5() {
    try {
      console.log('Initializing MT5 connection...');
      await mt5Service.connect();
      console.log('MT5 connected successfully');
      this.startSmartPriceUpdates();
    } catch (error) {
      console.error('MT5 initialization failed:', error);
      const delay = Math.min(this.CONNECTION_RETRY_DELAY * Math.pow(1.5, this.connectionRetries || 0), 300000);
      this.connectionRetries = (this.connectionRetries || 0) + 1;
      setTimeout(() => this.initializeMT5(), delay);
    }
  }

  startSmartPriceUpdates() {
    if (this.priceUpdateInterval) return;
    
    this.priceUpdateInterval = setInterval(async () => {
      if (this.isUpdating) return;
      
      this.isUpdating = true;
      try {
        const symbolsToUpdate = this.symbols.filter(symbol => {
          const mapped = this.symbolMapping[symbol] || symbol;
          return !mt5Service.isPriceFresh(mapped, this.PRICE_CACHE_TTL);
        });

        if (symbolsToUpdate.length === 0) {
          console.log('All prices are fresh, skipping update');
          return;
        }

        console.log(`Updating ${symbolsToUpdate.length} symbols`);
        await this.processMinimalUpdates(symbolsToUpdate);
        
      } catch (error) {
        console.error('Price update failed:', error);
        this.UPDATE_INTERVAL = Math.min(this.UPDATE_INTERVAL * 1.2, 30000);
      } finally {
        this.isUpdating = false;
      }
    }, this.UPDATE_INTERVAL);
    
    console.log(`Smart price updates started (${this.UPDATE_INTERVAL}ms interval)`);
  }

  async forcePriceUpdate(symbol) {
    const mapped = this.symbolMapping[symbol] || symbol;
    try {
      const priceData = await mt5Service.getPrice(mapped);
      console.log(`Forced price update for ${mapped}: ${JSON.stringify(priceData)}`);
      return priceData;
    } catch (error) {
      console.error(`Forced price update failed for ${mapped}:`, error);
      throw error;
    }
  }

  async processMinimalUpdates(symbols) {
    for (const symbol of symbols) {
      const mapped = this.symbolMapping[symbol] || symbol;
      try {
        const priceData = await mt5Service.getPrice(mapped);
        console.log(`Price updated for ${mapped}: ${priceData.bid}/${priceData.ask}`);
        await new Promise(resolve => setTimeout(resolve, 50));
      } catch (error) {
        console.error(`Price update failed for ${mapped}:`, error);
      }
    }
  }

  addSubscriber(clientId) {
    this.activeSubscribers.add(clientId);
    this.lastActivity = Date.now();
    console.log(`Added subscriber ${clientId}. Total: ${this.activeSubscribers.size}`);
    
    if (this.activeSubscribers.size === 1 && this.autoScaleMode) {
      this.scaleUp();
    }
  }

  removeSubscriber(clientId) {
    this.activeSubscribers.delete(clientId);
    this.lastActivity = Date.now();
    console.log(`Removed subscriber ${clientId}. Total: ${this.activeSubscribers.size}`);
  }

  setupInactivityMonitor() {
    if (!this.autoScaleMode) return;
    
    setInterval(() => {
      const inactiveTime = Date.now() - this.lastActivity;
      
      if (this.activeSubscribers.size === 0 && inactiveTime > this.INACTIVE_TIMEOUT) {
        console.log('No activity detected, preparing for scale-down');
        this.prepareForScaleDown();
      }
    }, 60000);
  }

  scaleUp() {
    console.log('Scaling up: Increasing update frequency');
    this.UPDATE_INTERVAL = Math.max(this.UPDATE_INTERVAL * 0.8, 5000);
    this.restartPriceUpdates();
  }

  prepareForScaleDown() {
    console.log('Preparing for scale-down: Reducing resource usage');
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
      console.log('Signaling AWS Auto Scaling for scale-down');
    } catch (error) {
      console.error('Failed to signal scale-down:', error);
    }
  }

  async getMarketData(symbol = 'XAUUSD_TTBAR.Fix', clientId = null) {
    if (clientId) {
      this.addSubscriber(clientId);
    }
    
    const mapped = this.symbolMapping[symbol] || symbol;
    
    let data = mt5Service.getCachedPrice(mapped);
    
    if (!data || !mt5Service.isPriceFresh(mapped, this.PRICE_CACHE_TTL)) {
      try {
        data = await this.forcePriceUpdate(mapped);
      } catch (error) {
        console.error(`Fresh data fetch failed for ${mapped}:`, error);
        if (data) {
          console.warn(`Using cached data for ${mapped}`);
        } else {
          return null;
        }
      }
    }
    
    return data ? { 
      symbol: mapped, 
      bid: data.bid, 
      ask: data.ask, 
      spread: data.spread, 
      timestamp: data.time,
      isFresh: mt5Service.isPriceFresh(mapped, this.PRICE_CACHE_TTL),
      source: data === mt5Service.getCachedPrice(mapped) ? 'cache' : 'fresh'
    } : null;
  }

  async getOpenPositions(phoneNumber, clientId = null) {
    if (clientId) {
      this.addSubscriber(clientId);
    }
    
    try {
      const positions = await mt5Service.getPositions();
      if (!positions || !Array.isArray(positions)) return [];
      
      return positions
        .filter(p => p.comment && p.comment.includes(phoneNumber))
        .map(p => ({
          orderId: p.ticket,
          type: p.type,
          volume: p.volume,
          openPrice: p.price_open,
          currentPrice: p.price_current,
          profit: p.profit,
          symbol: p.symbol
        }));
    } catch (error) {
      console.error('Positions fetch failed:', error);
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
        total: Math.round(memUsage.heapTotal / 1024 / 1024)
      },
      activeSubscribers: this.activeSubscribers.size,
      updateInterval: this.UPDATE_INTERVAL,
      connectionRetries: this.connectionRetries || 0,
      lastActivity: new Date(this.lastActivity).toISOString(),
      autoScaleMode: this.autoScaleMode
    };
  }

  getHealthStatus() {
    const isHealthy = this.isConnected && (Date.now() - this.lastActivity) < 600000;
    
    return {
      status: isHealthy ? 'healthy' : 'unhealthy',
      connected: this.isConnected,
      subscribers: this.activeSubscribers.size,
      lastActivity: this.lastActivity,
      uptime: process.uptime()
    };
  }

  async shutdown() {
    console.log('Initiating graceful shutdown...');
    
    if (this.priceUpdateInterval) {
      clearInterval(this.priceUpdateInterval);
      this.priceUpdateInterval = null;
    }
    
    this.activeSubscribers.clear();
    
    try {
      await mt5Service.disconnect();
      console.log('MT5 disconnected');
    } catch (error) {
      console.error('Error during MT5 disconnect:', error);
    }
    
    console.log('Market data service shut down gracefully');
  }
}

const optimizedMT5MarketDataService = new OptimizedMT5MarketDataService();

process.on('SIGTERM', async () => {
    console.log('Received SIGTERM, shutting down gracefully');
    await optimizedMT5MarketDataService.shutdown();
    process.exit(0);
});

process.on('SIGINT', async () => {
    console.log('Received SIGINT, shutting down gracefully');
    await optimizedMT5MarketDataService.shutdown();
    process.exit(0);
});

export default optimizedMT5MarketDataService;