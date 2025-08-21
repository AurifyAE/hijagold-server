import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';
import Order from '../models/OrderSchema.js'
import mt5Service from '../services/Trading/mt5Service.js';
import { SYMBOL_MAPPING } from '../config/symbols.js';

// Redis connection for BullMQ (blocking operations require maxRetriesPerRequest: null)
const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
  maxRetriesPerRequest: null, // Required for BullMQ blocking operations
  retryDelayOnFailover: 100,
  enableReadyCheck: false,
  maxRetriesPerRequest: null,
});

// Separate Redis connection for cache operations (non-blocking)
const cacheRedis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT || 6379,
  maxRetriesPerRequest: 3,
});

// Create queue
const stopLossTakeProfitQueue = new Queue('stop-loss-take-profit', {
  connection: redis,
  defaultJobOptions: {
    removeOnComplete: 100,
    removeOnFail: 50,
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 2000,
    },
  },
});

// Market data cache with TTL
class MarketDataCache {
  constructor() {
    this.cache = new Map();
    this.TTL = 5000; // 5 seconds TTL
  }

  set(symbol, data) {
    this.cache.set(symbol, {
      data,
      timestamp: Date.now(),
    });
  }

  get(symbol) {
    const cached = this.cache.get(symbol);
    if (!cached) return null;
    if (Date.now() - cached.timestamp > this.TTL) {
      this.cache.delete(symbol);
      return null;
    }
    return cached.data;
  }

  clear() {
    this.cache.clear();
  }
}

const marketDataCache = new MarketDataCache();

// Socket.io client for real-time market data
import io from 'socket.io-client';

const SOCKET_SERVER_URL = "https://mt5.aurify.ae";
const SECRET_KEY = "aurify@123";

class MarketDataManager {
  constructor() {
    this.socket = null;
    this.subscribedSymbols = new Set();
    this.isConnected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
  }

  initialize() {
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.disconnect();
    }

    this.socket = io(SOCKET_SERVER_URL, {
      query: { secret: SECRET_KEY },
      transports: ['websocket'],
      withCredentials: false,
      forceNew: true,
      reconnection: false,
      timeout: 20000,
    });

    this.socket.on('connect', () => {
      console.log('📊 Market data socket connected');
      this.isConnected = true;
      this.reconnectAttempts = 0;
      if (this.subscribedSymbols.size > 0) {
        this.socket.emit('request-data', Array.from(this.subscribedSymbols));
      }
    });

    this.socket.on('disconnect', (reason) => {
      console.log('📊 Market data socket disconnected:', reason);
      this.isConnected = false;
      if (reason !== 'io client disconnect') {
        this.attemptReconnect();
      }
    });

    this.socket.on('market-data', (data) => {
      if (data && data.symbol) {
        marketDataCache.set(data.symbol, data);
        console.log(`📊 Updated market data for ${data.symbol}: ${data.bid}/${data.ask}`);
      }
    });

    this.socket.on('connect_error', (error) => {
      console.error('📊 Market data connection error:', error);
      this.attemptReconnect();
    });
  }

  attemptReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('📊 Max reconnection attempts reached for market data');
      return;
    }
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
    this.reconnectAttempts++;
    console.log(`📊 Reconnecting to market data in ${delay}ms (attempt ${this.reconnectAttempts})`);
    setTimeout(() => this.socket?.connect(), delay);
  }

  subscribeToSymbols(symbols) {
    symbols.forEach(symbol => this.subscribedSymbols.add(symbol));
    if (this.isConnected) {
      this.socket.emit('request-data', Array.from(this.subscribedSymbols));
    }
  }

  unsubscribeFromSymbols(symbols) {
    symbols.forEach(symbol => this.subscribedSymbols.delete(symbol));
    if (this.isConnected) {
      this.socket.emit('stop-data', Array.from(symbols));
    }
  }

  getMarketData(symbol) {
    return marketDataCache.get(symbol);
  }

  disconnect() {
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.disconnect();
    }
    marketDataCache.clear();
  }
}

const marketDataManager = new MarketDataManager();
marketDataManager.initialize();

// Job processor
const processStopLossTakeProfit = async (job) => {
  const { batchSize = 50 } = job.data;

  console.log('🔄 Processing stop loss/take profit orders...');
  try {
    const orders = await Order.find({
      orderStatus: 'OPEN',
      isTradeSafe: true,
      $or: [{ stopLoss: { $gt: 0 } }, { takeProfit: { $gt: 0 } }]
    }).limit(batchSize).populate('user', 'balance').lean();

    if (orders.length === 0) {
      console.log('📊 No orders to process');
      return { processed: 0 };
    }

    console.log(`📊 Found ${orders.length} orders to check`);

    const symbols = [...new Set(orders.map(order => order.symbol))];
    marketDataManager.subscribeToSymbols(symbols);

    await new Promise(resolve => setTimeout(resolve, 500)); // Adjusted delay

    const processedOrders = [];
    const errors = [];

    for (const order of orders) {
      try {
        const shouldClose = await checkOrderForClosure(order);
        if (shouldClose) {
          await closeOrder(order, shouldClose.reason);
          processedOrders.push({
            orderNo: order.orderNo,
            reason: shouldClose.reason,
            price: shouldClose.price
          });
        }
      } catch (error) {
        console.error(`❌ Error processing order ${order.orderNo}:`, error);
        errors.push({ orderNo: order.orderNo, error: error.message });
      }
      await new Promise(resolve => setTimeout(resolve, 50)); // Small delay between orders
    }

    marketDataManager.unsubscribeFromSymbols(symbols);

    console.log(`✅ Processed ${processedOrders.length} orders, ${errors.length} errors`);
    return { processed: processedOrders.length, errors: errors.length, processedOrders, errors };
  } catch (error) {
    console.error('❌ Error in stop loss/take profit job:', error);
    throw error;
  }
};

// Check if order should be closed
const checkOrderForClosure = async (order) => {
  const marketData = marketDataManager.getMarketData(order.symbol);
  if (!marketData || !marketData.bid || !marketData.ask) {
    console.log(`⚠️ No market data available for ${order.symbol}`);
    return null;
  }

  const { bid, ask } = marketData;
  const currentPrice = order.type === 'BUY' ? bid : ask;

  if (order.stopLoss > 0) {
    const shouldCloseOnStopLoss = order.type === 'BUY' ? bid <= order.stopLoss : ask >= order.stopLoss;
    if (shouldCloseOnStopLoss) {
      console.log(`🛑 Stop Loss triggered for order ${order.orderNo}: ${currentPrice} vs SL ${order.stopLoss}`);
      return { reason: 'STOP_LOSS', price: currentPrice, marketPrice: currentPrice };
    }
  }

  if (order.takeProfit > 0) {
    const shouldCloseOnTakeProfit = order.type === 'BUY' ? bid >= order.takeProfit : ask <= order.takeProfit;
    if (shouldCloseOnTakeProfit) {
      console.log(`🎯 Take Profit triggered for order ${order.orderNo}: ${currentPrice} vs TP ${order.takeProfit}`);
      return { reason: 'TAKE_PROFIT', price: currentPrice, marketPrice: currentPrice };
    }
  }

  return null;
};

// Close order function
const closeOrder = async (order, reason) => {
  try {
    console.log(`🔄 Closing order ${order.orderNo} due to ${reason}`);
    const mt5CloseData = {
      ticket: order.ticket,
      symbol: SYMBOL_MAPPING[order.symbol] || order.symbol,
      volume: parseFloat(order.volume),
      type: order.type === 'BUY' ? 'SELL' : 'BUY',
      openingPrice: parseFloat(order.openingPrice),
    };

    const mt5CloseResult = await mt5Service.closeTrade(mt5CloseData);
    if (!mt5CloseResult.success) throw new Error(`MT5 close failed: ${mt5CloseResult.message}`);

    const marketData = marketDataManager.getMarketData(order.symbol);
    const closingPrice = order.type === 'BUY' ? marketData.bid : marketData.ask;
    const profit = order.type === 'BUY' ? (closingPrice - order.openingPrice) * order.volume : (order.openingPrice - closingPrice) * order.volume;

    const updatedOrder = await Order.findByIdAndUpdate(
      order._id,
      {
        orderStatus: 'CLOSED',
        closingPrice,
        closingDate: new Date(),
        profit,
        comment: `Auto-closed: ${reason}`
      },
      { new: true }
    );

    console.log(`✅ Order ${order.orderNo} closed successfully. Profit: ${profit}`);
    return updatedOrder;
  } catch (error) {
    console.error(`❌ Failed to close order ${order.orderNo}:`, error);
    await Order.findByIdAndUpdate(order._id, { notificationError: `Auto-close failed: ${error.message}` });
    throw error;
  }
};

// Worker to process the jobs
const worker = new Worker('stop-loss-take-profit', processStopLossTakeProfit, {
  connection: redis,
  concurrency: 1,
  limiter: {
    max: 10,
    duration: 60000,
  },
});

worker.on('completed', (job, result) => {
  console.log(`✅ Stop Loss/Take Profit job completed: ${JSON.stringify(result)}`);
});

worker.on('failed', (job, err) => {
  console.error(`❌ Stop Loss/Take Profit job failed:`, err);
});

worker.on('error', (err) => {
  console.error('❌ Worker error:', err);
});

// Graceful shutdown
const gracefulShutdown = async () => {
  console.log('🛑 Shutting down Stop Loss/Take Profit worker...');
  try {
    marketDataManager.disconnect();
    await worker.close();
    await redis.quit();
    await cacheRedis.quit();
    console.log('✅ Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

export const scheduleStopLossTakeProfitCheck = async (intervalSeconds = 5) => {
  try {
    await stopLossTakeProfitQueue.add(
      'check-orders',
      { batchSize: 50 },
      {
        repeat: { every: intervalSeconds * 1000 },
        jobId: 'sltp-check',
      }
    );
    console.log(`📊 Stop Loss/Take Profit checker scheduled every ${intervalSeconds} seconds`);
  } catch (error) {
    console.error('❌ Error scheduling stop loss/take profit check:', error);
    throw error;
  }
};

export const triggerStopLossTakeProfitCheck = async (batchSize = 50) => {
  try {
    await stopLossTakeProfitQueue.add('check-orders-manual', { batchSize });
    console.log('🔄 Manual Stop Loss/Take Profit check triggered');
  } catch (error) {
    console.error('❌ Error triggering manual check:', error);
    throw error;
  }
};

export { stopLossTakeProfitQueue, worker };