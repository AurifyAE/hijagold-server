import {
  scheduleStopLossTakeProfitCheck,
  triggerStopLossTakeProfitCheck,
} from "../../jobs/stopLossTakeProfitJob.js";
import Order from "../../models/OrderSchema.js";

class StopLossTakeProfitService {
  constructor() {
    this.isRunning = false;
    this.config = {
      checkIntervalSeconds: 5,
      batchSize: 50,
      enableAutoScheduling: true,
    };
  }

  async initialize(options = {}) {
    try {
      this.config = { ...this.config, ...options };
      console.log("🚀 Initializing Stop Loss/Take Profit Service...");
      if (this.config.enableAutoScheduling) {
        await this.startScheduler();
      }
      this.isRunning = true;
      console.log("✅ Stop Loss/Take Profit Service initialized successfully");
      return true;
    } catch (error) {
      console.error(
        "❌ Failed to initialize Stop Loss/Take Profit Service:",
        error
      );
      throw error;
    }
  }

  async startScheduler() {
    try {
      await scheduleStopLossTakeProfitCheck(this.config.checkIntervalSeconds);
      console.log(
        `⏰ Scheduler started - checking every ${this.config.checkIntervalSeconds} seconds`
      );
    } catch (error) {
      console.error("❌ Failed to start scheduler:", error);
      throw error;
    }
  }

  async triggerCheck(batchSize = null) {
    try {
      const size = batchSize || this.config.batchSize;
      await triggerStopLossTakeProfitCheck(size);
      console.log(`🔄 Manual check triggered for batch size: ${size}`);
      return true;
    } catch (error) {
      console.error("❌ Failed to trigger manual check:", error);
      throw error;
    }
  }

  async getMonitoringStats() {
    try {
      const stats = await Order.aggregate([
        {
          $match: {
            orderStatus: "OPEN",
            isTradeSafe: true,
            $or: [{ stopLoss: { $gt: 0 } }, { takeProfit: { $gt: 0 } }],
          },
        },
        {
          $group: {
            _id: null,
            totalOrders: { $sum: 1 },
            ordersWithStopLoss: {
              $sum: { $cond: [{ $gt: ["$stopLoss", 0] }, 1, 0] },
            },
            ordersWithTakeProfit: {
              $sum: { $cond: [{ $gt: ["$takeProfit", 0] }, 1, 0] },
            },
            ordersWithBoth: {
              $sum: {
                $cond: [
                  {
                    $and: [
                      { $gt: ["$stopLoss", 0] },
                      { $gt: ["$takeProfit", 0] },
                    ],
                  },
                  1,
                  0,
                ],
              },
            },
            symbolBreakdown: { $push: "$symbol" },
          },
        },
        {
          $project: {
            _id: 0,
            totalOrders: 1,
            ordersWithStopLoss: 1,
            ordersWithTakeProfit: 1,
            ordersWithBoth: 1,
            uniqueSymbols: { $size: { $setUnion: ["$symbolBreakdown", []] } },
          },
        },
      ]);

      const result = stats[0] || {
        totalOrders: 0,
        ordersWithStopLoss: 0,
        ordersWithTakeProfit: 0,
        ordersWithBoth: 0,
        uniqueSymbols: 0,
      };
      const symbolStats = await Order.aggregate([
        {
          $match: {
            orderStatus: "OPEN",
            isTradeSafe: true,
            $or: [{ stopLoss: { $gt: 0 } }, { takeProfit: { $gt: 0 } }],
          },
        },
        {
          $group: {
            _id: "$symbol",
            count: { $sum: 1 },
            buyOrders: { $sum: { $cond: [{ $eq: ["$type", "BUY"] }, 1, 0] } },
            sellOrders: { $sum: { $cond: [{ $eq: ["$type", "SELL"] }, 1, 0] } },
          },
        },
        { $sort: { count: -1 } },
      ]);

      return {
        ...result,
        symbolBreakdown: symbolStats,
        lastUpdated: new Date(),
        serviceStatus: this.isRunning ? "RUNNING" : "STOPPED",
      };
    } catch (error) {
      console.error("❌ Failed to get monitoring stats:", error);
      throw error;
    }
  }

  async getClosureHistory(limit = 50) {
    try {
      const closedOrders = await Order.find({
        orderStatus: "CLOSED",
        comment: { $regex: /Auto-closed/ },
        closingDate: { $gte: new Date(Date.now() - 24 * 60 * 60 * 1000) },
      })
        .sort({ closingDate: -1 })
        .limit(limit)
        .select(
          "orderNo symbol type volume openingPrice closingPrice profit comment closingDate"
        )
        .lean();

      const summary = {
        totalClosed: closedOrders.length,
        stopLossClosures: closedOrders.filter((order) =>
          order.comment.includes("STOP_LOSS")
        ).length,
        takeProfitClosures: closedOrders.filter((order) =>
          order.comment.includes("TAKE_PROFIT")
        ).length,
        totalProfit: closedOrders.reduce(
          (sum, order) => sum + (order.profit || 0),
          0
        ),
        avgProfit:
          closedOrders.length > 0
            ? closedOrders.reduce(
                (sum, order) => sum + (order.profit || 0),
                0
              ) / closedOrders.length
            : 0,
      };

      return { summary, orders: closedOrders, lastUpdated: new Date() };
    } catch (error) {
      console.error("❌ Failed to get closure history:", error);
      throw error;
    }
  }

  async healthCheck() {
    try {
      const stats = await this.getMonitoringStats();
      return {
        status: "HEALTHY",
        isRunning: this.isRunning,
        config: this.config,
        ordersBeingMonitored: stats.totalOrders,
        timestamp: new Date(),
      };
    } catch (error) {
      return {
        status: "UNHEALTHY",
        error: error.message,
        timestamp: new Date(),
      };
    }
  }

  updateConfig(newConfig) {
    this.config = { ...this.config, ...newConfig };
    console.log("⚙️ Configuration updated:", this.config);
  }

  async stop() {
    this.isRunning = false;
    console.log("🛑 Stop Loss/Take Profit Service stopped");
  }
}

const stopLossTakeProfitService = new StopLossTakeProfitService();

export default stopLossTakeProfitService;
