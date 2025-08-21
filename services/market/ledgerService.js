import Ledger from "../../models/LedgerSchema.js";

export const getUserLedger = async (userId, options = {}) => {
  try {
    const {
      page = 1,
      limit = 10,
      sortBy = 'date',
      sortOrder = 'desc'
    } = options;

    const query = { user: userId };
    const sort = { [sortBy]: sortOrder === 'asc' ? 1 : -1 };
    
    const result = await Ledger.paginate(query, {
      page,
      limit,
      sort,
      lean: true
    });

    return {
      success: true,
      data: {
        entries: result.docs,
        totalEntries: result.totalDocs,
        totalPages: result.totalPages,
        currentPage: result.page,
        hasNextPage: result.hasNextPage,
        hasPrevPage: result.hasPrevPage
      }
    };
  } catch (error) {
    console.error("Error fetching user ledger:", error);
    return {
      success: false,
      message: error.message || "Failed to fetch ledger entries"
    };
  }
};

/**
 * Get summary of user's ledger based on transaction types
 * @param {String} userId - The user's account ID
 * @returns {Object} Summary data
 */
export const getLedgerSummary = async (userId) => {
  try {
    // Get the latest entry to know the current balance
    const latestEntry = await Ledger.findOne({ user: userId })
      .sort({ date: -1 })
      .lean();
    
    // Count and sum different transaction types
    const [buyOrders, sellOrders, deposits, withdrawals] = await Promise.all([
      Ledger.countDocuments({ 
        user: userId, 
        "orderDetails.type": "BUY", 
        entryType: "ORDER" 
      }),
      Ledger.countDocuments({ 
        user: userId, 
        "orderDetails.type": "SELL", 
        entryType: "ORDER" 
      }),
      Ledger.countDocuments({ 
        user: userId, 
        "transactionDetails.type": "DEPOSIT", 
        entryType: "TRANSACTION" 
      }),
      Ledger.countDocuments({ 
        user: userId, 
        "transactionDetails.type": "WITHDRAWAL", 
        entryType: "TRANSACTION" 
      })
    ]);
    
    // Calculate profit from closed orders
    const profitPipeline = [
      { $match: { 
          user: userId, 
          entryType: "ORDER",
          "orderDetails.status": "CLOSED",
          "orderDetails.profit": { $exists: true }
        } 
      },
      { $group: { 
          _id: null, 
          totalProfit: { $sum: "$orderDetails.profit" } 
        } 
      }
    ];
    
    const profitResult = await Ledger.aggregate(profitPipeline);
    
    return {
      success: true,
      data: {
        currentBalance: latestEntry?.runningBalance || 0,
        transactions: {
          buyOrders,
          sellOrders,
          deposits,
          withdrawals
        },
        totalProfit: profitResult.length > 0 ? profitResult[0].totalProfit : 0
      }
    };
  } catch (error) {
    console.error("Error generating ledger summary:", error);
    return {
      success: false,
      message: error.message || "Failed to generate ledger summary"
    };
  }
};

/**
 * Format ledger entries for user display
 * @param {Array} entries - Ledger entries to format
 * @returns {String} Formatted string for WhatsApp display
 */
export const formatLedgerForDisplay = (entries) => {
  if (!entries || entries.length === 0) {
    return "No transactions found in your statement.";
  }
  
  let response = "*Your Recent Transactions:*\n\n";
  
  entries.forEach((entry, index) => {
    const date = new Date(entry.date).toLocaleDateString();
    const amount = entry.amount.toFixed(2);
    const nature = entry.entryNature;
    const balance = entry.runningBalance.toFixed(2);
    
    let details = "";
    if (entry.entryType === "ORDER") {
      const orderType = entry.orderDetails.type;
      const symbol = entry.orderDetails.symbol;
      const volume = entry.orderDetails.volume;
      const price = entry.orderDetails.entryPrice?.toFixed(2) || "N/A";
      const status = entry.orderDetails.status;
      
      if (status === "CLOSED" && entry.orderDetails.profit !== null) {
        const profit = entry.orderDetails.profit.toFixed(2);
        const profitSymbol = parseFloat(profit) >= 0 ? "+" : "";
        details = `${orderType} ${volume} ${symbol} @ $${price} [${status}] (${profitSymbol}$${profit})`;
      } else {
        details = `${orderType} ${volume} ${symbol} @ $${price} [${status}]`;
      }
    } else if (entry.entryType === "TRANSACTION") {
      details = `${entry.transactionDetails.type} ${entry.transactionDetails.asset}`;
    } else {
      details = entry.description;
    }
    
    response += `*${index + 1}.* ${date} | ${nature} $${amount}\n   ${details}\n   Balance: $${balance}\n\n`;
  });
  
  return response;
};

export default {
  getUserLedger,
  getLedgerSummary,
  formatLedgerForDisplay
};