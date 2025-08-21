import mongoose from "mongoose";
import Transaction from "../../models/Transaction.js";
import Account from "../../models/AccountSchema.js";
import { createAppError } from "../../utils/errorHandler.js";
import Ledger from "../../models/LedgerSchema.js";
import Admin from "../../models/AdminSchema.js";

const generateEntryId = (prefix) => {
  const timestamp = Date.now().toString();
  const randomStr = Math.random().toString(36).substring(2, 5).toUpperCase();
  return `${prefix}-${timestamp.substring(timestamp.length - 5)}-${randomStr}`;
};
export const createTransaction = async (transactionData) => {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const { type, asset, amount, user, adminId, entityType } = transactionData;
    const transactionId = generateEntryId("TRX");

    let previousBalance = 0;
    let newBalance = 0;
    let newAvailableBalance = 0
    let previousAvailableBalance = 0
    let entityId = null;

    // Check if this is a user transaction or LP transaction
    const isLPTransaction = entityType === "ADMIN" || !user;

    if (isLPTransaction) {
      // Handle LP transaction - update admin's balance
      const admin = await Admin.findById(adminId).session(session);

      if (!admin) {
        throw createAppError("Admin account not found", 404);
      }

      entityId = adminId; // Set admin as the entity

      if (asset === "CASH") {
        previousBalance = admin.cashBalance || 0;

        if (type === "DEPOSIT") {
          newBalance = previousBalance + amount;
        } else if (type === "WITHDRAWAL") {
          if (previousBalance < amount) {
            throw createAppError(
              "Insufficient cash balance for withdrawal",
              400
            );
          }
          newBalance = previousBalance - amount;
        }

        admin.cashBalance = newBalance;
      } else if (asset === "GOLD") {
        previousBalance = admin.goldBalance || 0;

        if (type === "DEPOSIT") {
          newBalance = previousBalance + amount;
        } else if (type === "WITHDRAWAL") {
          if (previousBalance < amount) {
            throw createAppError(
              "Insufficient gold balance for withdrawal",
              400
            );
          }
          newBalance = previousBalance - amount;
        }

        admin.goldBalance = newBalance;
      } else if (asset === "MARGIN") {
        previousBalance = admin.margin || 0;

        if (type === "DEPOSIT") {
          newBalance = previousBalance + amount;
        } else if (type === "WITHDRAWAL") {
          if (previousBalance < amount) {
            throw createAppError("Insufficient margin for withdrawal", 400);
          }
          newBalance = previousBalance - amount;
        }

        admin.margin = newBalance;
      } else {
        throw createAppError("Invalid asset type for LP transaction", 400);
      }

      await admin.save({ session });
    } else {
      // Handle user transaction - update user's balance
      const account = await Account.findById(user).session(session);

      if (!account) {
        throw createAppError("Account not found", 404);
      }

      entityId = user; // Set user as the entity
      if (asset === "CASH") {
        previousBalance = account.AMOUNTFC;
        previousAvailableBalance = account.reservedAmount;
        if (type === "DEPOSIT") {
          newBalance = previousBalance + amount;
          newAvailableBalance = previousAvailableBalance + amount;
        } else if (type === "WITHDRAWAL") {
          if (previousBalance < amount) {
            throw createAppError(
              "Insufficient cash balance for withdrawal",
              400
            );
          }
          newBalance = previousBalance - amount;
          newAvailableBalance = previousAvailableBalance - amount;
        }

        account.AMOUNTFC = newBalance;
        account.reservedAmount = newAvailableBalance;
      } else if (asset === "GOLD") {
        previousBalance = account.METAL_WT;

        if (type === "DEPOSIT") {
          newBalance = previousBalance + amount;
        } else if (type === "WITHDRAWAL") {
          if (previousBalance < amount) {
            throw createAppError(
              "Insufficient gold balance for withdrawal",
              400
            );
          }
          newBalance = previousBalance - amount;
        }

        account.METAL_WT = newBalance;
      } else {
        throw createAppError("Invalid asset type for user transaction", 400);
      }

      await account.save({ session });
    }

    // Create transaction record
    const transaction = new Transaction({
      transactionId,
      type,
      asset,
      amount,
      entityType: isLPTransaction ? "ADMIN" : "USER",
      user: isLPTransaction ? null : user,
      adminId: adminId,
      previousBalance,
      newBalance,
      status: "COMPLETED",
    });

    await transaction.save({ session });

    // Create ledger entry
    const ledgerEntryType = isLPTransaction ? "LP-TRANSACTION" : "TRANSACTION";
    const ledgerDescription = isLPTransaction
      ? `LP ${type} of ${asset} - ${amount}`
      : `${type} of ${asset} - ${amount}`;

    const ledgerEntry = new Ledger({
      entryId: generateEntryId("TRX"),
      entryType: ledgerEntryType,
      referenceNumber: transactionId,
      description: ledgerDescription,
      entryNature: type === "DEPOSIT" ? "CREDIT" : "DEBIT",
      amount: amount,
      runningBalance: newBalance,
      transactionDetails: {
        type: type,
        asset: asset,
        previousBalance: previousBalance,
      },
      user: isLPTransaction ? adminId : user, // Use adminId instead of null for LP transactions
      adminId: adminId,
      notes: `${asset} ${type.toLowerCase()} ${
        isLPTransaction ? "LP " : ""
      }transaction`,
    });

    await ledgerEntry.save({ session });
    await session.commitTransaction();
    session.endSession();

    return transaction;
  } catch (error) {
    await session.abortTransaction();
    session.endSession();
    throw error;
  }
};

export const getTransactionsByUser = async (adminId, userId) => {
  try {
    const transactions = await Transaction.find({
      adminId: adminId,
      user: userId,
    })
      .populate(
        "user",
        "firstName lastName ACCOUNT_HEAD email phoneNumber accountStatus"
      )
      .sort({ createdAt: -1 });

    return transactions;
  } catch (error) {
    throw createAppError(
      `Error fetching user transactions: ${error.message}`,
      500
    );
  }
};

export const getUserTransactions = async (userId, options) => {
  const { page, limit, type, asset, status, startDate, endDate } = options;

  const skip = (page - 1) * limit;

  // Build query filters
  const query = { user: userId };

  if (type) query.type = type;
  if (asset) query.asset = asset;
  if (status) query.status = status;

  // Add date range filter if provided
  if (startDate || endDate) {
    query.date = {};
    if (startDate) query.date.$gte = new Date(startDate);
    if (endDate) query.date.$lte = new Date(endDate);
  }

  // Get total count for pagination
  const total = await Transaction.countDocuments(query);

  // Get transactions
  const transactions = await Transaction.find(query)
    .populate("user", "REFMID ACCOUNT_HEAD ACCODE firstName lastName email")
    .sort({ date: -1 })
    .skip(skip)
    .limit(limit);

  return {
    transactions,
    pagination: {
      total,
      page,
      limit,
      pages: Math.ceil(total / limit),
    },
  };
};

export const getTransactionById = async (transactionId) => {
  return Transaction.findOne({ transactionId }).populate(
    "user",
    "REFMID ACCOUNT_HEAD ACCODE firstName lastName email"
  );
};

export const updateTransactionStatus = async (transactionId, status) => {
  const validStatuses = ["PENDING", "COMPLETED", "FAILED", "CANCELLED"];

  if (!validStatuses.includes(status)) {
    throw createAppError("Invalid transaction status", 400);
  }

  const transaction = await Transaction.findOne({ transactionId });

  if (!transaction) {
    throw createAppError("Transaction not found", 404);
  }

  // If cancelling or failing a completed transaction, we need to reverse the balance update
  if (
    (status === "CANCELLED" || status === "FAILED") &&
    transaction.status === "COMPLETED"
  ) {
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
      const account = await Account.findById(transaction.user).session(session);

      if (!account) {
        throw createAppError("Account not found", 404);
      }

      // Reverse the transaction
      if (transaction.asset === "CASH") {
        if (transaction.type === "DEPOSIT") {
          account.AMOUNTFC -= transaction.amount;
        } else if (transaction.type === "WITHDRAWAL") {
          account.AMOUNTFC += transaction.amount;
        }
      } else if (transaction.asset === "GOLD") {
        if (transaction.type === "DEPOSIT") {
          account.METAL_WT -= transaction.amount;
        } else if (transaction.type === "WITHDRAWAL") {
          account.METAL_WT += transaction.amount;
        }
      }

      // Save the updated account
      await account.save({ session });

      // Update the transaction status
      transaction.status = status;
      await transaction.save({ session });

      // Commit the transaction
      await session.commitTransaction();
      session.endSession();

      return transaction;
    } catch (error) {
      // Abort the transaction on error
      await session.abortTransaction();
      session.endSession();
      throw error;
    }
  }

  // For simpler status updates
  transaction.status = status;
  await transaction.save();

  return transaction;
};

export const getAllTransactions = async (options) => {
  const {
    page,
    limit,
    type,
    asset,
    status,
    startDate,
    endDate,
    userId,
    entityType,
    adminId,
  } = options;

  const skip = (page - 1) * limit;

  // Build query filters
  const query = {};

  if (type) query.type = type;
  if (asset) query.asset = asset;
  if (status) query.status = status;
  if (userId) query.user = userId;
  if (adminId) query.adminId = adminId;
  if (entityType) query.entityType = entityType;

  // Add date range filter if provided
  if (startDate || endDate) {
    query.date = {};
    if (startDate) query.date.$gte = new Date(startDate);
    if (endDate) query.date.$lte = new Date(endDate);
  }

  // Get total count for pagination
  const total = await Transaction.countDocuments(query);

  // Get transactions
  const transactions = await Transaction.find(query)
    .populate("user", "REFMID ACCOUNT_HEAD ACCODE firstName lastName email")
    .sort({ date: -1 })
    .skip(skip)
    .limit(limit);

  return {
    transactions,
    pagination: {
      total,
      page,
      limit,
      pages: Math.ceil(total / limit),
    },
  };
};

const buildQueryFilter = (userId, filters) => {
  const query = { user: userId };

  // Add filters if provided
  if (filters.entryType) {
    query.entryType = filters.entryType;
  }

  if (filters.entryNature) {
    query.entryNature = filters.entryNature;
  }

  // Admin ID filter
  if (filters.adminId) {
    query.adminId = filters.adminId;
  }

  // Date range filter
  if (filters.startDate || filters.endDate) {
    query.date = {};
    if (filters.startDate) {
      query.date.$gte = new Date(filters.startDate);
    }
    if (filters.endDate) {
      query.date.$lte = new Date(filters.endDate);
    }
  }

  // Amount range filter
  if (filters.minAmount !== undefined || filters.maxAmount !== undefined) {
    query.amount = {};
    if (filters.minAmount !== undefined) {
      query.amount.$gte = filters.minAmount;
    }
    if (filters.maxAmount !== undefined) {
      query.amount.$lte = filters.maxAmount;
    }
  }

  // Asset type filter
  if (filters.assetType) {
    query["transactionDetails.asset"] = filters.assetType;
  }

  // Order status filter
  if (filters.orderStatus) {
    query["orderDetails.status"] = filters.orderStatus;
  }

  // Symbol filter (applies to both orders and LP positions)
  if (filters.symbol) {
    query.$or = [
      { "orderDetails.symbol": filters.symbol },
      { "lpDetails.symbol": filters.symbol },
    ];
  }

  // Search term for description or reference number
  if (filters.searchTerm) {
    query.$or = [
      { description: { $regex: filters.searchTerm, $options: "i" } },
      { referenceNumber: { $regex: filters.searchTerm, $options: "i" } },
      { notes: { $regex: filters.searchTerm, $options: "i" } },
    ];
  }

  return query;
};

export const fetchLedgerEntries = async (
  userId,
  page,
  limit,
  sortBy,
  sortOrder,
  filters
) => {
  try {
    const query = buildQueryFilter(userId, filters);
    console.log(query);
    // Configure sort options
    const sort = {};
    sort[sortBy] = sortOrder === "asc" ? 1 : -1;

    // Calculate skip for pagination
    const skip = (page - 1) * limit;

    // Execute queries in parallel for better performance
    const [entries, totalCount] = await Promise.all([
      Ledger.find(query).sort(sort).skip(skip).limit(limit).lean(), // Use lean for better performance when you don't need Mongoose document methods

      Ledger.countDocuments(query),
    ]);

    return {
      entries,
      totalCount,
    };
  } catch (error) {
    console.error("Error in fetchLedgerEntries service:", error);
    throw error;
  }
};

export const generateLedgerStats = async (userId, filters) => {
  try {
    const query = buildQueryFilter(userId, filters);

    // Aggregate to get statistics
    const stats = await Ledger.aggregate([
      { $match: query },
      {
        $facet: {
          // Total counts by entry type
          entryCounts: [{ $group: { _id: "$entryType", count: { $sum: 1 } } }],

          // Total amounts by entry type and nature
          totals: [
            {
              $group: {
                _id: { type: "$entryType", nature: "$entryNature" },
                totalAmount: { $sum: "$amount" },
              },
            },
          ],

          // Latest entries
          latestEntries: [
            { $sort: { date: -1 } },
            { $limit: 5 },
            {
              $project: {
                entryId: 1,
                entryType: 1,
                amount: 1,
                entryNature: 1,
                date: 1,
                description: 1,
              },
            },
          ],

          // Current balance
          currentBalance: [
            { $sort: { date: -1 } },
            { $limit: 1 },
            { $project: { runningBalance: 1 } },
          ],
        },
      },
    ]);

    // Process the stats for better readability
    const processedStats = {
      entryCounts: {},
      totals: {
        debit: {},
        credit: {},
      },
      latestEntries: stats[0].latestEntries || [],
      currentBalance:
        stats[0].currentBalance.length > 0
          ? stats[0].currentBalance[0].runningBalance
          : 0,
    };

    // Process entry counts
    if (stats[0].entryCounts) {
      stats[0].entryCounts.forEach((item) => {
        processedStats.entryCounts[item._id] = item.count;
      });
    }

    // Process totals
    if (stats[0].totals) {
      stats[0].totals.forEach((item) => {
        if (item._id.nature === "DEBIT") {
          processedStats.totals.debit[item._id.type] = item.totalAmount;
        } else {
          processedStats.totals.credit[item._id.type] = item.totalAmount;
        }
      });
    }

    return processedStats;
  } catch (error) {
    console.error("Error in generateLedgerStats service:", error);
    throw error;
  }
};
