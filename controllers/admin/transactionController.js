import { v4 as uuidv4 } from "uuid";
import { createAppError } from "../../utils/errorHandler.js";
import * as transactionServices from "../../services/admin/transactionServices.js";
import ledgerService from "../../services/admin/ledgerService.js";

export const getUserTransactionsByAdmin = async (req, res, next) => {
  try {
    const { adminId, userId } = req.params;

    const transactions = await transactionServices.getTransactionsByUser(
      adminId,
      userId
    );

    res.json({
      status: 200,
      success: true,
      message: "User transactions retrieved successfully",
      data: transactions,
    });
  } catch (error) {
    next(error);
  }
};
export const createTransaction = async (req, res, next) => {
  try {
    const { userId, type, asset, amount, entityType = "TRANSACTION" } = req.body;
    const { adminId } = req.params;
    if (!type || !asset || amount === undefined) {
      return next(createAppError("Missing required transaction details", 400));
    }

    if (amount <= 0) {
      return next(
        createAppError("Transaction amount must be greater than zero", 400)
      );
    }

    const isLPTransaction = !userId;
    const transactionId = uuidv4();
    const transaction = await transactionServices.createTransaction({
      transactionId,
      type,
      asset,
      amount,
      user: userId || null,
      adminId,
      entityType: isLPTransaction ? "ADMIN" : "USER"
    });

    const transactionType = isLPTransaction ? "LP" : "user";
    
    res.status(201).json({
      status: 201,
      success: true,
      message: `${transactionType} ${type.toLowerCase()} transaction created successfully`,
      data: transaction,
    });
  } catch (error) {
    next(error);
  }
};

export const getUserTransactions = async (req, res, next) => {
  try {
    const { userId } = req.params;
    const {
      page = 1,
      limit = 10,
      type,
      asset,
      status,
      startDate,
      endDate,
    } = req.query;

    const transactions = await transactionServices.getUserTransactions(userId, {
      page: parseInt(page),
      limit: parseInt(limit),
      type,
      asset,
      status,
      startDate,
      endDate,
    });

    res.status(200).json({
      status: 200,
      success: true,
      message: "Transactions retrieved successfully",
      data: transactions,
    });
  } catch (error) {
    next(error);
  }
};

export const getTransactionById = async (req, res, next) => {
  try {
    const { transactionId } = req.params;

    const transaction = await transactionServices.getTransactionById(
      transactionId
    );

    if (!transaction) {
      return next(createAppError("Transaction not found", 404));
    }

    res.status(200).json({
      status: 200,
      success: true,
      message: "Transaction retrieved successfully",
      data: transaction,
    });
  } catch (error) {
    next(error);
  }
};

export const updateTransactionStatus = async (req, res, next) => {
  try {
    const { transactionId } = req.params;
    const { status } = req.body;

    if (!status) {
      return next(createAppError("Status is required", 400));
    }

    const updatedTransaction =
      await transactionServices.updateTransactionStatus(transactionId, status);

    res.status(200).json({
      status: 200,
      success: true,
      message: "Transaction status updated successfully",
      data: updatedTransaction,
    });
  } catch (error) {
    next(error);
  }
};

export const getAllTransactions = async (req, res, next) => {
  try {
    const {
      page = 1,
      limit = 10,
      type,
      asset,
      status,
      startDate,
      endDate,
      userId,
      adminId,
      entityType
    } = req.query;

    const transactions = await transactionServices.getAllTransactions({
      page: parseInt(page),
      limit: parseInt(limit),
      type,
      asset,
      status,
      startDate,
      endDate,
      userId,
      adminId,
      entityType
    });

    res.status(200).json({
      status: 200,
      success: true,
      message: "Transactions retrieved successfully",
      data: transactions,
    });
  } catch (error) {
    next(error);
  }
};

export const getLedgerData = async (req, res) => {
  try {
    const result = await ledgerService.getLedgerDataWithFilters(req.query);
    
    return res.status(200).json({
      success: true,
      message: 'Ledger data retrieved successfully',
      data: result.docs,
      pagination: {
        total: result.totalDocs,
        page: result.page,
        pages: result.totalPages,
        limit: result.limit,
        hasNextPage: result.hasNextPage,
        hasPrevPage: result.hasPrevPage
      }
    });
  } catch (error) {
    console.error('Error fetching ledger data:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to retrieve ledger data',
      error: error.message
    });
  }
};
