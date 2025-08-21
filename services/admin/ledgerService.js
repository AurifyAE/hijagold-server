import mongoose from "mongoose";
import Ledger from "../../models/LedgerSchema.js";

class LedgerService {
  async getLedgerDataWithFilters(queryParams) {
    try {
      const {
        // Pagination parameters
        page = 1,
        limit = 10,
        sortBy = "date",
        sortOrder = "desc",

        // Basic filters
        entryId,
        entryType,
        entryNature,
        referenceNumber,

        // Date range filters
        startDate,
        endDate,

        // Amount range filters
        minAmount,
        maxAmount,

        // Transaction details filters
        transactionType,
        asset,

        // Order details filters
        orderType,
        orderStatus,
        symbol,

        // LP details filters
        positionId,
        positionStatus,

        // ID filters
        userId,
        adminId,

        // Text search
        searchTerm,
      } = queryParams;

      // Build filters object
      const filters = {};

      // Basic filters
      if (entryId) filters.entryId = entryId;
      if (entryType) filters.entryType = entryType;
      if (entryNature) filters.entryNature = entryNature;
      if (referenceNumber)
        filters.referenceNumber = { $regex: referenceNumber, $options: "i" };

      // User and Admin filters - handle multiple ways they might be passed
      if (userId) {
        // Convert string to ObjectId if needed
        filters.user = mongoose.Types.ObjectId.isValid(userId)
          ? new mongoose.Types.ObjectId(userId)
          : userId;
      }

      if (adminId) {
        // Convert string to ObjectId if needed
        filters.adminId = mongoose.Types.ObjectId.isValid(adminId)
          ? new mongoose.Types.ObjectId(adminId)
          : adminId;
      }

      // Date range filter
      if (startDate || endDate) {
        filters.date = {};
        if (startDate) filters.date.$gte = new Date(startDate);
        if (endDate) filters.date.$lte = new Date(endDate);
      }

      // Amount range filter
      if (minAmount || maxAmount) {
        filters.amount = {};
        if (minAmount) filters.amount.$gte = parseFloat(minAmount);
        if (maxAmount) filters.amount.$lte = parseFloat(maxAmount);
      }

      // Transaction details filters
      if (transactionType) filters["transactionDetails.type"] = transactionType;
      if (asset) filters["transactionDetails.asset"] = asset;

      // Order details filters
      if (orderType) filters["orderDetails.type"] = orderType;
      if (orderStatus) filters["orderDetails.status"] = orderStatus;
      if (symbol) filters["orderDetails.symbol"] = symbol;

      // LP details filters
      if (positionId) filters["lpDetails.positionId"] = positionId;
      if (positionStatus) filters["lpDetails.status"] = positionStatus;

      // Text search across multiple fields
      if (searchTerm) {
        filters.$or = [
          { description: { $regex: searchTerm, $options: "i" } },
          { referenceNumber: { $regex: searchTerm, $options: "i" } },
          { "orderDetails.symbol": { $regex: searchTerm, $options: "i" } },
          { notes: { $regex: searchTerm, $options: "i" } },
        ];
      }

      // Set up query options
      const options = {
        page: parseInt(page),
        limit: parseInt(limit),
        sort: { [sortBy]: sortOrder === "desc" ? -1 : 1 },
        populate: [
          { path: "user", select: "username email ACCOUNT_HEAD" },
          { path: "adminId", select: "username email" },
        ],
      };

      // Execute pagination query
      const result = await this.getAllLedgers(filters, options);

      // Add debug information in development environment
      if (process.env.NODE_ENV === "development") {
        result.filters = filters;
      }

      return result;
    } catch (error) {
      console.error("LedgerService getLedgerDataWithFilters error:", error);
      throw error;
    }
  }

  async getAllLedgers(filters = {}, options = {}) {
    try {
      // Check if paginate is available, otherwise fallback to manual pagination
      if (typeof Ledger.paginate === 'function') {
        // Using mongoose-paginate-v2 for pagination
        const result = await Ledger.paginate(filters, options);
        return result;
      } else {
        console.warn("Ledger.paginate is not available. Using manual pagination.");
        
        // Manual pagination implementation
        const page = options.page || 1;
        const limit = options.limit || 10;
        const skip = (page - 1) * limit;
        
        // Create the query
        let query = Ledger.find(filters);
        
        // Apply sorting
        if (options.sort) {
          query = query.sort(options.sort);
        }
        
        // Apply population
        if (options.populate && Array.isArray(options.populate)) {
          options.populate.forEach(populateOption => {
            query = query.populate(populateOption);
          });
        }
        
        // Get total count
        const totalDocs = await Ledger.countDocuments(filters);
        
        // Execute query with pagination
        const docs = await query.skip(skip).limit(limit).exec();
        
        // Format result to match mongoose-paginate-v2 output
        return {
          docs,
          totalDocs,
          limit,
          page,
          totalPages: Math.ceil(totalDocs / limit),
          hasPrevPage: page > 1,
          hasNextPage: page < Math.ceil(totalDocs / limit),
          prevPage: page > 1 ? page - 1 : null,
          nextPage: page < Math.ceil(totalDocs / limit) ? page + 1 : null
        };
      }
    } catch (error) {
      console.error("LedgerService getAllLedgers error:", error);
      throw error;
    }
  }
}

export default new LedgerService();