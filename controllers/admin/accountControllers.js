import * as accountServices from "../../services/admin/accountServices.js";
import jwt from "jsonwebtoken";
import dotenv from "dotenv";
import Admin from "../../models/AdminSchema.js";

export const getAllData = async (req, res, next) => {
  try {
    const { adminId } = req.params;
  console.log("Admin ID:", adminId); // Debugging line
    const accounts = await accountServices.findAllAccounts(adminId);
    res.json({
      status: 200,
      success: true,
      data: accounts,
    });
  } catch (error) {
    next(error);
  }
};
export const freezeAccount = async (req, res, next) => {
  try {
    const { userId } = req.params;
    console.log("second");
    // Update account to set isFreeze to true
    const updatedAccount = await accountServices.freezeAccount(userId);
    
    res.json({
      status: 200,
      success: true,
      data: updatedAccount,
    });
  } catch (error) {
    next(error);
  }
};

export const sendAlertFunction = async (req, res, next) => {
  try {
    const { userId } = req.params;
    const { message } = req.body;

    // Send WhatsApp alert
    const result = await accountServices.sendWhatsAppAlert(userId, message);
    
    res.json({
      status: 200,
      success: true,
      data: result,
    });
  } catch (error) {
    next(error);
  }
};


export const updateBalance = async (req, res, next) => {
  try {
    console.log("Processing balance update");
    console.log("Request body:", req.body);
    
    const { userId } = req.params;
    console.log("User ID from params:", userId);
    
    const { amountFC, reservedAmount } = req.body;
    const io = req.app.get('io');

    // Validation
    if (!userId) {
      throw new Error("Invalid user ID");
    }
    if (amountFC !== undefined && typeof amountFC !== 'number') {
      throw new Error("amountFC must be a number");
    }
    if (reservedAmount !== undefined && typeof reservedAmount !== 'number') {
      throw new Error("reservedAmount must be a number");
    }

    // At least one field should be provided
    if (amountFC === undefined && reservedAmount === undefined) {
      throw new Error("At least one field (amountFC or reservedAmount) must be provided");
    }

    const updatedAccount = await accountServices.updateUserBalance(userId, amountFC, reservedAmount, io);
    
    res.status(200).json({
      success: true,
      data: {
        userId,
        AMOUNTFC: updatedAccount.AMOUNTFC,
        reservedAmount: updatedAccount.reservedAmount || 0,
        ACCOUNT_HEAD: updatedAccount.ACCOUNT_HEAD,
      },
    });
  } catch (error) {
    console.error("Error in updateBalance:", error.message);
    next(error);
  }
};

// Fixed getBalance controller
export const getBalance = async (req, res, next) => {
  try {
    console.log("Processing balance retrieval");
    // console.log("Request body:", req.body);
    
    const { userId } = req.params;
    console.log("User ID:", userId);
    
    const io = req.app.get('io');

    if (!userId) {
      throw new Error("Invalid user ID");
    }

    // Use the dedicated getUserBalanceOnly function (recommended)
    const account = await accountServices.getUserBalance(userId, io);
    
    // Or use the corrected getUserBalance function
    // const account = await getUserBalance(userId, io);
    
    res.status(200).json({
      success: true,
      data: {
        userId,
        AMOUNTFC: account.AMOUNTFC,
        reservedAmount: account.reservedAmount || 0,
        ACCOUNT_HEAD: account.ACCOUNT_HEAD,
      },
    });
  } catch (error) {
    console.error("Error in getBalance:", error.message);
    next(error);
  }
};
export const adminTokenVerificationApi = async (req, res, next) => {
  try {
    const token = req.body.token;
    if (!token) {
      return res
        .status(401)
        .json({ message: "Authentication token is missing" });
    }
    const decoded = jwt.verify(token, process.env.SECRET_KEY);
    const admin = await Admin.findById(decoded.adminId);
    if (!admin) {
      return res.status(404).json({ message: "Admin not found" });
    }
    res.status(200).json({
      admin: {
        adminId: admin._id,
      },
    });
  } catch (error) {
    if (error.name === "TokenExpiredError") {
      return res
        .status(401)
        .json({ message: "Token has expired", tokenExpired: true });
    } else if (error.name === "JsonWebTokenError") {
      return res
        .status(401)
        .json({ message: "Invalid token", tokenInvalid: true });
    }
    next(error);
  }
};
export const getUserProfile = async (req, res, next) => {
  try {
    const { adminId, userId } = req.params;

    const userData = await accountServices.findUserById(adminId, userId);

    if (!userData) {
      return res.status(404).json({
        status: 404,
        success: false,
        message: "User not found",
      });
    }

    res.json({
      status: 200,
      success: true,
      data: userData,
    });
  } catch (error) {
    next(error);
  }
};
export const updateUserProfile = async (req, res, next) => {
  try {
    const { adminId, userId } = req.params;
    const updateData = req.body;

    const updatedUser = await accountServices.updateUserById(
      adminId,
      userId,
      updateData
    );

    if (!updatedUser) {
      return res.status(404).json({
        status: 404,
        success: false,
        message:
          "User not found or you don't have permission to update this user",
      });
    }

    res.json({
      status: 200,
      success: true,
      message: "User profile updated successfully",
      data: updatedUser,
    });
  } catch (error) {
    next(error);
  }
};
export const getAccountByType = async (req, res, next) => {
  try {
    const { type } = req.query;
    if (!type) {
      return res.status(400).json({
        success: false,
        message: "Account type is required",
      });
    }

    const accounts = await accountServices.findAccountsByType(type);
    res.status(200).json({
      success: true,
      data: accounts,
    });
  } catch (error) {
    next(error);
  }
};

export const updateAccountType = async (req, res, next) => {
  try {
    const { accode, accountType } = req.body;
    const { adminId } = req.params;

    if (!accode || !accountType) {
      return res.status(400).json({
        success: false,
        message: "Account code and account type are required",
      });
    }

    const updatedAccount = await accountServices.updateAccountTypeById(
      accode,
      adminId,
      accountType
    );
    res.json({
      status: 200,
      success: true,
      data: updatedAccount,
    });
  } catch (error) {
    next(error);
  }
};

export const updateMarginAmount = async (req, res, next) => {
  try {
    const { accode, margin } = req.body;
    const { adminId } = req.params;
    if (!accode || margin === undefined) {
      return res.status(400).json({
        success: false,
        message: "Account code and margin amount are required",
      });
    }

    const updatedAccount = await accountServices.updateMargin(
      accode,
      adminId,
      margin
    );
    res.json({
      status: 200,
      success: true,
      data: updatedAccount,
    });
  } catch (error) {
    next(error);
  }
};

export const updateFavoriteStatus = async (req, res, next) => {
  try {
    const { accode, isFavorite } = req.body;
    const { adminId } = req.params;
    if (!accode || isFavorite === undefined) {
      return res.status(400).json({
        success: false,
        message: "Account code and favorite status are required",
      });
    }

    const updatedAccount = await accountServices.updateFavorite(
      accode,
      adminId,
      isFavorite
    );
    res.json({
      status: 200,
      success: true,
      data: updatedAccount,
    });
  } catch (error) {
    next(error);
  }
};

export const filterAccounts = async (req, res, next) => {
  try {
    const filters = req.query;

    // Convert string "true"/"false" to boolean for is_favorite
    if (filters.is_favorite) {
      filters.is_favorite = filters.is_favorite === "true";
    }

    const accounts = await accountServices.filterAccounts(filters);
    res.status(200).json({
      success: true,
      count: accounts.length,
      data: accounts,
    });
  } catch (error) {
    next(error);
  }
};

export const insertAccount = async (req, res, next) => {
  try {
    const adminId = req.params.adminId;
    const newAccount = await accountServices.createAccount(req.body, adminId);
    res.status(201).json({
      success: true,
      data: newAccount,
    });
  } catch (error) {
    next(error);
  }
};

export const updateAccount = async (req, res, next) => {
  try {
    const { ACCODE, adminId } = req.params;
    const updatedAccount = await accountServices.updateAccountByCode(
      ACCODE,
      adminId,
      req.body
    );
    res.status(200).json({
      success: true,
      data: updatedAccount,
    });
  } catch (error) {
    next(error);
  }
};

export const deleteAccount = async (req, res, next) => {
  try {
    const { ACCODE, adminId } = req.params;
    const deletedAccount = await accountServices.deleteAccountByCode(
      ACCODE,
      adminId
    );
    res.status(200).json({
      success: true,
      message: "Account deleted successfully",
      data: deletedAccount,
    });
  } catch (error) {
    next(error);
  }
};
