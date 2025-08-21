import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();
import Account from "../../models/AccountSchema.js";
import pkg from "twilio";
const { Twilio } = pkg;
import { createAppError } from "../../utils/errorHandler.js";
dotenv.config();

const generateRandomRefMid = () => {
  // Generate a random number between 10000 and 99999 (5 digits)
  return Math.floor(10000 + Math.random() * 90000);
};

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
let twilioPhoneNumber = process.env.TWILIO_WHATSAPP_NUMBER;
const client = new Twilio(accountSid, authToken);

// Ensure twilioPhoneNumber is WhatsApp-formatted
if (!twilioPhoneNumber.startsWith("whatsapp:")) {
  twilioPhoneNumber = `whatsapp:+${twilioPhoneNumber.replace(
    /^(whatsapp:)?[\+\s\-()]/g,
    ""
  )}`;
}

// Function to check if a REFMID is unique and generate a new one if needed
const getUniqueRefMid = async () => {
  let isUnique = false;
  let refMid;

  // Keep trying until we find a unique REFMID
  while (!isUnique) {
    refMid = generateRandomRefMid();
    // Check if this REFMID already exists
    const existingAccount = await Account.findOne({ REFMID: refMid });
    if (!existingAccount) {
      isUnique = true;
    }
  }

  return refMid;
};
export const freezeAccount = async (userId) => {
  try {
    const account = await Account.findOneAndUpdate(
      { _id: userId },
      { isFreeze: true },
      { new: true }
    );
    // console.log(account)
    if (!account) {
      throw new Error("Account not found");
    }

    return account;
  } catch (error) {
    throw new Error(`Failed to freeze account: ${error.message}`);
  }
};

export const sendWhatsAppAlert = async (userId, message) => {
  try {
    // Fetch user's phone number for WhatsApp messaging
    const account = await Account.findOne({ _id: userId });
    if (!account || !account.phoneNumber) {
      throw new Error("User account or phone number not found");
    }

    let phoneNumber = account.phoneNumber;
    if (!phoneNumber.startsWith("whatsapp:")) {
      phoneNumber = `whatsapp:+${phoneNumber.replace(
        /^(whatsapp:)?[\+\s\-()]/g,
        ""
      )}`;
    }

    // Send WhatsApp message
    const result = await client.messages.create({
      body: message,
      from: twilioPhoneNumber,
      to: phoneNumber,
    });

    console.log(`WhatsApp alert sent to ${phoneNumber}`);
    return { message: "Alert sent successfully", sid: result.sid };
  } catch (error) {
    // console.error(
    //   `Failed to send WhatsApp message: ${error.message}, Code: ${
    //     error.code
    //   }, Details: ${JSON.stringify(error)}`
    // );
    // throw new Error(`Failed to send WhatsApp alert: ${error.message}`);
  }
};

export const updateUserBalance = async (
  userId,
  amountFC,
  reservedAmount,
  io
) => {
  try {
    // Validate inputs
    if (!mongoose.isValidObjectId(userId)) {
      throw new Error("Invalid user ID format");
    }
    if (amountFC !== undefined && typeof amountFC !== "number") {
      throw new Error("AMOUNTFC must be a number");
    }
    if (reservedAmount !== undefined && typeof reservedAmount !== "number") {
      throw new Error("reservedAmount must be a number");
    }

    // Build update query for both fields
    const updateQuery = {};
    if (amountFC !== undefined) {
      updateQuery.$inc = { ...updateQuery.$inc, AMOUNTFC: amountFC };
    }
    if (reservedAmount !== undefined) {
      updateQuery.$inc = {
        ...updateQuery.$inc,
        reservedAmount: reservedAmount,
      };
    }

    // If no updates specified, just return current account
    if (Object.keys(updateQuery).length === 0) {
      updateQuery.$inc = {}; // Empty increment to trigger findByIdAndUpdate
    }

    const account = await Account.findByIdAndUpdate(userId, updateQuery, {
      new: true,
      select: "AMOUNTFC reservedAmount ACCOUNT_HEAD",
      runValidators: false,
    });

    if (!account) {
      throw new Error("Account not found");
    }

    // Initialize fields if undefined
    let needsSave = false;
    if (account.AMOUNTFC === undefined) {
      account.AMOUNTFC = 0;
      needsSave = true;
    }
    if (account.reservedAmount === undefined) {
      account.reservedAmount = 0;
      needsSave = true;
    }

    // Save if any field was initialized
    if (needsSave) {
      await account.save({ validateBeforeSave: false });
    }

    // Emit balance update to the user's room
    if (io) {
      io.to(userId).emit("balanceUpdate", {
        userId,
        balance: account.AMOUNTFC,
        reservedAmount: account.reservedAmount,
      });
    }

    return account;
  } catch (error) {
    console.error(`Error updating balance for user ${userId}:`, error.message);
    throw new Error(`Failed to update balance: ${error.message}`);
  }
};
export const getUserBalance = async (userId, io) => {
  return updateUserBalance(userId, undefined, undefined, io);
};

export const findUserById = async (adminId, userId) => {
  try {
    // Find a specific user that was added by the admin
    const user = await Account.findOne({
      _id: userId,
      addedBy: adminId,
    });

    return user;
  } catch (error) {
    throw createAppError(`Error fetching user: ${error.message}`, 500);
  }
};

export const findAllAccounts = async (adminId) => {
  try {
    // Find only accounts added by the specific admin
    const accounts = await Account.find().lean();

    return accounts;
  } catch (error) {
    throw createAppError(`Error fetching accounts: ${error.message}`, 500);
  }
};

export const findAccountsByType = async (accountType) => {
  try {
    const accounts = await Account.find({ Account_Type: accountType }).populate(
      "addedBy",
      "userName email"
    );
    return accounts;
  } catch (error) {
    throw createAppError(
      `Error fetching accounts with type ${accountType}`,
      500
    );
  }
};

export const updateAccountTypeById = async (accode, adminId, newType) => {
  try {
    const updatedAccount = await Account.findOneAndUpdate(
      { ACCODE: accode, addedBy: adminId },
      { Account_Type: newType },
      { new: true }
    );

    if (!updatedAccount) {
      throw createAppError("Account not found", 404);
    }

    return updatedAccount;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error updating account type: ${error.message}`, 500);
  }
};

export const updateMargin = async (accode, adminId, margin) => {
  try {
    const updatedAccount = await Account.findOneAndUpdate(
      { ACCODE: accode, addedBy: adminId },
      { margin: margin },
      { new: true }
    );

    if (!updatedAccount) {
      throw createAppError(
        "Account not found or you don't have permission to update it",
        404
      );
    }

    return updatedAccount;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error updating margin: ${error.message}`, 500);
  }
};

export const updateFavorite = async (accode, adminId, isFavorite) => {
  try {
    const updatedAccount = await Account.findOneAndUpdate(
      { ACCODE: accode, addedBy: adminId },
      { is_favorite: isFavorite },
      { new: true }
    );

    if (!updatedAccount) {
      throw createAppError(
        "Account not found or you don't have permission to update it",
        404
      );
    }

    return updatedAccount;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(
      `Error updating favorite status: ${error.message}`,
      500
    );
  }
};

export const filterAccounts = async (filterParams) => {
  try {
    const query = {};

    // Apply filters based on provided parameters
    if (filterParams.account_type) {
      query.Account_Type = filterParams.account_type;
    }

    if (filterParams.is_favorite !== undefined) {
      query.is_favorite = filterParams.is_favorite;
    }

    if (filterParams.search) {
      // Case-insensitive search across multiple fields
      query.$or = [
        { ACCOUNT_HEAD: { $regex: filterParams.search, $options: "i" } },
        { ACCODE: { $regex: filterParams.search, $options: "i" } },
      ];
    }

    const accounts = await Account.find(query).populate(
      "addedBy",
      "userName email"
    );
    return accounts;
  } catch (error) {
    throw createAppError(`Error filtering accounts: ${error.message}`, 500);
  }
};

export const createAccount = async (accountData, adminId) => {
  try {
    // Check if an account with the same REFMID already exists for this admin
    const existingAccount = await Account.findOne({
      ACCODE: accountData.ACCODE,
      addedBy: adminId,
    });

    if (existingAccount) {
      throw createAppError(
        "Account with this ACCODE already exists for your admin account",
        400
      );
    }

    accountData.addedBy = adminId;
    // Generate a unique REFMID
    accountData.REFMID = await getUniqueRefMid();

    const newAccount = new Account(accountData);
    await newAccount.save();
    return newAccount;
  } catch (error) {
    console.log(error)
    if (error.code === 11000) {
      throw createAppError("Account with this code already exists", 400);
    }
    throw createAppError(`Error creating account: ${error.message}`, 500);
  }
};

export const updateAccountByCode = async (accode, adminId, updateData) => {
  try {
    // Find and update account with the specific ACCODE and belonging to the admin
    const updatedAccount = await Account.findOneAndUpdate(
      { ACCODE: accode, addedBy: adminId },
      updateData,
      { new: true, runValidators: true }
    );

    if (!updatedAccount) {
      throw createAppError(
        "Account not found or you don't have permission to update it",
        404
      );
    }

    return updatedAccount;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error updating account: ${error.message}`, 500);
  }
};

export const updateUserById = async (adminId, userId, updateData) => {
  try {
    // Sanitize the update data to prevent modifying restricted fields
    const allowedUpdates = [
      "lastName",
      "firstName",
      "email",
      "phoneNumber",
      "address",
      "accountStatus",
      "kycStatus",
      "bidSpread",
      "askSpread",
      "isFreeze"
    ];

    const sanitizedData = {};
    Object.keys(updateData).forEach((key) => {
      if (allowedUpdates.includes(key)) {
        sanitizedData[key] = updateData[key];
      }
    });

    // Find user by ID and ensure it belongs to the admin
    const updatedUser = await Account.findOneAndUpdate(
      { _id: userId, addedBy: adminId },
      { $set: sanitizedData },
      { new: true } // Return the updated document
    );

    return updatedUser;
  } catch (error) {
    throw createAppError(`Error updating user: ${error.message}`, 500);
  }
};
export const deleteAccountByCode = async (accode, adminId) => {
  try {
    // Find and delete account with the specific ACCODE and belonging to the admin
    const deletedAccount = await Account.findOneAndDelete({
      ACCODE: accode,
      addedBy: adminId,
    });
    console.log(deletedAccount);
    if (!deletedAccount) {
      throw createAppError(
        "Account not found or you don't have permission to delete it",
        404
      );
    }

    return deletedAccount;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error deleting account: ${error.message}`, 500);
  }
};
