import Account from "../../models/AccountSchema.js";
import Order from "../../models/OrderSchema.js";
import { MINIMUM_BALANCE_PERCENTAGE, BASE_AMOUNT_PER_VOLUME } from "../../utils/constants.js";

export const getUserBalance = async (accountId) => {
  try {
    const account = await Account.findById(accountId);
    return account ? { cash: account.AMOUNTFC, gold: account.METAL_WT } : { cash: 0, gold: 0 };
  } catch (error) {
    console.error("Error fetching user balance:", error);
    return { cash: 0, gold: 0 };
  }
};

export const checkSufficientBalance = async (accountId, volume) => {
  try {
    const account = await Account.findById(accountId);
    if (!account || !account.AMOUNTFC) {
      return { success: false, message: "User account information not available" };
    }

    const userBalance = parseFloat(account.AMOUNTFC);
    const volumeValue = parseInt(volume) || 0;
   
    // Calculate required amount (2% of account balance)
    const requiredAmount = userBalance * 0.02; // 2% of account balance
    
    // Check if user has sufficient balance (2% of their account)
    const isTradeValid = userBalance >= requiredAmount;

    return {
      success: isTradeValid,
      userBalance: userBalance.toFixed(2),
      requiredAmount: requiredAmount.toFixed(2), // 2% of balance
      remainingBalance: (userBalance - requiredAmount).toFixed(2),
      remainingPercentage: "98.0", // Always 98% remaining after using 2%
      message: isTradeValid 
        ? "Sufficient balance for trade" 
        : "Insufficient balance. Account must have at least 2% available for trading",
    };
  } catch (error) {
    console.error("Error checking sufficient balance:", error);
    return { success: false, message: "Error checking account balance" };
  }
};