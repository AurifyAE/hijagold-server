import Account from "../../models/AccountSchema.js";
import Admin from "../../models/AdminSchema.js";
import { cleanPhoneNumber } from "../../utils/helpers.js";

export const isAuthorizedUser = async (phoneNumber) => {
  try {
    const cleanNumber = cleanPhoneNumber(phoneNumber);
    const account = await Account.findOne({
      phoneNumber: { $regex: cleanNumber, $options: "i" },
    });
    return account
      ? { isAuthorized: true, accountId: account._id, accountDetails: account }
      : { isAuthorized: false };
  } catch (error) {
    console.error("Error checking authorized user:", error);
    return { isAuthorized: false };
  }
};

export const getAdminUser = async () => {
  try {
    return await Admin.findOne({});
  } catch (error) {
    console.error("Error fetching admin user:", error);
    return null;
  }
};