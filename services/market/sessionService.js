import { cleanPhoneNumber } from "../../utils/helpers.js";
import { storeUserOrdersInSession } from "../../services/market/orderService.js";

const userSessions = {};

export const getUserSession = (phoneNumber) => {
  const cleanNumber = cleanPhoneNumber(phoneNumber);
  if (!userSessions[cleanNumber]) {
    userSessions[cleanNumber] = {
      state: "START",
      cart: {},
      lastActivity: Date.now(),
      currentOrder: null,
      accountId: null,
      adminId: null,
      openOrders: [],
    };
  }
  userSessions[cleanNumber].lastActivity = Date.now();
  storeUserOrdersInSession(userSessions[cleanNumber]); // Pre-fetch orders
  return userSessions[cleanNumber];
};

export const resetSession = (phoneNumber) => {
  const cleanNumber = cleanPhoneNumber(phoneNumber);
  userSessions[cleanNumber] = {
    state: "START",
    cart: {},
    lastActivity: Date.now(),
    currentOrder: null,
    accountId: null,
    adminId: null,
    openOrders: [],
  };
};