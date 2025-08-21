import Order from "../../models/OrderSchema.js";
import { createTrade, updateTradeStatus } from "../../services/admin/tradingServices.js";
import { getAdminUser } from "../../services/market/userService.js";
import { checkSufficientBalance } from "../../services/market/balanceService.js";
import marketDataService, { generateOrderId, isPriceFresh } from "../../services/market/marketDataService.js";
import { fetchLatestTTBPrices } from "../../services/market/priceService.js";

export const storeUserOrdersInSession = async (session) => {
  try {
    const orders = await Order.find({
      user: session.accountId,
      orderStatus: "PROCESSING",
    }).sort({ createdAt: -1 });
    session.openOrders = orders;
    return orders;
  } catch (error) {
    console.error("Error fetching user orders:", error);
    return [];
  }
};

export const processOrderPlacement = async (session, volume, type) => {
  const admin = await getAdminUser();
  if (!admin) throw new Error("No admin user found");

  const balanceCheck = await checkSufficientBalance(session.accountId, volume);
  if (!balanceCheck.success) {
    return { success: false, message: balanceCheck.message };
  }

  marketDataService.requestSymbols(["GOLD"]);
  const ttbPrices = await fetchLatestTTBPrices();
  const ttbPrice = type === "BUY" ? ttbPrices.askPrice : ttbPrices.bidPrice;
  if (!ttbPrice || ttbPrice === 0) {
    throw new Error("Unable to get valid market price.");
  }

  let goldPrice = 0;
  let marketDataTimestamp = Date.now();
  if (isPriceFresh("GOLD")) {
    const goldData = marketDataService.getMarketData("GOLD");
    if (goldData) {
      goldPrice = type === "BUY"
        ? (goldData.offer !== undefined ? goldData.offer : goldData.askPrice)
        : (goldData.bid !== undefined ? goldData.bid : goldData.bidPrice);
      marketDataTimestamp = marketDataService.lastUpdated.get("GOLD");
    }
  }
  if (!goldPrice || goldPrice === 0) {
    throw new Error("Unable to get valid gold market price.");
  }

  const baseAmount = parseFloat(balanceCheck.baseAmount);
  const marginRequirement = parseFloat(balanceCheck.marginAmount);
  const requiredMargin = baseAmount + marginRequirement;
  const orderNo = generateOrderId();

  const tradeData = {
    orderNo,
    type,
    volume,
    symbol: "GOLD",
    price: goldPrice,
    requiredMargin: balanceCheck.totalNeededAmount,
    openingPrice: goldPrice,
    openingDate: new Date(),
    marketDataTimestamp,
  };

  const tradeResult = await createTrade(admin._id, session.accountId, tradeData);

  return {
    success: true,
    orderNo,
    symbol: "GOLD",
    volume,
    price: ttbPrice,
    total: requiredMargin.toFixed(2),
    orderId: tradeResult.clientOrder._id,
  };
};

export const processOrderClose = async (session, orderId) => {
  const admin = await getAdminUser();
  if (!admin) throw new Error("No admin user found");

  const order = await Order.findOne({
    _id: orderId,
    user: session.accountId,
    orderStatus: "PROCESSING",
  });
  if (!order) {
    return { success: false, message: "Order not found or already closed" };
  }

  marketDataService.requestSymbols(["GOLD"]);
  const ttbPrices = await fetchLatestTTBPrices();
  const closeType = order.type === "BUY" ? "SELL" : "BUY";
  const currentPrice = closeType === "BUY" ? ttbPrices.askPrice : ttbPrices.bidPrice;
  if (!currentPrice || currentPrice === 0) {
    return { success: false, message: "Unable to get valid market price." };
  }

  const updateData = {
    orderStatus: "CLOSED",
    closingPrice: currentPrice,
    closingDate: new Date(),
    marketDataTimestamp: ttbPrices.timestamp,
  };

  const result = await updateTradeStatus(admin._id, orderId, updateData);

  return {
    success: true,
    orderNo: order.orderNo,
    symbol: order.symbol,
    volume: order.volume,
    openPrice: order.openingPrice,
    closePrice: currentPrice,
    profit: result.profit.client?.toFixed(2) || 0,
    newCashBalance: result.balances.cash.toFixed(2),
    newGoldBalance: result.balances.gold.toFixed(2),
  };
};