import mt5MarketDataService from "../Trading/mt5MarketDataService.js";
import mt5Service from "../Trading/mt5Service.js";
import {
  handleMainMenuMT5,
  handleVolumeInputMT5,
  handleOrderConfirmationMT5,
  handlePositionSelectionMT5,
} from "../../controllers/chat/whatsappController.js";

export const getPriceMessageMT5 = async () => {
  try {
    const marketData = await mt5MarketDataService.getMarketData(
      process.env.MT5_SYMBOL || "XAUUSD_TTBAR.Fix"
    );
    if (!marketData) return "⚠️ Unable to fetch gold prices. Please try again.";

    const timestamp = new Date(marketData.timestamp).toLocaleString("en-US", {
      timeZone: "Asia/Dubai",
      dateStyle: "short",
      timeStyle: "medium",
    });
    return `💰 Live Gold Prices (XAUUSD_TTBAR.Fix)\n📈 Ask: $${marketData.ask.toFixed(
      2
    )}\n📉 Bid: $${marketData.bid.toFixed(
      2
    )}\n📊 Spread: ${marketData.spread.toFixed(
      2
    )}\n🕐 Updated: ${timestamp}\n📡 Source: MT5\n\nType MENU to return.`;
  } catch (error) {
    console.error("Error getting MT5 prices:", error.message);
    return "❌ Error fetching prices. Try again or contact support.";
  }
};

export const processUserInputMT5 = async (
  message,
  session,
  twilioClient,
  from,
  twilioNumber,
  phoneNumber
) => {
  const input = message.trim().toLowerCase();
  console.log(
    `processUserInputMT5 called with input: ${input}, state: ${session.state}, phoneNumber: ${phoneNumber}`
  );

  try {
    switch (session.state) {
      case "MAIN_MENU":
        return await handleMainMenuMT5(input, session, phoneNumber);
      case "AWAITING_VOLUME":
        return await handleVolumeInputMT5(input, session, phoneNumber);
      case "CONFIRM_ORDER":
        console.log(
          `Routing to handleOrderConfirmationMT5 for input: ${input}`
        );
        return await handleOrderConfirmationMT5(input, session, phoneNumber);
      case "VIEW_POSITIONS":
        console.log(
          `Routing to handlePositionSelectionMT5 for input: ${input}`
        );
        return await handlePositionSelectionMT5(input, session, phoneNumber);
      default:
        console.error(`Unknown session state: ${session.state}`);
        session.state = "MAIN_MENU";
        return await getMainMenuMT5();
    }
  } catch (error) {
    console.error("Input processing error:", error.message);
    session.state = "MAIN_MENU";
    return "❌ Error occurred. Try again.\n\n" + (await getMainMenuMT5());
  }
};

export const getMainMenuMT5 = () => {
  return `🏆 Gold Trading Bot - MT5\n📊 Main Menu:\n1️⃣ Buy Gold\n2️⃣ Sell Gold\n3️⃣ Live Prices\n4️⃣ View Positions\n5️⃣ Close Position\n\n💡 Commands: Type number or keyword (e.g., '1', 'buy')\n🔔 Trades at live market prices`;
};

export const getPositionsMessageMT5 = async (session, phoneNumber) => {
  try {
    const positions = await mt5Service.getPositions();
    if (!positions.length)
      return `📊 Open Positions\n📝 No open positions.\n\nType MENU to return.`;

    session.openPositions = positions;
    session.state = "VIEW_POSITIONS";

    let message = `📊 Open Positions\n`;
    positions.forEach((position, index) => {
      const profit =
        position.profit > 0
          ? `+$${position.profit.toFixed(2)}`
          : `$${position.profit.toFixed(2)}`;
      message += `${index + 1}️⃣ Ticket ${position.ticket}\n   📊 ${
        position.type
      } ${position.volume} TTBAR\n   💰 Open: $${position.price_open.toFixed(
        2
      )}\n   📈 Current: $${position.price_current.toFixed(
        2
      )}\n   📊 P&L: ${profit}\n\n`;
    });
    return `${message}🔧 Select position number to close or type MENU.`;
  } catch (error) {
    console.error("Error fetching positions:", error.message);
    return "❌ Error fetching positions. Try again.\n\nType MENU to return.";
  }
};
