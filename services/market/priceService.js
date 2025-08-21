import marketDataService, { isPriceFresh } from "../../services/market/marketDataService.js";
import { TROY_OUNCE_GRAMS, GOLD_CONVERSION_FACTOR, TTB_FACTOR, TTB_BACKUP_PRICE } from "../../utils/constants.js";

export const fetchLatestTTBPrices = async () => {
  try {
    const ttbPrices = marketDataService.getTTBPrices();
    if (ttbPrices) {
      return {
        askPrice: ttbPrices.offer || ttbPrices.askPrice,
        bidPrice: ttbPrices.bid || ttbPrices.bidPrice,
        timestamp: ttbPrices.timestamp,
      };
    }

    if (isPriceFresh("GOLD")) {
      const goldData = marketDataService.getMarketData("GOLD");
      if (goldData) {
        const askPrice = goldData.offer !== undefined ? goldData.offer : goldData.askPrice;
        const bidPrice = goldData.bid !== undefined ? goldData.bid : goldData.bidPrice;
        if (askPrice && bidPrice) {
          return {
            askPrice: calculateTTBPrice(askPrice),
            bidPrice: calculateTTBPrice(bidPrice),
            timestamp: marketDataService.lastUpdated.get("GOLD"),
          };
        }
      }
    }

    console.log("Requesting fresh market data for GOLD");
    marketDataService.requestSymbols(["GOLD"]);
    return {
      askPrice: TTB_BACKUP_PRICE,
      bidPrice: TTB_BACKUP_PRICE * 0.995,
      timestamp: Date.now(),
    };
  } catch (error) {
    console.error("Error fetching TTB prices:", error);
    return {
      askPrice: TTB_BACKUP_PRICE,
      bidPrice: TTB_BACKUP_PRICE * 0.995,
      timestamp: Date.now(),
    };
  }
};

const calculateTTBPrice = (goldPrice) => {
  if (!goldPrice || isNaN(goldPrice)) return null;
  return (goldPrice / TROY_OUNCE_GRAMS) * GOLD_CONVERSION_FACTOR * TTB_FACTOR;
};