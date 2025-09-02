import axios from "axios";
import { io } from "socket.io-client";
import dotenv from "dotenv";

dotenv.config();
const BASE_URL = "http://127.0.0.1:5000";
const WS_SECRET = process.env.WS_SECRET || "aurify@123";

class MT5Service {
  constructor() {
    this.connections = new Map();
    this.sockets = new Map();
    this.loadAccounts();
  }

  loadAccounts() {
    const accountSet = new Set();
    for (const key in process.env) {
      if (key.startsWith("MT5_LOGIN_")) {
        const accKey = key.substring(10);
        accountSet.add(accKey);
      }
    }

    for (const accKey of accountSet) {
      const server = process.env[`MT5_SERVER_${accKey}`];
      const login = process.env[`MT5_LOGIN_${accKey}`];
      const password = process.env[`MT5_PASSWORD_${accKey}`];
      const symbol = process.env[`MT5_SYMBOL_${accKey}`];

      if (server && login && password && symbol) {
        this.connections.set(accKey, {
          server,
          login: parseInt(login),
          password,
          symbol,
          accountId: accKey,
          isConnected: false,
          priceData: new Map(),
          lastPriceUpdate: new Map(),
          subscribers: new Map(),
        });
      }
    }

    console.log(`Loaded ${this.connections.size} MT5 accounts`);
  }

  async setupWebSocket(accKey) {
    const conn = this.connections.get(accKey);
    if (!conn) throw new Error(`No configuration for account ${accKey}`);
    if (this.sockets.get(accKey)) return;

    const socket = io(BASE_URL, {
      query: { secret: WS_SECRET, account_id: accKey },
      transports: ["websocket", "polling"],
      reconnection: true,
      reconnectionAttempts: 5,
      reconnectionDelay: 1000,
      reconnectionDelayMax: 30000,
      timeout: 20000,
    });

    socket.on("connect", () => {
      console.log(`WebSocket connected for ${accKey}`);
      socket.emit("request-data", { account_id: accKey, symbols: [conn.symbol] });
    });

    socket.on("market-data", (data) => {
      if (data.accountId !== accKey) return;
      console.log(`Received WebSocket price update for ${accKey} - ${data.symbol}`);
      conn.priceData.set(data.symbol, {
        bid: data.bid,
        ask: data.ask,
        spread: data.spread,
        time: new Date(data.time),
        high: data.high,
        low: data.low,
        marketStatus: data.marketStatus,
      });
      conn.lastPriceUpdate.set(data.symbol, Date.now());
      const callbacks = conn.subscribers.get(data.symbol) || new Set();
      callbacks.forEach((cb) => cb(data));
    });

    socket.on("error", (error) => {
      console.error(`WebSocket error for ${accKey}:`, error);
    });

    socket.on("connect_error", (error) => {
      console.error(`WebSocket connect error for ${accKey}:`, error.message);
    });

    socket.on("disconnect", (reason) => {
      console.log(`WebSocket disconnected for ${accKey}: ${reason}`);
      this.sockets.delete(accKey);
    });

    this.sockets.set(accKey, socket);
  }

  async ensureConnected(accKey) {
    const conn = this.connections.get(accKey);
    if (!conn) throw new Error(`No configuration for account ${accKey}`);
    if (!conn.isConnected) {
      await this.connect(accKey);
      await this.setupWebSocket(accKey);
    }
  }

  async connect(accKey) {
    const conn = this.connections.get(accKey);
    if (!conn) throw new Error(`No configuration for account ${accKey}`);
    if (conn.isConnected) return conn.accountId;

    try {
      const response = await axios.post(`${BASE_URL}/connect`, {
        server: conn.server,
        login: conn.login,
        password: conn.password,
        account_id: conn.accountId,
      });

      if (!response.data.success) {
        throw new Error(response.data.error.message || "Connection failed");
      }

      conn.isConnected = true;
      console.log(`Connected account ${accKey} with ID ${conn.accountId}`);
      return conn.accountId;
    } catch (error) {
      console.error(`MT5 connection failed for ${accKey}:`, error.message);
      throw error;
    }
  }

  async disconnect(accKey) {
    const conn = this.connections.get(accKey);
    if (!conn || !conn.isConnected) return;

    try {
      const socket = this.sockets.get(accKey);
      if (socket) {
        socket.emit("stop-data", { account_id: accKey, symbols: [conn.symbol] });
        socket.disconnect();
        this.sockets.delete(accKey);
      }

      const response = await axios.post(`${BASE_URL}/disconnect`, {
        account_id: conn.accountId,
      });
      conn.isConnected = false;
      conn.priceData.clear();
      conn.lastPriceUpdate.clear();
      conn.subscribers.clear();
      console.log(`Disconnected account ${accKey}`);
      return response.data.data.message;
    } catch (error) {
      console.error(`MT5 disconnect failed for ${accKey}:`, error.message);
      throw error;
    }
  }

  subscribePrice(accKey, symbol, callback) {
    const conn = this.connections.get(accKey);
    if (!conn) throw new Error(`No configuration for account ${accKey}`);
    const callbacks = conn.subscribers.get(symbol) || new Set();
    callbacks.add(callback);
    conn.subscribers.set(symbol, callbacks);
    return () => {
      callbacks.delete(callback);
      if (callbacks.size === 0) {
        conn.subscribers.delete(symbol);
        const socket = this.sockets.get(accKey);
        if (socket?.connected) {
          socket.emit("stop-data", { account_id: accKey, symbols: [symbol] });
        }
      }
    };
  }

  async getSymbols(accKey) {
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    try {
      const response = await axios.get(`${BASE_URL}/symbols`, {
        params: { account_id: conn.accountId },
      });
      if (!response.data.success) throw new Error(response.data.error.message);
      return response.data.data;
    } catch (error) {
      console.error(`Symbol fetch failed for ${accKey}:`, error.message);
      throw error;
    }
  }

  async getSymbolInfo(accKey, symbol) {
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    try {
      const encodedSymbol = encodeURIComponent(symbol);
      const response = await axios.get(
        `${BASE_URL}/symbol_info/${encodedSymbol}`,
        { params: { account_id: conn.accountId } }
      );
      if (!response.data.success) throw new Error(response.data.error.message);
      return response.data.data;
    } catch (error) {
      console.error(`Symbol info fetch failed for ${accKey} - ${symbol}:`, error.message);
      throw error;
    }
  }

  async getPrice(accKey, symbol = null) {
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    symbol = symbol || conn.symbol;
    try {
      const encodedSymbol = encodeURIComponent(symbol);
      const response = await axios.get(`${BASE_URL}/price/${encodedSymbol}`, {
        params: { account_id: conn.accountId },
      });
      if (!response.data.success) throw new Error(response.data.error.message);
      const priceData = response.data.data;
      conn.priceData.set(symbol, {
        bid: priceData.bid,
        ask: priceData.ask,
        spread: priceData.spread,
        time: new Date(priceData.time),
        high: priceData.high,
        low: priceData.low,
        marketStatus: priceData.marketStatus,
      });
      conn.lastPriceUpdate.set(symbol, Date.now());
      return priceData;
    } catch (error) {
      console.error(`Price fetch failed for ${accKey} - ${symbol}:`, error.message);
      throw error;
    }
  }

  async placeTrade(accKey, tradeData, retryCount = 0) {
    const maxRetries = 3;
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    try {
      // Test connection
      const testResult = await this.testConnection(accKey);
      if (!testResult.success) {
        throw new Error(`MT5 connection test failed for ${accKey}: ${testResult.error}`);
      }

      // Validate symbol
      const symbol = await this.validateSymbol(accKey, tradeData.symbol || conn.symbol);
      const info = await this.getSymbolInfo(accKey, symbol);
      if (info.trade_mode === 0) {
        throw new Error(`Symbol ${symbol} is not tradable for ${accKey}`);
      }

      // Validate filling mode
      const supportedFillingModes = [];
      if (info.filling_mode & 1) supportedFillingModes.push("FOK");
      if (info.filling_mode & 2) supportedFillingModes.push("RETURN");
      if (info.filling_mode & 4) supportedFillingModes.push("IOC");
      if (supportedFillingModes.length === 0) {
        console.warn(`No supported filling modes for ${symbol} on ${accKey}, defaulting to IOC`);
        supportedFillingModes.push("IOC");
      }
      console.log(`Supported filling modes for ${symbol} on ${accKey}: ${supportedFillingModes.join(", ")}`);

      // Validate account permissions
      let accountInfo;
      try {
        accountInfo = await this.getAccountInfo(accKey);
        if (!accountInfo.trade_allowed) {
          throw new Error(`Trading not allowed for account ${accKey}`);
        }
        if (accountInfo.balance < tradeData.requiredMargin) {
          throw new Error(`Insufficient balance for trade: ${accountInfo.balance} < ${tradeData.requiredMargin}`);
        }
      } catch (error) {
        console.error(`Account info fetch failed for ${accKey}: ${error.message}`);
        throw new Error(`Failed to validate account permissions: ${error.message}`);
      }

      // Validate volume
      let volume = parseFloat(tradeData.volume);
      if (isNaN(volume) || volume <= 0) {
        throw new Error(`Invalid volume: ${volume}`);
      }
      if (volume < info.volume_min) {
        throw new Error(`Volume ${volume} below minimum ${info.volume_min}`);
      }
      if (volume > info.volume_max) {
        throw new Error(`Volume ${volume} exceeds maximum ${info.volume_max}`);
      }
      volume = Math.round(volume / info.volume_step) * info.volume_step;

      // Validate SL/TP distances
      let slDistance = tradeData.slDistance || tradeData.stopLoss;
      let tpDistance = tradeData.tpDistance || tradeData.takeProfit;
      if (slDistance !== null && slDistance !== undefined && slDistance > 0) {
        const stopLevel = info.stops_level * info.point;
        slDistance = parseFloat(slDistance);
        if (slDistance < stopLevel) slDistance = stopLevel;
      }
      if (tpDistance !== null && tpDistance !== undefined && tpDistance > 0) {
        const stopLevel = info.stops_level * info.point;
        tpDistance = parseFloat(tpDistance);
        if (tpDistance < stopLevel) tpDistance = stopLevel;
      }

      // Validate price
      const priceData = await this.getPrice(accKey, symbol);
      if (!priceData || !priceData.bid || !priceData.ask) {
        throw new Error(`Invalid price data for ${symbol} on ${accKey}`);
      }

      // Prepare trade request
      let comment = tradeData.comment || `Ord-${Date.now().toString().slice(-6)}`;
      if (comment.length > 26) {
        comment = comment.slice(0, 26);
      }

      const request = {
        account_id: conn.accountId,
        symbol: symbol,
        volume: volume,
        type: tradeData.type.toUpperCase(),
        sl_distance: slDistance,
        tp_distance: tpDistance,
        comment: comment,
        magic: tradeData.magic || 123456,
        deviation: tradeData.deviation || 20,
        filling_mode: supportedFillingModes[0], // Send the first supported filling mode
      };

      console.log(`Sending trade request for ${accKey}:`, JSON.stringify(request, null, 2));

      // Send trade request to MT5 server
      const response = await axios.post(`${BASE_URL}/trade`, request, {
        timeout: 10000,
      });
      console.log(`Trade response for ${accKey}:`, JSON.stringify(response.data, null, 2));
      if (!response.data.success) {
        const errorMessage = response.data.error?.message || 'Unknown MT5 error';
        const errorCode = response.data.error?.code || 'N/A';
        throw new Error(`MT5 trade failed for ${accKey}: ${errorMessage} (Code: ${errorCode})`);
      }

      console.log(`Trade placed successfully for ${accKey}: Order ID ${response.data.data.order}`);
      return {
        success: true,
        ticket: response.data.data.order,
        volume: response.data.data.volume,
        price: response.data.data.price,
        symbol: response.data.data.symbol || symbol,
        type: request.type,
        comment: response.data.data.comment,
        sl: response.data.data.sl || 0,
        tp: response.data.data.tp || 0,
      };
    } catch (error) {
      const errorCode = error.response?.data?.error?.code || 'N/A';
      const errorMessage = errorCode
        ? {
            10013: "Requote detected",
            10018: "Market closed",
            10019: "Insufficient funds",
            10020: "Prices changed",
            10021: "Invalid request (check volume, symbol, or market status)",
            10022: "Invalid SL/TP",
            10017: "Invalid parameters",
            10027: "AutoTrading disabled",
            10030: "Invalid order filling type",
            'N/A': `Unknown error: ${error.message}`,
          }[errorCode] || `Unknown error: ${error.message} (Code: ${errorCode})`
        : error.message.includes("connection")
        ? "MT5 connection issue"
        : error.message;

      console.error(`Trade placement failed for ${accKey}: ${errorMessage}`, {
        errorCode,
        errorDetails: error.response?.data || error.message,
        stack: error.stack,
        request: tradeData,
      });

      // Retry for specific recoverable errors
      if ((errorCode === 10013 || errorCode === 10020 || errorCode === 10021 || errorCode === 10030) && retryCount < maxRetries) {
        console.log(
          `Retrying trade (${retryCount + 1}/${maxRetries}) for ${accKey} due to ${errorMessage}`
        );
        await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, retryCount)));
        // Try a different filling mode on retry
        const newFillingMode = supportedFillingModes[retryCount % supportedFillingModes.length] || "IOC";
        return this.placeTrade(
          accKey,
          { ...tradeData, deviation: (tradeData.deviation || 20) + 10, filling_mode: newFillingMode },
          retryCount + 1
        );
      }

      throw new Error(`MT5 trade failed for ${accKey}: ${errorMessage} (Code: ${errorCode})`);
    }
  }

  async closeTrade(accKey, tradeData, retryCount = 0) {
    const maxRetries = 3;
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    try {
      if (!tradeData.ticket || isNaN(tradeData.ticket)) {
        throw new Error(`Invalid ticket: ${tradeData.ticket} for ${accKey}`);
      }
      if (!tradeData.symbol) {
        throw new Error(`Missing symbol for ${accKey}`);
      }
      const validSymbol = await this.validateSymbol(accKey, tradeData.symbol);
      const info = await this.getSymbolInfo(accKey, validSymbol);
      if (info.trade_mode === 0) {
        throw new Error(`Symbol ${validSymbol} is not tradable for ${accKey}`);
      }

      const positions = await this.getPositions(accKey);
      const position = positions.find((p) => p.ticket === parseInt(tradeData.ticket));

      let volume = tradeData.volume ? parseFloat(tradeData.volume) : position?.volume;
      if (!volume || isNaN(volume) || volume <= 0) {
        throw new Error(
          `Invalid position volume: ${volume} for ticket: ${tradeData.ticket} on ${accKey}`
        );
      }

      volume = Math.max(info.volume_min, Math.min(info.volume_max, Math.round(volume / info.volume_step) * info.volume_step));

      const request = {
        account_id: conn.accountId,
        ticket: parseInt(tradeData.ticket),
        symbol: validSymbol,
        volume: volume,
      };

      console.log(`Sending close trade request for ${accKey}:`, JSON.stringify(request, null, 2));
      const response = await axios.post(`${BASE_URL}/close`, request);
      if (!response.data.success) {
        if (response.data.error.message.includes("Position not found")) {
          console.warn(`Position ${tradeData.ticket} not found in MT5 on ${accKey}. Likely already closed.`);
          return {
            success: false,
            error: `Position ${tradeData.ticket} not found in MT5 on ${accKey}`,
            ticket: tradeData.ticket,
            likelyClosed: true,
          };
        }
        throw new Error(response.data.error.message || `Close failed for ${accKey}`);
      }

      console.log(`Trade closed successfully for ticket ${tradeData.ticket} on ${accKey}`);
      return {
        success: true,
        ticket: tradeData.ticket,
        closePrice: response.data.data.price,
        profit: response.data.data.profit,
        symbol: validSymbol,
        data: response.data.data,
      };
    } catch (error) {
      const errorCode = error.response?.data?.error?.code;
      const errorMessage = errorCode
        ? {
            10013: "Requote detected",
            10018: "Market closed",
            10019: "Insufficient funds",
            10020: "Prices changed",
            10021: "Invalid request (check volume, symbol, or market status)",
            10022: "Invalid SL/TP",
            10017: "Invalid parameters",
            10027: "AutoTrading disabled",
            "-2": `Invalid volume argument: Requested ${tradeData.volume}`,
          }[errorCode] || `Unknown error: ${error.message}`
        : error.message;
      if (errorCode === 10021 && retryCount < maxRetries) {
        console.log(
          `Retrying close (${retryCount + 1}/${maxRetries}) for ticket ${
            tradeData.ticket
          } on ${accKey} due to ${errorMessage}`
        );
        await new Promise((r) => setTimeout(r, 1000));
        return this.closeTrade(
          accKey,
          {
            ...tradeData,
            deviation: (tradeData.deviation || 20) + 10,
          },
          retryCount + 1
        );
      }
      console.error(`Trade close failed for ticket ${tradeData.ticket} on ${accKey}:`, errorMessage);
      return {
        success: false,
        error: errorMessage,
        ticket: tradeData.ticket,
        likelyClosed: errorMessage.includes("Position not found"),
      };
    }
  }

  async getPositions(accKey) {
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    try {
      const response = await axios.get(`${BASE_URL}/positions`, {
        params: { account_id: conn.accountId },
      });
      if (!response.data.success) throw new Error(response.data.error.message);
      return response.data.data;
    } catch (error) {
      console.error(`Positions fetch failed for ${accKey}:`, error.message);
      throw error;
    }
  }

  async getAccountInfo(accKey) {
    await this.ensureConnected(accKey);
    const conn = this.connections.get(accKey);
    try {
      const response = await axios.get(`${BASE_URL}/account_info`, {
        params: { account_id: conn.accountId },
        timeout: 5000,
      });
      if (!response.data.success) throw new Error(response.data.error.message);
      return response.data.data;
    } catch (error) {
      console.error(`Account info fetch failed for ${accKey}:`, error.message);
      throw error;
    }
  }

  getCachedPrice(accKey, symbol) {
    return this.connections.get(accKey)?.priceData.get(symbol);
  }

  isPriceFresh(accKey, symbol, maxAge = 5000) {
    const ts = this.connections.get(accKey)?.lastPriceUpdate.get(symbol);
    return ts && Date.now() - ts < maxAge;
  }

  async validateSymbol(accKey, symbol) {
    const symbols = await this.getSymbols(accKey);
    if (symbols.includes(symbol)) {
      const info = await this.getSymbolInfo(accKey, symbol);
      if (info.trade_mode !== 0) {
        console.log(
          `Validated symbol ${symbol} with filling mode: ${info.filling_mode} for ${accKey}`
        );
        return symbol;
      }
      console.warn(`Symbol ${symbol} not tradable for ${accKey}`);
    }
    const matches = symbols.filter(
      (s) =>
        s.toLowerCase().includes(symbol.toLowerCase()) ||
        s.toLowerCase().includes("xau") ||
        s.toLowerCase().includes("gold") ||
        s === "XAUUSD.#" ||
        s === "XAUUSD"
    );
    for (const match of matches) {
      const info = await this.getSymbolInfo(accKey, match);
      if (info.trade_mode !== 0) {
        console.log(
          `Alternative symbol ${match} validated with filling mode: ${info.filling_mode} for ${accKey}`
        );
        return match;
      }
    }
    throw new Error(
      `Symbol ${symbol} not found or tradable for ${accKey}. Alternatives: ${matches.join(
        ", "
      )}`
    );
  }

  async testConnection(accKey) {
    try {
      const conn = this.connections.get(accKey);
      if (!conn.isConnected) throw new Error(`Not connected for ${accKey}`);
      const testResult = await this.getPrice(accKey, conn.symbol);
      console.log(`Connection test passed for ${accKey}:`, testResult);
      return { success: true, message: "MT5 working", testPrice: testResult };
    } catch (error) {
      console.error(`Connection test failed for ${accKey}:`, error);
      return {
        success: false,
        message: "MT5 test failed",
        error: error.message,
      };
    }
  }
}

const mt5Service = new MT5Service();
export default mt5Service;