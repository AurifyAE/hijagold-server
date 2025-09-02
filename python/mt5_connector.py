from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, disconnect
import MetaTrader5 as mt5
import sys
import time
import threading
from datetime import datetime
import logging
from queue import Queue
import asyncio
import uuid
import concurrent.futures

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'aurify@123'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', engineio_logger=False, max_http_buffer_size=10**7)

class MT5Connector:
    def __init__(self, account_id):
        self.account_id = account_id
        self.connected = False
        self.active_subscriptions = {}
        self.price_threads = {}
        self.symbol_data = {}
        self.mt5_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self.last_activity = time.time()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

    def update_activity(self):
        self.last_activity = time.time()

    def connect(self, server, login, password, terminal_path=None):
        self.update_activity()
        try:
            with self.mt5_lock:
                if terminal_path:
                    if not mt5.initialize(path=terminal_path):
                        logger.error(f"MT5 initialization failed for account {self.account_id}")
                        return False, {"code": 1000, "message": "MT5 initialization failed"}
                else:
                    if not mt5.initialize():
                        logger.error(f"MT5 initialization failed for account {self.account_id}")
                        return False, {"code": 1000, "message": "MT5 initialization failed"}
                
                authorized = mt5.login(login, password=password, server=server)
                if not authorized:
                    error = mt5.last_error()
                    logger.error(f"Login failed for account {self.account_id}: {error}")
                    return False, {"code": error[0], "message": f"Login failed: {error[1]}"}
                
                self.connected = True
                self.shutdown_event.clear()
                account_info = mt5.account_info()
                if account_info and not account_info.trade_expert:
                    logger.warning(f"AutoTrading disabled for account {self.account_id}")
                    return False, {"code": 1001, "message": "AutoTrading disabled. Enable 'Algo Trading' in MT5"}
                
                logger.info(f"Connected to MT5, account: {account_info.login if account_info else None}")
                return True, {
                    "message": "Connected",
                    "account": account_info.login if account_info else None,
                    "trade_allowed": account_info.trade_allowed if account_info else False,
                    "balance": account_info.balance if account_info else 0
                }
        except Exception as e:
            logger.exception(f"Connection error for account {self.account_id}: {str(e)}")
            return False, {"code": 1002, "message": str(e)}

    def disconnect(self):
        self.update_activity()
        try:
            with self.mt5_lock:
                self.connected = False
                self.shutdown_event.set()
                
                for symbol in list(self.price_threads.keys()):
                    self.stop_price_stream(symbol)
                
                self.active_subscriptions.clear()
                self.price_threads.clear()
                self.executor.shutdown(wait=True)
                mt5.shutdown()
                logger.info(f"Disconnected from MT5 for account {self.account_id}")
                return True, {"message": "Disconnected"}
        except Exception as e:
            logger.exception(f"Disconnect error for account {self.account_id}: {str(e)}")
            return False, {"code": 1003, "message": str(e)}

    def get_symbols(self):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                symbols = mt5.symbols_get() or []
                return True, [symbol.name for symbol in symbols]
        except Exception as e:
            logger.exception(f"Error getting symbols for account {self.account_id}: {str(e)}")
            return False, {"code": 1005, "message": str(e)}

    def get_symbol_info(self, symbol):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                if not mt5.symbol_select(symbol, True):
                    last_err = mt5.last_error()
                    return False, {"code": 1006, "message": f"Symbol {symbol} not selected. Last error: {last_err}"}
                
                info = mt5.symbol_info(symbol)
                if not info:
                    return False, {"code": 1007, "message": f"Symbol {symbol} not found"}
                
                stops_level = getattr(info, 'stops_level', 0)
                return True, {
                    "name": info.name,
                    "point": info.point,
                    "digits": info.digits,
                    "spread": info.spread,
                    "trade_mode": info.trade_mode,
                    "volume_min": info.volume_min,
                    "volume_max": info.volume_max,
                    "volume_step": info.volume_step,
                    "stops_level": stops_level,
                    "filling_mode": info.filling_mode
                }
        except Exception as e:
            logger.exception(f"Error getting symbol info for {symbol} for account {self.account_id}: {str(e)}")
            return False, {"code": 1008, "message": str(e)}

    def get_price(self, symbol):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                if not mt5.symbol_select(symbol, True):
                    last_err = mt5.last_error()
                    return False, {"code": 1006, "message": f"Symbol {symbol} not selected. Last error: {last_err}"}
                
                tick = mt5.symbol_info_tick(symbol)
                
                if not tick:
                    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, 1)
                    if not rates or not len(rates):
                        return False, {"code": 1009, "message": f"No price data for {symbol}"}
                    
                    rate = rates[0]
                    return True, {
                        "symbol": symbol,
                        "bid": rate['close'],
                        "ask": rate['close'],
                        "spread": 0,
                        "time": datetime.fromtimestamp(rate['time']).isoformat(),
                        "timestamp": time.time(),
                        "high": rate['high'],
                        "low": rate['low'],
                        "marketStatus": "CLOSED"
                    }
                
                symbol_info = mt5.symbol_info(symbol)
                spread = (tick.ask - tick.bid) / symbol_info.point if symbol_info and symbol_info.point > 0 else 0
                
                current_data = self.symbol_data.get(symbol, {"high": None, "low": None})
                current_high = max(current_data["high"] or tick.bid, tick.bid, tick.ask)
                current_low = min(current_data["low"] or tick.bid, tick.bid, tick.ask)
                
                self.symbol_data[symbol] = {
                    "high": current_high,
                    "low": current_low,
                    "last_timestamp": datetime.fromtimestamp(tick.time).isoformat()
                }
                
                return True, {
                    "symbol": symbol,
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "spread": spread,
                    "time": datetime.fromtimestamp(tick.time).isoformat(),
                    "timestamp": time.time(),
                    "high": current_high,
                    "low": current_low,
                    "marketStatus": "TRADEABLE" if symbol_info and symbol_info.trade_mode != 0 else "CLOSED",
                    "accountId": self.account_id
                }
        except Exception as e:
            logger.exception(f"Error getting price for {symbol} for account {self.account_id}: {str(e)}")
            return False, {"code": 1010, "message": str(e)}

    def start_price_stream(self, symbol, client_id):
        self.update_activity()
        if symbol in self.price_threads:
            if client_id not in self.active_subscriptions.get(symbol, []):
                self.active_subscriptions.setdefault(symbol, []).append(client_id)
            return
        
        self.active_subscriptions[symbol] = [client_id]
        
        def price_worker():
            last_price = None
            error_count = 0
            max_errors = 5
            batch_interval = 0.05
            
            while symbol in self.price_threads and self.connected and not self.shutdown_event.is_set():
                try:
                    success, price_data = self.get_price(symbol)
                    if success:
                        current_price = f"{price_data['bid']}-{price_data['ask']}"
                        if current_price != last_price:
                            socketio.emit('market-data', price_data, room=f'account_{self.account_id}_symbol_{symbol}', namespace='/')
                            last_price = current_price
                            error_count = 0
                    else:
                        error_count += 1
                        if error_count >= max_errors:
                            break
                    
                    time.sleep(batch_interval)
                except Exception as e:
                    error_count += 1
                    if error_count >= max_errors:
                        break
            
            if symbol in self.price_threads:
                del self.price_threads[symbol]
            if symbol in self.active_subscriptions:
                del self.active_subscriptions[symbol]
        
        future = self.executor.submit(price_worker)
        self.price_threads[symbol] = future
        logger.info(f"Started price stream for {symbol} with client {client_id} for account {self.account_id}")

    def stop_price_stream(self, symbol, client_id=None):
        self.update_activity()
        try:
            if symbol not in self.active_subscriptions:
                return
            if client_id:
                if client_id in self.active_subscriptions[symbol]:
                    self.active_subscriptions[symbol].remove(client_id)
                if not self.active_subscriptions[symbol]:
                    del self.active_subscriptions[symbol]
                    if symbol in self.price_threads:
                        del self.price_threads[symbol]
            else:
                if symbol in self.active_subscriptions:
                    del self.active_subscriptions[symbol]
                if symbol in self.price_threads:
                    del self.price_threads[symbol]
        except Exception as e:
            logger.exception(f"Error stopping price stream for {symbol} for account {self.account_id}: {str(e)}")

    def place_trade(self, symbol, volume, order_type, sl_distance=None, tp_distance=None, comment="", magic=0, filling_mode="IOC"):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
        
            with self.mt5_lock:
                # Validate symbol
                if not mt5.symbol_select(symbol, True):
                    last_err = mt5.last_error()
                    logger.error(f"Symbol {symbol} selection failed for account {self.account_id}: {last_err}")
                    return False, {"code": 1006, "message": f"Symbol {symbol} not selected. Last error: {last_err}"}
            
                info = mt5.symbol_info(symbol)
                if not info or info.trade_mode == 0:
                    logger.error(f"Symbol {symbol} not found or not tradable for account {self.account_id}")
                    return False, {"code": 1007, "message": f"Symbol {symbol} not found or not tradable"}
            
                # Validate account
                account_info = mt5.account_info()
                if not account_info:
                    logger.error(f"Failed to retrieve account info for account {self.account_id}")
                    return False, {"code": 1011, "message": "Failed to retrieve account info"}
                if not account_info.trade_allowed:
                    logger.error(f"AutoTrading disabled for account {self.account_id}")
                    return False, {"code": 10027, "message": "AutoTrading disabled"}
                if account_info.balance < volume * 100:  # Rough margin check
                    logger.error(f"Insufficient balance for trade: {account_info.balance} < {volume * 100}")
                    return False, {"code": 10019, "message": "Insufficient funds"}

                # Get current price
                tick = mt5.symbol_info_tick(symbol)
                if not tick:
                    logger.error(f"No price data for {symbol} for account {self.account_id}")
                    return False, {"code": 1009, "message": f"No price for {symbol}"}
            
                # Set order type and price
                order_type = order_type.upper()
                if order_type == "BUY":
                    mt5_type = mt5.ORDER_TYPE_BUY
                    price = tick.ask
                    sl = round(price - sl_distance, info.digits) if sl_distance else 0
                    tp = round(price + tp_distance, info.digits) if tp_distance else 0
                elif order_type == "SELL":
                    mt5_type = mt5.ORDER_TYPE_SELL
                    price = tick.bid
                    sl = round(price + sl_distance, info.digits) if sl_distance else 0
                    tp = round(price - tp_distance, info.digits) if tp_distance else 0
                else:
                    logger.error(f"Invalid order type {order_type} for account {self.account_id}")
                    return False, {"code": 10017, "message": f"Invalid order type {order_type}"}
            
                # Validate volume
                volume = max(info.volume_min, min(info.volume_max, round(volume / info.volume_step) * info.volume_step))

                # Log MT5 constants and filling mode
                logger.info(f"MT5 Constants for account {self.account_id} - FOK: {mt5.ORDER_FILLING_FOK}, IOC: {mt5.ORDER_FILLING_IOC}, RETURN: {mt5.ORDER_FILLING_RETURN}")
                logger.info(f"Symbol {symbol} filling_mode bitmask: {info.filling_mode}")
            
                # Define filling modes with verified MT5 constants
                filling_modes = [
                    (mt5.ORDER_FILLING_IOC, "IOC", "Immediate or Cancel"),
                    (mt5.ORDER_FILLING_FOK, "FOK", "Fill or Kill"),
                    (mt5.ORDER_FILLING_RETURN, "RETURN", "Market execution")
                ]
            
                # Use the provided filling mode if valid, otherwise try supported modes
                supported_modes = [(mt5.ORDER_FILLING_IOC, "IOC", "Immediate or Cancel")]  # Default to IOC
                for mode_constant, mode_name, description in filling_modes:
                    if info.filling_mode & mode_constant:
                        supported_modes.append((mode_constant, mode_name, description))
                        logger.info(f"Symbol {symbol} supports filling mode: {mode_name} ({mode_constant}) - {description}")
            
                # Ensure the requested filling mode is supported
                requested_mode = None
                for mode_constant, mode_name, description in filling_modes:
                    if mode_name == filling_mode and (info.filling_mode & mode_constant or mode_name == "IOC"):
                        requested_mode = (mode_constant, mode_name, description)
                        break
                
                if requested_mode:
                    supported_modes.insert(0, requested_mode)  # Prioritize requested mode
                else:
                    logger.warning(f"Requested filling mode {filling_mode} not supported for {symbol}. Using default IOC.")
                
                # Remove duplicates while preserving order
                seen = set()
                supported_modes = [mode for mode in supported_modes if not (mode[1] in seen or seen.add(mode[1]))]
            
                # Try each supported filling mode
                last_error = None
                for mode_constant, mode_name, description in supported_modes:
                    request = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "volume": volume,
                        "type": mt5_type,
                        "price": price,
                        "deviation": 20,
                        "magic": magic,
                        "comment": comment,
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mode_constant
                    }
                
                    if sl_distance and sl > 0:
                        request["sl"] = sl
                    if tp_distance and tp > 0:
                        request["tp"] = tp
                
                    logger.info(f"Attempting trade with {mode_name} filling mode (constant={mode_constant}) for {symbol} on account {self.account_id}")
                    logger.debug(f"Full trade request: {request}")
                
                    result = mt5.order_send(request)
                    logger.info(f"MT5 result for {mode_name}: retcode={result.retcode}, comment='{result.comment}', request_id={result.request_id}")
                
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        logger.info(f"✅ Trade successful with {mode_name} filling mode for account {self.account_id}")
                        return True, {
                            "order": result.order,
                            "deal": result.deal,
                            "volume": result.volume,
                            "price": result.price,
                            "sl": sl,
                            "tp": tp,
                            "comment": comment,
                            "retcode": result.retcode,
                            "filling_mode_used": mode_name,
                            "filling_mode_constant": mode_constant
                        }
                    else:
                        error_messages = {
                            mt5.TRADE_RETCODE_REQUOTE: "Requote detected",
                            mt5.TRADE_RETCODE_REJECT: "Order rejected by server",
                            mt5.TRADE_RETCODE_INVALID: "Invalid request parameters",
                            mt5.TRADE_RETCODE_INVALID_FILL: "Invalid filling type",
                            mt5.TRADE_RETCODE_INVALID_VOLUME: "Invalid volume",
                            mt5.TRADE_RETCODE_NO_MONEY: "Insufficient funds",
                            mt5.TRADE_RETCODE_TRADE_DISABLED: "Trading disabled",
                            mt5.TRADE_RETCODE_MARKET_CLOSED: "Market closed",
                            mt5.TRADE_RETCODE_INVALID_PRICE: "Invalid price",
                            mt5.TRADE_RETCODE_INVALID_STOPS: "Invalid stops",
                            mt5.TRADE_RETCODE_INVALID_EXPIRATION: "Invalid expiration",
                            mt5.TRADE_RETCODE_CONNECTION: "No connection to trade server",
                            mt5.TRADE_RETCODE_ONLY_REAL: "Only real accounts allowed",
                            mt5.TRADE_RETCODE_LIMIT_ORDERS: "Order limit reached",
                            mt5.TRADE_RETCODE_LIMIT_VOLUME: "Volume limit reached",
                            mt5.TRADE_RETCODE_INVALID_ORDER: "Invalid order",
                            mt5.TRADE_RETCODE_POSITION_CLOSED: "Position already closed",
                            mt5.TRADE_RETCODE_INVALID_CLOSE_VOLUME: "Invalid close volume",
                            mt5.TRADE_RETCODE_CLOSE_ORDER_EXIST: "Close order already exists",
                            mt5.TRADE_RETCODE_LIMIT_POSITIONS: "Position limit reached",
                            mt5.TRADE_RETCODE_REJECT_CANCEL: "Request canceled",
                            mt5.TRADE_RETCODE_LONG_ONLY: "Only long positions allowed",
                            mt5.TRADE_RETCODE_SHORT_ONLY: "Only short positions allowed",
                            mt5.TRADE_RETCODE_CLOSE_ONLY: "Only position closing allowed",
                            mt5.TRADE_RETCODE_FIFO_CLOSE: "FIFO rule violation"
                        }
                    
                        error_msg = error_messages.get(result.retcode, f"Unknown error code: {result.retcode}")
                        last_error = {
                            "code": result.retcode,
                            "message": f"{error_msg} (tried {mode_name})",
                            "mt5_comment": result.comment,
                            "filling_mode_tried": mode_name,
                            "filling_mode_constant": mode_constant
                        }
                    
                        logger.warning(f"❌ Trade failed with {mode_name} (constant={mode_constant}): {error_msg} for account {self.account_id}")
                        logger.warning(f"MT5 comment: '{result.comment}'")
                    
                        if result.retcode in [
                            mt5.TRADE_RETCODE_NO_MONEY,
                            mt5.TRADE_RETCODE_TRADE_DISABLED,
                            mt5.TRADE_RETCODE_MARKET_CLOSED,
                            mt5.TRADE_RETCODE_INVALID_VOLUME,
                            mt5.TRADE_RETCODE_CONNECTION,
                            mt5.TRADE_RETCODE_ONLY_REAL
                        ]:
                            logger.error(f"Terminal error {result.retcode}, stopping attempts for account {self.account_id}")
                            break
                
                if last_error:
                    logger.error(f"All filling modes failed for {symbol}. Last error: {last_error} for account {self.account_id}")
                    return False, last_error
                else:
                    logger.error(f"All filling modes failed for {symbol} for account {self.account_id}")
                    return False, {"code": 10030, "message": f"All filling modes failed for {symbol}"}
                
        except Exception as e:
            logger.exception(f"Exception in place_trade for {symbol} account {self.account_id}: {str(e)}")
            return False, {"code": 1012, "message": f"Trade placement error: {str(e)}"}

    def close_trade(self, ticket, volume=None, symbol=None):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                position = mt5.positions_get(ticket=ticket)
                if not position:
                    return False, {"code": 1013, "message": f"Position {ticket} not found"}
                
                pos = position[0]
                symbol = symbol or pos.symbol
                close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY
                volume = volume or pos.volume
                
                info = mt5.symbol_info(symbol)
                if not info or info.trade_mode == 0:
                    return False, {"code": 1007, "message": f"Symbol {symbol} not tradable"}
                
                volume = max(info.volume_min, min(info.volume_max, round(volume / info.volume_step) * info.volume_step))
                
                # Get current tick
                tick = mt5.symbol_info_tick(symbol)
                if not tick:
                    return False, {"code": 1009, "message": f"No price for {symbol}"}
                
                price = tick.bid if pos.type == mt5.POSITION_TYPE_BUY else tick.ask
                
                filling_mode = info.filling_mode
                logger.info(f"Close trade - Symbol {symbol} filling_mode bitmask: {filling_mode}")
                
                filling_modes = [
                    (mt5.ORDER_FILLING_FOK, "FOK"),
                    (mt5.ORDER_FILLING_IOC, "IOC"),
                    (mt5.ORDER_FILLING_RETURN, "Return")
                ]
                
                supported_modes = []
                for mode_constant, mode_name in filling_modes:
                    if filling_mode & mode_constant:
                        supported_modes.append((mode_constant, mode_name))
                        logger.info(f"Close trade - Symbol {symbol} supports filling mode: {mode_name} ({mode_constant})")
                
                if not supported_modes:
                    logger.warning(f"Close trade - No standard filling modes detected for {symbol}. Using default IOC.")
                    supported_modes = [(mt5.ORDER_FILLING_IOC, "IOC")]
                
                last_error = None
                for mode_constant, mode_name in supported_modes:
                    request = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "volume": volume,
                        "type": close_type,
                        "position": ticket,
                        "price": price,
                        "magic": pos.magic,
                        "comment": f"Close {ticket}",
                        "type_filling": mode_constant,
                        "deviation": 20
                    }
                    
                    logger.info(f"Attempting close with {mode_name} filling mode ({mode_constant}) for {symbol}")
                    logger.debug(f"Close request: {request}")
                    
                    result = mt5.order_send(request)
                    logger.info(f"MT5 close result for {mode_name}: retcode={result.retcode}, comment='{result.comment}'")
                    
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        logger.info(f"Close successful with {mode_name} filling mode")
                        return True, {
                            "deal": result.deal,
                            "retcode": result.retcode,
                            "price": result.price,
                            "volume": result.volume,
                            "profit": pos.profit,
                            "symbol": symbol,
                            "filling_mode_used": mode_name
                        }
                    else:
                        error_msg = {
                            mt5.TRADE_RETCODE_REQUOTE: "Requote detected",
                            mt5.TRADE_RETCODE_REJECT: "Order rejected",
                            mt5.TRADE_RETCODE_INVALID: "Invalid request parameters",
                            mt5.TRADE_RETCODE_INVALID_FILL: "Invalid filling type",
                            mt5.TRADE_RETCODE_INVALID_VOLUME: "Invalid volume",
                            mt5.TRADE_RETCODE_NO_MONEY: "Insufficient funds",
                            mt5.TRADE_RETCODE_TRADE_DISABLED: "Trading disabled",
                            mt5.TRADE_RETCODE_MARKET_CLOSED: "Market closed",
                            mt5.TRADE_RETCODE_INVALID_PRICE: "Invalid price",
                            mt5.TRADE_RETCODE_INVALID_STOPS: "Invalid stops"
                        }.get(result.retcode, f"Unknown error code: {result.retcode}")
                        
                        last_error = {"code": result.retcode, "message": f"{error_msg} (tried {mode_name})"}
                        logger.warning(f"Close failed with {mode_name} filling mode: {error_msg}")
                        
                        if result.retcode in [
                            mt5.TRADE_RETCODE_TRADE_DISABLED,
                            mt5.TRADE_RETCODE_MARKET_CLOSED,
                            mt5.TRADE_RETCODE_INVALID_VOLUME
                        ]:
                            break
                
                if last_error:
                    return False, last_error
                else:
                    return False, {"code": 10030, "message": f"All filling modes failed for close of {symbol}"}
                    
        except Exception as e:
            logger.exception(f"Error closing trade {ticket} for account {self.account_id}: {str(e)}")
            return False, {"code": 1014, "message": str(e)}

    def get_positions(self):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
            with self.mt5_lock:
                positions = mt5.positions_get() or []
                result = []
                for pos in positions:
                    result.append({
                        "ticket": pos.ticket,
                        "symbol": pos.symbol,
                        "type": "BUY" if pos.type == mt5.POSITION_TYPE_BUY else "SELL",
                        "volume": pos.volume,
                        "price_open": pos.price_open,
                        "price_current": pos.price_current,
                        "sl": pos.sl,
                        "tp": pos.tp,
                        "profit": pos.profit,
                        "time": datetime.fromtimestamp(pos.time).isoformat(),
                        "comment": pos.comment,
                        "magic": pos.magic
                    })
                return True, result
        except Exception as e:
            logger.exception(f"Error getting positions for account {self.account_id}: {str(e)}")
            return False, {"code": 1014, "message": str(e)}

    def get_account_info(self):
        self.update_activity()
        try:
            if not self.connected:
                return False, {"code": 1004, "message": "Not connected"}
            with self.mt5_lock:
                account_info = mt5.account_info()
                if not account_info:
                    return False, {"code": 1011, "message": "Failed to retrieve account info"}
                return True, {
                    "login": account_info.login,
                    "balance": account_info.balance,
                    "equity": account_info.equity,
                    "margin": account_info.margin,
                    "margin_free": account_info.margin_free,
                    "trade_allowed": account_info.trade_allowed,
                    "trade_expert": account_info.trade_expert
                }
        except Exception as e:
            logger.exception(f"Error getting account info for account {self.account_id}: {str(e)}")
            return False, {"code": 1015, "message": str(e)}

connectors = {}
connectors_lock = threading.Lock()

def get_connector(account_id):
    with connectors_lock:
        if account_id not in connectors:
            connectors[account_id] = MT5Connector(account_id)
        return connectors[account_id]

def remove_connector(account_id):
    with connectors_lock:
        if account_id in connectors:
            connector = connectors[account_id]
            connector.disconnect()
            del connectors[account_id]

def cleanup_inactive_connectors():
    with connectors_lock:
        current_time = time.time()
        for account_id in list(connectors.keys()):
            connector = connectors[account_id]
            if not connector.connected or (current_time - connector.last_activity > 1800 and not connector.active_subscriptions):
                connector.disconnect()
                del connectors[account_id]

@socketio.on('connect')
def handle_connect():
    client_id = request.sid
    secret = request.args.get('secret')
    account_id = request.args.get('account_id')
    if secret != app.config['SECRET_KEY']:
        emit('error', {'code': 2000, 'message': 'Authentication failed'})
        disconnect()
        return False
    if not account_id:
        emit('error', {'code': 2004, 'message': 'No account_id provided'})
        disconnect()
        return False
    emit('connected', {'message': f'Connected to MT5 WebSocket server for account {account_id}'})

@socketio.on('disconnect')
def handle_disconnect():
    client_id = request.sid
    account_id = request.args.get('account_id')
    if account_id:
        connector = get_connector(account_id)
        for symbol in list(connector.active_subscriptions.keys()):
            connector.stop_price_stream(symbol, client_id)

@socketio.on('request-data')
def handle_request_data(data):
    client_id = request.sid
    account_id = data.get('account_id') or request.args.get('account_id')
    symbols = data.get('symbols', [])
    if not account_id:
        return emit('error', {'code': 2004, 'message': 'No account_id provided'})
    connector = get_connector(account_id)
    if not connector.connected:
        return emit('error', {'code': 2002, 'message': 'MT5 not connected'})
    for symbol in symbols:
        socketio.server.enter_room(client_id, f'account_{account_id}_symbol_{symbol}')
        connector.start_price_stream(symbol, client_id)

@socketio.on('stop-data')
def handle_stop_data(data):
    client_id = request.sid
    account_id = data.get('account_id') or request.args.get('account_id')
    symbols = data.get('symbols', [])
    if not account_id:
        return emit('error', {'code': 2004, 'message': 'No account_id provided'})
    connector = get_connector(account_id)
    for symbol in symbols:
        socketio.server.leave_room(client_id, f'account_{account_id}_symbol_{symbol}')
        connector.stop_price_stream(symbol, client_id)

@app.route('/connect', methods=['POST'])
def connect():
    data = request.json
    server = data.get('server')
    login = data.get('login')
    password = data.get('password')
    terminal_path = data.get('terminal_path')
    account_id = data.get('account_id', str(uuid.uuid4()))
    if not all([server, login, password]):
        return jsonify({"success": False, "error": {"code": 3001, "message": "Missing parameters"}}), 400
    connector = get_connector(account_id)
    success, result = connector.connect(server, login, password, terminal_path)
    if success:
        return jsonify({"success": True, "data": {**result, "account_id": account_id}})
    else:
        remove_connector(account_id)
        return jsonify({"success": False, "error": result}), 400

@app.route('/disconnect', methods=['POST'])
def disconnect_endpoint():
    data = request.json
    account_id = data.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.disconnect()
    if success:
        remove_connector(account_id)
        return jsonify({"success": True, "data": result})
    else:
        return jsonify({"success": False, "error": result}), 400

@app.route('/symbols', methods=['GET'])
def get_symbols_endpoint():
    account_id = request.args.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.get_symbols()
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/symbol_info/<symbol>', methods=['GET'])
def get_symbol_info_endpoint(symbol):
    account_id = request.args.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.get_symbol_info(symbol)
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/price/<symbol>', methods=['GET'])
def get_price_endpoint(symbol):
    account_id = request.args.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.get_price(symbol)
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/trade', methods=['POST'])
def trade_endpoint():
    data = request.json
    account_id = data.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.place_trade(
        data.get('symbol'),
        data.get('volume', 0.1),
        data.get('type'),
        data.get('sl_distance'),
        data.get('tp_distance'),
        data.get('comment', ''),
        data.get('magic', 0),
        data.get('filling_mode', 'IOC')  # Default to IOC
    )
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/close', methods=['POST'])
def close_endpoint():
    data = request.json
    account_id = data.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.close_trade(data.get('ticket'), data.get('volume'), data.get('symbol'))
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/positions', methods=['GET'])
def get_positions_endpoint():
    account_id = request.args.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.get_positions()
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/account_info', methods=['GET'])
def get_account_info_endpoint():
    account_id = request.args.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    connector = get_connector(account_id)
    success, result = connector.get_account_info()
    logger.info(f"Account info requested for {account_id}: Success={success}, Result={result}")
    return jsonify({"success": success, "data" if success else "error": result})

@app.route('/debug/symbol/<symbol>', methods=['GET'])
def debug_symbol_endpoint(symbol):
    account_id = request.args.get('account_id')
    if not account_id:
        return jsonify({"success": False, "error": {"code": 3008, "message": "No account_id provided"}}), 400
    
    connector = get_connector(account_id)
    
    try:
        if not connector.connected:
            return jsonify({"success": False, "error": {"code": 1004, "message": "Not connected"}})
        
        with connector.mt5_lock:
            if not mt5.symbol_select(symbol, True):
                last_err = mt5.last_error()
                return jsonify({"success": False, "error": {"code": 1006, "message": f"Symbol {symbol} not selected. Last error: {last_err}"}})
            
            info = mt5.symbol_info(symbol)
            if not info:
                return jsonify({"success": False, "error": {"code": 1007, "message": f"Symbol {symbol} not found"}})
            
            filling_mode = info.filling_mode
            
            filling_analysis = {
                "raw_filling_mode": filling_mode,
                "supported_modes": [],
                "mode_details": {}
            }
            
            modes = [
                (mt5.ORDER_FILLING_FOK, "FOK", "Fill or Kill"),
                (mt5.ORDER_FILLING_IOC, "IOC", "Immediate or Cancel"),
                (mt5.ORDER_FILLING_RETURN, "Return", "Partial fills allowed")
            ]
            
            for mode_constant, mode_name, description in modes:
                is_supported = bool(filling_mode & mode_constant)
                filling_analysis["mode_details"][mode_name] = {
                    "constant": mode_constant,
                    "supported": is_supported,
                    "description": description
                }
                if is_supported:
                    filling_analysis["supported_modes"].append(mode_name)
            
            tick = mt5.symbol_info_tick(symbol)
            tick_info = {
                "available": bool(tick),
                "bid": tick.bid if tick else None,
                "ask": tick.ask if tick else None,
                "time": datetime.fromtimestamp(tick.time).isoformat() if tick else None
            }
            
            return jsonify({
                "success": True,
                "data": {
                    "symbol": symbol,
                    "symbol_info": {
                        "name": info.name,
                        "trade_mode": info.trade_mode,
                        "trade_allowed": info.trade_mode != 0,
                        "point": info.point,
                        "digits": info.digits,
                        "volume_min": info.volume_min,
                        "volume_max": info.volume_max,
                        "volume_step": info.volume_step,
                        "stops_level": getattr(info, 'stops_level', 0)
                    },
                    "filling_mode_analysis": filling_analysis,
                    "current_tick": tick_info,
                    "account_info": {
                        "login": mt5.account_info().login if mt5.account_info() else None,
                        "trade_allowed": mt5.account_info().trade_allowed if mt5.account_info() else None,
                        "trade_expert": mt5.account_info().trade_expert if mt5.account_info() else None
                    }
                }
            })
            
    except Exception as e:
        logger.exception(f"Error in debug endpoint for {symbol} for account {account_id}: {str(e)}")
        return jsonify({"success": False, "error": {"code": 1016, "message": str(e)}})

@app.route('/health', methods=['GET'])
def health():
    account_id = request.args.get('account_id')
    if account_id:
        connector = get_connector(account_id)
        return jsonify({
            "success": True,
            "data": {
                "status": "running",
                "account_id": account_id,
                "connected": connector.connected,
                "active_subscriptions": list(connector.active_subscriptions.keys()),
                "timestamp": datetime.now().isoformat()
            }
        })
    return jsonify({
        "success": True,
        "data": {
            "status": "running",
            "connected_accounts": list(connectors.keys()),
            "timestamp": datetime.now().isoformat()
        }
    })

if __name__ == '__main__':
    logger.info("Starting MT5 WebSocket server...")
    def cleanup_loop():
        while True:
            time.sleep(300)
            cleanup_inactive_connectors()
    cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
    cleanup_thread.start()
    
    # Handle graceful shutdown
    import signal
    def handle_shutdown(signum, frame):
        logger.info("Received shutdown signal, initiating graceful shutdown...")
        for account_id in list(connectors.keys()):
            remove_connector(account_id)
        logger.info("Shutdown complete")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)