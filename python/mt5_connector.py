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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'aurify@123'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', engineio_logger=False)

class MT5Connector:
    def __init__(self):
        self.connected = False
        self.active_subscriptions = {}
        self.price_threads = {}
        self.symbol_data = {}
        self.mt5_lock = threading.Lock()
        self.data_queue = Queue()
        self.batch_interval = 0.1
        self.shutdown_event = threading.Event()

    def connect(self, server, login, password):
        try:
            with self.mt5_lock:
                if not mt5.initialize():
                    logger.error("MT5 initialization failed")
                    return False, {"code": 1000, "message": "MT5 initialization failed"}
                
                authorized = mt5.login(login, password=password, server=server)
                if not authorized:
                    error = mt5.last_error()
                    logger.error(f"Login failed: {error}")
                    return False, {"code": error[0], "message": f"Login failed: {error[1]}"}
                
                self.connected = True
                self.shutdown_event.clear()
                account_info = mt5.account_info()
                if account_info and not account_info.trade_expert:
                    logger.warning("AutoTrading disabled")
                    return False, {"code": 1001, "message": "AutoTrading disabled. Enable 'Algo Trading' in MT5"}
                
                logger.info(f"Connected to MT5, account: {account_info.login if account_info else None}")
                return True, {"message": "Connected", "account": account_info.login if account_info else None}
        except Exception as e:
            logger.exception(f"Connection error: {str(e)}")
            return False, {"code": 1002, "message": str(e)}

    def disconnect(self):
        try:
            with self.mt5_lock:
                self.connected = False
                self.shutdown_event.set()
                
                for symbol in list(self.price_threads.keys()):
                    self.stop_price_stream(symbol)
                
                for thread in list(self.price_threads.values()):
                    if thread.is_alive():
                        thread.join(timeout=2)
                
                self.active_subscriptions.clear()
                self.price_threads.clear()
                mt5.shutdown()
                logger.info("Disconnected from MT5")
                return True, {"message": "Disconnected"}
        except Exception as e:
            logger.exception(f"Disconnect error: {str(e)}")
            return False, {"code": 1003, "message": str(e)}

    def get_symbols(self):
        try:
            if not self.connected:
                logger.warning("Attempted to get symbols while not connected")
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                symbols = mt5.symbols_get() or []
                return True, [symbol.name for symbol in symbols]
        except Exception as e:
            logger.exception(f"Error getting symbols: {str(e)}")
            return False, {"code": 1005, "message": str(e)}

    def get_symbol_info(self, symbol):
        try:
            if not self.connected:
                logger.warning(f"Attempted to get symbol info for {symbol} while not connected")
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                if not mt5.symbol_select(symbol, True):
                    logger.error(f"Symbol {symbol} not selected")
                    return False, {"code": 1006, "message": f"Symbol {symbol} not selected"}
                
                info = mt5.symbol_info(symbol)
                if not info:
                    logger.error(f"Symbol {symbol} not found")
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
            logger.exception(f"Error getting symbol info for {symbol}: {str(e)}")
            return False, {"code": 1008, "message": str(e)}

    def get_price(self, symbol):
        try:
            if not self.connected:
                logger.warning(f"Attempted to get price for {symbol} while not connected")
                return False, {"code": 1004, "message": "Not connected"}
            
            with self.mt5_lock:
                if not mt5.symbol_select(symbol, True):
                    logger.error(f"Symbol {symbol} not selected")
                    return False, {"code": 1006, "message": f"Symbol {symbol} not selected"}
                
                time.sleep(0.05)
                tick = mt5.symbol_info_tick(symbol)
                
                if not tick:
                    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M1, 0, 1)
                    if not rates or not len(rates):
                        logger.error(f"No price data for {symbol}")
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
                
                current_data = self.symbol_data.get(symbol, {"high": None, "low": None, "last_close": None, "last_timestamp": None})
                current_high = current_data["high"] or tick.bid
                current_low = current_data["low"] or tick.bid
                current_high = max(current_high, tick.bid, tick.ask)
                current_low = min(current_low, tick.bid, tick.ask)
                
                self.symbol_data[symbol] = {
                    "high": current_high,
                    "low": current_low,
                    "last_close": tick.bid if symbol_info and symbol_info.trade_mode == 0 else current_data.get("last_close"),
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
                    "marketStatus": "TRADEABLE" if symbol_info and symbol_info.trade_mode != 0 else "CLOSED"
                }
        except Exception as e:
            logger.exception(f"Error getting price for {symbol}: {str(e)}")
            return False, {"code": 1010, "message": str(e)}

    def start_price_stream(self, symbol, client_id):
        if symbol in self.price_threads:
            if client_id not in self.active_subscriptions.get(symbol, []):
                self.active_subscriptions.setdefault(symbol, []).append(client_id)
            return
        
        self.active_subscriptions[symbol] = [client_id]
        
        def price_worker():
            last_price = None
            error_count = 0
            max_errors = 5
            
            while symbol in self.price_threads and self.connected and not self.shutdown_event.is_set():
                try:
                    success, price_data = self.get_price(symbol)
                    if success:
                        current_price = f"{price_data['bid']}-{price_data['ask']}"
                        if current_price != last_price:
                            socketio.emit('market-data', price_data, room=f'symbol_{symbol}')
                            last_price = current_price
                            error_count = 0
                    else:
                        error_count += 1
                        logger.error(f"Failed to get price for {symbol}: {price_data.get('message', 'Unknown error')}")
                        if error_count >= max_errors:
                            logger.error(f"Too many errors for {symbol}, stopping stream")
                            break
                    
                    time.sleep(self.batch_interval)
                except Exception as e:
                    error_count += 1
                    logger.exception(f"Error streaming {symbol}: {str(e)}")
                    if error_count >= max_errors:
                        logger.error(f"Too many errors for {symbol}, stopping stream")
                        break
            if symbol in self.price_threads:
                del self.price_threads[symbol]
            if symbol in self.active_subscriptions:
                del self.active_subscriptions[symbol]
            logger.info(f"Price stream ended for {symbol}")
        
        thread = threading.Thread(target=price_worker, daemon=True, name=f"PriceStream-{symbol}")
        self.price_threads[symbol] = thread
        thread.start()
        logger.info(f"Started price stream for {symbol} with client {client_id}")

    def stop_price_stream(self, symbol, client_id=None):
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
                    logger.info(f"Stopped price stream for {symbol} (client {client_id})")
            else:
                if symbol in self.active_subscriptions:
                    del self.active_subscriptions[symbol]
                if symbol in self.price_threads:
                    del self.price_threads[symbol]
                logger.info(f"Stopped price stream for {symbol} (all clients)")
        except Exception as e:
            logger.exception(f"Error stopping price stream for {symbol}: {str(e)}")

    def place_trade(self, symbol, volume, order_type, sl_distance=None, tp_distance=None, comment="", magic=0):
        try:
            if not self.connected:
                return False, "Not connected"
            with self.mt5_lock:
                if not mt5.symbol_select(symbol, True):
                    return False, f"Symbol {symbol} not selected"
                info = mt5.symbol_info(symbol)
                if not info:
                    return False, f"Symbol {symbol} not found"
                if info.trade_mode == 0:
                    return False, f"Symbol {symbol} not tradable"
                stop_level = getattr(info, 'stops_level', 0) * info.point
                tick = mt5.symbol_info_tick(symbol)
                if not tick:
                    return False, f"No price for {symbol}"
                order_type = order_type.upper()
                if order_type == "BUY":
                    mt5_type = mt5.ORDER_TYPE_BUY
                    price = tick.ask
                    sl = round(price - sl_distance, info.digits) if sl_distance and sl_distance > 0 else 0
                    tp = round(price + tp_distance, info.digits) if tp_distance and tp_distance > 0 else 0
                elif order_type == "SELL":
                    mt5_type = mt5.ORDER_TYPE_SELL
                    price = tick.bid
                    sl = round(price + sl_distance, info.digits) if sl_distance and sl_distance > 0 else 0
                    tp = round(price - tp_distance, info.digits) if tp_distance and tp_distance > 0 else 0
                else:
                    return False, f"Invalid order type {order_type}"
                volume = max(info.volume_min, min(info.volume_max, round(volume / info.volume_step) * info.volume_step))

                # Determine the appropriate filling type
                filling_mode = info.filling_mode
                supported_fillings = []
                if filling_mode & 1:  # FOK
                    supported_fillings.append(mt5.ORDER_FILLING_FOK)
                if filling_mode & 2:  # IOC
                    supported_fillings.append(mt5.ORDER_FILLING_IOC)
                if filling_mode & 4:  # RETURN
                    supported_fillings.append(mt5.ORDER_FILLING_RETURN)

                if not supported_fillings:
                    logger.error(f"No supported filling modes for {symbol}")
                    return False, f"No supported filling modes for {symbol}"

                # Prefer FOK, then IOC, then RETURN
                filling_type = supported_fillings[0]  # Take the first supported filling mode
                logger.info("Selected filling type: {filling_type} for symbol {symbol}")

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
                    "type_filling": filling_type
                }
                if sl_distance and sl_distance > 0 and sl != 0:
                    if sl_distance < stop_level:
                        sl_distance = stop_level
                        sl = round(price - sl_distance if order_type == "BUY" else price + sl_distance, info.digits)
                    request["sl"] = sl
                if tp_distance and tp_distance > 0 and tp != 0:
                    if tp_distance < stop_level:
                        tp_distance = stop_level
                        tp = round(price + tp_distance if order_type == "BUY" else price - tp_distance, info.digits)
                    request["tp"] = tp
                result = mt5.order_send(request)
                if result is None:
                    error = mt5.last_error()
                    error_code = error[0] if isinstance(error, tuple) else getattr(error, 'code', -1)
                    error_comment = error[1] if isinstance(error, tuple) else getattr(error, 'comment', 'Unknown error')
                    if error_code == 10013:  # Requote, try with higher deviation
                        request["deviation"] = 50
                        result = mt5.order_send(request)
                        if result is None:
                            error = mt5.last_error()
                            error_code = error[0] if isinstance(error, tuple) else getattr(error, 'code', -1)
                            error_comment = error[1] if isinstance(error, tuple) else getattr(error, 'comment', 'Unknown error')
                            return False, f"Order failed: Code: {error_code} - {error_comment}"
                    else:
                        return False, f"Order failed: Code: {error_code} - {error_comment}"
                if result.retcode == mt5.TRADE_RETCODE_DONE:
                    return True, {
                        "order": result.order,
                        "deal": result.deal,
                        "volume": result.volume,
                        "price": result.price,
                        "sl": sl,
                        "tp": tp,
                        "comment": comment,
                        "retcode": result.retcode
                    }
                error_codes = {
                    10018: "Market closed",
                    10019: "Insufficient funds",
                    10020: "Prices changed",
                    10021: "Invalid request (check volume, symbol, or market status)",
                    10022: "Invalid SL/TP",
                    10017: "Invalid parameters",
                    10027: "AutoTrading disabled",
                    10030: "Invalid order filling type"
                }
                error_msg = error_codes.get(result.retcode, f"Error {result.retcode}")
                return False, f"Order failed: {error_msg}"
        except Exception as e:
            logger.exception(f"Error placing trade: {str(e)}")
            return False, str(e)

    def close_trade(self, ticket, volume=None, symbol=None, max_retries=3):
        try:
            if not self.connected:
                return False, "Not connected"
            with self.mt5_lock:
                position = mt5.positions_get(ticket=ticket)
                if not position:
                    return False, f"Position {ticket} not found"
                pos = position[0]
                symbol = symbol or pos.symbol
                position_type = "BUY" if pos.type == mt5.POSITION_TYPE_BUY else "SELL"
                close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY
                if volume is None:
                    volume = pos.volume
                else:
                    volume = min(volume, pos.volume)
                if not mt5.symbol_select(symbol, True):
                    return False, f"Symbol {symbol} not selected"
                info = mt5.symbol_info(symbol)
                if not info:
                    return False, f"Symbol {symbol} not found"
                if info.trade_mode == 0:
                    return False, f"Symbol {symbol} not tradable"
                volume = max(info.volume_min, min(info.volume_max, round(volume / info.volume_step) * info.volume_step))
                if volume < info.volume_min:
                    return False, f"Volume {volume} below minimum {info.volume_min}"
                if volume > info.volume_max:
                    return False, f"Volume {volume} exceeds maximum {info.volume_max}"
                filling_mode = info.filling_mode
                supported_fillings = []
                if filling_mode & 1:
                    supported_fillings.append(mt5.ORDER_FILLING_FOK)
                if filling_mode & 2:
                    supported_fillings.append(mt5.ORDER_FILLING_IOC)
                if filling_mode & 4:
                    supported_fillings.append(mt5.ORDER_FILLING_RETURN)
                if not supported_fillings:
                    logger.error(f"No supported filling modes for {symbol}")
                    return False, f"No supported filling modes for {symbol}"
                filling_type = supported_fillings[0]  # First supported
                for attempt in range(max_retries):
                    tick = mt5.symbol_info_tick(symbol)
                    if not tick:
                        return False, f"No price for {symbol}"
                    price = tick.bid if pos.type == mt5.POSITION_TYPE_BUY else tick.ask
                    request = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "volume": volume,
                        "type": close_type,
                        "position": ticket,
                        "price": price,
                        "magic": pos.magic,
                        "comment": f"Close {ticket}",
                        "type_filling": filling_type,
                        "deviation": 20 + attempt * 10
                    }
                    result = mt5.order_send(request)
                    if result is None:
                        error = mt5.last_error()
                        error_code = error[0] if isinstance(error, tuple) else getattr(error, 'code', -1)
                        error_comment = error[1] if isinstance(error, tuple) else getattr(error, 'comment', 'Unknown error')
                        return False, f"Close failed: Code: {error_code} - {error_comment}"
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        return True, {
                            "deal": result.deal,
                            "retcode": result.retcode,
                            "price": result.price,
                            "volume": result.volume,
                            "profit": pos.profit,
                            "symbol": symbol,
                            "position_type": position_type
                        }
                    if result.retcode == 10021 and attempt < max_retries - 1:
                        filling_type = supported_fillings[1] if len(supported_fillings) > 1 else filling_type
                        time.sleep(0.5)
                        continue
                    error_codes = {
                        10018: "Market closed",
                        10019: "Insufficient funds",
                        10020: "Prices changed",
                        10021: "Invalid request (check volume, symbol, or market status)",
                        10022: "Invalid SL/TP",
                        10017: "Invalid parameters",
                        10027: "AutoTrading disabled",
                        10030: "Invalid order filling type"
                    }
                    error_msg = error_codes.get(result.retcode, f"Error {result.retcode}")
                    return False, f"Close failed: {error_msg}"
                return False, f"Close failed after {max_retries} attempts"
        except Exception as e:
            logger.exception(f"Error closing trade: {str(e)}")
            return False, str(e)

    def get_positions(self):
        try:
            if not self.connected:
                logger.warning("Attempted to get positions while not connected")
                return False, {"code": 1004, "message": "Not connected"}
            with self.mt5_lock:
                positions = mt5.positions_get()
                if not positions:
                    return True, []
                result = []
                for pos in positions:
                    try:
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
                    except Exception as e:
                        logger.error(f"Error processing position {pos.ticket}: {str(e)}")
                        continue
                logger.info(f"Retrieved {len(result)} open positions")
                return True, result
        except Exception as e:
            logger.exception(f"Error getting positions: {str(e)}")
            return False, {"code": 1014, "message": str(e)}

# Global connector instance
connector = MT5Connector()

# WebSocket Event Handlers
@socketio.on('connect')
def handle_connect():
    client_id = request.sid
    secret = request.args.get('secret')
    if secret != app.config['SECRET_KEY']:
        logger.warning(f"Authentication failed for client {client_id}")
        emit('error', {'code': 2000, 'message': 'Authentication failed'})
        disconnect()
        return False
    logger.info(f"Client connected: {client_id}")
    emit('connected', {'message': 'Connected to MT5 WebSocket server'})

@socketio.on('disconnect')
def handle_disconnect():
    client_id = request.sid
    logger.info(f"Client disconnected: {client_id}")
    for symbol in list(connector.active_subscriptions.keys()):
        connector.stop_price_stream(symbol, client_id)

@socketio.on('connect_error')
def handle_connect_error(error):
    logger.error(f"Connection error: {str(error)}")
    emit('error', {'code': 2001, 'message': f'Connection error: {str(error)}'})

@socketio.on('request-data')
def handle_request_data(symbols):
    client_id = request.sid
    logger.info(f"Client {client_id} requesting data for symbols: {symbols}")
    if not connector.connected:
        logger.warning(f"Client {client_id} requested data while MT5 not connected")
        emit('error', {'code': 2002, 'message': 'MT5 not connected'})
        return
    if not isinstance(symbols, list):
        symbols = [symbols] if symbols else []
    for symbol in symbols:
        try:
            socketio.server.enter_room(client_id, f'symbol_{symbol}')
            connector.start_price_stream(symbol, client_id)
        except Exception as e:
            logger.error(f"Error starting stream for {symbol}: {str(e)}")
            emit('error', {'code': 2003, 'message': f'Error starting stream for {symbol}: {str(e)}'})

@socketio.on('stop-data')
def handle_stop_data(symbols):
    client_id = request.sid
    logger.info(f"Client {client_id} stopping data for symbols: {symbols}")
    if not isinstance(symbols, list):
        symbols = [symbols] if symbols else []
    for symbol in symbols:
        try:
            socketio.server.leave_room(client_id, f'symbol_{symbol}')
            connector.stop_price_stream(symbol, client_id)
        except Exception as e:
            logger.error(f"Error stopping stream for {symbol}: {str(e)}")

# REST API Endpoints
@app.route('/connect', methods=['POST'])
def connect():
    try:
        data = request.json
        if not data:
            logger.error("No JSON data provided in connect request")
            return jsonify({"success": False, "error": {"code": 3000, "message": "No JSON data provided"}}), 400
        server = data.get('server')
        login = data.get('login')
        password = data.get('password')
        if not all([server, login, password]):
            logger.error("Missing server, login, or password in connect request")
            return jsonify({"success": False, "error": {"code": 3001, "message": "Missing server, login, or password"}}), 400
        success, result = connector.connect(server, login, password)
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in connect endpoint: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3002, "message": str(e)}}), 500

@app.route('/disconnect', methods=['POST'])
def disconnect_endpoint():
    try:
        success, result = connector.disconnect()
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in disconnect endpoint: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3003, "message": str(e)}}), 500

@app.route('/symbols', methods=['GET'])
def get_symbols():
    try:
        success, result = connector.get_symbols()
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in symbols endpoint: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3004, "message": str(e)}}), 500

@app.route('/symbol_info/<symbol>', methods=['GET'])
@app.route('/symbol/<symbol>', methods=['GET'])
def get_symbol_info(symbol):
    try:
        success, result = connector.get_symbol_info(symbol)
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in symbol_info endpoint for {symbol}: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3005, "message": str(e)}}), 500

@app.route('/price/<symbol>', methods=['GET'])
def get_price(symbol):
    try:
        success, result = connector.get_price(symbol)
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in price endpoint for {symbol}: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3006, "message": str(e)}}), 500

@app.route('/trade', methods=['POST'])
def trade():
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        symbol = data.get('symbol')
        volume = float(data.get('volume', 0.1))
        order_type = data.get('type')
        sl_distance = float(data.get('sl_distance', 0)) if data.get('sl_distance') else None
        tp_distance = float(data.get('tp_distance', 0)) if data.get('tp_distance') else None
        comment = data.get('comment', '')
        magic = int(data.get('magic', 0))
        if not all([symbol, order_type]):
            return jsonify({"success": False, "error": "Missing symbol or order type"}), 400
        success, result = connector.place_trade(symbol, volume, order_type, sl_distance, tp_distance, comment, magic)
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except ValueError as e:
        return jsonify({"success": False, "error": f"Invalid numeric value: {str(e)}"}), 400
    except Exception as e:
        logger.exception(f"Error in trade endpoint: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/close', methods=['POST'])
def close():
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400
        ticket = data.get('ticket')
        volume = data.get('volume')
        symbol = data.get('symbol')
        if not ticket:
            return jsonify({"success": False, "error": "Missing ticket"}), 400
        try:
            ticket = int(ticket)
            volume = float(volume) if volume else None
        except ValueError as e:
            return jsonify({"success": False, "error": f"Invalid ticket or volume format: {str(e)}"}), 400
        success, result = connector.close_trade(ticket, volume, symbol)
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in close endpoint: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/positions', methods=['GET'])
def get_positions():
    try:
        success, result = connector.get_positions()
        if success:
            return jsonify({"success": True, "data": result})
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in positions endpoint: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3013, "message": str(e)}}), 500

@app.route('/symbol_filling/<symbol>', methods=['GET'])
def get_symbol_filling(symbol):
    try:
        success, result = connector.get_symbol_info(symbol)
        if success:
            filling_mode = result.get('filling_mode', 0)
            supported_fillings = []
            if filling_mode & 1:
                supported_fillings.append('FOK')
            if filling_mode & 2:
                supported_fillings.append('IOC')
            if filling_mode & 4:
                supported_fillings.append('RETURN')
            return jsonify({
                "success": True,
                "data": {
                    "symbol": symbol,
                    "filling_mode": filling_mode,
                    "supported_fillings": supported_fillings
                }
            })
        else:
            return jsonify({"success": False, "error": result}), 400
    except Exception as e:
        logger.exception(f"Error in symbol_filling endpoint for {symbol}: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3007, "message": str(e)}}), 500

@app.route('/health', methods=['GET'])
def health():
    try:
        return jsonify({
            "success": True,
            "data": {
                "status": "running",
                "connected": connector.connected,
                "active_subscriptions": list(connector.active_subscriptions.keys()),
                "active_threads": len([t for t in connector.price_threads.values() if t.is_alive()]),
                "timestamp": datetime.now().isoformat()
            }
        })
    except Exception as e:
        logger.exception(f"Error in health endpoint: {str(e)}")
        return jsonify({"success": False, "error": {"code": 3012, "message": str(e)}}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "error": {"code": 404, "message": "Endpoint not found"}}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.exception(f"Internal server error: {str(error)}")
    return jsonify({"success": False, "error": {"code": 500, "message": "Internal server error"}}), 500

if __name__ == '__main__':
    try:
        logger.info("Starting MT5 WebSocket server...")
        socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("Server shutdown requested...")
        connector.disconnect()
    except Exception as e:
        logger.exception(f"Failed to start server: {str(e)}")
    finally:
        logger.info("Server stopped.")