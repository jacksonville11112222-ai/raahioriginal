import logging
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, Any, Optional, List
import requests
import hashlib, pyotp
from dotenv import load_dotenv
from brokers.base import BrokerBase
from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
from threading import Thread

from logger import logger


load_dotenv()


# --- Zerodha Broker ---
class ZerodhaBroker(BrokerBase):
    def __init__(self, access_token: Optional[str] = None):
        super().__init__()
        self.kite = KiteConnect(api_key=os.getenv('BROKER_API_KEY'))
        
        if access_token:
            self.kite.set_access_token(access_token)
            # For websocket, which still needs the full auth response data structure
            self.auth_response_data = {"access_token": access_token}
        else:
            self.auth_response_data = self.authenticate()
            self.kite.set_access_token(self.auth_response_data["access_token"])

        # Initialize websocket if access token is available
        if self.kite.access_token:
            self.kite_ws = KiteTicker(
                api_key=os.getenv('BROKER_API_KEY'),
                access_token=self.kite.access_token
            )
            self.tick_counter = 0
            self.symbols = []
        else:
            self.kite_ws = None
            logger.warning("KiteTicker not initialized due to missing access token.")

    def authenticate(self) -> Dict[str, Any]:
        """
        Authenticates the user using the Kite Connect login flow.
        Supports both interactive and non-interactive (TOTP-based) authentication.
        """
        api_key = os.getenv('BROKER_API_KEY')
        api_secret = os.getenv('BROKER_API_SECRET')
        totp_enabled = os.getenv('BROKER_TOTP_ENABLE', 'false').lower() == 'true'

        if not api_key or not api_secret:
            raise ValueError("BROKER_API_KEY and BROKER_API_SECRET must be set in the environment.")

        if totp_enabled:
            logger.info("Attempting non-interactive TOTP-based authentication.")
            user_id = os.getenv('BROKER_ID')
            password = os.getenv('BROKER_PASSWORD')
            totp_secret = os.getenv('BROKER_TOTP_KEY')

            if not all([user_id, password, totp_secret]):
                raise ValueError("BROKER_ID, BROKER_PASSWORD, and BROKER_TOTP_KEY must be set for TOTP authentication.")

            try:
                # Generate the TOTP
                totp = pyotp.TOTP(totp_secret).now()

                # The generate_session method can handle TOTP by passing user_id, password, and the generated totp
                session_data = self.kite.generate_session(user_id, password, totp)
                logger.info("TOTP-based authentication successful.")
                return session_data
            except Exception as e:
                logger.error(f"TOTP-based authentication failed: {e}")
                logger.warning("Falling back to interactive authentication due to an error.")
                return self._interactive_login(api_secret)
        else:
            return self._interactive_login(api_secret)

    def _interactive_login(self, api_secret: str) -> Dict[str, Any]:
        """Handles the interactive login flow."""
        logger.info("Starting interactive authentication.")
        print(f"Please grant access by visiting the following URL:\n{self.kite.login_url()}")
        request_token = input("Enter the request_token from the redirect URL: ")

        try:
            session_data = self.kite.generate_session(request_token, api_secret=api_secret)
            logger.info("Interactive authentication successful.")
            return session_data
        except Exception as e:
            logger.error(f"Interactive authentication failed: {e}")
            raise
    
    def get_orders(self):
        return self.kite.orders()
    
    def get_quote(self, symbol: str, exchange: Optional[str] = None) -> Dict[str, Any]:
        """Retrieves a quote for a given symbol."""
        if exchange and ":" not in symbol:
            symbol = f"{exchange}:{symbol}"
        return self.kite.quote(symbol)

    def place_gtt_order(self, symbol: str, quantity: int, price: float, transaction_type: str, order_type: str, exchange: str, product: str, tag: str = "Unknown") -> int:
        """Places a GTT (Good Till Triggered) order."""
        if order_type not in [self.kite.ORDER_TYPE_LIMIT, self.kite.ORDER_TYPE_MARKET]:
            raise ValueError(f"Invalid order type: {order_type}")

        if transaction_type not in [self.kite.TRANSACTION_TYPE_BUY, self.kite.TRANSACTION_TYPE_SELL]:
            raise ValueError(f"Invalid transaction type: {transaction_type}")

        # Fetch the last price for the condition
        full_symbol = f"{exchange}:{symbol}"
        quote_data = self.get_quote(symbol=symbol, exchange=exchange)
        if full_symbol not in quote_data:
            raise ValueError(f"Could not retrieve quote for symbol: {full_symbol}")
        last_price = quote_data[full_symbol]['last_price']

        # Define the order to be placed when the GTT is triggered
        order = {
            "exchange": exchange,
            "tradingsymbol": symbol,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "order_type": order_type,
            "product": product,
            "price": price,
        }

        try:
            # The place_gtt method expects Python objects, not JSON strings
            order_result = self.kite.place_gtt(
                trigger_type=self.kite.GTT_TYPE_SINGLE,
                tradingsymbol=symbol,
                exchange=exchange,
                trigger_values=[price],
                last_price=last_price,
                orders=[order]
            )
            return order_result['trigger_id']
        except Exception as e:
            logger.error(f"GTT order placement failed: {e}")
            raise

    def place_order(self, symbol, quantity, price, transaction_type, order_type, variety, exchange, product, tag="Unknown"):
        # Standardize order parameters using KiteConnect constants
        order_type_map = {"LIMIT": self.kite.ORDER_TYPE_LIMIT, "MARKET": self.kite.ORDER_TYPE_MARKET}
        transaction_type_map = {"BUY": self.kite.TRANSACTION_TYPE_BUY, "SELL": self.kite.TRANSACTION_TYPE_SELL}
        variety_map = {"REGULAR": self.kite.VARIETY_REGULAR}

        final_order_type = order_type_map.get(order_type)
        if not final_order_type:
            raise ValueError(f"Invalid order type: {order_type}")

        final_transaction_type = transaction_type_map.get(transaction_type)
        if not final_transaction_type:
            raise ValueError(f"Invalid transaction type: {transaction_type}")

        final_variety = variety_map.get(variety)
        if not final_variety:
            raise ValueError(f"Invalid variety: {variety}")

        logger.info(f"Placing order for {symbol} with quantity {quantity} at {price} with order type {final_order_type} and transaction type {final_transaction_type}, variety {final_variety}, exchange {exchange}, product {product}, tag {tag}")
        
        try:
            order_id = self.kite.place_order(
                variety=final_variety,
                exchange=exchange,
                tradingsymbol=symbol,
                transaction_type=final_transaction_type,
                quantity=quantity,
                product=product,
                order_type=final_order_type,
                price=price if final_order_type == self.kite.ORDER_TYPE_LIMIT else None,
                tag=tag
            )
            logger.info(f"Order placed: {order_id}")
            return order_id
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return None
    

    def get_positions(self):
        return self.kite.positions()

    def symbols_to_subscribe(self, symbols):
        self.symbols = symbols

    ## Websocket Calllbacks
    def on_ticks(self, ws, ticks):  # noqa
        """
        This callback is called when the websocket receives a tick.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        # Callback to receive ticks.
        logger.info("Ticks: {}".format(ticks))
        # self.tick_counter += 1

    def on_connect(self, ws, response):  # noqa
        """
        This callback is called when the websocket is connected.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
        logger.info("Connected")
        # Set RELIANCE to tick in `full` mode.
        ws.subscribe(self.symbols)
        ws.set_mode(ws.MODE_FULL, self.symbols)


    def on_order_update(self, ws, data):
        """
        This callback is called when the websocket receives an order update.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Order update : {}".format(data))

    def on_close(self, ws, code, reason):
        """
        This callback is called when the websocket is closed.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Connection closed: {code} - {reason}".format(code=code, reason=reason))


    # Callback when connection closed with error.
    def on_error(self, ws, code, reason):
        """
        This callback is called when the websocket encounters an error.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Connection error: {code} - {reason}".format(code=code, reason=reason))


    # Callback when reconnect is on progress
    def on_reconnect(self, ws, attempts_count):
        """
        This callback is called when the websocket is reconnecting.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Reconnecting: {}".format(attempts_count))


    # Callback when all reconnect failed (exhausted max retries)
    def on_noreconnect(self, ws):
        """
        This callback is called when the websocket fails to reconnect.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Reconnect failed.")
    
    def download_instruments(self):
        instruments = self.kite.instruments()
        self.instruments_df = pd.DataFrame(instruments)
    
    def get_instruments(self):
        return self.instruments_df
    
    def connect_websocket(self):
        self.kite_ws.on_ticks = self.on_ticks
        self.kite_ws.on_connect = self.on_connect
        self.kite_ws.on_order_update = self.on_order_update
        self.kite_ws.on_close = self.on_close
        self.kite_ws.on_error = self.on_error
        self.kite_ws.on_reconnect = self.on_reconnect
        self.kite_ws.on_noreconnect = self.on_noreconnect
        self.kite_ws.connect(threaded=True)
        
