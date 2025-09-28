import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from logger import logger
import time
import argparse
from dispatcher import DataDispatcher
from orders import OrderTracker
from brokers.zerodha import ZerodhaBroker
from queue import Queue
import traceback
import warnings
warnings.filterwarnings("ignore")
import logging

class SurvivorStrategy:
    """
    Survivor Options Trading Strategy - Buying Logic

    This strategy implements a systematic approach to options **buying** based on price movements
    of the NIFTY index. The core concept is to **buy** options (both PE and CE) when the underlying
    index moves beyond certain thresholds, aiming to profit from continued momentum.

    STRATEGY OVERVIEW:
    ==================

    1. **Dual-Side Momentum Trading**: The strategy capitalizes on market momentum:
       - **CE (Call) Buying**: Triggered when NIFTY price moves **UP** beyond `ce_gap` threshold.
       - **PE (Put) Buying**: Triggered when NIFTY price moves **DOWN** beyond `pe_gap` threshold.

    2. **Gap-Based Execution**:
       - Maintains reference points for CE and PE sides (`nifty_ce_last_value`, `nifty_pe_last_value`).
       - Executes trades when the price deviation from a reference point exceeds the configured gap.
       - Uses a multiplier to scale trade quantity based on the magnitude of the price movement.

    3. **Dynamic Strike Selection**:
       - Selects Out-of-the-Money (OTM) option strikes based on `ce_symbol_gap` and `pe_symbol_gap`.
       - Ensures the selected option has a premium above `min_price_to_buy`.
       - Checks for instrument liquidity before placing a trade.

    4. **Reference Point Reset Mechanism**:
       - After a trade, the reference point is updated based on `post_trade_ref_update` setting.
       - On price reversals, reference points are reset to the current market price to keep the
         strategy adaptive, based on `ce_reset_gap` and `pe_reset_gap`.

    TRADING LOGIC EXAMPLE (CE side):
    ================================
    - **Initial State**: NIFTY at 24,500, `ce_gap`=20, `ce_ref`=24,500.
    - **Market Movement**: NIFTY rises to 24,525 (delta = 25).
    - **Trade Trigger**: Since delta (25) >= `ce_gap` (20), a CE buy is triggered.
    - **Multiplier**: `floor(25 / 20)` = 1. Quantity = `ce_quantity` * 1.
    - **Strike Selection**: If `ce_symbol_gap`=200, select CE strike near 24,700.
    - **Reference Update**: New `ce_ref` is updated based on the `post_trade_ref_update` rule.

    CONFIGURATION PARAMETERS (from survivor.yml):
    =============================================
    - `symbol_initials`: Base symbol for the option series (e.g., 'NIFTY25807').
    - `ce_gap`/`pe_gap`: Price movement thresholds to trigger trades.
    - `ce_symbol_gap`/`pe_symbol_gap`: Distance from spot price to select OTM strikes.
    - `ce_reset_gap`/`pe_reset_gap`: Reversal thresholds to reset reference points.
    - `ce_quantity`/`pe_quantity`: Base quantity for trades.
    - `buy_multiplier_threshold`: Caps the trade quantity multiplier.
    - `multiplier_cap_action`: Action to take if multiplier exceeds threshold ('block' or 'cap').
    - `min_price_to_buy`: Minimum option premium required to execute a trade.

    RISK MANAGEMENT:
    ================
    1. **Premium Filtering**: Avoids trading options with premium below `min_price_to_buy`.
    2. **Position Capping**: Limits position size by capping or blocking trades when the
       multiplier exceeds `buy_multiplier_threshold`.
    3. **Dynamic Reset**: The reset logic prevents reference points from becoming stale,
       ensuring the strategy stays aligned with recent market action.
    """
    def __init__(self, broker, config, order_manager):
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)

        self.broker = broker
        self.symbol_initials = self.strat_var_symbol_initials
        self.order_manager = order_manager
        self.broker.download_instruments()
        self.instruments = self.broker.instruments_df[self.broker.instruments_df['tradingsymbol'].str.startswith(self.symbol_initials)]
        if self.instruments.shape[0] == 0:
            logger.error(f"No instruments found for {self.symbol_initials}. Please check the symbol initials.")
            return

        self.strike_difference = None
        self._initialize_state()
        self.strike_difference = self._get_strike_difference()
        if self.strike_difference:
            logger.info(f"Strike step for {self.symbol_initials} is {self.strike_difference}")

    def _nifty_quote(self):
        symbol_code = self.strat_var_index_symbol
        return self.broker.get_quote(symbol_code)

    def _initialize_state(self):
        self.pe_reset_gap_flag = 0
        self.ce_reset_gap_flag = 0

        try:
            quote = self._nifty_quote()
            current_price = quote[self.strat_var_index_symbol]['last_price']
            logger.info(f"Initializing strategy with current NIFTY price: {current_price}")
        except Exception as e:
            logger.error(f"Could not fetch initial NIFTY price: {e}. Exiting.")
            sys.exit(1)

        self.nifty_pe_last_value = self.strat_var_pe_start_point if self.strat_var_pe_start_point != 0 else current_price
        self.nifty_ce_last_value = self.strat_var_ce_start_point if self.strat_var_ce_start_point != 0 else current_price

        logger.info(f"Initial PE reference: {self.nifty_pe_last_value}, Initial CE reference: {self.nifty_ce_last_value}")

    def _get_strike_difference(self):
        if self.strike_difference is not None:
            return self.strike_difference

        ce_instruments = self.instruments[self.instruments['tradingsymbol'].str.endswith('CE')]
        if ce_instruments.shape[0] < 2:
            logger.warning(f"Not enough CE instruments to determine strike step. Falling back to config value: {self.strat_var_strike_step}")
            return self.strat_var_strike_step

        ce_instruments_sorted = ce_instruments.sort_values('strike')
        self.strike_difference = abs(ce_instruments_sorted.iloc[1]['strike'] - ce_instruments_sorted.iloc[0]['strike'])
        return self.strike_difference

    def on_ticks_update(self, ticks):
        current_price = ticks['last_price']
        self._handle_ce_trade(current_price)
        self._handle_pe_trade(current_price)
        self._reset_reference_values(current_price)

    def _handle_multiplier_cap(self, multiplier):
        threshold = self.strat_var_buy_multiplier_threshold
        action = self.strat_var_multiplier_cap_action.lower()
        if multiplier > threshold:
            logger.warning(f"Calculated multiplier {multiplier} exceeds threshold {threshold}.")
            if action == "block":
                logger.warning("Action is 'block', trade is blocked.")
                return None
            elif action == "cap":
                logger.warning(f"Action is 'cap', capping multiplier to {threshold}.")
                return threshold
        return multiplier

    def _update_reference_point(self, side, current_price, gap, multiplier):
        update_rule = self.strat_var_post_trade_ref_update.lower()
        if side == "CE":
            if update_rule == "set_to_current":
                self.nifty_ce_last_value = current_price
            elif update_rule == "advance_by_gap":
                self.nifty_ce_last_value += gap * multiplier
            logger.info(f"CE reference point updated to {self.nifty_ce_last_value:.2f} (rule: {update_rule})")
        elif side == "PE":
            if update_rule == "set_to_current":
                self.nifty_pe_last_value = current_price
            elif update_rule == "advance_by_gap":
                self.nifty_pe_last_value -= gap * multiplier
            logger.info(f"PE reference point updated to {self.nifty_pe_last_value:.2f} (rule: {update_rule})")

    def _handle_ce_trade(self, current_price):
        if current_price <= self.nifty_ce_last_value + self.strat_var_ce_gap:
            self._log_stable_market(current_price, "CE")
            return

        price_diff = round(current_price - self.nifty_ce_last_value, 2)
        buy_multiplier = self._handle_multiplier_cap(int(price_diff / self.strat_var_ce_gap))
        if buy_multiplier is None: return

        self._update_reference_point("CE", current_price, self.strat_var_ce_gap, buy_multiplier)
        total_quantity = buy_multiplier * self.strat_var_ce_quantity
        
        self._execute_trade("CE", current_price, total_quantity)

    def _handle_pe_trade(self, current_price):
        if current_price >= self.nifty_pe_last_value - self.strat_var_pe_gap:
            self._log_stable_market(current_price, "PE")
            return

        price_diff = round(self.nifty_pe_last_value - current_price, 2)
        buy_multiplier = self._handle_multiplier_cap(int(price_diff / self.strat_var_pe_gap))
        if buy_multiplier is None: return

        self._update_reference_point("PE", current_price, self.strat_var_pe_gap, buy_multiplier)
        total_quantity = buy_multiplier * self.strat_var_pe_quantity

        self._execute_trade("PE", current_price, total_quantity)

    def _execute_trade(self, option_type, current_price, quantity):
        temp_gap = self.strat_var_ce_symbol_gap if option_type == "CE" else self.strat_var_pe_symbol_gap
        
        while True:
            instrument = self._find_nifty_symbol_from_gap(option_type, current_price, gap=temp_gap)
            if not instrument:
                logger.warning(f"No suitable {option_type} instrument found with gap {temp_gap}")
                return

            if self.strat_var_check_liquidity:
                symbol_code = self.strat_var_exchange + ":" + instrument['tradingsymbol']
                quote = self.broker.get_quote(symbol_code)[symbol_code]
                if quote['last_price'] < self.strat_var_min_price_to_buy:
                    logger.info(f"{option_type} {instrument['tradingsymbol']} price {quote['last_price']:.2f} is below min_price_to_buy {self.strat_var_min_price_to_buy}. Trying closer strike.")
                    temp_gap -= self.strike_difference
                    if temp_gap < 0: temp_gap = 0
                    continue

            logger.info(f"Executing {option_type} BUY: {instrument['tradingsymbol']} × {quantity} @ Market Price")
            self._place_order(instrument['tradingsymbol'], quantity)
            if option_type == "CE": self.ce_reset_gap_flag = 1
            else: self.pe_reset_gap_flag = 1
            break

    def _reset_reference_values(self, current_price):
        if self.ce_reset_gap_flag and (self.nifty_ce_last_value - current_price) >= self.strat_var_ce_reset_gap:
            logger.info(f"Price reversal detected. Resetting CE reference from {self.nifty_ce_last_value:.2f} to {current_price:.2f}")
            self.nifty_ce_last_value = current_price
            self.ce_reset_gap_flag = 0

        if self.pe_reset_gap_flag and (current_price - self.nifty_pe_last_value) >= self.strat_var_pe_reset_gap:
            logger.info(f"Price reversal detected. Resetting PE reference from {self.nifty_pe_last_value:.2f} to {current_price:.2f}")
            self.nifty_pe_last_value = current_price
            self.pe_reset_gap_flag = 0

    def _find_nifty_symbol_from_gap(self, option_type, ltp, gap):
        target_strike = ltp + gap if option_type == "CE" else ltp - gap
        df = self.instruments[self.instruments['instrument_type'] == option_type]
        if df.empty: return None

        df['target_strike_diff'] = (df['strike'] - target_strike).abs()
        best_match = df.sort_values('target_strike_diff').iloc[0]
        return best_match.to_dict()

    def _place_order(self, symbol, quantity):
        try:
            # Ensure all required parameters, including price, are passed to place_order
            order_id = self.broker.place_order(
                symbol=symbol,
                quantity=quantity,
                price=0,  # Price is 0 for market orders
                transaction_type=self.strat_var_trans_type,
                order_type=self.strat_var_order_type,
                variety="regular",
                exchange=self.strat_var_exchange,
                product=self.strat_var_product_type,
                tag="Survivor"
            )
            if not order_id:
                logger.error(f"Order placement failed for {symbol} × {quantity}.")
                return

            logger.info(f"Order placed successfully for {symbol} × {quantity}. Order ID: {order_id}")
            from datetime import datetime
            self.order_manager.add_order({
                "order_id": order_id, "symbol": symbol, "transaction_type": self.strat_var_trans_type,
                "quantity": quantity, "price": None, "timestamp": datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Exception placing order for {symbol}: {e}")

    def _log_stable_market(self, current_val, side):
        logger.debug(f"Market stable for {side}. Ref={self.nifty_ce_last_value if side=='CE' else self.nifty_pe_last_value:.2f}, Current={current_val:.2f}")

if __name__ == "__main__":
    logger.setLevel(logging.INFO)
    
    config_file = os.path.join(os.path.dirname(__file__), "configs/survivor.yml")
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    def create_argument_parser(defaults):
        parser = argparse.ArgumentParser(description="Survivor Options Buying Strategy", formatter_class=argparse.RawTextHelpFormatter)
        for key, value in defaults.items():
            arg_name = f'--{key.replace("_", "-")}'
            parser.add_argument(arg_name, type=type(value), default=value, help=f"Default: {value}")
        parser.add_argument('--show-config', action='store_true', help='Display final config and exit.')
        return parser

    def show_config(conf):
        print("\n" + "="*80 + "\nSURVIVOR STRATEGY FINAL CONFIGURATION\n" + "="*80)
        for key, value in conf.items():
            print(f"  {key:25}: {value}")
        print("="*80)

    parser = create_argument_parser(config)
    args = parser.parse_args()

    # Override config with CLI args
    for key, value in vars(args).items():
        # only update if the value is different from the default
        if value != parser.get_default(key):
            config[key] = value

    if args.show_config:
        show_config(config)
        sys.exit(0)

    # Setup Broker
    broker = ZerodhaBroker(access_token=os.getenv("BROKER_ACCESS_TOKEN"))
    order_tracker = OrderTracker()

    try:
        quote_data = broker.get_quote(config['index_symbol'])
        instrument_token = quote_data[config['index_symbol']]['instrument_token']
        logger.info(f"✓ Index instrument token for {config['index_symbol']} is {instrument_token}")
    except Exception as e:
        logger.error(f"Failed to get instrument token for {config['index_symbol']}: {e}")
        sys.exit(1)

    dispatcher = DataDispatcher()
    dispatcher.register_main_queue(Queue())

    def on_ticks(ws, ticks): dispatcher.dispatch(ticks)
    def on_connect(ws, response):
        logger.info(f"Websocket connected: {response}")
        ws.subscribe([instrument_token])
        ws.set_mode(ws.MODE_FULL, [instrument_token])
    def on_order_update(ws, data): order_tracker.update_order(data)

    broker.on_ticks = on_ticks
    broker.on_connect = on_connect
    broker.on_order_update = on_order_update
    broker.connect_websocket()

    strategy = SurvivorStrategy(broker, config, order_tracker)
    
    logger.info("🚀 Survivor strategy initialized. Starting trading loop...")
    try:
        while True:
            try:
                tick_data = dispatcher._main_queue.get(timeout=60)
                if tick_data: strategy.on_ticks_update(tick_data[0])
            except Queue.Empty:
                logger.warning("No ticks received in 60s. Websocket connected: %s", broker.is_connected())
                if not broker.is_connected():
                    logger.error("Websocket is disconnected! Shutting down.")
                    break
            except KeyboardInterrupt:
                logger.info("Shutdown requested. Stopping strategy...")
                break
            except Exception as e:
                logger.error(f"Error in trading loop: {e}")
                traceback.print_exc()
                time.sleep(1)
    finally:
        broker.close_websocket()
        logger.info("STRATEGY SHUTDOWN COMPLETE")