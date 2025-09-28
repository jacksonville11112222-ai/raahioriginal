import os
import sys
import time
import yaml
import argparse
import datetime
from queue import Queue
import traceback
import warnings

from rich.console import Console
from rich.live import Live
from rich.table import Table
import beepy

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import logger
from brokers.zerodha import ZerodhaBroker

warnings.filterwarnings("ignore")

class OITrackerStrategy:
    """
    OI Tracker Strategy

    This strategy tracks the Open Interest (OI) changes for NIFTY options
    at different time intervals and displays them in a live-updating table.
    """

    def __init__(self, broker, config):
        """
        Initializes the OITrackerStrategy.

        Args:
            broker (ZerodhaBroker): An instance of the ZerodhaBroker.
            config (dict): A dictionary containing the strategy configuration.
        """
        self.broker = broker
        self.config = config

        # Load config values
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)

        self.broker.download_instruments()
        self.instruments_df = self.broker.get_instruments()

        # Filter for NFO instruments
        self.nfo_instruments = self.instruments_df[self.instruments_df['segment'] == 'NFO-OPT']

        # Filter for the specific option series
        self.option_series_instruments = self.nfo_instruments[
            self.nfo_instruments['tradingsymbol'].str.startswith(self.strat_var_symbol_initials)
        ]

        if self.option_series_instruments.empty:
            logger.error(f"No instruments found for symbol prefix: {self.strat_var_symbol_initials}")
            sys.exit(1)

        logger.info(f"Successfully loaded {len(self.option_series_instruments)} instruments for {self.strat_var_symbol_initials}")

        self.instrument_data = {}

    def _get_nifty_ltp(self):
        """Fetches the last traded price and instrument token of NIFTY 50."""
        try:
            quote = self.broker.get_quote(self.strat_var_index_symbol)
            ltp = quote[self.strat_var_index_symbol]['last_price']
            token = quote[self.strat_var_index_symbol]['instrument_token']
            return ltp, token
        except Exception as e:
            logger.error(f"Error fetching NIFTY LTP: {e}")
            return None, None

    def _get_atm_strike(self, ltp):
        """Calculates the At-The-Money (ATM) strike based on the LTP."""
        if ltp is None:
            return None
        strike_diff = self.strat_var_nifty_strike_difference
        return round(ltp / strike_diff) * strike_diff

    def _get_option_details(self, atm_strike):
        """
        Gets the instrument details for the required call and put options.

        Args:
            atm_strike (float): The current ATM strike price.

        Returns:
            tuple: A tuple containing two lists: (call_options, put_options)
                   Each list contains dictionaries with instrument details.
        """
        if atm_strike is None:
            return [], []

        strikes_to_find = [atm_strike + i * self.strat_var_nifty_strike_difference for i in range(-self.strat_var_strike_count, self.strat_var_strike_count + 1)]

        call_options = []
        put_options = []

        for strike in strikes_to_find:
            # Find Call Option
            call_instrument = self.option_series_instruments[
                (self.option_series_instruments['strike'] == strike) &
                (self.option_series_instruments['instrument_type'] == 'CE')
            ]
            if not call_instrument.empty:
                call_options.append(call_instrument.iloc[0].to_dict())

            # Find Put Option
            put_instrument = self.option_series_instruments[
                (self.option_series_instruments['strike'] == strike) &
                (self.option_series_instruments['instrument_type'] == 'PE')
            ]
            if not put_instrument.empty:
                put_options.append(put_instrument.iloc[0].to_dict())

        # Sort by strike price
        call_options.sort(key=lambda x: x['strike'])
        put_options.sort(key=lambda x: x['strike'])

        return call_options, put_options

    def _update_historical_data(self, instruments_to_update):
        """
        Fetches and updates historical data for the given instruments.
        """
        to_date = datetime.datetime.now()
        from_date = to_date - datetime.timedelta(hours=3, minutes=30) # A bit of buffer

        for instrument in instruments_to_update:
            token = instrument['instrument_token']
            try:
                records = self.broker.get_historical_data(
                    instrument_token=token,
                    from_date=from_date,
                    to_date=to_date,
                    interval='minute',
                    oi=True
                )
                self.instrument_data[token] = records
                logger.info(f"Updated historical data for {instrument['tradingsymbol']} ({len(records)} records)")
            except Exception as e:
                logger.error(f"Failed to fetch historical data for {instrument['tradingsymbol']}: {e}")
                self.instrument_data[token] = [] # Store empty list on failure

    def _calculate_change(self, token, minutes_ago, data_key='oi'):
        """
        Calculates the absolute and percentage change for a given instrument and time.
        """
        history = self.instrument_data.get(token, [])
        if len(history) < 2:
            return 0, 0.0, None # Not enough data

        # Ensure history is sorted by date, most recent first
        history.sort(key=lambda x: x['date'], reverse=True)

        current_record = history[0]
        current_value = current_record[data_key]

        target_time = current_record['date'] - datetime.timedelta(minutes=minutes_ago)

        # Find the closest record to the target time
        past_record = min(history, key=lambda x: abs(x['date'] - target_time))

        # Ensure we are not comparing the same record
        if past_record['date'] == current_record['date'] and len(history) > 1:
             past_record = history[1]


        past_value = past_record[data_key]

        if past_value == 0:
            return 0, 0.0, "na" # Avoid division by zero

        abs_change = current_value - past_value
        perc_change = (abs_change / past_value) * 100

        return abs_change, perc_change

    def _generate_options_table(self, title, options, atm_strike):
        """
        Generates a table for call or put options.
        Returns the table and the count of red cells.
        """
        table = Table(title=title, show_header=True, header_style="bold magenta")
        table.add_column("Strike", style="dim", width=12)
        table.add_column("Current OI", justify="right")
        table.add_column("3min Δ", justify="right")
        table.add_column("5min Δ", justify="right")
        table.add_column("10min Δ", justify="right")
        table.add_column("15min Δ", justify="right")
        table.add_column("30min Δ", justify="right")
        table.add_column("3hr Δ", justify="right")

        time_intervals = {'3min': 3, '5min': 5, '10min': 10, '15min': 15, '30min': 30, '3hr': 180}
        red_cell_count = 0
        total_data_cells = 0

        for option in options:
            token = option['instrument_token']
            history = self.instrument_data.get(token, [])
            current_oi = history[0]['oi'] if history else 0

            strike_style = "bold yellow" if option['strike'] == atm_strike else ""
            row_data = [f"[{strike_style}]{option['strike']}[/{strike_style}]"]
            row_data.append(f"{current_oi:,}")

            for key, minutes in time_intervals.items():
                total_data_cells += 1
                abs_change, perc_change = self._calculate_change(token, minutes, 'oi')

                # Determine style based on threshold
                threshold = self.strat_var_color_thresholds.get(key.replace('min', 'm').replace('hr', 'h'), 1000)
                style = ""
                if perc_change > threshold:
                    style = "red"
                    red_cell_count += 1

                cell_content = f"[{style}]{perc_change:+.1f}% ({abs_change:,})[/{style}]"
                row_data.append(cell_content)

            table.add_row(*row_data)

        return table, red_cell_count, total_data_cells

    def _generate_nifty_table(self, ltp, token):
        """Generates a table for NIFTY value changes."""
        table = Table(title="NIFTY", show_header=True, header_style="bold cyan")
        table.add_column("Current Price", justify="right")
        table.add_column("3min Δ", justify="right")
        table.add_column("5min Δ", justify="right")
        table.add_column("10min Δ", justify="right")
        table.add_column("15min Δ", justify="right")
        table.add_column("30min Δ", justify="right")
        table.add_column("3hr Δ", justify="right")

        time_intervals = {'3min': 3, '5min': 5, '10min': 10, '15min': 15, '30min': 30, '3hr': 180}

        row_data = [f"{ltp:,.2f}"]

        for key, minutes in time_intervals.items():
            abs_change, perc_change = self._calculate_change(token, minutes, 'close')
            cell_content = f"{perc_change:+.2f}% ({abs_change:,.2f})"
            row_data.append(cell_content)

        table.add_row(*row_data)
        return table

    def run(self):
        """Starts the main loop of the strategy."""
        console = Console()

        with Live(console=console, screen=True, redirect_stderr=False) as live:
            while True:
                try:
                    ltp, nifty_token = self._get_nifty_ltp()
                    atm_strike = self._get_atm_strike(ltp)

                    if not ltp or not atm_strike or not nifty_token:
                        live.update("[bold red]Could not fetch NIFTY LTP. Retrying...[/bold red]")
                        time.sleep(self.strat_var_update_interval_seconds)
                        continue

                    call_options, put_options = self._get_option_details(atm_strike)

                    nifty_instrument = [{'instrument_token': nifty_token, 'tradingsymbol': 'NIFTY 50'}]
                    all_instruments = nifty_instrument + call_options + put_options

                    self._update_historical_data(all_instruments)

                    # Generate tables
                    calls_table, red_calls, total_calls = self._generate_options_table("CALLS", call_options, atm_strike)
                    puts_table, red_puts, total_puts = self._generate_options_table("PUTS", put_options, atm_strike)
                    nifty_table = self._generate_nifty_table(ltp, nifty_token)

                    # Check for alerts
                    total_red_cells = red_calls + red_puts
                    total_data_cells = total_calls + total_puts
                    if total_data_cells > 0:
                        red_percentage = (total_red_cells / total_data_cells) * 100
                        if red_percentage > self.strat_var_alert_threshold_percentage:
                            beepy.beep(sound='ping')
                            logger.warning(f"ALERT: Red cell percentage ({red_percentage:.1f}%) exceeded threshold.")

                    # Group tables for display
                    from rich.panel import Panel
                    from rich.layout import Layout
                    from rich.align import Align

                    layout = Layout()
                    layout.split(
                        Layout(name="header", size=3),
                        Layout(ratio=1, name="main"),
                        Layout(size=3, name="footer")
                    )

                    header_text = Align.center(f"NIFTY @ {ltp:,.2f} (ATM: {atm_strike}) | Last updated: {datetime.datetime.now().strftime('%H:%M:%S')}", vertical="middle")
                    layout["header"].update(header_text)

                    layout["main"].split_row(Layout(name="left"), Layout(name="right"))
                    layout["left"].update(calls_table)
                    layout["right"].update(puts_table)

                    layout["footer"].update(nifty_table)

                    live.update(layout)

                    time.sleep(self.strat_var_update_interval_seconds)

                except KeyboardInterrupt:
                    logger.info("Shutdown requested by user.")
                    break
            except Exception as e:
                logger.error(f"An error occurred in the main loop: {e}")
                traceback.print_exc()
                time.sleep(self.strat_var_update_interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OI Tracker Strategy")
    parser.add_argument('--config', type=str, default='strategy/configs/oi_tracker.yml', help='Path to the configuration file.')
    args = parser.parse_args()

    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)['default']
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading configuration file: {e}")
        sys.exit(1)

    try:
        broker = ZerodhaBroker(without_totp=True) # Assuming non-TOTP login for simplicity
        logger.info("Broker initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing broker: {e}")
        sys.exit(1)

    strategy = OITrackerStrategy(broker, config)
    strategy.run()
