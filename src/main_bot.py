from oandapyV20 import API
from oandapyV20.endpoints import accounts

from src.database_functions import set_instruments_table, fetch_historical_data, get_instrument_list

from tools.my_tools import compute_indicator
import pandas_ta as ta

import logging

from src.grid_bot import GridBot

from src.trend_bot import TrendingBot

from src.stream_handler import StreamHandler


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')



class MainBot:
    def __init__(self, access_token, environment="demo"):
        self.max_grids = 1
        self.max_trenders = 1
        self.api = API(access_token=access_token, environment=environment)
        self.access_token = access_token
        self.account_id = self.get_primary_account_id()
        self.set_account_instruments()
        self.viable_instruments_for_grid = []
        self.viable_instruments_for_trending = []

        self.grid_amount = 0.5  # Amount of the account balance to be used for grid trading
        self.trending_amount = 0.5  # Amount of the account balance to be used for trending trading
        self.chop_filters = {"high": 61.8, "low": 38.2}
        self.chop_settings = {"length": 14, "atr_length": 1}
        self.bb_settings = {"length": 20, "std_dev": 2}
        self.bb_filters = {"high": 0.8, "low": 0.2}
        self.fetch_settings = {"granularity": 'H1', "count": 30}

        self.grid_settings = {
            "order_limit": 5,
            "sl_atr_factor": 1.5,
            "tp_atr_factor": 1.5,
            "entry_atr_factor": 0.25,  # ATR factor for entry
            "order_size_percent": 3
        }

    def get_available_balance(self):
        response = self.api.request(accounts.AccountDetails(self.account_id))
        logging.info(f"Available balance: {response['account']['marginAvailable']}")
        return float(response['account']['marginAvailable'])


    def set_available_funds(self):
        available_balance = self.get_available_balance()
        self.funds_available_for_grid = self.grid_amount * available_balance / self.max_grids
        self.funds_available_for_trending = self.trending_amount * available_balance / self.max_trenders
        logging.info(f"Funds available for grid: {self.funds_available_for_grid}")

    def set_account_instruments(self):
        r = accounts.AccountInstruments(accountID=self.account_id)
        response = self.api.request(r)
        set_instruments_table(response.get('instruments'))
        logging.info(f"Account instruments set")

    def get_primary_account_id(self):
        accounts_response = self.api.request(accounts.AccountList())
        return accounts_response['accounts'][0]['id']

    def evaluate_instruments(self):
        for instrument in get_instrument_list():
            data = fetch_historical_data(instrument=instrument, **self.fetch_settings, access_token=self.access_token)
            chop_value = compute_indicator(data=data, indicator_func=ta.chop, include_volume=False,
                                           length=self.chop_settings['length'])[-1]

            bb_perc = compute_indicator(data=data, indicator_func=ta.bbands, include_volume=False, length=self.bb_settings['length'],
                                     std=self.bb_settings['std_dev'])[-1][4]
            # bb_perc = (bband[-1][4])  # Bollinger Bands Percentage

            # Skip instrument if bb_perc is higher than the filter's high threshold
            if bb_perc > self.bb_filters['high']:
                continue

            # Add instrument to the respective list along with its bb_perc
            if chop_value > self.chop_filters['high']:
                self.viable_instruments_for_grid.append((instrument, bb_perc))
                logging.info(f"Instrument: {instrument} added as instrument for grid, Last CHOP Value: {chop_value}")
            elif chop_value < self.chop_filters['low']:
                self.viable_instruments_for_trending.append((instrument, bb_perc))
                logging.info(
                    f"Instrument: {instrument} added as instrument for trending, Last CHOP Value: {chop_value}")

        # Sort the lists by bb_perc value from low to high
        self.viable_instruments_for_grid.sort(key=lambda x: x[1])
        self.viable_instruments_for_trending.sort(key=lambda x: x[1])

        # Logging the sorted lists
        for instrument, bb_perc in self.viable_instruments_for_grid:
            logging.info(f"Grid Instrument: {instrument}, BB_PERC: {bb_perc}")
        for instrument, bb_perc in self.viable_instruments_for_trending:
            logging.info(f"Trending Instrument: {instrument}, BB_PERC: {bb_perc}")

    def run_strategies(self):
        self.set_available_funds()
        self.evaluate_instruments()
        self.run_grid_strategy()
        # self.run_trending_strategy()
        logging.info("Strategies run")
        self.run_stream()


    def run_grid_strategy(self):
        for i in range(self.max_grids):
            if self.viable_instruments_for_grid:
                instrument = self.viable_instruments_for_grid.pop(0)[0]
                grid_bot = GridBot(self, instrument)
                grid_bot.run_strategy()
                logging.info(f"Grid strategy run for {instrument}")

    def run_trending_strategy(self):
        for i in range(self.max_trenders):
            if self.viable_instruments_for_trending:
                instrument = self.viable_instruments_for_trending.pop(0)[0]
                trending_bot = TrendingBot(self, instrument)
                trending_bot.run_strategy()
                logging.info(f"Trending strategy run for {instrument}")

    def start_stream_handler(self, stream_type, instruments=["EUR_USD"]):
        self.stream_handler = StreamHandler(stream_type, self, instruments)
        self.stream_handler.run_stream()

    def stop_stream_handler(self):
        self.stream_handler.stop_stream()

    def get_backtesting_data(self):
        for instrument in get_instrument_list():
            data = fetch_historical_data(instrument=instrument, granularity='M1', count = 260640, access_token=self.access_token)

