from oandapyV20 import API
import oandapyV20.endpoints.orders as orders
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.positions as positions
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.pricing as pricing
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta
import logging
from database_functions import get_instrument_value, get_instrument_list, fetch_historical_data, set_instruments_table

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

class OandaGrid:
    def __init__(self, access_token, instrument, environment="demo"):
        self.api = API(access_token=access_token, environment=environment)
        self.access_token = access_token
        self.account_id = self.get_primary_account_id()
        self.set_account_instruments()
        self.instrument = instrument
        self.grid_settings = {
            "size_pct": 0.01,
            "num": 5,
            "tp_pct": 0.01,
            "sl_pct": 0.01,
            "order_size_percent": 3
        }
        self.timer = datetime.utcnow() - timedelta(days=1)
        self.is_grid_active = False
        self.grid_setup_time = datetime.utcnow() - timedelta(days=8)
        self.initialize_grid_parameters()


    def initialize_grid_parameters(self):
        self.set_conversion_factors()
        self.pip_location = self.get_pip_location()


    def set_account_instruments(self):
        r = accounts.AccountInstruments(accountID=self.account_id)
        response = self.api.request(r)
        set_instruments_table(response.get('instruments'))

    def get_primary_account_id(self):
        accounts_response = self.api.request(accounts.AccountList())
        print(accounts_response)
        return accounts_response['accounts'][0]['id']

    def get_account_balance(self):
        response = self.api.request(accounts.AccountDetails(self.account_id))
        return float(response['account']['balance'])

    def get_current_price(self):
        response = self.api.request(instruments.InstrumentsCandles(instrument=self.instrument, params={"count": 1, "granularity": "S5"}))
        return float(response['candles'][0]['mid']['c'])

    def compute_indicator(self, data, indicator_func, **kwargs):
        return indicator_func(data, **kwargs).to_numpy()

    def select_instrument_based_on_chop(self):
        self.chop_settings = {"high": 61.8, "low": 38.2, "length": 14, "atr_length": 1}
        self.fetch_settings = {"granularity": 'D', "count": 30}
        closest_instrument, closest_chop_diff = None, float('inf')
        for instrument in get_instrument_list():
            chop_value = self.compute_indicator(fetch_historical_data(instrument=instrument, **self.fetch_settings, access_token=self.access_token), ta.chop, length=self.chop_settings['length'])
            chop_diff = abs(chop_value - self.chop_settings['high'])
            if chop_diff < closest_chop_diff:
                closest_instrument, closest_chop_diff = instrument, chop_diff
        self.instrument = closest_instrument

    def adjust_price_to_pip_location(self, price):
        return round(price, abs(self.pip_location))

    def set_conversion_factors(self):
        response = self.api.request(
            pricing.PricingInfo(accountID=self.account_id, params={"instruments": self.instrument}))
        factors = response['prices'][0]['quoteHomeConversionFactors']
        self.conversion_factor_pos = float(factors['positiveUnits'])
        self.conversion_factor_neg = float(factors['negativeUnits'])

    def place_grid_orders(self, is_buy):
        current_price = self.get_current_price()
        for i in range(1, self.grid_settings["num"] + 1):
            price_adjustment = i * self.grid_settings["size_pct"] * current_price
            order_price = self.adjust_price_to_pip_location(current_price + (-price_adjustment if is_buy else price_adjustment))
            self.create_order(is_buy, order_price)

    def get_pip_location(self):
        try:
            return int(get_instrument_value(self.instrument, 'pipLocation'))
        except:
            print(get_instrument_value(self.instrument, 'pipLocation'))
            raise ValueError(f"Could not find pip location for {self.instrument}")

    def create_order(self, is_buy, price):
        units = self.calculate_order_size(is_buy, price)
        take_profit, stop_loss = self.calculate_order_targets(price, is_buy)
        order_data = {
            "instrument": self.instrument, "units": units, "type": "LIMIT", "price": price,
            "takeProfitOnFill": {"price": take_profit}, "stopLossOnFill": {"price": stop_loss},
            "positionFill": "DEFAULT"
        }
        self.api.request(orders.OrderCreate(self.account_id, data={"order": order_data}))

    def calculate_order_size(self, is_buy, price):
        balance = self.get_account_balance()
        order_size_base = balance * self.grid_settings["order_size_percent"] / 100
        current_price = self.get_current_price()

        # Adjust order size based on the conversion factor
        if is_buy:
            adjusted_size = order_size_base / current_price * self.conversion_factor_pos
        else:
            adjusted_size = order_size_base / current_price * self.conversion_factor_neg

        return int(adjusted_size) if is_buy else -int(adjusted_size)

    def calculate_order_targets(self, price, is_buy):
        tp_adjustment = price * self.grid_settings["tp_pct"]
        sl_adjustment = price * self.grid_settings["sl_pct"]
        take_profit = self.adjust_price_to_pip_location(price + (tp_adjustment if is_buy else -tp_adjustment))
        stop_loss = self.adjust_price_to_pip_location(price - (sl_adjustment if is_buy else +sl_adjustment))
        return take_profit, stop_loss

    def reset_grid(self):
        self.close_all_positions()
        self.cancel_all_orders()
        self.is_grid_active = False
        self.timer = datetime.utcnow()
        logging.info("Grid reset.")

    def close_all_positions(self):
        positions = self.get_current_positions()
        if positions:
            r = positions.PositionClose(self.account_id, self.instrument,
                                        data={"longUnits": "ALL", "shortUnits": "ALL"})
            self.api.request(r)

    def get_current_positions(self):
        r = positions.OpenPositions(self.account_id)
        return self.api.request(r)['positions']

    def cancel_all_orders(self):
        r = orders.OrdersPending(self.account_id)
        open_orders = self.api.request(r)['orders']
        for order in open_orders:
            r = orders.OrderCancel(self.account_id, order['id'])
            self.api.request(r)

    def run_strategy(self):
        if datetime.utcnow() >= self.timer + timedelta(hours=24):
            self.select_instrument_based_on_chop()
            if self.is_market_condition_favorable():
                self.activate_grid()

    def is_market_condition_favorable(self):
        # Implementation to check market conditions based on indicators
        pass

    def activate_grid(self):
        if not self.is_grid_active:
            self.is_grid_active = True
            self.place_grid_orders(is_buy=True)
            self.place_grid_orders(is_buy=False)
            self.grid_setup_time = datetime.utcnow()