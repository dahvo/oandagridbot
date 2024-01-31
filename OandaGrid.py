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

logging.basicConfig(
    #filename="logs/algo.log",
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s : %(message)s',
)


class OandaGrid:
    def __init__(self, access_token, instrument, environment="practice"):
        self.api = API(access_token=access_token, environment=environment)
        self.access_token = access_token
        self.account_id = self.get_account_id()
        self.instrument = instrument
        self.environment = environment
        self.grid_size_pct = 0.01 # Grid size as a percentage of the current price
        self.grid_num = 5 # Number of grid orders
        self.tp_pct = 0.01 # Take profit percentage
        self.sl_pct = 0.01 # Stop loss percentage
        self.order_size_percent = 3 # Order size as a percentage of the account balance
        self.timer = datetime.utcnow() - timedelta(days=1)  # Ensure the first run is immediate
        self.is_running_grid = False
        self.grid_setup_time = datetime.utcnow() - timedelta(days=8)  # Ensure grid setup on the first run
        self.access_token = access_token

        #self.reset_grid()
        self.pip_location = None
        self.conversion_factor_pos = None
        self.conversion_factor_neg = None

        self.high_chop = 61.8
        self.low_chop = 38.2
        self.chop_granularity = 'D'
        self.chop_count = 30
        self.chop_length = 14
        self.chop_atr_length = 1

    def set_conversion_factor(self):
        r = pricing.PricingInfo(accountID=self.account_id, params={"instruments": self.instrument})
        quoteHomeConversionFactors = self.api.request(r)['prices'][0]['quoteHomeConversionFactors']
        self.conversion_factor_pos, self.conversion_factor_neg = quoteHomeConversionFactors['positiveUnits'], quoteHomeConversionFactors['negativeUnits']

    def get_account_id(self):

        client = API(access_token=self.access_token, environment=self.environment)

        r = accounts.AccountList()
        client.request(r)
        accounts_data = r.response.get('accounts')
        ids = [account.get('id') for account in accounts_data]
        return ids[0]

    def get_account_balance(self):
        r = accounts.AccountDetails(self.account_id)
        return self.api.request(r)['account']['balance']

    def get_current_price(self):
        r = instruments.InstrumentsCandles(instrument=self.instrument, params={"count": 1, "granularity": "S5"})
        data = self.api.request(r)
        return float(data['candles'][0]['mid']['c'])

    def compute_mfi(self, data):
        mfi_series = ta.mfi(high=data['high'], low=data['low'], close=data['close'], volume=data['volume'], length=14)
        return mfi_series.to_numpy()

    def compute_chop(self, data):
        # Filter out incomplete bars
        data = data[data['complete'] == True]
        # Use pandas_ta to calculate the Choppiness Index
        chop = ta.chop(high=data['high'].astype(float),
                       low=data['low'].astype(float),
                       close=data['close'].astype(float),
                       length=self.chop_length,
                       atr_length=self.chop_atr_length)
        # Return the Choppiness Index series
        return chop


    def select_instrument(self):
        """
        Sets the instrument for trading based on the chop index closest to the target chop value.

        :param target_chop: Target chop index value to select the instrument.
        """
        closest_instrument = None
        closest_chop_diff = float('inf')
        self.reset_grid()  # Reset the grid before selecting a new instrument

        instruments_list = get_instrument_list()
        contenders = {}
        for instrument in instruments_list:
            self.instrument = instrument  # Temporarily set the instrument to fetch its data
            # Fetch historical data for the given instrument
            data = fetch_historical_data(count=self.chop_count,
                                         instrument=self.instrument,
                                         granularity=self.chop_granularity,
                                         access_token=self.access_token)

            chop_series = self.compute_chop(data)  # Compute the chop index for the instrument
            latest_chop_value = chop_series.iloc[-1]
            logging.info(f"Latest chop value for {instrument}: {latest_chop_value}")
            second_latest_chop_value = chop_series.iloc[-2]
            if latest_chop_value > self.high_chop and second_latest_chop_value <= self.high_chop:
                contenders.update({'instrument': instrument, 'chop': latest_chop_value})
                logging.info(f"Instrument added to contenders: {instrument}, Chop: {latest_chop_value}")

        if not contenders:
            logging.warning("No suitable instrument found based on chop index.")


    def adjust_price_to_pip_location(self, price):
        """Adjusts the price according to the instrument's pip location."""
        if self.pip_location is None:
            self.set_pip_location()
        # Since pip_location is negative, convert it to positive to indicate the number of decimal places
        decimal_places = abs(self.pip_location)

        # Now, round the price to the correct number of decimal places
        adjusted_price = round(price, decimal_places)

        # Finally, adjust the price to the correct pip location
        return adjusted_price

    def place_grid_orders(self, current_price, is_buy):
        for i in range(1, self.grid_num + 1):
            price_adjustment = i * self.grid_size_pct * current_price
            price = current_price - price_adjustment if is_buy else current_price + price_adjustment
            price = self.adjust_price_to_pip_location(price)  # Adjust the price to the correct pip location
            take_profit = self.adjust_price_to_pip_location(
                price * (1 + self.tp_pct)) if is_buy else self.adjust_price_to_pip_location(price * (1 - self.tp_pct))
            stop_loss = self.adjust_price_to_pip_location(
                price * (1 - self.sl_pct)) if is_buy else self.adjust_price_to_pip_location(price * (1 + self.sl_pct))
            units = self.trade_size if is_buy else -self.trade_size

            self.create_order(units, price, take_profit, stop_loss)

    def set_pip_location(self):
        """Retrieve the pip location for the instrument."""
        # Use your existing database function or API call to get pipLocation
        self.pip_location = int(get_instrument_value(self.instrument, 'pipLocation'))
        if self.pip_location is None:
            logging.error(f"Could not retrieve pip location for {self.instrument}. Setting default value.")
            self.pip_location = -4  # Default pip location for most pairs, change as needed

    def create_order(self, units, price, take_profit, stop_loss):
        data = {
            "order": {
                "instrument": self.instrument,
                "units": str(units),
                "type": "LIMIT",
                "price": str(price),
                "takeProfitOnFill": {"price": str(take_profit)},
                "stopLossOnFill": {"price": str(stop_loss)},
                "positionFill": "DEFAULT"
            }
        }
        r = orders.OrderCreate(self.account_id, data=data)
        logging.info(f"Placing order: {data}")
        self.api.request(r)

    def reset_grid(self):
        # Close all positions to reset the grid
        self.close_all_positions()
        self.is_running_grid = False
        self.pip_location = None
        self.cancel_all_orders()
        self.close_all_positions()
        self.timer = datetime.utcnow()  # Reset the timer
        self.instrument = None  # Reset the instrument
        logging.info(f"Reset grid.")

    def close_all_positions(self):
        positions = self.get_current_positions()
        if positions:
            r = positions.PositionClose(self.account_id, self.instrument, data={"longUnits": "ALL", "shortUnits": "ALL"})
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
            self.set_instrument_by_chop()
            self.timer = datetime.utcnow()
            data = fetch_historical_data(count=self.chop_count, instrument=self.instrument,
                                         granularity=self.chop_granularity,
                                         access_token=self.access_token)  # Fetch historical data for the given instrument
            chop_series = self.compute_chop(data)

            is_ranging = True

            if chop_series[-1] > 50 and chop_series[-2] <= 50:
                is_ranging = False

            if not self.is_running_grid and is_ranging:
                self.is_running_grid = True
                self.grid_setup_time = datetime.utcnow()

                data = fetch_historical_data(count=1, instrument=self.instrument, granularity='D',
                                             access_token=self.access_token)  # Fetch historical data for the given instrument
                current_price = float(data[0]['mid']['c'])

                # Setup buy and sell grids
                self.place_grid_orders(current_price, is_buy=True)
                self.place_grid_orders(current_price, is_buy=False)

            if datetime.utcnow() > self.grid_setup_time + timedelta(days=7):
                self.reset_grid()

    def get_account_instruments(self):#account_id, access_token):
        client = API(access_token=self.access_token)
        r = accounts.AccountInstruments(accountID=self.account_id)
        client.request(r)
        return r.response.get('instruments')

    def get_historical_data(count, granularity='D', access_token=None, instrument='EUR_USD'):

        client = API(access_token=access_token)

        params = {
            "count": count,
            "granularity": granularity,
        }
        r = instruments.InstrumentsCandles(instrument=instrument, params=params)
        resp = client.request(r)
        return resp.get('candles')
