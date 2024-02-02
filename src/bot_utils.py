from oandapyV20.endpoints import pricing, orders, positions, instruments

from tools.my_tools import compute_indicator

from src.database_functions import fetch_historical_data, get_instrument_value

import logging

import pandas_ta as ta

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')


# -----------------Shared Functions-----------------#

def adjust_price_to_pip_location(pip_location, price):
    if pip_location is not None:
        # Calculate the factor to use for rounding based on pip_location
        rounding_factor = 10 ** (-pip_location)
        # Adjust the price by the rounding factor
        adjusted_price = round(price / rounding_factor) * rounding_factor
    else:
        # If pip_location is not set, do not adjust the price
        adjusted_price = price

    logging.info(f"Original price: {price}, Adjusted price: {adjusted_price}, Pip location: {pip_location}")
    return adjusted_price


class BotUtils:
    def __init__(self, main_bot):
        self.api = main_bot.api
        self.access_token = main_bot.access_token
        self.account_id = main_bot.account_id
        self.instrument = None  # Ensure this is set per instance
        self.grid_settings = main_bot.grid_settings
        # Initialize these as instance attributes to avoid sharing between instances
        self.pip_location = None
        self.available_units = None

    def get_conversion_factors(self):
        response = self.api.request(
            pricing.PricingInfo(accountID=self.account_id, params={"instruments": self.instrument}))
        factors = response['prices'][0]['quoteHomeConversionFactors']
        conversion_factor_pos, conversion_factor_neg = float(factors['positiveUnits']), float(factors['negativeUnits'])
        logging.info(f"Conversion factors for {self.instrument}: {conversion_factor_pos, conversion_factor_neg}")
        return conversion_factor_pos, conversion_factor_neg


    def get_current_price(self):
        response = self.api.request(
            instruments.InstrumentsCandles(instrument=self.instrument, params={"count": 1, "granularity": "S5"}))
        logging.info(f"Current price for {self.instrument}: {response['candles'][0]['mid']['c']}")
        return float(response['candles'][0]['mid']['c'])

    def get_pip_location(self):
        self.pip_location = get_instrument_value(self.instrument, 'pipLocation')
        logging.info(f"Pip location for {self.instrument}: {self.pip_location}")
        return self.pip_location

    def get_pip_value(self):
        try:
            return 10 ** abs(int(self.pip_location))
        except Exception as e:
            logging.error(f"Could not find pip location for {self.instrument} - {e}")

    def place_order(self, price=None, units=None):

        order_data = {
            "instrument": self.instrument,
            "units": str(units),
            "type": "LIMIT",
            "price": str(price),
            "positionFill": "DEFAULT"
        }
        logging.info(f"Order data: {order_data}")
        self.execute_order(order_data)

    def get_trailing_stop_loss(self, distance):
        self.minimum_trailing_stop = get_instrument_value(self.instrument, 'trailingStop')
        self.maximum_trailing_stop = get_instrument_value(self.instrument, 'maximumTrailingStop')

        if distance < self.minimum_trailing_stop:
            raise ValueError(f"Trailing stop loss distance too low: {distance}")
            #distance = self.minimum_trailing_stop
        elif distance > self.maximum_trailing_stop:
            raise ValueError(f"Trailing stop loss distance too High: {distance}")
            #distance = self.maximum_trailing_stop
        else:
            logging.info(f"Trailing stop loss distance fine: {distance}")
            distance = distance

        return {"trailingStopLossOnFill": {"distance": str(distance)}}
    def get_recent_atr(self):
        data = fetch_historical_data(self.instrument, 'H1', 15, self.access_token)
        # Compute the most recent ATR value based on the fetched data
        atr = compute_indicator(
            data=data,
            indicator_func=ta.atr,
            include_volume=False,
            length=14
        )[-1]
        logging.info(f"Recent ATR for {self.instrument}: {atr}")

        return atr

    def calculate_tp_sl_targets(self, price, is_buy):
        atr = self.get_recent_atr()
        sl_distance = atr * self.grid_settings['sl_atr_factor']
        tp_distance = atr * self.grid_settings['tp_atr_factor']
        adjusted_take_profit = price + tp_distance if is_buy else price - tp_distance
        logging.info(f"Adjusted take profit: {adjusted_take_profit}, SL distance: {sl_distance}")
        return self.adjust_price_to_pip_location(adjusted_take_profit), self.adjust_price_to_pip_location(
            sl_distance)

    def execute_order(self, order_data):
        try:
            response = self.api.request(orders.OrderCreate(self.account_id, data={"order": order_data}))
            logging.info(f"Order created: {response}")
        except Exception as e:
            if "insufficient" in str(e):
                logging.error(f"Insufficient funds to create order: {e}")
            elif "minimum" in str(e):
                logging.error(f"Minimum order size not met: {e}")
            elif "units" in str(e):
                logging.error(f"Invalid units: {e}")
            elif "price" in str(e):
                logging.error(f"Invalid price: {e}")
            elif "takeProfit" in str(e):
                logging.error(f"Invalid take profit: {e}")
            elif "trailingStopLoss" in str(e):
                logging.error(f"Invalid trailing stop loss: {e}")
            elif "instrument" in str(e):
                logging.error(f"Invalid instrument: {e}")
            logging.error(f"Failed to create order: {e}")
            raise e

    def close_all_positions(self):
        has_positions = self.get_current_positions()
        if has_positions:
            r = positions.PositionClose(self.account_id, self.instrument,
                                        data={"longUnits": "ALL", "shortUnits": "ALL"})
            self.api.request(r)

    def get_current_positions(self):
        r = positions.OpenPositions(self.account_id)
        return self.api.request(r)['positions']

    def get_open_orders(self):
        r = orders.OrdersPending(self.account_id)
        return self.api.request(r)['orders']

    def cancel_all_orders(self):
        open_orders = self.get_open_orders()
        for order in open_orders:
            r = orders.OrderCancel(self.account_id, order['id'])
            self.api.request(r)
