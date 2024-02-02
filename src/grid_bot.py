import logging
from datetime import datetime

from src.database_functions import get_instrument_value

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

from src.bot_utils import BotUtils, adjust_price_to_pip_location


# -----------------Grid Bot Classes-----------------#

class GridBot:
    def __init__(self, main_bot, instrument):
        self.utils = BotUtils(main_bot)  # Creates a new BotUtils instance for each GridBot
        self.utils.instrument = instrument
        self.account_id = main_bot.account_id
        self.api = main_bot.api
        self.access_token = main_bot.access_token
        self.instrument = instrument

        self.pip_location = self.utils.get_pip_location()
        self.pip_value = self.utils.get_pip_value()
        self.get_current_price = self.utils.get_current_price
        self.available_funds = main_bot.funds_available_for_grid
        self.grid_settings = main_bot.grid_settings
        self.get_recent_atr = self.utils.get_recent_atr

        self.conversion_factor_pos, self.conversion_factor_neg = self.utils.get_conversion_factors()
        self.long_available_units, self.short_available_units = self.get_available_units()

        # Initialize other necessary instance-specific attributes here...
        # self.initialize_grid_parameters()  # Make sure this is called to set up instance-specific settings

        self.is_grid_active = None
        self.is_long = None
        self.is_ranging = None

    def get_available_units(self):
        logging.info(f"Setting available units for {self.instrument}")

        long_available_units, short_available_units = int(
            self.available_funds * self.conversion_factor_pos / self.grid_settings[
                'order_limit']), int(
            self.available_funds * self.conversion_factor_neg / self.grid_settings['order_limit'])
        logging.info(
            f"Available units for grid using {self.instrument}: "
            f"Long: {long_available_units}, Short: {short_available_units}")
        return long_available_units, short_available_units

    def initialize_grid_parameters(self):
        self.is_grid_active = True
        logging.info(f"Grid initialized for {self.instrument}.")
        logging.info(
            f"Available units for grid using {self.instrument}: "
            f"Long: {self.long_available_units}, "
            f"Short: {self.short_available_units}, "
            f"Pip Location: {self.pip_location}, "
            f"Pip Value: {self.pip_value}")

    def reset_grid(self):
        # TODO - ENSURE ONLY GRID ORDERS ARE CANCELLED
        # self.close_all_positions()
        # self.cancel_all_orders()
        self.is_grid_active = False
        logging.info("Grid reset.")
        self.activate_grid()

    def run_strategy(self):
        self.activate_grid()
        # self.check_grid_status()

    def check_grid_status(self):
        logging.info(f"Checking grid status for {self.instrument}")
        pos = self.utils.get_current_positions()
        if pos:
            if not self.is_market_condition_favorable():
                self.reset_grid()
        else:
            orders = self.utils.get_open_orders()

            if orders:
                if not self.is_market_condition_favorable():
                    self.reset_grid()
            else:
                self.reset_grid()

    def place_atr_based_orders(self):
        self.pip_location = 4
        current_price = self.get_current_price()
        atr = self.get_recent_atr()
        adjusted_atr = round(atr, self.pip_location)

        atr_factor = self.grid_settings['entry_atr_factor']

        logging.info(
            f"Placing orders for {self.instrument} using ATR: {atr}, "
            f"ATR Factor: {atr_factor}, Current Price: {current_price}")
        long_entry_price = current_price - adjusted_atr
        short_entry_price = current_price + adjusted_atr

        logging.info(f"Long entry price: {long_entry_price}, Short entry price: {short_entry_price}")

        adjusted_long_entry_price = round(long_entry_price, self.pip_location)
        adjusted_short_entry_price = round(short_entry_price, self.pip_location)
        logging.info(
            f"Adjusted long entry price: {adjusted_long_entry_price}, "
            f"Adjusted short entry price: {adjusted_short_entry_price}")
        sl_distance = atr * self.grid_settings['sl_atr_factor']
        tp_distance = atr * self.grid_settings['tp_atr_factor']

        logging.info(f"SL distance: {sl_distance}, TP distance: {tp_distance}")
        adjusted_sl_distance = round(sl_distance, self.pip_location)
        adjusted_tp_distance = round(tp_distance, self.pip_location)

        logging.info(f"Adjusted SL distance: {adjusted_sl_distance}Adjust TP distance: {adjusted_tp_distance}")

        buy_take_profit = current_price + tp_distance
        sell_take_profit = current_price - tp_distance

        adjusted_buy_take_profit = round(buy_take_profit, self.pip_location)
        adjusted_sell_take_profit = round(sell_take_profit, self.pip_location)

        logging.info(
            f"Buy take profit: {buy_take_profit}, "
            f"Sell take profit: {sell_take_profit}")
        logging.info(f"Adjusted Buy take profit: {adjusted_buy_take_profit}, "
                     f"Sell take profit: {adjusted_sell_take_profit}")

        # Calculate and place orders
        self.utils.place_order(price=adjusted_long_entry_price,
                               units=self.long_available_units,)
# Long order at current price + ATR

        self.utils.place_order(price=adjusted_short_entry_price,
                               units=-self.short_available_units,)
     # Short order at current price - ATR
        logging.info(f"Orders placed for {self.instrument}")

    def is_market_condition_favorable(self):
        logging.info(f"Checking market condition for {self.instrument}")
        return True

    def activate_grid(self):
        if not self.is_grid_active:
            logging.info(f"Activating grid for {self.instrument}")
            self.is_grid_active = True
            self.initialize_grid_parameters()
            self.place_atr_based_orders()
            self.grid_setup_time = datetime.utcnow()
