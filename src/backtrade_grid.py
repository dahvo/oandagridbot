from src.database_functions import get_instrument_value, fetch_historical_data

import backtrader as bt
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

class AdvancedGridStrategy(bt.Strategy):
    params = (
        ('entry_atr_factor', 1.0),
        ('sl_atr_factor', 0.5),
        ('tp_atr_factor', 2.0),
        ('pip_location', 4),
        ('funds_percentage', 0.1),  # Percentage of funds to use for each grid level
        ('grid_levels', 5),  # Number of levels in the grid
        ('rebalance_freq', 10),  # Frequency of rebalance in terms of bars
    )

    def __init__(self):
        self.atr = bt.indicators.AverageTrueRange(period=14)
        self.order_dict = {}  # To keep track of orders at each grid level
        self.grid_setup()

    def grid_setup(self):
        self.grid_levels = {i: {'order': None, 'entry': None, 'sl': None, 'tp': None} for i in range(self.params.grid_levels)}

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        logging.info(f'{dt.isoformat()} {txt}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            exec_type = 'BUY' if order.isbuy() else 'SELL'
            self.log(f'{exec_type} EXECUTED, Price: {order.executed.price}, Cost: {order.executed.value}, Comm: {order.executed.comm}')

            # Update the grid level's order status
            for level, info in self.grid_levels.items():
                if info['order'] == order:
                    info['order'] = None  # Reset the order reference
                    break

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

            # Reset the order reference in the grid level
            for level, info in self.grid_levels.items():
                if info['order'] == order:
                    info['order'] = None
                    break

    def next(self):
        # Check if it's time to rebalance the grid
        if len(self) % self.params.rebalance_freq == 0:
            self.rebalance_grid()

        for level, info in self.grid_levels.items():
            if not info['order']:
                # Place new orders if there are no pending orders for the grid level
                self.place_grid_orders(level)

    def place_grid_orders(self, level):
        current_price = self.data.close[0]
        atr_value = self.atr[0]

        # Calculate the funds available for this trade
        account_value = self.broker.getvalue()
        funds_for_trade = account_value * self.params.funds_percentage

        # Adjust for the number of grid levels
        funds_for_trade /= self.params.grid_levels

        # Calculate the entry price for this grid level
        entry_price = current_price + (atr_value * self.params.entry_atr_factor * (level - self.params.grid_levels / 2))
        entry_price = round(entry_price, self.params.pip_location)

        # Calculate position size based on the funds allocated for this trade
        position_size = self.calculate_position_size(funds_for_trade, entry_price)

        # Determine the direction based on the level relative to the current price
        if level < self.params.grid_levels / 2:
            self.grid_levels[level]['order'] = self.sell(size=position_size, price=entry_price, exectype=bt.Order.Limit)
        else:
            self.grid_levels[level]['order'] = self.buy(size=position_size, price=entry_price, exectype=bt.Order.Limit)

        self.log(f'Grid Level {level}: Order placed at {entry_price} for size {position_size}')

    def calculate_position_size(self, funds, price):
        # This function calculates the position size based on available funds and price
        # This is a simplified version and might need adjustments for leverage, margin, and commission considerations
        return funds / price

    def rebalance_grid(self):
        # Rebalance the grid based on new market conditions
        self.log('Rebalancing grid')
        # Cancel all existing orders
        for level, info in self.grid_levels.items():
            if info['order']:
                self.cancel(info['order'])
                info['order'] = None

        # Recalculate grid levels based on current price and ATR
        self.grid_setup()
