from src.bot_utils import BotUtils

class TrendingBot:
    def __init__(self, main_bot, instrument):
        self.utils = BotUtils(main_bot)
        self.utils.instrument = instrument
        self.account_id = main_bot.account_id
        self.api = main_bot.api
        self.access_token = main_bot.access_token
        self.instrument = instrument

    def run_strategy(self):
        pass
        # self.check_grid_status()