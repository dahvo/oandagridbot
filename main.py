from src.main_bot import MainBot
import logging

import os
from dotenv import load_dotenv

from tools.my_tools import get_historical_data

import time




logging.basicConfig(
    filename="logs/main.log",
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s : %(message)s',
)





if __name__ == '__main__':
    load_dotenv()
    access_token = os.getenv('DEMO_ACCESS_TOKEN')

    environment = 'practice'

    main_bot = MainBot(access_token, environment)

    while True:
        try:
            main_bot.get_backtesting_data()
        except Exception as e:
            logging.error(f"Error: {e} traceback: {e.__traceback__}")
            raise e
        break








