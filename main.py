from MyStrat import OandaGrid
from oandapyV20 import API
from tools.my_tools import get_account_instruments, get_historical_data
import logging
from database_functions import set_instruments_table, fetch_historical_data
import os
from dotenv import load_dotenv
import time
import pandas_ta as ta
import pandas as pd



logging.basicConfig(
    filename="logs/main.log",
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s : %(message)s',
)





if __name__ == '__main__':
    load_dotenv()
    access_token = os.getenv('DEMO_ACCESS_TOKEN')

    # help(ta.thermo)

    #set_instruments_table(access_token)

    # Example usage
    algo = OandaGrid(access_token=access_token, instrument='EUR_USD', environment="practice")
    # pos = algo.select_instrument_based_on_chop()
    # print(pos)
    # help(ta.chop)
    while True:
        algo.run_strategy()
        time.sleep(10)
        # Sleep or wait for a specific time before next iteration






