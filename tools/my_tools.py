from oandapyV20 import API
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.instruments as instruments
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_iso8601_date



def get_account_instruments(account_id, access_token):
    client = API(access_token=access_token)
    r = accounts.AccountInstruments(accountID=account_id)
    client.request(r)
    return r.response.get('instruments')


def calculate_start_date_from_count(latest_date, count, granularity):
    """Calculate the start date for data fetching based on count and granularity."""
    total_minutes = granularity_to_minutes(granularity) * count
    start_date = latest_date - timedelta(minutes=total_minutes)
    return start_date


def granularity_to_minutes(granularity):
    """
    Convert a granularity string to minutes.

    Args:
    granularity (str): The granularity string (e.g., 'M1', 'H1', 'D', 'W').

    Returns:
    int: The number of minutes that the granularity represents.
    """
    # Map each granularity to its duration in minutes
    granularity_map = {
        'S5': 5 / 60,  # 5 seconds
        'S10': 10 / 60,  # 10 seconds
        'S15': 15 / 60,  # 15 seconds
        'S30': 30 / 60,  # 30 seconds
        'M1': 1,  # 1 minute
        'M2': 2,  # 2 minutes
        'M4': 4,  # 4 minutes
        'M5': 5,  # 5 minutes
        'M10': 10,  # 10 minutes
        'M15': 15,  # 15 minutes
        'M30': 30,  # 30 minutes
        'H1': 60,  # 1 hour
        'H2': 120,  # 2 hours
        'H3': 180,  # 3 hours
        'H4': 240,  # 4 hours
        'H6': 360,  # 6 hours
        'H8': 480,  # 8 hours
        'H12': 720,  # 12 hours
        'D': 1440,  # 1 day
        'W': 10080,  # 1 week
        'M': 43800,  # Approximate average for one month
    }

    # Return the corresponding minutes, defaulting to 0 if granularity is not recognized
    return granularity_map.get(granularity, 0)


def get_historical_data(count=None, granularity='D', access_token=None, instrument=None, start_date=None, end_date=None):
    client = API(access_token=access_token)

    # Initialize params dictionary with granularity; count will be added if it's not None
    params = {"granularity": granularity}

    # If count is provided, add it to the params
    if count is not None:
        params["count"] = count

    # If start date is provided, add it to the params. Note that OANDA API expects the 'from' parameter for the start date.
    if start_date is not None:
        params["from"] = start_date.isoformat()

    # If end date is provided, add it to the params. Note that OANDA API expects the 'to' parameter for the end date.
    if end_date is not None:
        params["to"] = end_date.isoformat()

    # Create the request with the specified instrument and parameters
    r = instruments.InstrumentsCandles(instrument=instrument, params=params)

    # Make the request to the API
    resp = client.request(r)

    # Return the 'candles' part of the response which contains the historical data
    return resp.get('candles')

def compute_indicator(data, indicator_func, include_volume=False, **kwargs):
    params = {'high': data['high'], 'low': data['low'], 'close': data['close']}
    if include_volume:
        params['volume'] = data['volume']
    return indicator_func(**params, **kwargs).to_numpy()



if __name__ == '__main__':
    pass
    # print(get_account_instruments(account_id, access_token))
    # print(get_historical_data(5, access_token=access_token))