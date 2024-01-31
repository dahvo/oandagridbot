from oandapyV20 import API
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.instruments as instruments
import pandas as pd



def get_account_instruments(account_id, access_token):
    client = API(access_token=access_token)
    r = accounts.AccountInstruments(accountID=account_id)
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

if __name__ == '__main__':
    access_token = "eeac3f2b6e1d03b113bfe7a89f42629e-4c628a07f4d115aa5e3e1e74dc55fc7f"
    data = get_historical_data(5, access_token=access_token)
    print(data)