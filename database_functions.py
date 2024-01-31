import sqlite3
import logging
from tools.my_tools import get_account_instruments, get_historical_data
import pandas as pd
from datetime import datetime, timedelta
from contextlib import contextmanager



# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@contextmanager
def connect_to_db(db_path='data/oanda_data.db'):
    connection = None
    try:
        connection = sqlite3.connect(db_path)
        yield connection
        connection.commit()  # Ensure commit happens here
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error("Database error", exc_info=True)
        raise e
    finally:
        if connection:
            connection.close()


def execute_db_query(connection, query, parameters=(), fetch_one=False, fetch_all=False):
    """Execute a query in the SQLite database."""
    cursor = connection.cursor()
    cursor.execute(query, parameters)
    if fetch_one:
        #logging.info(f"Database query: {query} with parameters: {parameters} as a fetch_one query.")
        return cursor.fetchone()
    if fetch_all:
        #logging.info(f"Database query: {query} with parameters: {parameters} as a fetch_all query.")
        return cursor.fetchall()


def set_instruments_table(data):
    #data = get_account_instruments(accountID, access_token)
    with connect_to_db() as connection:
        # Create the instruments table if it does not exist
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS instruments (
                name TEXT PRIMARY KEY,
                type TEXT,
                displayName TEXT,
                pipLocation INTEGER,
                displayPrecision INTEGER,
                tradeUnitsPrecision INTEGER,
                minimumTradeSize TEXT,
                maximumTrailingStopDistance TEXT,
                minimumTrailingStopDistance TEXT,
                maximumPositionSize TEXT,
                maximumOrderUnits TEXT,
                marginRate TEXT,
                guaranteedStopLossOrderMode TEXT
            )
        '''
        execute_db_query(connection, create_table_query)

        # Iterate through each item in the data and insert or update the record
        for item in data:
            upsert_query = '''
                INSERT INTO instruments (name, type, displayName, pipLocation, displayPrecision, tradeUnitsPrecision, 
                minimumTradeSize, maximumTrailingStopDistance, minimumTrailingStopDistance, maximumPositionSize, 
                maximumOrderUnits, marginRate, guaranteedStopLossOrderMode) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                type = excluded.type, displayName = excluded.displayName, pipLocation = excluded.pipLocation, 
                displayPrecision = excluded.displayPrecision, tradeUnitsPrecision = excluded.tradeUnitsPrecision, 
                minimumTradeSize = excluded.minimumTradeSize, maximumTrailingStopDistance = excluded.maximumTrailingStopDistance, 
                minimumTrailingStopDistance = excluded.minimumTrailingStopDistance, maximumPositionSize = excluded.maximumPositionSize, 
                maximumOrderUnits = excluded.maximumOrderUnits, marginRate = excluded.marginRate, 
                guaranteedStopLossOrderMode = excluded.guaranteedStopLossOrderMode
            '''
            execute_db_query(connection, upsert_query, (
                item['name'], item['type'], item['displayName'], item['pipLocation'],
                item['displayPrecision'], item['tradeUnitsPrecision'], item['minimumTradeSize'],
                item['maximumTrailingStopDistance'], item['minimumTrailingStopDistance'],
                item['maximumPositionSize'], item['maximumOrderUnits'], item['marginRate'],
                item['guaranteedStopLossOrderMode']
            ))

        # Committing is handled by the context manager
        #logging.info("Instruments table updated.")

def extract_bar_data(data):
    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    # Extract the 'mid' column into separate 'open', 'high', 'low', and 'close' columns
    df[['open', 'high', 'low', 'close']] = df['mid'].apply(lambda x: pd.Series([x['o'], x['h'], x['l'], x['c']]))

    # Drop the 'mid' column as it's no longer needed
    df.drop(columns=['mid'], inplace=True)

    # Reorder the columns as per your desired order
    df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'complete']]

    return df

def ensure_bars_tables_exists():
    with connect_to_db() as connection:
        # Create the necessary tables if they don't exist
        create_tables_queries = [
            '''
                CREATE TABLE IF NOT EXISTS instruments (
                    name TEXT PRIMARY KEY
                )
            ''',
            '''
                CREATE TABLE IF NOT EXISTS granularities (
                    name TEXT PRIMARY KEY
                )
            ''',
            '''
                CREATE TABLE IF NOT EXISTS bars (
                    instrument_name TEXT NOT NULL,
                    granularity_name TEXT NOT NULL,
                    time TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    complete BOOLEAN,
                    PRIMARY KEY (instrument_name, granularity_name, time),
                    FOREIGN KEY(instrument_name) REFERENCES instruments(name),
                    FOREIGN KEY(granularity_name) REFERENCES granularities(name)
                )
            '''
        ]

        for query in create_tables_queries:
            execute_db_query(connection, query)

        #logging.info("History tables created.")

def save_historical_data(historical_data, instrument, granularity):
    ensure_bars_tables_exists()
    with connect_to_db() as connection:
        # Ensure the instrument and granularity exist in their respective tables
        execute_db_query(connection, 'INSERT OR IGNORE INTO instruments (name) VALUES (?)', (instrument,))
        execute_db_query(connection, 'INSERT OR IGNORE INTO granularities (name) VALUES (?)', (granularity,))

        # Prepare and execute the insert or update query for each entry in the historical data
        for entry in historical_data:
            bar_time = entry['time']
            is_complete = entry['complete']
            open_price = entry['mid']['o']
            high_price = entry['mid']['h']
            low_price = entry['mid']['l']
            close_price = entry['mid']['c']
            volume = entry['volume']

            insert_bar_query = '''
                INSERT INTO bars (time, instrument_name, granularity_name, open, high, low, close, volume, complete)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_name, granularity_name, time) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume, complete=excluded.complete
            '''
            execute_db_query(connection, insert_bar_query, (
                bar_time, instrument, granularity, open_price, high_price, low_price, close_price, volume, is_complete
            ))

        logging.info("Historical data save complete.")


# Helper function to parse ISO 8601 formatted date
def parse_iso8601_date(iso_date):
    """Parse an ISO 8601 formatted date."""
    return datetime.strptime(iso_date.split('.')[0], '%Y-%m-%dT%H:%M:%S')


# Helper function to calculate expected bars based on count and granularity
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

def fetch_historical_data(instrument, granularity, count, access_token):
    ensure_bars_tables_exists()
    with connect_to_db() as connection:
        # Fetch historical data from the database
        fetch_query = '''
            SELECT time, open, high, low, close, volume, complete
            FROM bars
            WHERE instrument_name = ? AND granularity_name = ?
            ORDER BY time DESC
            LIMIT ?
        '''
        result = execute_db_query(connection, fetch_query, (instrument, granularity, count), fetch_all=True)

        if result:
            logging.info(f"Retrieved {len(result)} rows of historical data for {instrument} at {granularity} granularity.")
            if len(result) < count:
                logging.warning(f"Insufficient data found for {instrument} at {granularity} granularity. Fetching from API.")
                api_data = get_historical_data(count, granularity, access_token, instrument)
                save_historical_data(api_data, instrument, granularity)
                return extract_bar_data(api_data)
            bar_df = pd.DataFrame(result, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'complete']).sort_values(by='time').reset_index(drop=True)

            latest_bar_time = parse_iso8601_date(bar_df.iloc[-1]['time'])  # Assuming the data is sorted in ascending order
            if datetime.utcnow() > latest_bar_time + timedelta(minutes=granularity_to_minutes(granularity)):

                logging.info("Latest bar is outdated. Fetching new data from API.")
                api_data = get_historical_data(count=count, granularity=granularity, access_token=access_token, instrument=instrument)
                save_historical_data(api_data, instrument, granularity)
                # Update the DataFrame with new data
                df_new_data = extract_bar_data(api_data)
                # Concatenate and sort if new data is not empty
                if not df_new_data.empty:
                    bar_df = pd.concat([bar_df, df_new_data], ignore_index=True).sort_values(by='time').reset_index(drop=True)

            return bar_df

        else:
            logging.warning(f"No data found for {instrument} with granularity {granularity}. Fetching from API.")
            api_data = get_historical_data(count, granularity, access_token, instrument)
            save_historical_data(api_data, instrument, granularity)
            bars_df = extract_bar_data(api_data)
            #save_historical_data(api_data, instrument, granularity)
            return bars_df


def get_instrument_value(currency_name, attribute):
    """
    Retrieve a specific attribute value for a given currency pair from the database.

    Args:
    currency_name (str): The name of the currency pair (e.g., 'EUR_USD').
    attribute (str): The attribute of the currency pair to retrieve (e.g., 'displayName').

    Returns:
    The value of the specified attribute for the currency pair, or None if not found.
    """
    with connect_to_db() as connection:
        # Prepare the query to retrieve the attribute for the specified currency pair
        # It's important to ensure that 'attribute' is a valid and safe string since it can't be parameterized
        # Consider whitelisting allowed attributes to prevent SQL injection
        # if attribute not in ['safe_attribute1', 'safe_attribute2', 'displayName']:  # Example whitelist
        #     logging.error(f"Invalid attribute name: {attribute}")
        #     return None

        query = f"SELECT {attribute} FROM instruments WHERE name = ?"

        # Execute the query
        result = execute_db_query(connection, query, (currency_name,), fetch_one=True)

        if result:
            logging.info(f"Retrieved {attribute} for {currency_name}: {result[0]}")
            return result[0]
        else:
            logging.warning(f"No data found for {currency_name} with attribute {attribute}.")
            return None


def get_instrument_list():
    """
    Retrieve a list of all currency pairs from the database.

    Returns:
    A list of currency pairs.
    """
    with connect_to_db() as connection:
        # Query to retrieve all currency pairs
        query = "SELECT name FROM instruments"

        # Execute the query
        result = execute_db_query(connection, query, fetch_all=True)

        if result:
            # Convert the result to a list of currency pairs
            instrument_list = [item[0] for item in result]
            # Uncomment the next line if you want to log the retrieved instrument list
            # logging.info(f"Retrieved instrument list: {instrument_list}")
            return instrument_list
        else:
            logging.warning("No data found for instrument list.")
            return None
