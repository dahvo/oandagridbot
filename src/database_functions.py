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
        connection.commit()
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
        # logging.info(f"Database query: {query} with parameters: {parameters} as a fetch_one query.")
        return cursor.fetchone()
    if fetch_all:
        # logging.info(f"Database query: {query} with parameters: {parameters} as a fetch_all query.")
        return cursor.fetchall()


def set_instruments_table(data):
    # data = get_account_instruments(accountID, access_token)
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
                guaranteedStopLossOrderMode TEXT,
                last_updated TIMESTAMP
            )
        '''
        execute_db_query(connection, create_table_query)

        # Iterate through each item in the data and insert or update the record
        for item in data:
            upsert_query = '''
                INSERT INTO instruments (name, type, displayName, pipLocation, displayPrecision, tradeUnitsPrecision,
                minimumTradeSize, maximumTrailingStopDistance, minimumTrailingStopDistance, maximumPositionSize,
                maximumOrderUnits, marginRate, guaranteedStopLossOrderMode, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                type = excluded.type, displayName = excluded.displayName, pipLocation = excluded.pipLocation,
                displayPrecision = excluded.displayPrecision, tradeUnitsPrecision = excluded.tradeUnitsPrecision,
                minimumTradeSize = excluded.minimumTradeSize, maximumTrailingStopDistance = excluded.maximumTrailingStopDistance,
                minimumTrailingStopDistance = excluded.minimumTrailingStopDistance, maximumPositionSize = excluded.maximumPositionSize,
                maximumOrderUnits = excluded.maximumOrderUnits, marginRate = excluded.marginRate,
                guaranteedStopLossOrderMode = excluded.guaranteedStopLossOrderMode, last_updated = excluded.last_updated
            '''
            execute_db_query(connection, upsert_query, (
                item['name'], item['type'], item['displayName'], item['pipLocation'],
                item['displayPrecision'], item['tradeUnitsPrecision'], item['minimumTradeSize'],
                item['maximumTrailingStopDistance'], item['minimumTrailingStopDistance'],
                item['maximumPositionSize'], item['maximumOrderUnits'], item['marginRate'],
                item['guaranteedStopLossOrderMode'], datetime.now()
            ))


def extract_bar_data(data):
    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    # Extract the 'mid' column into separate 'open', 'high', 'low', and 'close' columns
    df[['open', 'high', 'low', 'close']] = df['mid'].apply(
        lambda x: pd.Series([x['o'], x['h'], x['l'], x['c']]).astype(float))

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

        # logging.info("History tables created.")


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


def log_order(order_response):
    logging.info(f"Order response: {order_response}")
    with connect_to_db() as connection:
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                instrument TEXT,
                units INTEGER,
                time TEXT,
                price REAL,
                type TEXT,
                side TEXT,
                stop_loss REAL,
                take_profit REAL,
                expiry TEXT,
                lower_bound REAL,
                upper_bound REAL,
                trailing_stop REAL
            )'''
        # Prepare the insert query
        insert_order_query = '''
            INSERT INTO orders (id, instrument, units, time, price, type, side, stop_loss, take_profit, expiry, lower_bound, upper_bound, trailing_stop)
        '''


# Helper function to parse ISO 8601 formatted date
def parse_iso8601_date(iso_date):
    """Parse an ISO 8601 formatted date."""
    return datetime.strptime(iso_date.split('.')[0], '%Y-%m-%dT%H:%M:%S')


# Helper function to calculate expected bars based on count and granularity
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

def fetch_historical_data(instrument, granularity, count, access_token):
    ensure_bars_tables_exists()
    with connect_to_db() as connection:
        # Attempt to fetch the latest available data from the database
        latest_data_query = '''
            SELECT time
            FROM bars
            WHERE instrument_name = ? AND granularity_name = ?
            ORDER BY time DESC
            LIMIT 1
        '''
        latest_result = execute_db_query(connection, latest_data_query, (instrument, granularity), fetch_one=True)
        latest_bar_time = parse_iso8601_date(latest_result[0]) if latest_result else datetime.utcnow()

        # Calculate the start date for the data request
        start_date = calculate_start_date_from_count(latest_bar_time, count, granularity)

        # Modify the fetch_query to select data after the calculated start date
        fetch_query = '''
            SELECT time, open, high, low, close, volume, complete
            FROM bars
            WHERE instrument_name = ? AND granularity_name = ? AND time >= ?
            ORDER BY time DESC
        '''
        result = execute_db_query(connection, fetch_query, (instrument, granularity, start_date.isoformat()), fetch_all=True)

        if result:
            if len(result) < count:
                logging.warning(f"Insufficient data found for {instrument} at {granularity} granularity. Fetching from API.")
                api_data = get_historical_data(count - len(result), granularity, access_token, instrument, start_date=start_date)
                save_historical_data(api_data, instrument, granularity)
                result.extend(extract_bar_data(api_data))

            bar_df = pd.DataFrame(result, columns=['time', 'open', 'high', 'low', 'close', 'volume', 'complete']).sort_values(by='time').reset_index(drop=True)
            bar_df[['open', 'high', 'low', 'close']] = bar_df[['open', 'high', 'low', 'close']].astype(float)
            return bar_df

        else:
            logging.warning(f"No data found for {instrument} with granularity {granularity}. Fetching from API.")
            start_date = calculate_start_date_from_count(latest_bar_time, count, granularity)
            api_data = get_historical_data(start_date=start_date, granularity=granularity, access_token=access_token, instrument=instrument)
            save_historical_data(api_data, instrument, granularity)
            bars_df = extract_bar_data(api_data)
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
            return instrument_list
        else:
            logging.warning("No data found for instrument list.")
            return None
