import requests
import time
import datetime
import json

API_KEY = ''
API_SECRET = ''

base_url = 'https://fapi.binance.com'


# Function to fetch klines (candlestick data)
def get_klines(symbol, interval, start_time=None, end_time=None, limit=1000):
    url = f'{base_url}/fapi/v1/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit,
        'startTime': start_time,
        'endTime': end_time
    }
    response = requests.get(url, params=params)

    # Check for successful response
    if response.status_code != 200:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return []

    try:
        return response.json()
    except json.JSONDecodeError:
        print("Failed to parse JSON response")
        return []


# Function to collect data for specified date ranges
def get_data_for_intervals_by_week(symbol, interval, week_ranges):
    all_data = {}
    for start_time, end_time in week_ranges:
        # Convert timestamp to string for file naming
        start_str = datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d')
        end_str = datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d')

        print(f"Fetching M5 data from {start_str} to {end_str}")
        data = []
        current_time = start_time
        while current_time < end_time:
            batch = get_klines(symbol, interval, start_time=current_time, end_time=end_time)

            if len(batch) == 0:
                print(f"No data returned for M5 at time {current_time}")
                break  # Exit the loop if no data is returned

            data.extend(batch)
            current_time = batch[-1][6]  # Move to the next set of data based on the last candle's close time
            time.sleep(1)  # Pause to avoid rate limiting

        # Save the weekly data to a file
        if data:
            filename = f'btc_{interval}_{start_str}_to_{end_str}_data.json'
            with open(filename, 'w') as f:
                json.dump(data, f)
            print(f"Data for M5 from {start_str} to {end_str} saved successfully.")
        else:
            print(f"No data to save for M5 from {start_str} to {end_str}")


# Function to generate weekly ranges
def generate_week_ranges(start_date, end_date):
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    delta = datetime.timedelta(days=7)

    week_ranges = []
    while start_dt < end_dt:
        week_start = int(start_dt.timestamp() * 1000)  # Convert to milliseconds
        week_end = int((start_dt + delta).timestamp() * 1000)
        week_ranges.append((week_start, week_end))
        start_dt += delta

    return week_ranges


# Example of defining the date ranges
symbol = 'BTCUSDT'
interval = '5m'  # M5 interval

# Set the start and end dates for the data collection
week_ranges = generate_week_ranges("2020-01-01", "2024-10-18")  # Specify your date range

# Fetching the data
get_data_for_intervals_by_week(symbol, interval, week_ranges)

print("M5 data fetching process complete.")