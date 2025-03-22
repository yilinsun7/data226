# Importing 
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

# Defining snowflake connection 
def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task 
def extract(apikey, num_of_days, stock_symbol):

	# Define API endpoint
	API_URL = "https://www.alphavantage.co/query"

	# Last 180 days date range
	today = datetime.today()  
	start_date = today - timedelta(days=num_of_days)

	# API request parameters
	params = {
		"function": "TIME_SERIES_DAILY",
		"symbol": stock_symbol,
		"start_date": start_date.strftime("%Y-%m-%d"), "end_date": today.strftime("%Y-%m-%d"), "interval": "daily",
		"apikey": apikey 
		}

	# Send API request
	response = requests.get(API_URL, params=params)


	# Extract data
	if response.status_code == 200: 
		data = response.json() # Convert response to JSON print("Data retrieved successfully!")
		return data 

	else:
		print(f"Error: {response.status_code}, Message: {response.text}")



@task 
def transform(input_data, num_of_days, stock_symbol):
	time_series = input_data.get("Time Series (Daily)", {})

	# Initialize 
	stock_data = []
	today = datetime.today()
	start_date = today - timedelta(days=num_of_days)   # Get last n days

	# Populate 
	for date, values in time_series.items():
		if datetime.strptime(date, "%Y-%m-%d").date() >= start_date.date():
			stock_data.append({
				"symbol": stock_symbol,
				"date": date,
				"open": float(values["1. open"]), "close": float(values["4. close"]), "high": float(values["2. high"]), "low": float(values["3. low"]), "volume": int(values["5. volume"])
				})

	return stock_data



@task 
def load(cursor, target_table, stock_data_input):
	try: 
		cursor.execute("BEGIN;")
		cursor.execute(f"""
			CREATE OR REPLACE TABLE {target_table} ( 
			symbol STRING,
			date DATE,
			open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume BIGINT,
            PRIMARY KEY (symbol, date)
            );
			""")
		cursor.execute(f"""DELETE FROM {target_table}""")

		for item in stock_data_input:
			# Get all data 
			symbol = item['symbol']
			date = item['date']
			open = item['open']
			close = item['close']
			high = item['high']
			low = item['low']
			volume = item['volume']

			# Insert 
			sql = f"INSERT INTO {target_table} (symbol, date, open, close, high, low, volume) VALUES ('{symbol}', '{date}', {open}, {close}, {high}, {low}, {volume})"
			cursor.execute(sql)

		cursor.execute("COMMIT;")

	except Exception as e:
		cursor.execute("ROLLBACK;") 
		print(e)
		raise(e)



# Connect to snowflake
cursor = return_snowflake_conn()

# Get API key from airflow 
api_key = Variable.get("api_key")

stock_symbol = "AAPL"
num_of_days = 180
target_table = "dev.raw.lab1_stock_data"


with DAG(
    dag_id = 'hw5task',
    start_date = datetime(2025,3,20),
    catchup=False,
    tags=['hw5', 'ELT'],
    schedule = '10 * * * *'
) as dag:

    data = extract(api_key, num_of_days, stock_symbol)
    transformed_data = transform(data, num_of_days, stock_symbol)
    load_task = load(cursor, target_table, transformed_data)

    # Dependency 
    data >> transformed_data >> load_task 