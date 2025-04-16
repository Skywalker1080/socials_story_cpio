

#@title Establish connection DB
import logging
from sqlalchemy import create_engine
import psycopg2
import sys
import logging
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

# 🔹 Force logging to print to the console
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# 🔹 Database Configuration
DB_CONFIG = {
    "host": "34.55.195.199",
    "user": "yogass09",
    "password": "jaimaakamakhya",
    "port": 5432,
    "database": "dbcp",
    "database_bt": "cp_backtest",
}

# 🔹 Create SQLAlchemy Engine (with Error Handling)
def create_db_engine():
    try:
        print("⏳ Connecting to the primary database...")  # Debugging
        logging.info("⏳ Connecting to the primary database...")

        engine = create_engine(
            f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
        )

        print("🔍 Testing connection...")
        with engine.connect() as conn:
            print("✅ Connection success!")  # Debugging
            logging.info("✅ Database connection established successfully!")

        return engine

    except Exception as e:
        print(f"❌ Exception occurred: {e}")  # Debugging
        logging.error(f"❌ Exception occurred: {e}", exc_info=True)
        return None


# 🔹 Create SQLAlchemy Engine for Backtesting Database
def create_db_engine_backtest():
    try:
        print("⏳ Connecting to the backtest database...")  # Debugging
        logging.info("⏳ Connecting to the backtest database...")

        engine = create_engine(
            f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database_bt"]}'
        )

        print("🔍 Testing connection...")
        with engine.connect() as conn:
            print("✅ Connection success!")  # Debugging
            logging.info("✅ Database connection established successfully!")

        return engine

    except Exception as e:
        print(f"❌ Exception occurred: {e}")  # Debugging
        logging.error(f"❌ Exception occurred: {e}", exc_info=True)
        return None


# 🔹 Test Connection
if __name__ == "__main__":
    print("🚀 Starting database connection test...")  # Debugging line

    engine = create_db_engine()
    engine_bt = create_db_engine_backtest()

    if engine:
        logging.info("✅ Primary database connection is ready to use.")
    else:
        logging.error("❌ Primary database connection failed.")

    if engine_bt:
        logging.info("✅ Backtest database connection is ready to use.")
    else:
        logging.error("❌ Backtest database connection failed.")

    print("✅ Database connection test completed.")  # Debugging line

#@title JSON for DB Schema
import pandas as pd
import json
import warnings
from sqlalchemy import create_engine

warnings.filterwarnings('ignore')


"""# CoinMarkCal Fetch [Airdrops]"""

#@title Events Fetch for category "8" [Airdrops]
import pandas as pd
import requests
from datetime import datetime, timedelta

# API endpoint and key
url = "https://developers.coinmarketcal.com/v1/events"
api_key = "dVliuLG3ZW21Le6whlTb79tp5XhwfxWi9ixTS8B5"

# Date range: today to today+7
today = datetime.today().date()
next_week = today + timedelta(days=7)

# Query parameters
querystring = {
    "page": 1,
    "max": 75,
    "dateRangeStart": str(today),
    "dateRangeEnd": str(next_week),
    "coins": "",
    "categories": "8",
    "sortBy": "influential_events",
    #"showOnly": "popular_events"
}

headers = {
    'x-api-key': api_key,
    'Accept-Encoding': "deflate, gzip",
    'Accept': "application/json"
}

# Fetch data safely
response = requests.get(url, headers=headers, params=querystring)

print("Status Code:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))
print("Raw Text Preview:", response.text[:300])

try:
    data = response.json()
except ValueError:
    print("Failed to decode JSON.")
    data = {}

# Proceed if valid data
if 'body' in data and isinstance(data['body'], list):
    df = pd.DataFrame(data['body'])

    # Explode and flatten 'coins'
    if 'coins' in df.columns:
        df = df.explode('coins', ignore_index=True)
        coins_df = pd.json_normalize(df['coins'], sep='_').add_prefix('coin_')
        df = df.drop(columns=['coins']).reset_index(drop=True)
        df = pd.concat([df, coins_df], axis=1)

    # Explode and flatten 'categories'
    if 'categories' in df.columns:
        df = df.explode('categories', ignore_index=True)
        cats_df = pd.json_normalize(df['categories'], sep='_').add_prefix('cat_')
        df = df.drop(columns=['categories']).reset_index(drop=True)
        df = pd.concat([df, cats_df], axis=1)

   #print(df.head())
else:
    print("No valid 'body' data found in response.")


df_filtered = df[df['cat_id'] == 8]

columns_to_keep = ['coin_id', 'coin_rank', 'coin_symbol','-','displayed_date',  'source', 'proof']
df_filtered = df_filtered[columns_to_keep]
df_filtered


# Rename columns
df_filtered = df_filtered.rename(columns={
    'coin_id': 'slug',
    'coin_rank': 'cmc_rank',
    'coin_symbol': 'symbol',
    '-': 'title',
    'displayed_date': 'event_date'
})


#@title SQL querry for fetchingh info for airdrop slugs
query ="""
SELECT
*
FROM
  "FE_CC_INFO_URL"
"""
cc_info= pd.read_sql_query(query, engine)


df_filtered = pd.merge(df_filtered, cc_info, on='slug', how='left')

# Keep only necessary columns
columns_to_keep = ['cmc_rank', 'slug', 'symbol', 'title', 'event_date', 'proof', 'logo']
df_filtered = df_filtered[columns_to_keep]



# Assuming df_filtered and engine are defined as in your provided code.

try:
    df_filtered.to_sql('NEWS_AIRDROPS_W', engine, if_exists='replace', index=False)
    print("✅ Data successfully pushed to NEWS_AIRDROPS_W table.")
except Exception as e:
    print(f"❌ An error occurred: {e}")
