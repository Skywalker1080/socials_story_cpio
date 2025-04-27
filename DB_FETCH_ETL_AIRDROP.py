def fetch_and_push_airdrop_events():
    import logging
    import sys
    import json
    import pandas as pd
    import requests
    import warnings
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine

    warnings.filterwarnings('ignore')

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # DB Config
    DB_CONFIG = {
        "host": "34.55.195.199",
        "user": "yogass09",
        "password": "jaimaakamakhya",
        "port": 5432,
        "database": "dbcp"
    }

    def create_db_engine():
        try:
            logging.info("⏳ Connecting to the primary database...")
            engine = create_engine(
                f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
            )
            with engine.connect():
                logging.info("✅ Database connection established successfully!")
            return engine
        except Exception as e:
            logging.error(f"❌ Exception occurred: {e}", exc_info=True)
            return None

    engine = create_db_engine()
    if engine is None:
        return

    # API config
    url = "https://developers.coinmarketcal.com/v1/events"
    api_key = "dVliuLG3ZW21Le6whlTb79tp5XhwfxWi9ixTS8B5"
    today = datetime.today().date()
    next_week = today + timedelta(days=7)

    querystring = {
        "page": 1,
        "max": 75,
        "dateRangeStart": str(today),
        "dateRangeEnd": str(next_week),
        "coins": "",
        "categories": "8",
        "sortBy": "influential_events"
    }

    headers = {
        'x-api-key': api_key,
        'Accept-Encoding': "deflate, gzip",
        'Accept': "application/json"
    }

    response = requests.get(url, headers=headers, params=querystring)
    logging.info(f"API Status Code: {response.status_code}")

    try:
        data = response.json()
    except ValueError:
        logging.error("Failed to decode JSON.")
        return

    if 'body' not in data or not isinstance(data['body'], list):
        logging.error("No valid 'body' data found in response.")
        return

    df = pd.DataFrame(data['body'])

    if 'coins' in df.columns:
        df = df.explode('coins', ignore_index=True)
        coins_df = pd.json_normalize(df['coins'], sep='_').add_prefix('coin_')
        df = df.drop(columns=['coins']).reset_index(drop=True)
        df = pd.concat([df, coins_df], axis=1)

    if 'categories' in df.columns:
        df = df.explode('categories', ignore_index=True)
        cats_df = pd.json_normalize(df['categories'], sep='_').add_prefix('cat_')
        df = df.drop(columns=['categories']).reset_index(drop=True)
        df = pd.concat([df, cats_df], axis=1)

    df_filtered = df[df['cat_id'] == 8]

    columns_to_keep = ['coin_id', 'coin_rank', 'coin_symbol','-','displayed_date',  'source', 'proof']
    df_filtered = df_filtered[columns_to_keep]

    df_filtered = df_filtered.rename(columns={
        'coin_id': 'slug',
        'coin_rank': 'cmc_rank',
        'coin_symbol': 'symbol',
        '-': 'title',
        'displayed_date': 'event_date'
    })

    query = """
        SELECT * FROM "FE_CC_INFO_URL"
    """

    cc_info = pd.read_sql_query(query, engine)
    # Merge the data
    df_filtered = pd.merge(df_filtered, cc_info, on='slug', how='left')

    # Filter rows where cmc_rank < 500
    df_filtered = df_filtered[df_filtered['cmc_rank'] < 500]

    final_columns = ['cmc_rank', 'slug', 'symbol', 'title', 'event_date', 'proof', 'logo']
    df_filtered = df_filtered[final_columns]

    try:
        df_filtered.to_sql('NEWS_AIRDROPS_W', engine, if_exists='replace', index=False)
        logging.info("✅ Data successfully pushed to NEWS_AIRDROPS_W table.")
        return df_filtered
    except Exception as e:
        logging.error(f"❌ An error occurred while writing to DB: {e}")
