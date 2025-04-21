def fetch_tokenomics_news():
    """
    Fetches tokenomics news from CoinMarketCal API, processes it, and returns filtered dataframe
    with only the specified columns. Also prints the first few rows of the result.
    
    Returns:
        DataFrame: Processed tokenomics news data with only the requested columns
    """
    import pandas as pd
    import requests
    from datetime import datetime, timedelta
    import re
    import google.generativeai as genai
    import os
    from sqlalchemy import create_engine
    
    # Database Configuration
    DB_CONFIG = {
        "host": "34.55.195.199",
        "user": "yogass09",
        "password": "jaimaakamakhya",
        "port": 5432,
        "database": "dbcp",
        "database_bt": "cp_backtest",
    }
    
    # Create database engine
    def create_db_engine():
        try:
            engine = create_engine(
                f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}'
            )
            with engine.connect():
                pass
            return engine
        except Exception as e:
            print(f"❌ Database connection error: {e}")
            return None
    
    # Initialize database engine
    engine = create_db_engine()
    if not engine:
        return pd.DataFrame()  # Return empty dataframe if connection fails
    
    # CoinMarketCal API configuration
    url = "https://developers.coinmarketcal.com/v1/events"
    api_key = "dVliuLG3ZW21Le6whlTb79tp5XhwfxWi9ixTS8B5"
    
    # Set date range: today to today+7
    today = datetime.today().date()
    next_week = today + timedelta(days=7)
    
    # Query parameters for API
    querystring = {
        "page": 1,
        "max": 75,
        "dateRangeStart": str(today),
        "dateRangeEnd": str(next_week),
        "coins": "",
        "categories": "3",  # Tokenomics category
        "sortBy": "influential_events"
    }
    
    headers = {
        'x-api-key': api_key,
        'Accept-Encoding': "deflate, gzip",
        'Accept': "application/json"
    }
    
    # Fetch data from API
    try:
        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
    except Exception as e:
        print(f"❌ API request failed: {e}")
        return pd.DataFrame()
    
    # Process API response data
    if 'body' in data and isinstance(data['body'], list):
        df = pd.DataFrame(data['body'])
        
        # Process 'coins' field
        if 'coins' in df.columns:
            df = df.explode('coins', ignore_index=True)
            coins_df = pd.json_normalize(df['coins'], sep='_').add_prefix('coin_')
            df = df.drop(columns=['coins']).reset_index(drop=True)
            df = pd.concat([df, coins_df], axis=1)
        
        # Process 'categories' field
        if 'categories' in df.columns:
            df = df.explode('categories', ignore_index=True)
            cats_df = pd.json_normalize(df['categories'], sep='_').add_prefix('cat_')
            df = df.drop(columns=['categories']).reset_index(drop=True)
            df = pd.concat([df, cats_df], axis=1)
        
        print("📋 Columns in df:", df.columns.tolist())
        print("🔍 Sample row:\n", df.head(1).T)

        # Filter for tokenomics category (id=3)
        df_filtered = df[df['cat_id'] == 3]
        
        # Select and rename columns
        columns_to_keep = ['coin_id', 'coin_name', 'coin_rank', 'coin_symbol', '-', 'displayed_date', 'source', 'proof']
        df_filtered = df_filtered[columns_to_keep]
        df_filtered = df_filtered.rename(columns={
            'coin_id': 'slug',
            'coin_name': 'name',
            'coin_rank': 'cmc_rank',
            'coin_symbol': 'symbol',
            '-': 'title',
            'displayed_date': 'event_date'
        })
        
        # Filter for coins with CMC rank < 500
        df_filtered = df_filtered[df_filtered['cmc_rank'] < 500]
        
        # Get additional coin info from database
        query = """
        SELECT
            slug,
            name,
            logo,
            description
        FROM
            "FE_CC_INFO_URL"
        """
        cc_info = pd.read_sql_query(query, engine)
        
        # Merge with coin info
        df_filtered = pd.merge(df_filtered, cc_info, on='slug', how='left')
        
        # Get list of slugs for market data query
        slugs = df_filtered['slug'].tolist()
        
        # Fetch market data for these coins
        query = """
        SELECT
            slug,
            name,
            cmc_rank,
            market_cap,
            percent_change30d,
            circulating_supply,
            percent_change1h,
            percent_change24h,
            percent_change7d
        FROM
            "crypto_listings_latest_1000"
        """
        market_data = pd.read_sql_query(query, engine)
        
        # Merge with market data
        df_filtered = pd.merge(df_filtered, market_data, on='slug', how='left')
        
        # Generate AI analysis if there are any rows
        if not df_filtered.empty:
            # Configure Google Generative AI
            os.environ["GEMINI_API_KEY"] = "AIzaSyCrlt6acTeNnUHNvm71g1tFpzWBu_qsp-Y"
            genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
            
            # Model configuration for analysis
            MODEL_NAME = "gemini-1.5-flash"
            SYSTEM_INSTRUCTION = """You analyze token unlock events. For any given token data:
            1. Calculate Unlock Impact Ratio = upcoming_unlock / circulating_supply (respond in M units or units of %)
            2. Calculate Market Cap to Unlock = market_cap / upcoming_unlock (respond in M units or units of %)
            3. Calculate Price Momentum = percent_change7d + percent_change30d (respond in M units or units of %)
            4. Estimate Volatility Score from price changes (respond in M units or units of %)
            
            Always return concise analysis (under 80 words) between // markers like:
            //Your analysis here//"""
            
            # Function to generate analysis for a token
            def generate_analysis(token_data):
                model = genai.GenerativeModel(
                    MODEL_NAME,
                    system_instruction=SYSTEM_INSTRUCTION
                )
                response = model.generate_content(token_data)
                return response.text
            
            # Function to extract analysis text between markers
            def extract_analysis(text):
                matches = re.findall(r"//(.*?)//", text, re.DOTALL)
                return matches[0].strip() if matches else None
            
            # Example token for analysis - this would be replaced with actual data in a loop
            token_data = """Token: Official Trump
            Rank: 55
            Symbol: TRUMP
            Market Cap: 1539478818.6290417
            Event Title: 40MM Token Unlock
            Event Date: 18 Apr 2025
            30-Day Price Change: -31.78468633
            Circulating Supply: 199999416.617915
            1-Hour Price Change: -0.36017265
            24-Hour Price Change: -1.62150844
            7-Day Price Change: -5.00083276"""
            
            # Generate and extract analysis
            analysis = generate_analysis(token_data)
            analysis_text = extract_analysis(analysis)
            
            # Add analysis to dataframe
            df_filtered["analysis"] = analysis_text
            
            # Optionally save to database
            try:
                df_filtered.to_sql('NEWS_TOKENOMICS_W', engine, if_exists='replace', index=False)
                print("✅ Data successfully pushed to NEWS_TOKENOMICS_W table.")
            except Exception as e:
                print(f"❌ Failed to save to database: {e}")
                
            # Select only the requested columns for the final dataframe
            final_columns = [
                'symbol',
                'title',
                'event_date',
                'proof',
                'logo',
                'name_y',
                'cmc_rank_y',
                'analysis'
            ]
            
            # Create final dataframe with only selected columns
            result_df = df_filtered[final_columns]
            
            # Print the head of the dataframe
            print("\n✅ Tokenomics News Data:")
            print(result_df.head())
            
            # Return the filtered dataframe
            return result_df
    else:
        print("No valid 'body' data found in API response.")
        return pd.DataFrame()
    
    # Return an empty dataframe if we get here (shouldn't happen with proper data)
    return pd.DataFrame()

df = fetch_tokenomics_news()
