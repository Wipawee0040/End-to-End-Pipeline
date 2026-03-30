import pandas as pd
import yfinance as yf
import psycopg2

symbol_list = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA',]

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="stock_data",
    user="postgres",
    password="1234"
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        date DATE,
        symbol VARCHAR(10),
        open_price NUMERIC,
        high_price NUMERIC,
        low_price NUMERIC,
        close_price NUMERIC,
        volume BIGINT,
        PRIMARY KEY (date, symbol)
    )
""")

conn.commit()

for symbol in symbol_list:
    print(f"Downloading data for {symbol}...")
    data = yf.download(symbol, start='2025-01-01', end='2026-01-01')
    
    if data.empty:
        print(f"No data found for {symbol}")
        continue
    
    data = data.reset_index()
    
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]

    print(data.columns)
    
    for index, row in data.iterrows():
        cur.execute("""
            INSERT INTO stock_prices (
                date, symbol, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, symbol) DO NOTHING
        """, (row['Date'], 
              symbol, 
              row['Open'], 
              row['High'], 
              row['Low'], 
              row['Close'], 
              int(row['Volume']) if pd.notna(row['Volume']) else None))
        
    conn.commit()
    print(f"Data for {symbol} loaded to Postgres.")   

cur.close()
conn.close()

print("Success! Data for 7 stocks loaded to Postgres.")