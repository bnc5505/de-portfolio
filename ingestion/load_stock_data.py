import os
import time
import yfinance as yf
import psycopg2

# Read credentials from environment variables
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", 5432),
    dbname=os.getenv("POSTGRES_DB", "finance"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "depassword")
)
cur = conn.cursor()

# Create tables if they don't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id      SERIAL PRIMARY KEY,
        ticker  VARCHAR(10) NOT NULL,
        date    DATE NOT NULL,
        open    NUMERIC(10,2),
        high    NUMERIC(10,2),
        low     NUMERIC(10,2),
        close   NUMERIC(10,2),
        volume  BIGINT,
        UNIQUE (ticker, date)
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS sectors (
        ticker  VARCHAR(10) PRIMARY KEY,
        company VARCHAR(100),
        sector  VARCHAR(50)
    )
""")

conn.commit()

# Sector reference data
sector_data = [
    ("JPM",  "JPMorgan Chase", "Fintech"),
    ("XYZ",  "Block Inc",      "Fintech"),
    ("UNH",  "UnitedHealth",   "Health-tech"),
    ("WMT",  "Walmart",        "Retail"),
    ("AMZN", "Amazon",         "Retail"),
]

cur.executemany(
    "INSERT INTO sectors (ticker, company, sector) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
    sector_data
)

# Load 2 years of daily prices
tickers = [row[0] for row in sector_data]

for ticker in tickers:
    hist = yf.Ticker(ticker).history(period="2y", auto_adjust=True)

    if hist.empty:
        print(f"Skipping {ticker} — no data returned")
        continue

    hist = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
    hist.columns = ["date", "open", "high", "low", "close", "volume"]
    hist["ticker"] = ticker
    hist["date"] = hist["date"].dt.tz_localize(None).dt.date

    for _, row in hist.iterrows():
        cur.execute("""
            INSERT INTO prices (ticker, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (row.ticker, row.date, row.open, row.high, row.low, row.close, row.volume))

    print(f"Loaded {len(hist)} rows for {ticker}")

conn.commit()
cur.close()
conn.close()
print("All done.")