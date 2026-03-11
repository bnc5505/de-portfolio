import yfinance as yf
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="finance",
    user="postgres",
    password="depassword"
)
cur = conn.cursor()

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

tickers = [row[0] for row in sector_data]

for ticker in tickers:
    hist = yf.Ticker(ticker).history(period="2y", auto_adjust=True)

    if hist.empty:
        print(f"Skipping {ticker} — no data returned")
        continue

    hist = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
    hist.columns = ["date", "open", "high", "low", "close", "volume"]
    hist["ticker"] = ticker

    # Strip timezone then convert to date
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
print("Done.")