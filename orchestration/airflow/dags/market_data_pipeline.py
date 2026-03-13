import os
from datetime import datetime, timedelta

import yfinance as yf
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "bhargav",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB", "finance"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "depassword"),
    )


def pull_market_data(**context):
    tickers = ["JPM", "XYZ", "UNH", "WMT", "AMZN"]
    conn = get_conn()
    cur = conn.cursor()

    for ticker in tickers:
        hist = yf.Ticker(ticker).history(period="5d", auto_adjust=True)

        if hist.empty:
            print(f"No data for {ticker} — skipping")
            continue

        hist = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
        hist.columns = ["date", "open", "high", "low", "close", "volume"]
        hist["ticker"] = ticker
        hist["date"] = hist["date"].dt.tz_localize(None).dt.date

        for _, row in hist.iterrows():
            cur.execute("""
                INSERT INTO prices (ticker, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO NOTHING
            """, (row.ticker, row.date, row.open, row.high,
                  row.low, row.close, row.volume))

        print(f"Processed {ticker}")

    conn.commit()
    cur.close()
    conn.close()
    print("Pull complete.")


def run_quality_check(**context):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT ticker, COUNT(*) as row_count
        FROM prices
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY ticker
    """)
    results = cur.fetchall()

    cur.close()
    conn.close()

    print("Quality check results (last 7 days):")
    for ticker, count in results:
        print(f"  {ticker}: {count} rows")

    if len(results) < 5:
        raise ValueError(
            f"Quality check failed — expected 5 tickers, got {len(results)}"
        )

    print("Quality check passed.")


with DAG(
    dag_id="market_data_pipeline",
    description="Daily market data ingestion for Fintech portfolio",
    start_date=datetime(2024, 1, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    default_args=default_args,
    tags=["finance", "ingestion"],
) as dag:

    task_pull = PythonOperator(
        task_id="pull_market_data",
        python_callable=pull_market_data,
    )

    task_quality = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_check,
    )

    task_pull >> task_quality