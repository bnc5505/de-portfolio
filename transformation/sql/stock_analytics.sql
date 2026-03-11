-- Phase 1: Stock Market SQL Analytics


-- 1. Rolling 30-day average close price
SELECT
    ticker,
    date,
    close,
    ROUND(
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2
    ) AS rolling_30d_avg
FROM prices
ORDER BY ticker, date;


-- 2. Daily returns — flag big moves (>3% or <-3%)
WITH daily_returns AS (
    SELECT
        ticker,
        date,
        close,
        LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
        ROUND(
            (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date))
            / LAG(close) OVER (PARTITION BY ticker ORDER BY date) * 100
        , 2) AS return_pct
    FROM prices
)
SELECT
    ticker,
    date,
    close,
    return_pct
FROM daily_returns
WHERE return_pct > 3 OR return_pct < -3
ORDER BY ABS(return_pct) DESC;


-- 3. Index for performance optimisation
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices(ticker, date);

-- Check query plan (run separately to inspect)
EXPLAIN ANALYZE
SELECT * FROM prices
WHERE ticker = 'AMZN'
AND date BETWEEN '2025-01-01' AND '2025-12-31';


-- 4. Sector performance ranking
WITH sector_returns AS (
    SELECT
        p.ticker,
        s.sector,
        ROUND(
            (MAX(close) - MIN(close)) / MIN(close) * 100
        , 2) AS growth_pct
    FROM prices p
    JOIN sectors s ON p.ticker = s.ticker
    GROUP BY p.ticker, s.sector
)
SELECT
    sector,
    ticker,
    growth_pct,
    RANK() OVER (PARTITION BY sector ORDER BY growth_pct DESC) AS rank_in_sector
FROM sector_returns
ORDER BY sector, rank_in_sector;