with prices as (
    select * from {{ ref('stg_prices') }}
),

sectors as (
    select * from {{ ref('stg_sectors') }}
),

returns as (
    select * from {{ ref('int_daily_returns') }}
),

sector_metrics as (
    select
        s.sector,
        p.ticker,
        s.company,
        min(p.date)                                             as first_date,
        max(p.date)                                             as last_date,
        round(min(p.close_price), 2)                           as min_price,
        round(max(p.close_price), 2)                           as max_price,
        round(
            (max(p.close_price) - min(p.close_price))
            / min(p.close_price) * 100
        , 2)                                                    as total_return_pct,
        round(avg(p.volume) / 1000000.0, 2)                    as avg_daily_vol_millions,
        round(stddev(r.daily_return_pct)::numeric, 4)          as volatility,
        max(r.daily_return_pct)                                 as best_day_pct,
        min(r.daily_return_pct)                                 as worst_day_pct
    from prices p
    join sectors s on p.ticker = s.ticker
    join returns r on p.ticker = r.ticker and p.date = r.date
    group by s.sector, p.ticker, s.company
)

select
    sector,
    ticker,
    company,
    first_date,
    last_date,
    min_price,
    max_price,
    total_return_pct,
    avg_daily_vol_millions,
    volatility,
    best_day_pct,
    worst_day_pct,
    rank() over (partition by sector order by total_return_pct desc) as rank_in_sector
from sector_metrics
order by sector, rank_in_sector