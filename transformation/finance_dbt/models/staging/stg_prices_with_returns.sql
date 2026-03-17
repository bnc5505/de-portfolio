with prices as (
    select * from {{ ref('stg_prices') }}
),

returns as (
    select * from {{ ref('int_daily_returns') }}
),

joined as (
    select
        p.ticker,
        p.date,
        p.close_price,
        p.volume,
        p.is_high_volume,
        r.daily_return_pct
    from prices p
    join returns r on p.ticker = r.ticker and p.date = r.date
)

select * from joined