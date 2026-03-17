with prices as (
    select * from {{ ref('stg_prices') }}
),

returns as (
    select * from {{ ref('int_daily_returns') }}
),

monthly as (
    select
        p.ticker,
        date_trunc('month', p.date) as month,
        round(avg(p.close_price), 2)     as avg_monthly_close,
        sum(p.volume)                    as total_monthly_volume,
        max(r.daily_return_pct)          as best_day_return,
        min(r.daily_return_pct)          as worst_day_return
    from prices p
    join returns r on p.ticker = r.ticker and p.date = r.date
    group by p.ticker, date_trunc('month', p.date)
)

select * from monthly
order by ticker, month