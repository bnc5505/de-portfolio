with weekly_totals as (
    select
        ticker,
        date_trunc('week', date) as week_start,
        avg(close_price)         as avg_close_price,
        sum(volume)              as total_volume
    from {{ ref('stg_prices') }}
    group by ticker, date_trunc('week', date)
),

weekly_with_rolling as (
    select
        ticker,
        week_start,
        avg_close_price,
        total_volume,
        avg(total_volume) over (
            partition by ticker
            order by week_start
            rows between 3 preceding and current row
        ) as rolling_4wk_avg_volume
    from weekly_totals
)

select
    ticker,
    week_start,
    avg_close_price,
    total_volume,
    rolling_4wk_avg_volume,
    case
        when total_volume > rolling_4wk_avg_volume * 1.10 then 'accumulation'
        when total_volume < rolling_4wk_avg_volume * 0.90 then 'distribution'
        else 'neutral'
    end as signal
from weekly_with_rolling
order by ticker, week_start
