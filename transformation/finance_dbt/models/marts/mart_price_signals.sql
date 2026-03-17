with rolling as (
    select * from {{ ref('int_rolling_averages') }}
),

returns as (
    select * from {{ ref('int_daily_returns') }}
),

signals as (
    select
        r.ticker,
        r.date,
        r.close_price,
        ro.ma_7d,
        ro.ma_30d,
        ro.avg_volume_30d,
        r.daily_return_pct,
        case
            when r.close_price > ro.ma_30d then 'above_30d_avg'
            when r.close_price < ro.ma_30d then 'below_30d_avg'
            else 'at_30d_avg'
        end as price_vs_ma30,
        case
            when abs(r.daily_return_pct) > 5 then 'large_move'
            when abs(r.daily_return_pct) > 2 then 'moderate_move'
            else 'normal'
        end as move_category
    from returns r
    join rolling ro on r.ticker = ro.ticker and r.date = ro.date
)

select * from signals
order by ticker, date