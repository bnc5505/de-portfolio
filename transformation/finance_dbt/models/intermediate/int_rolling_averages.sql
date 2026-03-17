with prices as (
    select * from {{ ref('stg_prices') }}
),

rolling as (
    select
        ticker,
        date,
        close_price,
        volume,
        round(avg(close_price) over (
            partition by ticker
            order by date
            rows between 6 preceding and current row
        ), 2) as ma_7d,
        round(avg(close_price) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ), 2) as ma_30d,
        round(avg(volume) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ), 0) as avg_volume_30d
    from prices
)

select * from rolling