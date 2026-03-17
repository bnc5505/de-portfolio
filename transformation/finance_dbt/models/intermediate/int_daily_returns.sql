with prices as (
    select * from {{ ref('stg_prices') }}
),

returns as (
    select
        ticker,
        date,
        close_price,
        lag(close_price) over (
            partition by ticker
            order by date
        ) as prev_close_price,
        round(
            (close_price - lag(close_price) over (
                partition by ticker order by date
            )) / lag(close_price) over (
                partition by ticker order by date
            ) * 100
        , 2) as daily_return_pct
    from prices
)

select * from returns
where prev_close_price is not null