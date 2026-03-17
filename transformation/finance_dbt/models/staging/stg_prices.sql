with source as (
    select * from {{ source('finance', 'prices') }}
),

renamed as (
    select
        id,
        ticker,
        date,
        open        as open_price,
        high        as high_price,
        low         as low_price,
        close       as close_price,
        volume
    from source
)

select * from renamed