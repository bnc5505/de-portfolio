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
        volume,
        case when volume > 10000000 then true else false end as is_high_volume
    from source
)

select * from renamed