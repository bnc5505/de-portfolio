with source as (
    select * from {{ source('finance', 'sectors') }}
),

renamed as (
    select
        ticker,
        company,
        sector
    from source
)

select * from renamed