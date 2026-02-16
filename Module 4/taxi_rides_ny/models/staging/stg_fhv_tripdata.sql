with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(PULocationID as integer) as pickup_location_id,
        cast(DOLocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(Affiliated_base_number as integer) as affiliated_base_number,
        {{ safe_cast('SR_FLAG', 'integer') }} as sr_flag,

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed