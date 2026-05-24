with source as (
    select * from {{ source('raw', 'raw_availability') }}
),

cleaned as (
    select
        stationcode::varchar                        as station_id,
        name::varchar                               as station_name,
        numdocksavailable::int                      as docks_available,
        numbikesavailable::int                      as bikes_available,
        mechanical::int                             as mechanical_bikes,
        ebike::int                                  as electric_bikes,
        is_installed::boolean                       as is_installed,
        is_renting::boolean                         as is_renting,
        is_returning::boolean                       as is_returning,
        cast(duedate as timestamp_tz)               as updated_at,
        cast(ingested_at as timestamp_tz)           as ingested_at,
        coordonnees_geo:lat::float                  as latitude,
        coordonnees_geo:lon::float                  as longitude
    from source
    where stationcode is not null
      and numbikesavailable is not null
)

select * from cleaned
qualify row_number() over (
    partition by station_id, ingested_at
    order by updated_at desc
) = 1
