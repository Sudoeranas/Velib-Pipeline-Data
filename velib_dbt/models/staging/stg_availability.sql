with source as (
    select * from {{ source('raw', 'raw_availability') }}
),

flattened as (
    select
        f.value:stationcode::varchar                as station_id,
        f.value:name::varchar                       as station_name,
        f.value:numdocksavailable::int              as docks_available,
        f.value:numbikesavailable::int              as bikes_available,
        f.value:mechanical::int                     as mechanical_bikes,
        f.value:ebike::int                          as electric_bikes,
        f.value:is_installed::boolean               as is_installed,
        f.value:is_renting::boolean                 as is_renting,
        f.value:is_returning::boolean               as is_returning,
        f.value:duedate::timestamp_tz               as updated_at,
        f.value:coordonnees_geo:lat::float          as latitude,
        f.value:coordonnees_geo:lon::float          as longitude,
        source.data:ingested_at::timestamp_tz       as ingested_at
    from source,
    lateral flatten(input => source.data:records) f
    where f.value:stationcode is not null
      and f.value:numbikesavailable is not null
)

select * from flattened
qualify row_number() over (
    partition by station_id, ingested_at
    order by updated_at desc
) = 1
