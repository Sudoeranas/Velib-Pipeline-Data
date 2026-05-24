with source as (
    select * from {{ source('raw', 'raw_stations') }}
),

cleaned as (
    select
        stationcode::varchar                        as station_id,
        name::varchar                               as station_name,
        capacity::int                               as capacity,
        coordonnees_geo:lat::float                  as latitude,
        coordonnees_geo:lon::float                  as longitude,
        nom_arrondissement_communes::varchar        as city_district,
        cast(ingested_at as timestamp_tz)           as ingested_at
    from source
    where stationcode is not null
      and capacity > 0
)

select * from cleaned
qualify row_number() over (
    partition by station_id
    order by ingested_at desc
) = 1
