with source as (
    select * from {{ source('raw', 'raw_stations') }}
),

flattened as (
    select
        f.value:stationcode::varchar                    as station_id,
        f.value:name::varchar                           as station_name,
        f.value:capacity::int                           as capacity,
        f.value:coordonnees_geo:lat::float              as latitude,
        f.value:coordonnees_geo:lon::float              as longitude,
        f.value:nom_arrondissement_communes::varchar    as city_district,
        source.data:ingested_at::timestamp_tz           as ingested_at
    from source,
    lateral flatten(input => source.data:records) f
    where f.value:stationcode is not null
      and f.value:capacity::int > 0
)

select * from flattened
qualify row_number() over (
    partition by station_id
    order by ingested_at desc
) = 1
