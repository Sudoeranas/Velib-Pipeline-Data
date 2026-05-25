with source as (
    select * from {{ source('raw', 'raw_stations') }}
),

availability_source as (
    select * from {{ source('raw', 'raw_availability') }}
),

flattened as (
    select
        f.value:stationcode::varchar                    as station_id,
        f.value:name::varchar                           as station_name,
        f.value:capacity::int                           as capacity,
        f.value:coordonnees_geo:lat::float              as latitude,
        f.value:coordonnees_geo:lon::float              as longitude,
        source.data:ingested_at::timestamp_tz           as ingested_at
    from source,
    lateral flatten(input => source.data:records) f
    where f.value:stationcode is not null
      and f.value:capacity::int > 0
),

district_lookup as (
    select
        f.value:stationcode::varchar                        as station_id,
        f.value:nom_arrondissement_communes::varchar        as city_district
    from availability_source,
    lateral flatten(input => availability_source.data:records) f
    qualify row_number() over (
        partition by station_id
        order by availability_source.ingested_at desc
    ) = 1
),

stations as (
    select * from flattened
    qualify row_number() over (
        partition by station_id
        order by ingested_at desc
    ) = 1
)

select
    s.station_id,
    s.station_name,
    s.capacity,
    s.latitude,
    s.longitude,
    d.city_district,
    s.ingested_at
from stations s
left join district_lookup d on s.station_id = d.station_id
