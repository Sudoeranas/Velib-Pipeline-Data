with availability as (
    select * from {{ ref('stg_availability') }}
),

stations as (
    select * from {{ ref('stg_stations') }}
),

hourly as (
    select
        a.station_id,
        s.station_name,
        s.city_district,
        s.capacity,
        s.latitude,
        s.longitude,
        date_trunc('hour', a.ingested_at)           as hour_bucket,
        avg(a.bikes_available)::int                 as avg_bikes_available,
        avg(a.docks_available)::int                 as avg_docks_available,
        avg(a.mechanical_bikes)::int                as avg_mechanical,
        avg(a.electric_bikes)::int                  as avg_electric,
        min(a.bikes_available)                      as min_bikes,
        max(a.bikes_available)                      as max_bikes,
        count(*)                                    as sample_count
    from availability a
    left join stations s using (station_id)
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from hourly
