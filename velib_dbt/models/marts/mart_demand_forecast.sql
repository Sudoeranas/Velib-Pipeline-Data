with availability as (
    select * from {{ ref('stg_availability') }}
),

stations as (
    select * from {{ ref('stg_stations') }}
),

aggregated as (
    select
        a.station_id,
        s.station_name,
        s.city_district,
        s.capacity,
        s.latitude,
        s.longitude,
        dayofweek(a.ingested_at)                    as day_of_week,
        hour(a.ingested_at)                         as hour_of_day,
        avg(a.bikes_available)::float               as avg_bikes_available,
        avg(a.docks_available)::float               as avg_docks_available,
        stddev(a.bikes_available)::float            as stddev_bikes,
        count(*)                                    as observation_count,
        min(a.ingested_at)                          as first_seen,
        max(a.ingested_at)                          as last_seen
    from availability a
    left join stations s using (station_id)
    group by 1, 2, 3, 4, 5, 6, 7, 8
)

select * from aggregated
