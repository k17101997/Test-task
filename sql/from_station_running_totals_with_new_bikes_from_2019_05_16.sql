with new_bike_rentals as (
    select bike_id
    from test.trips
    group by bike_id
    having min(start_time) >= to_timestamp('2019-05-16','yyyy-MM-dd')
),
running_totals as (
    select from_station_id, count(from_station_id) as count
    from test.trips
    join new_bike_rentals
    on trips.bike_id = new_bike_rentals.bike_id
    group by from_station_id
)
select id, name, count
from test.stations 
join running_totals
on stations.id = running_totals.from_station_id;