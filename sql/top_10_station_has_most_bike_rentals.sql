-- from_station
with top_10_from_station as(
    select from_station_id, count(from_station_id) as count
    from test.trips
    group by from_station_id
    order by count desc
    limit 10
)
select id, name, count
from test.stations 
join top_10_from_station
on stations.id = top_10_from_station.from_station_id;

-- to_station
with top_10_to_station as(
    select to_station_id, count(to_station_id) as count
    from test.trips
    group by to_station_id
    order by count desc
    limit 10
)
select id, name, count
from test.stations 
join top_10_to_station
on stations.id = top_10_to_station.to_station_id;