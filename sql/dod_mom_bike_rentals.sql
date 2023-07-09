-- dod
with total_retals as (
    select date(start_time) start_date, count(start_time)
    from test.trips
    group by date(start_time)
),
previous_day_total_rentals as (
    select start_date date, 
        count current_day_total_bike_rentals, 
        lag(count, 1) over (order by start_date) previous_day_total_bike_rentals
    from total_retals
)
select date, 
    current_day_total_bike_rentals, 
    previous_day_total_bike_rentals,
    current_day_total_bike_rentals - previous_day_total_bike_rentals difference_with_previous_day,
    round((current_day_total_bike_rentals - previous_day_total_bike_rentals)*100/current_day_total_bike_rentals::numeric, 2) || '%' percent_difference_with_previous_day 
from previous_day_total_rentals;

-- mom
with count_retals as (
    select date_part('MONTH', date(start_time)) "month", count(start_time)
    from test.trips
    group by date_part('MONTH', date(start_time))
),
previous_month_total_retals as (
    select month, 
        count current_month_total_bike_rentals,
        lag(count, 1) over (order by month) previous_month_total_bike_rentals
    from count_retals
)
select month, 
current_month_total_bike_rentals, 
previous_month_total_bike_rentals,
current_month_total_bike_rentals - previous_month_total_bike_rentals difference_with_previous_month,
round((current_month_total_bike_rentals - previous_month_total_bike_rentals)*100/current_month_total_bike_rentals::numeric, 2) || '%' percent_difference_with_previous_month
from previous_month_total_retals;