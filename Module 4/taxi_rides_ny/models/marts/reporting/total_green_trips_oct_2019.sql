-- Data mart total number of trips for Green taxi service in Oct 2019

select 
    sum(total_monthly_trips) as total_trips
from {{ ref('fct_monthly_zone_revenue') }}
where 
    service_type = 'Green' and 
    extract(year from revenue_month) = 2019 and 
    extract(month from revenue_month) = 10
