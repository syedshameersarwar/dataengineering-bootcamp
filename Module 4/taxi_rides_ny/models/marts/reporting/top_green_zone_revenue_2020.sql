-- Data mart for best performing zone for green taxis in year 2020
-- Enables analysis of most profitable zone for green taxis in 2020

select 
    pickup_zone, 
    sum(revenue_monthly_total_amount) as total_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where 
    service_type = 'Green' and 
    extract(year from revenue_month) = 2020
group by pickup_zone
qualify ROW_NUMBER() over (order by total_revenue desc) = 1
