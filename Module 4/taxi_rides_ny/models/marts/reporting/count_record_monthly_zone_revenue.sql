-- Data mart for coubt of records for fct_monthly_zone_revenue data model

select count(*) as count from {{ ref('fct_monthly_zone_revenue') }}
