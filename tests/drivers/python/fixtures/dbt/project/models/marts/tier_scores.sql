{{ config(materialized='table') }}
select tier, count(*) as customers, sum(score) as total_score, max(signup_date) as last_signup
from {{ ref('stg_customers') }}
group by tier
