{{ config(materialized='incremental', unique_key='id', incremental_strategy='delete+insert') }}
select c.id, n.shout, c.signup_date
from {{ ref('stg_customers') }} c
join {{ ref('customer_names') }} n on n.id = c.id
{% if is_incremental() %}
where c.signup_date >= (select max(signup_date) from {{ this }})
{% endif %}
