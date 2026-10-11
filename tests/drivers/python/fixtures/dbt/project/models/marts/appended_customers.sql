{{ config(materialized='incremental', incremental_strategy='append') }}
select id, name from {{ ref('stg_customers') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
