{{ config(materialized='ephemeral') }}
select id, upper(name) as shout
from {{ ref('stg_customers') }}
