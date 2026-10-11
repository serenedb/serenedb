{% snapshot customers_snapshot %}
{{ config(target_schema='snapshots', unique_key='id', strategy='check', check_cols=['score', 'tier']) }}
select id, name, score, tier from {{ ref('stg_customers') }}
{% endsnapshot %}
