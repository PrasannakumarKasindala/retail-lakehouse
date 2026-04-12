-- SCD2 on customers: dbt tracks version history using the source change
-- timestamp. Each run closes rows whose updated_at advanced and opens a new
-- version, populating dbt_valid_from / dbt_valid_to.
{% snapshot customer_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}
select customer_id, name, segment, city, updated_at
from {{ ref('stg_customers') }}
{% endsnapshot %}
