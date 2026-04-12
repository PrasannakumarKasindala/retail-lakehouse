-- SCD2 customer dimension. The version hash (dbt_scd_id) is the surrogate key,
-- so every historical version has a stable, unique key a fact can point at.
with snap as (select * from {{ ref('customer_snapshot') }})
select
    dbt_scd_id                    as customer_sk,
    customer_id,
    name,
    segment,
    city,
    dbt_valid_from                as valid_from,
    dbt_valid_to                  as valid_to,
    (dbt_valid_to is null)        as is_current
from snap
