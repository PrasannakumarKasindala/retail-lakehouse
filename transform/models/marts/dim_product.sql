select
    product_id as product_sk,
    product_id,
    name,
    category,
    unit_price
from {{ ref('stg_products') }}
