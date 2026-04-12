select
    cast(order_item_id as integer) as order_item_id,
    cast(order_id as integer)      as order_id,
    cast(product_id as integer)    as product_id,
    cast(quantity as integer)      as quantity,
    cast(unit_price as decimal(12,2)) as unit_price
from read_parquet('{{ var("serving_dir") }}/order_items.parquet')
