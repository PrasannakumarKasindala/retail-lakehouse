select
    cast(order_id as integer)     as order_id,
    cast(customer_id as integer)  as customer_id,
    cast(order_ts as timestamp)   as order_ts,
    cast(status as varchar)       as status
from read_parquet('{{ var("serving_dir") }}/orders.parquet')
