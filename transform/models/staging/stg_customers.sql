-- Current silver snapshot of customers (natural key + change timestamp).
select
    cast(customer_id as integer)      as customer_id,
    cast(name as varchar)             as name,
    cast(segment as varchar)          as segment,
    cast(city as varchar)             as city,
    cast(updated_at as timestamp)     as updated_at
from read_parquet('{{ var("serving_dir") }}/customers.parquet')
