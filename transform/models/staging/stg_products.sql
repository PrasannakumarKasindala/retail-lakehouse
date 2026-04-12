select
    cast(product_id as integer)   as product_id,
    cast(name as varchar)         as name,
    cast(category as varchar)     as category,
    cast(unit_price as decimal(12,2)) as unit_price
from read_parquet('{{ var("serving_dir") }}/products.parquet')
