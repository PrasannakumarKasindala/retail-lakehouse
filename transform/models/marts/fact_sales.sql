-- Sales fact at order-line grain. Customer is resolved point-in-time against
-- the SCD2 dimension: each line joins to the customer version that was valid at
-- the order timestamp, not the current version.
with items as (
    select
        oi.order_item_id, oi.order_id, oi.product_id, oi.quantity, oi.unit_price,
        o.customer_id, o.order_ts, o.status
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
),
cust as (select * from {{ ref('dim_customer') }})
select
    i.order_item_id,
    i.order_id,
    c.customer_sk,
    i.product_id                                    as product_sk,
    cast(i.order_ts as date)                        as date_day,
    i.quantity,
    i.unit_price,
    cast(i.quantity * i.unit_price as decimal(14,2)) as amount,
    i.status
from items i
left join cust c
    on  c.customer_id = i.customer_id
    and i.order_ts >= c.valid_from
    and i.order_ts <  c.valid_to
