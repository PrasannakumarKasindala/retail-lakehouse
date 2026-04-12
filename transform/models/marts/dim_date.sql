with bounds as (
    select cast(min(order_ts) as date) as lo,
           cast(max(order_ts) as date) as hi
    from {{ ref('stg_orders') }}
),
spine as (
    select unnest(range(lo, hi + interval 1 day, interval 1 day)) as d
    from bounds
)
select
    cast(d as date)          as date_day,
    extract(year from d)     as year,
    extract(month from d)    as month,
    extract(day from d)      as day,
    dayname(d)               as weekday
from spine
