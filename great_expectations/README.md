# Great Expectations suites

These JSON files are the silver-layer data contracts, one suite per table. They
are the single source of truth: `rlh.expectations.SUITES` builds Great
Expectations objects from the same specs at runtime, and `rlh write-suites`
regenerates these files.

The `validate_ge` step in the Airflow DAG (and `rlh validate`) runs them against
the current serving snapshots and fails the batch if any expectation is not met,
so a data-quality regression never reaches the gold layer.

| Suite | Guards |
|---|---|
| `silver_customers` | `customer_id` present and unique; `segment` in {SMB, MID, ENT}; `city` present |
| `silver_products` | `product_id` present and unique; `category` in {HW, SW, ACC} |
| `silver_orders` | `order_id` present and unique; `customer_id` present; `status` in the allowed set |
| `silver_order_items` | `order_item_id` present and unique; `quantity` >= 1 |

Regenerate after changing the specs:

```
rlh write-suites --out great_expectations/expectations
```
