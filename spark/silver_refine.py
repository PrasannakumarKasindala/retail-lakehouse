"""Production silver refine (PySpark + Delta Lake).

Cluster-scale equivalent of rlh.silver: rebuild the current cleaned snapshot from
the accumulated bronze history. For each table keep the latest row per natural
key (a window over the ingest timestamp), normalize fields, and enforce
referential integrity so the star schema builds cleanly. Writes the silver Delta
tables (overwrite) and a single serving parquet per table for the warehouse.

Validated by tests/test_spark_structure.py (parsed, not executed, in CI).
"""

from __future__ import annotations

import argparse


def build_spark(app_name: str = "rlh-silver-refine"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def _latest_per_key(df, key: str, order_col: str = "_ingested_at"):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .where(F.col("_rn") == 1)
              .drop("_rn", "_extract_date", "_ingested_at"))


def refine(spark, bronze_dir: str, silver_dir: str, serving_dir: str) -> None:
    from pyspark.sql import functions as F

    def bronze(table):
        return spark.read.format("delta").load(f"{bronze_dir}/{table}")

    customers = (_latest_per_key(bronze("customers"), "customer_id")
                 .withColumn("segment", F.upper(F.trim(F.col("segment"))))
                 .withColumn("city", F.coalesce(F.col("city"), F.lit("UNKNOWN"))))
    products = _latest_per_key(bronze("products"), "product_id")
    orders = (_latest_per_key(bronze("orders"), "order_id")
              .where(F.col("status").isNotNull())
              .join(customers.select("customer_id"), "customer_id", "left_semi"))
    order_items = (_latest_per_key(bronze("order_items"), "order_item_id")
                   .where(F.col("quantity") > 0)
                   .join(orders.select("order_id"), "order_id", "left_semi")
                   .join(products.select("product_id"), "product_id", "left_semi"))

    for name, df in (("customers", customers), ("products", products),
                     ("orders", orders), ("order_items", order_items)):
        df.write.format("delta").mode("overwrite").save(f"{silver_dir}/{name}")
        df.coalesce(1).write.mode("overwrite").parquet(f"{serving_dir}/{name}.parquet")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Silver refine (Spark + Delta)")
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver", required=True)
    p.add_argument("--serving", required=True)
    args = p.parse_args(argv)
    spark = build_spark()
    try:
        refine(spark, args.bronze, args.silver, args.serving)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
