"""Production bronze ingest (PySpark + Delta Lake).

This is the cluster-scale equivalent of rlh.bronze: it lands a daily raw extract
into an append-only Delta table with lineage columns. The local pipeline uses
delta-rs for the same effect without a JVM; on a cluster this job runs on Spark
against object storage (S3/ADLS/GCS) with the Glue/Hive catalog.

Run (example):
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \\
        spark/bronze_ingest.py --raw s3://lake/raw/2026-03-02 \\
        --bronze s3://lake/bronze --extract-date 2026-03-02

Validated by tests/test_spark_structure.py (parsed, not executed, in CI).
"""

from __future__ import annotations

import argparse

TABLES = ("customers", "products", "orders", "order_items")


def build_spark(app_name: str = "rlh-bronze-ingest"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ingest(spark, raw_dir: str, bronze_dir: str, extract_date: str) -> None:
    from pyspark.sql import functions as F

    for table in TABLES:
        df = spark.read.parquet(f"{raw_dir}/{table}.parquet")
        df = (df
              .withColumn("_extract_date", F.lit(extract_date))
              .withColumn("_ingested_at", F.current_timestamp()))
        (df.write.format("delta").mode("append")
           .save(f"{bronze_dir}/{table}"))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Bronze ingest (Spark + Delta)")
    p.add_argument("--raw", required=True, help="raw extract dir for the date")
    p.add_argument("--bronze", required=True, help="bronze Delta root")
    p.add_argument("--extract-date", required=True)
    args = p.parse_args(argv)
    spark = build_spark()
    try:
        ingest(spark, args.raw, args.bronze, args.extract_date)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
