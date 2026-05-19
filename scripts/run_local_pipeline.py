import os
from pathlib import Path

from pyspark.sql import SparkSession

from mage_project.order_events_pipeline.data_exporter import export_curated_orders
from mage_project.order_events_pipeline.data_loader import load_order_events
from mage_project.order_events_pipeline.transformer import transform_order_events
from src.interview_task.config import OUTPUT_DIR


def build_spark() -> SparkSession:
    # Spark can fall back to Hadoop user lookup, which breaks on some newer JDKs.
    os.environ.setdefault("SPARK_USER", "interview")
    return (
        SparkSession.builder.appName("domain-data-engineer-interview")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    try:
        orders_df = load_order_events(spark)
        curated_df = transform_order_events(spark, orders_df)

        print("Loaded rows:", orders_df.count())
        print("Curated rows:", curated_df.count())
        curated_df.show(truncate=False)

        export_curated_orders(curated_df)
        print(f"Wrote output to {Path(OUTPUT_DIR / 'curated_orders')}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
