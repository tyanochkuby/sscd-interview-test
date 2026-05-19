import os
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.interview_task.config import OUTPUT_PATH


def build_spark() -> SparkSession:
    os.environ.setdefault("SPARK_USER", "interview")
    return (
        SparkSession.builder.appName("domain-data-engineer-interview-validator")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> int:
    if not Path(OUTPUT_PATH).exists():
        print("Missing output dataset. Run `uv run python -m scripts.run_local_pipeline` first.")
        return 1

    spark = build_spark()
    try:
        df = spark.read.parquet(str(OUTPUT_PATH))

        errors = []

        if df.filter(F.col("total_amount") <= 0).count() != 0:
            errors.append("Dataset still contains rows with total_amount <= 0.")

        duplicate_orders = df.groupBy("order_id").count().filter(F.col("count") > 1).count()
        if duplicate_orders != 0:
            errors.append("Dataset still contains more than one row per order_id.")

        expected_statuses = {
            "o-100": "SHIPPED",
            "o-102": "SHIPPED",
            "o-104": "CANCELLED",
        }
        actual_statuses = {
            row["order_id"]: row["status"]
            for row in df.select("order_id", "status").collect()
            if row["order_id"] in expected_statuses
        }

        for order_id, expected_status in expected_statuses.items():
            if actual_statuses.get(order_id) != expected_status:
                errors.append(
                    f"Expected latest status for {order_id} to be {expected_status}, got {actual_statuses.get(order_id)}."
                )

        if "customer_id" not in df.columns:
            errors.append("Dataset must contain customer_id.")

        if "order_date" not in df.columns:
            errors.append("Dataset must contain order_date.")

        if errors:
            print("Validation failed:")
            for error in errors:
                print(f"- {error}")
            return 1

        print("Validation passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
