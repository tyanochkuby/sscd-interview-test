from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.interview_task.config import INPUT_CUSTOMERS_PATH
from src.interview_task.schemas import customers_schema


def build_curated_orders(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    """Build a curated latest-state orders dataset.

    Interview task:
    - filter invalid amounts
    - keep latest event per order
    - optionally join customers
    - shape output for analytical consumption

    The current implementation is intentionally incomplete.
    """
    customers_df = spark.read.schema(customers_schema()).json(str(Path(INPUT_CUSTOMERS_PATH)))

    cleaned_df = (
        orders_df.withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("order_date", F.to_date("event_timestamp"))
        .filter(F.col("total_amount") > 0)
    )

    # TODO: complete deduplication by keeping the latest event per order_id.
    # Suggested approach: window function ordered by event_ts descending.
    latest_df = cleaned_df

    # TODO: decide whether to keep the customer join.
    # This optional join is left here so the candidate can discuss trade-offs.
    curated_df = latest_df.join(customers_df, on="customer_id", how="left")

    # TODO: finalize selected columns and any renaming needed for consumption.
    return curated_df.select(
        "order_id",
        "customer_id",
        "customer_name",
        "customer_segment",
        "event_timestamp",
        "status",
        "total_amount",
        "order_date",
    )


def example_latest_status_window() -> Window:
    """Helper left as a hint for the candidate."""
    return Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())
