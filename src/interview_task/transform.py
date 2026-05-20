from pyspark.sql import DataFrame, Window, WindowSpec
from pyspark.sql import functions as F


def build_curated_orders(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """Build a curated latest-state orders dataset.

    Interview task:
    - filter invalid amounts
    - keep latest event per order
    - optionally join customers
    - shape output for analytical consumption

    The current implementation is intentionally incomplete.
    """
    # TODO: remove invalid rows
    cleaned_df = (
        orders_df.withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("order_date", F.to_date("event_timestamp"))
    )

    # TODO: complete deduplication by keeping the latest event per order_id.
    # Suggested approach: window function ordered by event_ts descending.
    latest_df = cleaned_df

    ## TODO: join with customers
    curated_df = latest_df

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


def example_latest_status_window() -> WindowSpec:
    """Helper left as a hint for the candidate."""
    return Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())
