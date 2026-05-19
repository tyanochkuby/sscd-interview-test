from pyspark.sql import DataFrame, SparkSession

from src.interview_task.transform import build_curated_orders


def transform_order_events(spark: SparkSession, orders_df: DataFrame) -> DataFrame:
    """Mage Transformer block stub."""
    return build_curated_orders(spark, orders_df)
