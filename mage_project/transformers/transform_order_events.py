from pyspark.sql import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

from src.interview_task.transform import build_curated_orders


@transformer
def transform_order_events(orders_df: DataFrame, customers_df: DataFrame, **kwargs) -> DataFrame:
    del kwargs
    return build_curated_orders(orders_df, customers_df)
