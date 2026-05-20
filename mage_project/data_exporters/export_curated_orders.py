from pathlib import Path

from pyspark.sql import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from src.interview_task.config import OUTPUT_PATH


@data_exporter
def export_curated_orders(curated_df: DataFrame, **kwargs) -> None:
    del kwargs

    try:
        curated_df.write.mode("overwrite").partitionBy("order_date").parquet(str(Path(OUTPUT_PATH)))
    finally:
        curated_df.sparkSession.stop()
