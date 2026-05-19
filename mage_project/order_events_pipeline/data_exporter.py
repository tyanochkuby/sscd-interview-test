from pathlib import Path

from pyspark.sql import DataFrame

from src.interview_task.config import OUTPUT_PATH


def export_curated_orders(curated_df: DataFrame) -> None:
    """Mage Data Exporter block stub.

    Replace this local Parquet export with Iceberg or Hudi if desired.
    """
    curated_df.write.mode("overwrite").partitionBy("order_date").parquet(str(Path(OUTPUT_PATH)))
