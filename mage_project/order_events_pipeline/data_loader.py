from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.interview_task.config import INPUT_ORDERS_PATH
from src.interview_task.schemas import order_events_schema


def load_order_events(spark: SparkSession) -> DataFrame:
    """Mage Data Loader block stub."""
    return spark.read.schema(order_events_schema()).json(str(Path(INPUT_ORDERS_PATH)))
