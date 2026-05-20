from pathlib import Path

from pyspark.sql import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from src.interview_task.config import INPUT_CUSTOMERS_PATH
from src.interview_task.schemas import customers_schema
from src.interview_task.spark import build_local_spark


@data_loader
def load_customers(**kwargs) -> DataFrame:
    del kwargs

    spark = build_local_spark("domain-data-engineer-interview")
    return spark.read.option("multiLine", True).schema(customers_schema()).json(str(Path(INPUT_CUSTOMERS_PATH)))
