from pathlib import Path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from src.interview_task.config import INPUT_CUSTOMERS_PATH, INPUT_ORDERS_PATH, OUTPUT_PATH
from src.interview_task.schemas import customers_schema, order_events_schema
from src.interview_task.spark import build_local_spark
from src.interview_task.transform import build_curated_orders


@data_loader
def run_order_events_pipeline(**kwargs):
    del kwargs

    spark = build_local_spark("domain-data-engineer-interview")
    try:
        orders_df = spark.read.schema(order_events_schema()).json(str(Path(INPUT_ORDERS_PATH)))
        customers_df = (
            spark.read.option("multiLine", True)
            .schema(customers_schema())
            .json(str(Path(INPUT_CUSTOMERS_PATH)))
        )
        curated_df = build_curated_orders(orders_df, customers_df)

        summary = {
            "loaded_rows": orders_df.count(),
            "loaded_customers": customers_df.count(),
            "curated_rows": curated_df.count(),
            "output_path": str(OUTPUT_PATH),
        }

        curated_df.write.mode("overwrite").partitionBy("order_date").parquet(str(Path(OUTPUT_PATH)))
        return summary
    finally:
        spark.stop()
