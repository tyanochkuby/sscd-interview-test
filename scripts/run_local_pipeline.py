from pathlib import Path

from mage_project.order_events_pipeline.data_exporter import export_curated_orders
from mage_project.order_events_pipeline.data_loader import load_order_events
from mage_project.order_events_pipeline.transformer import transform_order_events
from src.interview_task.config import OUTPUT_DIR
from src.interview_task.spark import build_local_spark


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_local_spark("domain-data-engineer-interview")
    try:
        orders_df = load_order_events(spark)
        curated_df = transform_order_events(spark, orders_df)

        print("Loaded rows:", orders_df.count())
        print("Curated rows:", curated_df.count())
        curated_df.show(truncate=False)

        export_curated_orders(curated_df)
        print(f"Wrote output to {Path(OUTPUT_DIR / 'curated_orders')}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
