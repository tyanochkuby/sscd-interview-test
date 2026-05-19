from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def order_events_schema() -> StructType:
    return StructType(
        [
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("event_timestamp", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("total_amount", DoubleType(), nullable=False),
        ]
    )


def customers_schema() -> StructType:
    return StructType(
        [
            StructField("customer_id", StringType(), nullable=False),
            StructField("customer_name", StringType(), nullable=False),
            StructField("customer_segment", StringType(), nullable=False),
        ]
    )
