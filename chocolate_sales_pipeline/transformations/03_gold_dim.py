from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name = "liebiedieva_gold.dim_date",
    comment="Date dimension. Enriched with derived month_name, day_name, quarter, is_weekend.",
    table_properties={"quality": "gold"}
)
def dim_date():
    return (
        spark.table("liebiedieva_silver.chocolate_dates_silver")
        .withColumn(
            # Surrogate key: integer in YYYYMMDD format
            "date_key",
                F.date_format(F.col("date"), "yyyyMMdd").cast("int")
        )
        .withColumn(
            "month_name",
            F.date_format(F.col("date"), "MMMM"),
        )
        .withColumn(
            "day_name",
            F.date_format(F.col("date"), "EEEE"),
        )
        .withColumn(
            "quarter",
            F.ceil(F.col("month") / 3).cast("integer"),
        )
        .withColumn(
            # day_of_week: 0 = Monday ... 6 = Sunday
            "is_weekend",
            F.col("day_of_week").isin(5, 6),
        )
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "month_name",
            "day",
            "day_name",
            "day_of_week",
            "week",
            "quarter",
            "is_weekend",
        ))
   

@dp.materialized_view(
    name = "liebiedieva_gold.dim_product",
    comment="Product dimension. Surrogate key added. weight_tier derived from weight_g.",
    table_properties={"quality": "gold"}
)
def dim_product():
    return (
        spark.table("liebiedieva_silver.chocolate_products_silver")
        .withColumn(
            "product_key",
            F.dense_rank().over(Window.orderBy("product_id")),
        )
        .withColumn(
            "weight_tier",
            F.when(F.col("weight_g") < 100, "Small")
             .when(F.col("weight_g") <= 150, "Medium")
             .otherwise("Large"),
        )
        .select(
            "product_key",
            "product_id",
            "product_name",
            "brand",
            "category",
            "cocoa_percent",
            "weight_g",
            "weight_tier",
        )
    )


@dp.materialized_view(
    name = "Liebiedieva_gold.dim_store",
    comment="Store dimension, surrogate key added",
    table_properties={"quality": "gold"}
)
def dim_store():
    return (
        spark.table("liebiedieva_silver.chocolate_stores_silver")
        .withColumn(
            "store_key",
            F.dense_rank().over(Window.orderBy("store_id")),
        )
        .select(
            "store_key",
            "store_id",
            "store_name",
            "city",
            "country",
            "store_type",
        )
    )