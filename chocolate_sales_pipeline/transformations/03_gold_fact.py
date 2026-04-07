from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.table(
    name = "liebiedieva_gold.fact_sales",
    comment=(
        "Core sales fact table. Surrogate keys resolved from dim_date, dim_product, dim_store. "
    ),
    table_properties={"quality": "gold"}
)

@dp.expect_all({
    "valid_date_key": "date_key IS NOT NULL",
    "valid_product_key": "product_key IS NOT NULL",
    "valid_store_key": "store_key IS NOT NULL"})

def fact_sales():
    sales = spark.readStream.table("liebiedieva_silver.chocolate_sales_silver")
    dates    = spark.table("liebiedieva_gold.dim_date")
    products = spark.table("liebiedieva_gold.dim_product")
    stores   = spark.table("liebiedieva_gold.dim_store")
 
    return (
        sales
        # Resolve date_key
        .join(
            dates.select("date_key", "date"),
            sales["order_date"] == dates["date"],
            how="left",
        )
        # resolve product_key
        .join(
            products.select("product_key", "product_id"),
            on="product_id",
            how="left",
        )
        .withColumn("product_key", F.coalesce(F.col("product_key"), F.lit(-1)))
        # resolve store_key
        .join(
            stores.select("store_key", "store_id"),
            on="store_id",
            how="left",
        )
        .select(
            sales["order_id"],

            F.col("date_key"),
            F.col("product_key"),
            F.col("store_key"),

            sales["customer_id"],

            sales["quantity"],
            sales["unit_price"],
            sales["discount"],
            sales["revenue"],

            sales["cost"],

            sales["profit"],
        )
    )


