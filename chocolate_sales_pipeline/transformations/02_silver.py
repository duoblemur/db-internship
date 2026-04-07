from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name = "liebiedieva_silver.chocolate_sales_silver",
    comment="Cleaned sales table. Rows with null order_id or order_date are dropped.",
    table_properties={"quality": "silver"}
)

@dp.expect_all_or_drop({
    "order_id not null": "order_id IS NOT NULL",
    "product_id not null": "product_id IS NOT NULL",
    "store_id not null": "store_id IS NOT NULL",
    "quantity is valid": "quantity > 0",
    "cost is valid": "cost > 0"
})

# warn only — customer might not be in the system
@dp.expect("customer_id is not null",
    f"customer_id IS NOT NULL")

def silver_sales():
    return (
        spark.readStream.table("chocolate_sales_bronze")
        .select(
            F.col("order_id").cast("string"),
            F.col("order_date").cast("date"),
            F.col("product_id").cast("string"),
            F.col("store_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("quantity").cast("integer"),
            F.col("unit_price").cast("double"),
            F.col("discount").cast("double"),
            F.col("revenue").cast("double"),
            F.col("cost").cast("double"),
            F.col("profit").cast("double"),
        )
    )


# ---------------------------------------------------------------------------
@dp.table(
    name = "liebiedieva_silver.chocolate_products_silver",
    comment = "Cleaned and validated product information with proper types",
    table_properties = {
        "quality": "silver"
    }
)

@dp.expect_all_or_drop({
    "product_id not null": "product_id IS NOT NULL",
    "category not null": "category IS NOT NULL",
    "cocoa_percent is valid": "cocoa_percent > 0"
})

def silver_products():
    return (
        spark.readStream.table("chocolate_products_bronze")
        .select(
            F.col("product_id").cast("string"),
            F.col("product_name").cast("string"),
            F.col("brand").cast("string"),
            F.col("category").cast("string"),
            F.col("cocoa_percent").cast("integer"),
            F.col("weight_g").cast("integer"),
        )
    )

# ---------------------------------------------------------------------------
@dp.table(
    name = "liebiedieva_silver.chocolate_stores_silver",
    comment = "Cleaned and validated stores information with proper types",
    table_properties = {
        "quality": "silver"
    }
)
@dp.expect_all_or_drop({
    "store_id not null": "store_id IS NOT NULL",
    "city not null": "city IS NOT NULL",
    "country not null": "country IS NOT NULL"
})

def silver_stores():
    return (
        spark.readStream.table("chocolate_stores_bronze")
        .select(
            F.col("store_id").cast("string"),
            F.col("store_name").cast("string"),
            F.col("city").cast("string"),
            F.col("country").cast("string"),
            F.col("store_type").cast("string"),
        )
    )

# ---------------------------------------------------------------------------
@dp.table(
    name = "liebiedieva_silver.chocolate_dates_silver",
    comment = "Cleaned and validated dates table with proper types",
    table_properties = {
        "quality": "silver"
    }
)

@dp.expect_all_or_drop({
    "date not null": "date IS NOT NULL"
})

def silver_calendar():
    return (
        spark.readStream.table("chocolate_dates_bronze")
        .select(
            F.col("date").cast("date"),
            F.col("year").cast("integer"),
            F.col("month").cast("integer"),
            F.col("day").cast("integer"),
            F.col("week").cast("integer"),
            F.col("day_of_week").cast("integer"),
        )
    )