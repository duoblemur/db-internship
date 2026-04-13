from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name = "liebiedieva_gold.agg_monthly_sales",
    comment=(
        "Monthly sales aggregate: year > month > category > country. "
        "Supports revenue trending, discount analysis, and market-level comparisons."
    ),
    table_properties={"quality": "gold"},
)
def agg_monthly_sales():
    fact = spark.table("liebiedieva_gold.fact_sales")
    dim_d = spark.table("liebiedieva_gold.dim_date")
    dim_p = spark.table("liebiedieva_gold.dim_product")
    dim_s = spark.table("liebiedieva_gold.dim_store")
 
    return (
        fact
        .join(dim_d.select("date_key", "year", "month", "month_name"), on="date_key")
        .join(dim_p.select("product_key", "category"), on="product_key")
        .join(dim_s.select("store_key", "country"), on="store_key")
        .groupBy("year", "month", "month_name", "category", "country")
        .agg(
            F.round(F.sum("revenue"),2).alias("total_revenue"),
            F.sum("quantity").alias("total_quantity"),
            F.round(F.sum("profit"),2).alias("total_profit"),
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.avg("discount"),4).alias("avg_discount"),
        )
        .orderBy("year", "month", "category", "country")
    )


@dp.materialized_view(
    name = "liebiedieva_gold.agg_store_performance",
    comment=(
        "Store performance aggregate: store > year > month"
    ),
    table_properties={"quality": "gold"}
)
def agg_store_performance():
    fact = spark.table("liebiedieva_gold.fact_sales")
    dim_d = spark.table("liebiedieva_gold.dim_date")
    dim_s = spark.table("liebiedieva_gold.dim_store")
 
    return (
        fact
        .join(dim_d.select("date_key", "year", "month", "month_name"), on="date_key")
        .join(
            dim_s.select("store_key", "store_id", "store_name", "city", "country", "store_type"),
            on="store_key",
        )
        .groupBy(
            "store_key", "store_id", "store_name",
            "city", "country", "store_type",
            "year", "month", "month_name",
        )
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("revenue") /
                    F.countDistinct("order_id"), 2).alias("avg_order_value"),
        )
        .orderBy("year", "month", "store_name")
    )


@dp.materialized_view(
    name = "liebiedieva_gold.agg_weekly_patterns",
    comment=(
        "Weekly buying pattern aggregate: day_of_week > store_type"
    ),
    table_properties={"quality": "gold"}
)
def agg_weekly_patterns():
    fact = spark.table("liebiedieva_gold.fact_sales")
    dim_d = spark.table("liebiedieva_gold.dim_date")
    dim_s = spark.table("liebiedieva_gold.dim_store")
 
    # Daily revenue per (date, store_type) as an intermediate step,
    # then average across days within each (day_of_week, store_type) group.
    daily = (
        fact
        .join(
            dim_d.select("date_key", "date", "day_of_week", "day_name", "is_weekend"),
            on="date_key",
        )
        .join(dim_s.select("store_key", "store_type"), on="store_key")
        .groupBy("date", "day_of_week", "day_name", "is_weekend", "store_type")
        .agg(
            F.sum("revenue").alias("daily_revenue"),
            F.countDistinct("order_id").alias("daily_orders"),
            F.sum("quantity").alias("daily_quantity"),
        )
    )
 
    return (
        daily
        .groupBy("day_of_week", "day_name", "is_weekend", "store_type")
        .agg(
            F.round(F.avg("daily_revenue"),2).alias("avg_daily_revenue"),
            F.round(F.avg("daily_orders"),2).alias("avg_daily_orders"),
            F.round(F.avg("daily_quantity"),2).alias("avg_daily_quantity"),
        )
        .orderBy("day_of_week", "store_type")
    )