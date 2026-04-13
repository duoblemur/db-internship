from pyspark import pipelines as dp
from pyspark.sql import functions as F
 
# ---------------------------------------------------------------------------
# CONFIGURATIONs
# ---------------------------------------------------------------------------

SOURCE_BASE = spark.conf.get("base_path")
 
SALES_PATH    = f"{SOURCE_BASE}/sales.csv"
PRODUCTS_PATH = f"{SOURCE_BASE}/products.csv"
STORES_PATH   = f"{SOURCE_BASE}/stores.csv"
CALENDAR_PATH = f"{SOURCE_BASE}/calendar.csv"



@dp.table(
    name="chocolate_sales_bronze",
    comment="Raw sales records ingested from cloud storage. No transforms applied",
    table_properties={"quality": "bronze"},
)
def bronze_sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(SALES_PATH)
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
 
 
@dp.table(
    name="chocolate_products_bronze",
    comment="Raw products records ingested from cloud storage. No transforms applied.",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(PRODUCTS_PATH)
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
 
 
@dp.table(
    name="chocolate_stores_bronze",
    comment="Raw stores records ingested from cloud storage. No transforms applied",
    table_properties={"quality": "bronze"},
)
def bronze_stores():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(STORES_PATH)
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
 
 
@dp.table(
    name="chocolate_dates_bronze",
    comment="Raw calendar records ingested from cloud storage",
    table_properties={"quality": "bronze"},
)
def bronze_calendar():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(CALENDAR_PATH)
        .withColumn("_ingest_time", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )