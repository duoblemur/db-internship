CATALOG = dbutils.widgets.get("catalog")
GOLD = f"{CATALOG}.liebiedieva_gold"

# --- set tags for the tables created in the pipeline after its refresh ----------

statements = [
    f"ALTER TABLE {GOLD}.fact_sales ALTER COLUMN customer_id SET TAGS ('sensitivity' = 'pii')",
    f"ALTER TABLE {GOLD}.fact_sales ALTER COLUMN cost SET TAGS ('sensitivity' = 'financial')",
    f"ALTER TABLE {GOLD}.fact_sales ALTER COLUMN profit SET TAGS ('sensitivity' = 'financial')",
    f"ALTER TABLE {GOLD}.agg_store_performance SET TAGS ('rls' = 'store_scoped')",
    f"ALTER TABLE {GOLD}.agg_store_performance ALTER COLUMN country SET TAGS ('rls' = 'country')",
]

for stmt in statements:
    print(f"Executing: {stmt}")
    spark.sql(stmt)
    print(" OK")


# --- create rls and cls functions -------------------------------------------------

spark.sql(f"""          
    CREATE OR REPLACE FUNCTION {GOLD}.mask_financial_double(val DOUBLE)
    RETURNS DOUBLE
    COMMENT 'Column mask for financial measures (cost, profit). Returns NULL to unauthorized users. Group exclusions configured via ABAC policy.'
    RETURN NULL;  
          """)


spark.sql(f"""    
    CREATE OR REPLACE FUNCTION {GOLD}.mask_sensitive_string(val STRING)
    RETURNS STRING
    COMMENT 'Column mask for sensitive string columns (customer_id). Returns **Redacted** to unauthorized users. Group exclusions configured via ABAC policy.'
    RETURN "**Redacted**";
       """)


spark.sql(f"""
    CREATE OR REPLACE FUNCTION {GOLD}.row_filter_store_country(country STRING)
    RETURNS BOOLEAN
    RETURN
    IS_ACCOUNT_GROUP_MEMBER('data_engineers')
    OR IS_ACCOUNT_GROUP_MEMBER('finance_analysts')
    OR IS_ACCOUNT_GROUP_MEMBER(CONCAT('store_managers_', country));
    -- store_managers_canada, store_managers_australia
          """)


# --- grant read permissions to the tables -----------------------------------------

gold_tables = [
    "dim_date", "dim_product", "dim_store",
    "fact_sales",
    "agg_monthly_sales", "agg_store_performance", "agg_weekly_patterns"
]

for table in gold_tables:
    spark.sql(f"""
        GRANT SELECT ON TABLE {GOLD}.{table}
        TO `account users`
    """)