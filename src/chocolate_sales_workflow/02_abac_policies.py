CATALOG = dbutils.widgets.get("catalog")
GOLD = f"{CATALOG}.liebiedieva_gold"

spark.sql(f"USE CATALOG {CATALOG};")

# --- create policies for row level security ------------------------

spark.sql(f"""       
    CREATE OR REPLACE POLICY rls_store_scoped
    ON SCHEMA liebiedieva_gold
    ROW FILTER liebiedieva_gold.row_filter_store_country
    TO `account users`
    EXCEPT `data_engineers`
    FOR TABLES
    WHEN has_tag_value('rls', 'store_scoped')
    MATCH COLUMNS has_tag_value('rls', 'country') AS country
    USING COLUMNS (country); 
          """)



# --- create policies for column level security --------------------

spark.sql(f"""         
    CREATE OR REPLACE POLICY mask_financial_columns
    ON SCHEMA liebiedieva_gold
    COMMENT 'Masks DOUBLE columns tagged sensitivity=financial for all users except finance_analysts and data_engineers.'
    COLUMN MASK liebiedieva_gold.mask_financial_double
    TO `account users`
    EXCEPT `data_engineers`
    FOR TABLES
    MATCH COLUMNS has_tag_value('sensitivity', 'financial') AS financial_col
    ON COLUMN financial_col;""")

spark.sql(f"""
    CREATE OR REPLACE POLICY mask_pii_columns
    ON SCHEMA liebiedieva_gold
    COMMENT 'Masks STRING columns tagged sensitivity=pii for all users except finance_analysts and data_engineers.'
    COLUMN MASK liebiedieva_gold.mask_sensitive_string
    TO `account users`
    EXCEPT `data_engineers`
    FOR TABLES
    MATCH COLUMNS has_tag_value('sensitivity', 'pii') AS pii_col
    ON COLUMN pii_col;  
          """)