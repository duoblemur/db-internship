# config.py

import os

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_HOST:
    raise ValueError("DATABRICKS_HOST is not set")

if not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_TOKEN is not set")