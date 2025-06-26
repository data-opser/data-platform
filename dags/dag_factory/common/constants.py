"""Module to store constants required for the factory."""
import os
import sys
from pathlib import Path

ENV = os.getenv("ENV", "dev")

DAGS_PATH = os.path.abspath(os.path.dirname(__file__))  # points to /home/airflow/gcs/dags
if DAGS_PATH not in sys.path:
    sys.path.insert(0, DAGS_PATH)

TRANSFORM_PATH = f'{DAGS_PATH}/transform'

DAG_CONFIG_PATH = f'{DAGS_PATH}/pipeline_configs'
TRANSFORM_DBT_DAGS_CONFIG = f'{DAG_CONFIG_PATH}/transform'
INGEST_PATH = f'{DAGS_PATH}/ingest'
INGEST_CONFIG_PATH = f"{DAG_CONFIG_PATH}/ingest"
DBT_PROJECT_PATH = f"{TRANSFORM_PATH}/dbt"
DBT_PROFILES_PATH = f"{DAGS_PATH}/transform/dbt/profiles.yml"
INGEST_CONFIG_PATH = f"{DAGS_PATH}/pipeline_configs/ingest"
INGEST_QUERY_FILE_PATH = f'{DAGS_PATH}/ingest/push/extract_queries'

VICTORIA_URL = ""

DAG_OWNERS = {
    "all": ["dlt", "ingest", "dbt", "transform"],
}