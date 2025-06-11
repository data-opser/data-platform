"""This module use dag_factory lib for generating dags"""
import json
import os
import sys

from dag_factory.dag_runner import DagGenerator
from airflow import DAG

from ingest.pull.ga4_pipeline.ga4_pipeline import ga4_source

from dag_factory.common import constants

DAGS_FOLDER = os.path.abspath(os.path.dirname(__file__))
if DAGS_FOLDER not in sys.path:
    sys.path.insert(0, DAGS_FOLDER)

os.environ["ENV"] = constants.ENV
os.environ["DAG_OWNERS"] = json.dumps(constants.DAG_OWNERS)
os.environ["DAG_CONFIG_PATH"] = constants.DAG_CONFIG_PATH

DLT_TASKS = {
    "ga4": {"ga4_source": ga4_source},
}

dag_runner = DagGenerator(constants=constants, on_failure="NOT_IGNORE")

current_dag_id = None
if len(sys.argv) > 3 and sys.argv[1:3] == ['tasks', 'run']:
    current_dag_id = sys.argv[3]

dag_runner.create_dags(
    directory=f"{constants.DAGS_PATH}/pipeline_configs",
    current_dag_id=current_dag_id,
    dlt_tasks=DLT_TASKS
)
