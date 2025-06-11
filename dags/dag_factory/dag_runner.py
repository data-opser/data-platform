"""
This module provides functionality to generate Airflow DAGs from YAML configuration files.
It defines the `DataOpsDagGenerator` class, which scans a given directory for YAML files
and creates DAGs based on the specified configuration, integrating with tools like dbt, dlt,
and Facebook CAPI.
"""
import os
from typing import Optional

import yaml

from dag_factory.common.models import Dag

from dag_factory.common.config.config_tools import render_sql_template
from dag_factory.dag_builder import generate_dbt_dags, generate_dlt_dag


class DagGenerator:
    """
        Class to process directories and generate Airflow DAGs based on YAML configuration files.

        Attributes:
            dags_mapping (DataOpsMapping): Instance of DataOpsMapping to map DAG types to their generation functions.
    """
    def __init__(self, constants, on_failure):
        DAGS = {
            "dbt": generate_dbt_dags,
            "dlt": generate_dlt_dag,
        }
        self.dags_mapping = DAGS
        self.constants = constants
        self.on_failure = on_failure

    def create_dags(self, directory, current_dag_id: str, dlt_tasks: Optional[dict]):
        """
            Process a given directory to read YAML configuration files and generate Airflow DAGs.

            Args:
                directory (str): The path to the directory containing the YAML configuration files.
                current_dag_id (str): Id of the current dag from command line in worker
        """

        kwargs = {
            "DLT_TASKS": dlt_tasks,
            "AIRFLOW_ENV": self.constants.ENV,
            "render_sql_template": render_sql_template,
            "DBT_PROJECT_PATH": self.constants.DBT_PROJECT_PATH,
            "DBT_PROFILES_PATH": self.constants.DBT_PROFILES_PATH,
            "DAGS_PATH": self.constants.DAGS_PATH,
            "TRANSFORM_PATH": self.constants.TRANSFORM_PATH,
        }

        for subdir in os.listdir(directory):
            subdir_path = os.path.join(directory, subdir)

            if os.path.isdir(subdir_path):
                print(f"Reading YAML files in '{subdir}' directory:")

                for filename in os.listdir(subdir_path):
                    file_path = os.path.join(subdir_path, filename)

                    # Check if the file is a YAML file
                    if filename.endswith('.yaml') or filename.endswith('.yml'):
                        with open(file_path, 'r', encoding='utf-8') as file:
                            yaml_data = yaml.safe_load(file)
                            print(yaml_data["dags"].keys())
                            for item in yaml_data["dags"].keys():
                                if current_dag_id is not None and current_dag_id != item:
                                    continue
                                yaml_data["dags"][item]["dag_config"]["dag_id"] = item
                                config = Dag(**yaml_data["dags"][item])
                                try:
                                    self.dags_mapping[config.tool](pipeline_config=config, **kwargs)
                                except Exception as e:  # pylint: disable=W0718
                                    print(f"Error in DAG '{item}': {str(e)}")
                                    if self.on_failure == "IGNORE":
                                        continue
                                    raise e
