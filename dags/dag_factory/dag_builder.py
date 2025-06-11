"""Utilities for DAGs generation."""

import re
from datetime import timedelta

import dlt
from airflow.datasets import Dataset
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from cosmos import (DbtTaskGroup, LoadMode, ProfileConfig, ProjectConfig,
                    RenderConfig, ExecutionConfig)
from cosmos.constants import DbtResourceType, TestBehavior
from dlt.helpers.airflow_helper import PipelineTasksGroup

from airflow.sensors.external_task import ExternalTaskSensor

from dag_factory.common.models import Dag
from dag_factory.common.tasks import end_task, start_task
from dag_factory.common.utils import name


def generate_dbt_dags(pipeline_config: Dag, **kwargs) -> DAG:
    """
    Generates DBT (Data Build Tool) Airflow DAGs based on configurations loaded from YAML files.
    This function reads configurations from YAML files, creates DAGs for each specified pipeline,
    and orchestrates the execution of DBT tasks within the Airflow environment.
    Args:
    pipeline_config (Dag): The pipeline configuration object containing DAG and task configurations.

    The function expects the following system arguments:
    - ['tasks', 'run', <current_dag_id>]: To trigger the specific DBT DAG with the given ID.
    """

    airflow_env = kwargs.get("AIRFLOW_ENV")
    env_vars = {
        "ENV": airflow_env,
    }

    dbt_project_config = ProjectConfig(
        dbt_project_path=kwargs.get("DBT_PROJECT_PATH")
    )

    dbt_profile_config = ProfileConfig(
        target_name=airflow_env,
        profile_name="bigquery",
        profiles_yml_filepath=kwargs.get("DBT_PROFILES_PATH"),
    )

    dbt_run_config = RenderConfig(
        load_method=LoadMode.AUTOMATIC,
        select=[",".join(f"tag:{tag}" for tag in pipeline_config.dag_config.tags)],
        test_behavior=TestBehavior.AFTER_EACH,
    )

    cosmos_config = {
        "project_config": dbt_project_config,
        "profile_config": dbt_profile_config,
        "operator_args": {
            "dbt_cmd_global_flags": ["--debug"],
            "vars": {"etl_ts": "{{ data_interval_end.strftime('%Y-%m-%d-%H:%M:%S') }}"},
            "ENV": env_vars,
            "install_deps": True,
        },
    }

    with DAG(**pipeline_config.dag_config.dict()) as dag:
        start = start_task()

        outlet_name = (pipeline_config.dag_config.dag_id if pipeline_config.outlet else None)
        trigger_rule = TriggerRule.ONE_SUCCESS if pipeline_config.detach_tests else TriggerRule.ALL_SUCCESS
        end = end_task(outlet_name, trigger_rule)

        dbt_run_tg = DbtTaskGroup(group_id="dbt_run_tg", render_config=dbt_run_config, **cosmos_config)

        sensor_group = None
        if pipeline_config.depends_on:
            with TaskGroup("waiters", tooltip="Sensor tasks") as sensor_group:
                for (
                    depends_on_dag_id,
                    depends_on_params,
                ) in pipeline_config.depends_on.items():
                    if airflow_env not in depends_on_params.allowed_envs:
                        continue

                    sensor = ExternalTaskSensor(
                        task_id=depends_on_dag_id,
                        external_dag_id=depends_on_dag_id,
                        mode=depends_on_params.mode,
                        poke_interval=depends_on_params.poke_interval,
                        allowed_states=depends_on_params.allowed_states,
                        external_task_id=depends_on_params.task_id,
                        timeout=depends_on_params.timeout,
                        execution_timeout=depends_on_params.execution_timeout,
                        retries=0,
                    )

        cursor = start

        if sensor_group:
            cursor >> sensor_group
            cursor = sensor_group

        cursor >> dbt_run_tg
        cursor = dbt_run_tg

        cursor >> end

    return dag

def generate_dlt_dag(pipeline_config: Dag, **kwargs) -> DAG:
    """
    General function to generate an Airflow DLT DAG based on the given configuration.

    Args:
        pipeline_config (Dag): The pipeline configuration object containing DAG and task configurations.

    Returns:
        DAG: The generated Airflow DAG.
    """
    airflow_env = kwargs.get("AIRFLOW_ENV")
    with DAG(**pipeline_config.dag_config.dict()) as dag:
        start = start_task()

        outlet_name = (
            pipeline_config.dag_config.dag_id if pipeline_config.outlet else None
        )
        end = end_task(outlet_name)

        for resource in pipeline_config.transformers or [None]:
            resource_name = (
                pipeline_config.transformers[resource].get("name") if resource else None
            )
            write_outlet = (
                pipeline_config.transformers[resource].get("outlet")
                if resource
                else False
            )
            with PipelineTasksGroup(
                resource or pipeline_config.pipeline.pipeline_name,
                **pipeline_config.pipeline.group_config.dict(),
            ) as pipeline_group:

                pipeline = dlt.pipeline(
                    pipeline_name=name(
                        pipeline_config.pipeline.pipeline_name + (resource or "")
                    ),
                    dataset_name=name(
                        pipeline_config.pipeline.destination_name + (resource or "")
                    ),
                    **pipeline_config.pipeline.common_config.dict(),
                )

                prev_task = start
                for task in pipeline_config.pipeline.tasks:
                    stream_name = (
                        {"stream_name": resource_name}
                        if resource_name is not None
                        else {}
                    )

                    if task.source in kwargs.get("DLT_TASKS")[pipeline_config.dag_type]:
                        f = kwargs.get("DLT_TASKS")[pipeline_config.dag_type][
                            task.source
                        ](**stream_name, **task.source_parameters or {})
                    else:
                        raise ValueError(f"Unknown task source: {task.source}")

                    additional_args = {
                        "outlets": None,
                    }

                    if write_outlet:
                        additional_args["outlets"] = [
                            Dataset(f"gcs://dataset-bucket/{resource}")
                        ]

                    if f.resources:
                        tasks = pipeline_group.add_run(
                            pipeline,
                            f,
                            **task.run_parameters.dict(),
                            **additional_args,
                        )
                    else:
                        continue
                    for t in tasks:
                        prev_task >> t
                        prev_task = t

            if pipeline_config.depends_on:
                with TaskGroup("waiters", tooltip="Sensor tasks") as sensor_group:
                    for (
                        depends_on_dag_id,
                        depends_on_params,
                    ) in pipeline_config.depends_on.items():
                        if (
                            airflow_env
                            not in depends_on_params.allowed_envs
                        ):
                            continue

                        sensor = ExternalTaskSensor(
                            task_id=depends_on_dag_id,
                            external_dag_id=depends_on_dag_id,
                            mode=depends_on_params.mode,
                            poke_interval=depends_on_params.poke_interval,
                            allowed_states=depends_on_params.allowed_states,
                            external_task_id=depends_on_params.task_id,
                            timeout=depends_on_params.timeout,
                            execution_timeout=depends_on_params.execution_timeout,
                            retries=0,
                        )

                        start >> sensor

                start >> sensor_group >> pipeline_group
            else:
                start >> pipeline_group

            pipeline_group >> end

    return dag
