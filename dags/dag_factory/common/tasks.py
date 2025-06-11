"""Default tasks"""
from airflow.operators.empty import EmptyOperator
from airflow.models import BaseOperator
from airflow.datasets import Dataset
from airflow.utils.trigger_rule import TriggerRule


def start_task() -> BaseOperator:
    """
    Generates a starting point in a directed workflow.

    This function creates a DummyOperator representing the initial task in a directed acyclic graph (DAG).
    The DummyOperator acts as a placeholder and does not perform any actual work.
    It can be used to trigger downstream tasks.

    :return: DummyOperator representing the starting point of a workflow.
    """
    return EmptyOperator(
        task_id='start'
    )


def end_task(dag_id=None, trigger_rule=TriggerRule.ALL_SUCCESS) -> BaseOperator:
    """
    Generates an ending point in a directed workflow.

    This function creates a DummyOperator representing the concluding task in a DAG.
    The DummyOperator acts as a placeholder and doesn't perform any actual work.
    Its main purpose is to signify the end of a DAG.

    If a dag_id is provided, the function will create a Dataset using the dag_id and set it as the outlet of the
    DummyOperator.

    :param dag_id: The id of the DAG.
                   If provided, a Dataset with it will be created and set as the outlet of the DummyOperator.
                   Defaults to None.
    :return: A DummyOperator object, with the task_id set to 'end' and outlets set to
             [Dataset(f"s3://dataset-bucket/{dag_id}")] if dag_id is not None. Otherwise, outlets is not set.
    """
    if dag_id is None:
        return EmptyOperator(
            task_id='end',
            trigger_rule=trigger_rule,
        )

    return EmptyOperator(
        task_id='end',
        outlets=[Dataset(f"bigquery://{dag_id}")],
        trigger_rule=trigger_rule,
    )
