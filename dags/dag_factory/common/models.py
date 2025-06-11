"""Pydantic models for DAGs generation."""
from datetime import datetime
from datetime import timedelta
from typing import Any, Optional, List, Dict, Union
from functools import partial
import pendulum
from airflow.models import Variable
from airflow.datasets import Dataset
from pydantic.v1 import BaseModel, root_validator

from dag_factory.common.utils import get_var

ENV = Variable.get("env",  "development")


class PipelineTaskConfig(BaseModel):
    """
    Configuration for individual tasks in a data pipeline.

    Attributes:
        decompose (Optional[str]): The method used for task decomposition (default: "parallel-isolated").
        trigger_rule (Optional[str]): The rule used to trigger the task (default: "all_done").
        retries (Optional[int]): The number of times the task execution should be retried in case of failures
                                 (default: 0).
        provide_context (Optional[bool]): Flag to indicate whether execution context should be provided to the task
                                          (default: True).
        queue (Optional[str]): Name of the queue where the task should be placed for execution (default: 'default').
        executor_config (Optional[Dict[str, Any]]): Task-specific configuration for the executor (default: None).

    Root Validator:
        validate_kubernetes_executor_config: A root validator that preprocesses inputs to validate and structure
                                             executor_config if the queue is "kubernetes".
    """

    decompose: Optional[str] = "parallel-isolated"
    trigger_rule: Optional[str] = "all_done"
    retries: Optional[int] = 0
    provide_context: Optional[bool] = True
    queue: Optional[str] = 'default'
    executor_config: Optional[Dict[str, Any]] = None

    @root_validator(pre=True)
    def validate_kubernetes_executor_config(cls, values):
        """
        Preprocesses inputs to validate and structure executor_config if the queue is "kubernetes".

        If the provided queue is "kubernetes", the executor_config is updated accordingly. Known keys related to
        resources requests and limits are moved within the 'KubernetesExecutor' key of executor config.

        Args:
            values (dict): A dictionary of field names and their corresponding values.

        Returns:
            dict: The updated dictionary with 'executor_config' structured for KubernetesExecutor if 'queue' was
                  "kubernetes".
        """

        if values.get("queue") == "kubernetes" and values.get("executor_config"):
            executor_config = values["executor_config"]
            kube_exec = executor_config.setdefault("KubernetesExecutor", {})

            known_keys = ["request_memory", "limit_memory", "request_cpu", "limit_cpu"]
            for key in known_keys:
                if key in executor_config:
                    kube_exec[key] = executor_config.pop(key)

            values["executor_config"]["KubernetesExecutor"] = kube_exec

        return values


class Task(BaseModel):
    """
    Configuration for a single task in a pipeline.

    Attributes:
        source (str): The source of the task, default is 'dlt'.
        run_parameters (Optional[PipelineTaskConfig]): Configuration parameters for the task execution
            (default: an instance of PipelineTaskConfig).
        source_parameters (Optional[dict]): Source-specific parameters for the task (default: None).
    """
    source: str
    run_parameters: Optional[PipelineTaskConfig] = PipelineTaskConfig()
    source_parameters: Optional[dict] = None


class DefaultArgs(BaseModel):
    """
    Default configuration for default arguments used in Airflow DAGs.

    Attributes:
        owner (Optional[str]): Owner of the DAG (default: 'airflow').
        retries (Optional[int]): Number of retries for task execution (default: 3).
        retry_delay (Optional[pendulum.Duration]): Delay between task retries (default: 1 minute).
        retry_exponential_backoff (Optional[bool]): Whether to use exponential backoff for retries (default: True).
    """
    owner: Optional[str] = 'airflow'
    retries: Optional[int] = 3
    retry_delay: Optional[pendulum.Duration] = pendulum.duration(minutes=1)
    retry_exponential_backoff: Optional[bool] = True
    sla: Optional[pendulum.Duration] = pendulum.duration(seconds=300)
    start_date: Optional[datetime] = datetime(2024, 4, 11)
    max_active_runs: Optional[int] = 1
    execution_timeout: Optional[timedelta] = None

    @root_validator(pre=True)
    def validate_execution_timeout(cls, values):
        """
        Ensures 'execution_timeout' is a valid timedelta object.
        Converts it from minutes if provided as an integer.
        """
        if values.get('execution_timeout') is not None:
            if isinstance(values['execution_timeout'], (int, float)):
                values['execution_timeout'] = timedelta(minutes=values['execution_timeout'])
            elif not isinstance(values['execution_timeout'], timedelta):
                raise ValueError("execution_timeout must be a timedelta or an integer (minutes).")
        return values


class DependsOnItems(BaseModel):
    """
    Items for defining Airflow DAG sensors.
    Check: airflow.sensors.base.BaseSensorOperator

    Attributes:
        schedule (cron): Schedule interval of DAG that sensor looking for (default: None).
        execution_timeout (Optional[timedelta]): Execution timeout for DAG sensor in minutes (default: 60).
        timeout: (Optional[timedelta]):
            Time elapsed before the task times out and fails.
            Timeout for DAG sensor in minutes (default: 60).
        mode: (Optional[str]): sensor mode. Default is 'poke'
        poke_interval: (Optional[timedelta]): time in sec interval for poke the task status
        task_id: (Optional[str]): name of the task
        allowed_states: (Optional[List[str]]): acceptable states of sensor to continue the execution
        allowed_envs: (Optional[List[str]]): acceptable environments of sensor to adding it to the task group
    """
    schedule: Optional[str] = None
    execution_timeout: Optional[pendulum.Duration] = pendulum.duration(minutes=60)
    timeout: Optional[pendulum.Duration] = pendulum.duration(minutes=60)
    mode: Optional[str] = 'poke'
    poke_interval: Optional[pendulum.Duration] = pendulum.duration(minutes=1)
    task_id: Optional[str] = 'end'
    allowed_states: Optional[list] = ['success']
    allowed_envs: Optional[list] = ['prod', 'stage', 'development']

    @root_validator(pre=True)
    def validate_time(cls, values):
        """
        Validates and converts time-related fields in the input values to `pendulum.duration` objects.

        This method checks for the presence of 'execution_timeout', 'timeout' and 'poke_interval'
            in the input dictionary.
        If these fields are found, it converts their values from minutes to `pendulum.duration` objects.

        Args:
            cls (type): The class this validator is attached to.
            values (dict): A dictionary of field names and their corresponding values.

        Returns:
            dict: The updated dictionary with 'execution_timeout', 'timeout' and 'poke_interval'
            converted to `pendulum.duration` objects if they were present.
        """
        if values.get('execution_timeout'):
            values['execution_timeout'] = pendulum.duration(minutes=values.get('execution_timeout'))
        if values.get('timeout'):
            values['timeout'] = pendulum.duration(minutes=values.get('timeout'))
        if values.get('poke_interval'):
            values['poke_interval'] = pendulum.duration(minutes=values.get('poke_interval'))

        return values


class GroupConfig(BaseModel):
    """
    Configuration for grouping tasks in a pipeline.

    Attributes:
        use_data_folder (Optional[bool]): Whether to use a data folder (default: False).
        wipe_local_data (Optional[bool]): Whether to wipe local data (default: True).
        use_task_logger (Optional[bool]): Whether to use a task logger (default: False).
    """
    use_data_folder: Optional[bool] = False
    wipe_local_data: Optional[bool] = True
    use_task_logger: Optional[bool] = False


class CommonConfig(BaseModel):
    """
    Common configuration for pipeline tasks.

    Attributes:
        destination (Optional[str]): Destination for the pipeline (default: "bigquery").
        progress (Optional[str]): Progress logging method.
    """
    destination: Optional[str] = "bigquery"
    progress: Optional[str]


class Pipeline(BaseModel):
    """
    Configuration for a pipeline consisting of multiple tasks.

    Attributes:
        common_config (Optional[CommonConfig]): Common configuration for the pipeline (default: CommonConfig).
        group_config (Optional[GroupConfig]): Group configuration for the pipeline (default: GroupConfig).
        tasks (List[Task]): List of tasks in the pipeline.
    """
    pipeline_name: Optional[str] = None
    destination_name: Optional[str] = None
    common_config: Optional[CommonConfig] = CommonConfig()
    group_config: Optional[GroupConfig] = GroupConfig()
    tasks: List[Task]


class DagConfig(BaseModel):
    """
    Base configuration for defining Airflow DAGs.

    Attributes:
        dag_id (str): Unique identifier for the DAG.
        schedule (str): Frequency at which the DAG should run or datasets that dag look for updating.
        description (Optional[str]): Description of the DAG (default: 'A DAG to pull data').
        render_template_as_native_obj (bool): Whether to render templates as native Python objects (default: True).
        default_args (Optional[DefaultArgs]): Default arguments for tasks in the DAG (default: DefaultArgs()).
        tags (List[str]): List of tags associated with the DAG (default: None).
        max_active_runs (int): Maximum number of active runs for the DAG (default: 1).
        catchup (bool): Whether to catch up on missed runs (default: False).
        start_date (Any): The start date for the DAG (default: 1 day ago).
        on_failure_callback (Any): Callback function to execute on task failure
            (default: send_slack_notification_on_failure).
        is_paused_upon_creation:  (bool | None) – Specifies if the dag is paused when created for the first time.
            If the dag exists already, this flag will be ignored.
            If this optional parameter is not specified, the global config setting will be used.
    """
    dag_id: str
    schedule: Optional[Union[str, list[Dataset]]] = None
    description: Optional[str] = 'A DAG to pull data'
    render_template_as_native_obj: bool = True
    default_args: Optional[DefaultArgs] = DefaultArgs()
    tags: List[str] = None
    max_active_runs: int = 1
    catchup: bool = False
    start_date: Any = pendulum.today("UTC").subtract(days=1)
    on_failure_callback: Any = None
    is_paused_upon_creation: bool = ENV != 'prod'

    class Config:
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def validate_dataset_schedule(cls, values):
        """
        A root validation method for the 'schedule' attribute.
        It converts a list of dataset strings into Dataset objects, each representing a distinct dataset.

        Args:
            cls: The class object. Required by Pydantic for root validators.
            values (dict): The values to be validated and potentially modified.

        Returns:
            dict: The validated and potentially modified dictionary of values.
        """

        schedule = values.get('schedule')

        if isinstance(schedule, list):
            dataset_schedule = []

            for dataset in schedule:
                dataset_schedule.append(Dataset(dataset))

            values['schedule'] = dataset_schedule

        return values


class Dag(BaseModel):
    """
    Base model for defining an Airflow DAG configuration.

    Attributes:
        dag_config (DagConfig): Core configuration of the DAG.
        account_id (Optional[str]): Optional account identifier related to the DAG.
        depends_on (Optional[Dict[str, DependsOnItems]]): Optional dependencies the DAG relies on.
        additional_fields (Optional[Dict[str, str]]): Any additional metadata associated with the DAG.
        tool (str): The tool used to define or manage the DAG (e.g., 'dbt', 'capi').
        outlet (Optional[bool]): Flag indicating whether this DAG is an outlet (default: False).
        detach_tests (Optional[bool]): Flag indicating that dbt tests must be rendered in parallel (default: False).
        source_tags (List[str]): List of tags identifying the DAG’s data sources.
        dag_type (Optional[str]): Specific type identifier for the DAG.
        pipeline (Optional[Pipeline]): The pipeline configuration associated with the DAG.
        transformers (Optional[Dict[str, Dict[str, Any]]]): Transformer configurations used in the DAG.
        pipeline_name (Optional[str]): Name of the pipeline (if not inferred).
        destination_name (Optional[str]): Name of the destination for pipeline output.
    """
    dag_config: DagConfig
    account_id: Optional[str] = None
    depends_on: Optional[Dict[str, DependsOnItems]] = None
    additional_fields: Optional[Dict[str, str]] = None
    tool: str
    outlet: Optional[bool] = False
    detach_tests: Optional[bool] = False
    source_tags: List[str] = None
    dag_type: Optional[str]
    pipeline: Optional[Pipeline] = None
    transformers: Optional[Dict[str, Dict[str, Any]]] = None

    @root_validator(pre=True)
    def validate_capi_logs_table(cls, values):
        """
            Validates and processes the fields in the 'capi_logs' table based on
            the values provided for 'tool', 'dag_type', and other configurations.

            This method dynamically modifies values for certain fields based on
            conditional logic, including:
            - 'pipeline': Initialized as `None` or validated based on the 'tool' field.
            - 'dag_config': Initialized as an instance of `CAPIDagConfig` or `DagConfig`
              based on the 'tool' field.
            - 'additional_fields': If 'dag_type' is `facebook_capi_pusher`, an instance
              of `CAPIAdditionalFields` is created; otherwise, the existing value is used.
            - 'transformers': The 'transformers' field is updated based on the environment,
              appending '-stage' if not in 'prod'.

            Args:
                cls: The class object. Required by Pydantic for root validators.
                values: A dictionary containing the values to be validated and modified.

            Returns:
                dict: The validated and potentially modified dictionary of values.
        """
        tool = values.get('tool')
        dag_type = values.get('dag_type')

        if tool == "dbt":
            values['pipeline'] = None
        else:
            values['pipeline'] = Pipeline(**values.get('pipeline'))
        values['dag_config'] = DagConfig(**values.get('dag_config'))
        if values.get('transformers') is not None:
            # values['transformers'] = values.get('transformers')
            for item in values['transformers']:
                values['transformers'][item]['name'] = (f"{values['transformers'][item]['name']}"
                                                f"{'-stage' if get_var('env', -1) != 'prod' else ''}")
        return values


class Config(BaseModel):
    """
    Configuration model for defining multiple DAGs.

    Attributes:
        dags (Optional[Dict[str, Dag]]): Dictionary of DAGs (default: None).
    """
    dags: Optional[Dict[str, Dag]] = None
