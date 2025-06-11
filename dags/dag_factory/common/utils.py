"""This module contains functions for managing Airflow tasks."""
import logging

import subprocess
from typing import Callable, Any, Optional
import json
from time import time
from os import path
from pickle import dump, load

from airflow.models import Variable

from dag_factory.common.constants import ENV

logger = logging.getLogger('airflow')


def get_with_cache(fn: Callable = None, cache_path: str = None, expire_at_sec: int = 86400, *args, **kwargs) -> Any:  # pylint: disable=W1113
    """A function, intended to be used at top-level in a dag, to return database/networked results
    with the intent of building dynamic dags / tasks, while keeping scheduler degradation to a minimum.

    :param fn: A function, with optional arguments, receives *args
    :param expire_at_sec: Number of seconds after which cached results should be considered stale=
    default 1 day (86400 seconds), cache will be refreshed eventually after, but before this number
    :param cache_path: a string which is a file path to store the pickled object as a disk-based cache
    :return: the results of whichever function is given as "fn"
    """

    if cache_path is None or fn is None:
        raise RuntimeError("Function and cache path required, cache failed to load!")
    if expire_at_sec < 0 and path.isfile(cache_path):
        return load(open(cache_path, 'rb'))
    if expire_at_sec > 0 and path.isfile(cache_path) and int(time() - path.getmtime(cache_path)) < expire_at_sec:
        return load(open(cache_path, 'rb'))
    results = fn(*args, **kwargs)
    with open(cache_path, 'wb') as f:
        dump(results, f)
    return results


def get_var(name: str, expire_at_sec: int, default_value: Any = None) -> Any:
    return get_with_cache(
        Variable.get,
        f"{name}.cache",
        expire_at_sec=expire_at_sec,
        key=name,
        default_var=default_value,
    )


def name(name: str):
    return name if ENV == "prod" else f"{ENV}_" + name


def pause_all_dags():
    """Pause all DAGs using Airflow CLI."""
    try:
        # Get all DAG IDs
        result = subprocess.run(["airflow", "dags", "list"], capture_output=True, text=True, check=True)
        dag_lines = result.stdout.split("\n")[1:]  # Skip header row
        for line in dag_lines:
            parts = line.split()
            if parts:
                dag_id = parts[0]  # DAG ID is the first column
                subprocess.run(["airflow", "dags", "pause", dag_id], check=True)
                print(f"Paused DAG: {dag_id}")
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
