"""
This module provides utilities to parse configurations from YAML files and
to search for a particular configuration item within a directory of YAML files.
"""
import logging
import os
from typing import Any, Optional, Dict
import yaml


logger = logging.getLogger('airflow')


def _read_config(path: str) -> dict[str, Any]:
    """
    Read configuration from a YAML file.

    :param path: Path to the configuration file.
    :return: A dictionary with the parsed YAML file contents.
    """
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return yaml.full_load(f)
    else:
        logger.info('File %s not found', path)
        return None


def _search_in_all_yaml_configs(item: str, config_dir: str) -> Optional[Dict]:
    """
    Downstream search for configuration with specified configuration item in directory with YAML configs.

    :param item: Specified configuration item.
    :param config_dir: Path to the configuration directory.
    :return: A dictionary with the parsed YAML file contents if the item is found, else None.
    """
    for root, _, files in os.walk(config_dir):
        for file in files:
            if file.endswith('.yaml'):
                config_path = os.path.join(root, file)
                config = _read_config(config_path)
                if item in config["dags"]:
                    return config["dags"][item]
    return None
