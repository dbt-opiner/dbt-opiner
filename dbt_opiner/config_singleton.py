import os
from pathlib import Path

import yaml
from loguru import logger


class ConfigSingleton:
    """Store the configuration of the dbt-opiner package.
    This is initialized only once and the configuration is stored in the instance.
    Next time the class is called, the same instance is returned.
    Use config = ConfigSingleton().get_config() to get the configuration.
    """

    _instance = None
    _config = None
    _config_file_path = None

    def __new__(cls, root_dir: Path = None) -> "ConfigSingleton":
        """Initialize the singleton instance with the configuration from the dbt-opiner.yaml file.

        Args:
            root_dir: The directory to start the search for the dbt-opiner.yaml file.
        Returns:
            ConfigSingleton: The instance of the ConfigSingleton class.
        """

        if cls._instance is None:
            if root_dir is None:
                raise ValueError(
                    "Root directory is required to initialize the ConfigSingleton."
                )  # type: ignore[unreachable]
            cls._instance = super(ConfigSingleton, cls).__new__(cls)
            cls._instance._initialize(root_dir)
        return cls._instance

    def _initialize(self, root_dir: Path) -> None:
        """Find the dbt-opiner.yaml file in the specified directory or its subdirectories.

        Args:
            root_dir: The directory to start the search from.
        Returns:
            dict: The configuration from the .dbt-opiner.yaml file.
        """
        for root, dirs, files in os.walk(root_dir):
            if ".dbt-opiner.yaml" in files:
                self._config_file_path = os.path.join(root, ".dbt-opiner.yaml")
                break
        if self._config_file_path:
            with open(self._config_file_path, "r") as file:
                self._config = yaml.safe_load(file)
        else:
            self._config = {}
            logger.warning(
                "Config file 'dbt-opiner.yaml' not found. Empty configuration loaded."
            )

    def get_config(self):
        return self._config

    def get_config_file_path(self):
        return self._config_file_path
