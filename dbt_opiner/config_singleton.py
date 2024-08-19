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

    def __new__(cls) -> "ConfigSingleton":
        """Initialize the singleton instance with the configuration from the dbt-opiner.yaml file.

        Args:
            root_dir: The directory to start the search for the dbt-opiner.yaml file.
        Returns:
            ConfigSingleton: The instance of the ConfigSingleton class.
        """

        if cls._instance is None:
            cls._instance = super(ConfigSingleton, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """Find the .dbt-opiner.yaml file in the .git root directory and subdirectories.
        Load it into the _config attribute.
        """
        logger.debug("Initializing ConfigSingleton")
        current_path = Path(os.getcwd()).resolve()
        while current_path != current_path.parent:
            if (current_path / ".git").exists():
                logger.debug(f"git root is: {current_path}")
                break
            current_path = current_path.parent

        for root, dirs, files in os.walk(current_path):
            dirs[:] = [
                d for d in dirs if d != ".venv"
            ]  # ignore .venv directory in the search
            if ".dbt-opiner.yaml" in files:
                self._config_file_path = Path(root) / ".dbt-opiner.yaml"

                break
        if self._config_file_path:
            with open(self._config_file_path, "r") as file:
                self._config = yaml.safe_load(file)
            logger.debug(f"Config file loaded from: {self._config_file_path}")
        else:
            self._config = {}
            logger.warning(
                "Config file 'dbt-opiner.yaml' not found. Empty configuration loaded."
            )

    def get_config(self):
        return self._config

    def get_config_file_path(self):
        return self._config_file_path
