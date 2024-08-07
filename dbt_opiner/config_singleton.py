import os

import yaml


class ConfigSingleton:
    """
    Singleton class to store the configuration of the dbt-opiner package.
    This is initialized only once and the configuration is stored in the instance.
    Next time the class is called, the same instance is returned.
    Use config = ConfigSingleton().get_config() to get the configuration.
    """

    _instance = None
    _config = None
    _config_file_path = None

    def __new__(cls, root_dir=None):
        if cls._instance is None:
            if root_dir is None:
                raise ValueError(
                    "root_dir must be provided for the first initialization"
                )
            cls._instance = super(ConfigSingleton, cls).__new__(cls)
            cls._instance._initialize(root_dir)
        return cls._instance

    def _initialize(self, root_dir):
        self._config = self._find_config(root_dir)

    def _find_config(self, root_dir):
        for root, dirs, files in os.walk(root_dir):
            if "dbt-opiner.yaml" in files:
                self._config_file_path = os.path.join(root, "dbt-opiner.yaml")
                break
        if self._config_file_path:
            with open(self._config_file_path, "r") as file:
                return yaml.safe_load(file)
        else:
            raise FileNotFoundError(
                "Config file 'dbt-opiner.yaml' not found in the specified directory or its subdirectories."
            )

    def get_config(self):
        return self._config

    def get_config_file_path(self):
        return self._config_file_path
