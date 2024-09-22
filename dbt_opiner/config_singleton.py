import os
import re
import shutil
import sys
from pathlib import Path

import yaml
from loguru import logger

from dbt_opiner.git import clone_git_repo_and_checkout_revision


class ConfigSingleton:
    """Store the configuration of the dbt-opiner package.
    This is initialized only once and the configuration is stored in the instance.
    Next time the class is called, the same instance is returned.
    Use config = ConfigSingleton().get_config() to get the configuration.
    """

    _instance = None
    _config = None
    _config_file_path = None
    # Define the schema for the configuration file
    # The schema is a dictionary where the key is the name of the configuration key
    # and the value is a tuple with the expected type and whether the key is optional.
    _config_schema = {
        "sqlglot_dialect": (str, True),
        "shared_config": (
            {
                "repository": (str, False),
                "rev": (str, True),
                "overwrite": (bool, True),
            },
            True,
        ),
        "opinions_config": (
            {
                "ignore_opinions": (str, True),
                "ignore_files": (dict, True),
                "extra_opinions_config": (dict, True),
                "custom_opinions": (
                    {
                        "source": (str, False),
                        "repository": (str, False),
                        "rev": (str, True),
                    },
                    True,
                ),
            },
            True,
        ),
        "files": (
            {
                "sql": (str, True),
                "yaml": (str, True),
                "md": (str, True),
            },
            True,
        ),
    }

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

        # Search for the config file in the git repository
        self._config_file_path = self._search_config_file(current_path)

        if self._config_file_path:
            # Load configuration
            config = self._load_config_from_file(self._config_file_path)
            logger.critical(config)

            # Check for shared configuration defnition
            if config.get("shared_config"):
                config = self._get_shared_config(config)

            # Validate final configuration
            is_valid, validation_error = self._validate_config(
                config, self._config_schema
            )
            if is_valid:
                self._config = config
                logger.debug(f"Config file loaded from: {self._config_file_path}")
            else:
                logger.critical(
                    "Configuration file is not valid. "
                    f"{validation_error}. "
                    "Please check the configuration documentation and adjust accordingly."
                )
                sys.exit(1)
        else:
            self._config = {}
            logger.warning(
                "Config file 'dbt-opiner.yaml' not found. Empty configuration loaded."
            )

    @staticmethod
    def _load_config_from_file(file_path: Path) -> dict:
        """Load the configuration from the dbt-opiner.yaml file and replace environment variables.

        Returns: The configuration dictionary with environment variables replaced.
        """
        # Add environment variables to the configuration
        with open(file_path, "r") as file:
            config_content = file.read()
        env_vars = re.findall(r"\$\{\s*(.*?)\s*\}", config_content)
        for match in env_vars:
            env_var_value = os.getenv(match, "")
            config_content = re.sub(
                r"\$\{\s*" + re.escape(match) + r"\s*\}",
                env_var_value,
                config_content,
            )
        return yaml.safe_load(config_content)

    def _search_config_file(self, root_dir: Path) -> Path:
        """Search for the dbt-opiner.yaml file in the root directory and subdirectories.
        Args:
            root_dir: The directory to start the search for the dbt-opiner.yaml file.
        Returns: The path to the dbt-opiner.yaml file.
        """
        current_path = root_dir

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
                return Path(root) / ".dbt-opiner.yaml"

    def _validate_config(self, config, schema) -> tuple[bool, str]:
        """Validates the dictionary structure based on the provided schema.
        Args:
          config: The config dictionary to validate.
        Returns True if valid, else False. An error string describing the error if invalid.
        """
        if not isinstance(config, dict):
            return False, f"Expected a dictionary but got {type(config).__name__}"

        for key, (expected_type, optional) in schema.items():
            if key not in config:
                if optional:
                    continue  # Skip optional keys if they're missing
                return False, f"Missing required key: {key}"

            value = config[key]
            if isinstance(expected_type, dict):
                if not isinstance(value, dict):
                    return (
                        False,
                        f"Expected {key} to be a dictionary but got {type(value).__name__}",
                    )
                is_valid, error = self._validate_config(value, expected_type)
                if not is_valid:
                    return False, error
            else:
                if not isinstance(value, expected_type):
                    return (
                        False,
                        f"Expected {key} to be of type {expected_type.__name__}, but got {type(value).__name__}",
                    )

        for key in config:
            if key not in schema:
                return False, f"Unexpected key: {key}"

        return True, None

    def _get_shared_config(self, original_config):
        """Load the shared configuration from a git repository."""

        git_repo = original_config["shared_config"]["repository"]
        revision = original_config["shared_config"].get("rev")
        temp_dir = clone_git_repo_and_checkout_revision(git_repo, revision)
        shared_config_path = self._search_config_file(temp_dir)
        if shared_config_path:
            shared_config = self._load_config_from_file(shared_config_path)
        else:
            shared_config = {}
        shutil.rmtree(temp_dir)

        if (
            "overwrite" not in original_config["shared_config"]
            or original_config["shared_config"]["overwrite"]
        ):
            return shared_config
        else:
            return self._merge_configs(original_config, shared_config)

    def get_config(self):
        return self._config

    def get_config_file_path(self):
        return self._config_file_path

    def _merge_configs(self, original, new):
        """
        Recursively merges 'new' dictionary into 'original' dictionary, but preserves
        existing values in 'original'.

        Args:
        original (dict): The original configuration dictionary.
        new (dict): The new configuration dictionary to merge into the original.

        Returns:
        The updated original dictionary with new configurations merged.
        """
        for key, value in new.items():
            if key in original:
                if isinstance(value, dict) and isinstance(original[key], dict):
                    self._merge_configs(original[key], value)
            else:
                original[key] = value
        return original
