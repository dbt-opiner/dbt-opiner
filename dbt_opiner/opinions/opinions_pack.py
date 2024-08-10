import importlib.util
import inspect
import pathlib
import subprocess
import sys
import tempfile

import pkg_resources
from loguru import logger

from dbt_opiner.config_singleton import ConfigSingleton
from dbt_opiner.opinions.base_opinion import BaseOpinion


class OpinionsPack:
    """
    Loads and holds all the opinions and the custom opinions.
    """

    def __init__(self):
        self._opinions = []
        self._config = ConfigSingleton().get_config()
        self._ignored_opinions = self._config.get("global", {}).get(
            "ignore_opinions", []
        )

        # Load default opinions
        from dbt_opiner.opinions import opinion_classes

        for opinion_class in opinion_classes:
            if opinion_class.__name__ not in self._ignored_opinions:
                self._opinions.append(opinion_class(config=self._config))

        # Load custom opinions
        self._opinions.extend(self._load_custom_opinions())

        logger.debug(
            f"Loaded opinions:\n{'\n'.join([opinion.code for opinion in self._opinions])}"
        )

    # Organize opinons by file and node type
    def get_opinions(self, file_type, node_type):
        """
        Get all the opinions that apply to a file and node type.
        """
        opinions = [
            opinion
            for opinion in self._opinions
            if opinion.applies_to_file_type == file_type
        ]

        if node_type:
            return [
                opinion
                for opinion in opinions
                if opinion.applies_to_node_type == node_type
            ]
        return opinions

    def _load_custom_opinions(self):
        custom_opinions = []

        source = self._config.get("global", {}).get("custom_opinions", {}).get("source")
        if not source:
            logger.info(
                "No custom opinions source defined. Skipping custom opinions loading."
            )
        elif source == "local":
            path = (
                pathlib.Path(ConfigSingleton().get_config_file_path()).parent
                / "custom_opinions"
            )
            logger.debug(f"Loading custom opinions from local source: {path}")
            custom_opinions.extend(self._load_opinions_from_path(path))

        elif source == "git":
            # TODO Use persistent directory path?
            with tempfile.TemporaryDirectory() as temp_dir:
                git_repo = (
                    self._config.get("global", {})
                    .get("custom_opinions", {})
                    .get("repository")
                )
                revision = (
                    self._config.get("global", {}).get("custom_opinions", {}).get("rev")
                )

                if not git_repo:
                    logger.critical(
                        "Custom opinions source is git but repository is not defined."
                    )
                    sys.exit(1)

                logger.debug(
                    f"Loading custom opinions from git repository: {git_repo}."
                )
                # TODO: handle git credentials for private repo as env variable
                try:
                    subprocess.run(
                        ["git", "clone", "--quiet", git_repo, temp_dir],
                        check=True,
                        stderr=subprocess.PIPE,
                    )
                    if revision:
                        # Revision is optional.
                        # If not provided default main branch will be used.
                        subprocess.run(
                            ["git", "reset", "--quiet", "--hard", revision],
                            check=True,
                            stderr=subprocess.PIPE,
                            cwd=temp_dir,
                        )
                except subprocess.CalledProcessError as e:
                    logger.critical(
                        f"Could not clone git repository: {git_repo}. Error: {e.stderr.decode('utf-8')}"
                    )
                    sys.exit(1)
                logger.debug(f"Cloned git repository: {git_repo} to {temp_dir}")
                path = pathlib.Path(temp_dir) / "custom_opinions"
                custom_opinions.extend(self._load_opinions_from_path(path))
        else:
            logger.warning(
                f"Custom opinions source {source} not supported. Skipping custom opinions loading."
            )

        return custom_opinions

    def _load_opinions_from_path(self, path):
        loaded_opinions = []
        for file in path.glob("*.py"):
            module_name = file.stem
            spec = importlib.util.spec_from_file_location(module_name, file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Only load BaseOpinion children classes
                if (
                    name not in self._ignored_opinions
                    and issubclass(obj, BaseOpinion)
                    and obj is not BaseOpinion
                ):
                    logger.debug(f"Found class {name} in {file}")

                    # Install required packages for the opinion.
                    # We do it like this because:
                    #  - there are different ways of defining packages in python projects
                    #  - if an opinion is ignored and not loaded, we don't want to install the packages
                    for package_name in obj.required_packages:
                        logger.debug(
                            f"Checking if package {package_name} is installed."
                        )
                        try:
                            pkg_resources.get_distribution(package_name)
                        except pkg_resources.DistributionNotFound:
                            logger.debug(
                                f"Package {package_name} not found. Installing."
                            )
                            subprocess.check_call(
                                [sys.executable, "-m", "pip", "install", package_name]
                            )
                    # Inject the config to the opinion
                    loaded_opinions.append(obj(config=self._config))
        return loaded_opinions
