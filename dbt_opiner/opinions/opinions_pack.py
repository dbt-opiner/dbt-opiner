import importlib
import inspect
import pathlib
import shutil
import subprocess
import sys

from loguru import logger

from dbt_opiner import config_singleton
from dbt_opiner import git
from dbt_opiner.opinions import base_opinion


class OpinionsPack:
    """
    Loads and holds all the opinions and the custom opinions.
    """

    def __init__(self, no_ignore: bool = False) -> None:
        """
        Args:
            no_ignore: Flag to supress the noqa ignored opinions. Defaults to False.
        """
        self._opinions = []
        self._config = config_singleton.ConfigSingleton().get_config()
        self._ignored_opinions = self._config.get("opinions_config", {}).get(
            "ignore_opinions", []
        )
        if no_ignore:
            self._ignored_opinions = []

        # Load default opinions
        from dbt_opiner.opinions import opinion_classes

        for opinion_class in opinion_classes:
            if opinion_class.__name__ not in self._ignored_opinions:
                self._opinions.append(opinion_class(config=self._config))

        # Load custom opinions
        self._load_custom_opinions()
        opinions_str = "\n".join([opinion.code for opinion in self._opinions])
        logger.debug(f"Loaded opinions:\n{opinions_str}")

    def get_opinions(self):
        """Returns all the loaded opinions."""
        return [opinion for opinion in self._opinions]

    def _load_custom_opinions(self):
        source = (
            self._config.get("opinions_config", {})
            .get("custom_opinions", {})
            .get("source")
        )

        if not source:
            logger.info(
                "No custom opinions source defined. Skipping custom opinions loading."
            )
            return

        custom_opinions = []
        if source == "local":
            path = (
                pathlib.Path(
                    config_singleton.ConfigSingleton().get_config_file_path()
                ).parent
                / "custom_opinions"
            )
            logger.debug(f"Loading custom opinions from local source: {path}")
            custom_opinions.extend(self._load_opinions_from_path(path))
        elif source == "git":
            custom_opinions.extend(self._load_opinions_from_git())
        else:
            logger.warning(
                f"Custom opinions source {source} not supported. Skipping custom opinions loading."
            )
            return

        return self._opinions.extend(custom_opinions)

    def _load_opinions_from_path(self, path):
        loaded_opinions = []
        for file in path.glob("*.py"):
            logger.debug(file)
            module_name = file.stem
            spec = importlib.util.spec_from_file_location(module_name, file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Only load BaseOpinion children classes
                if (
                    name not in self._ignored_opinions
                    and issubclass(obj, base_opinion.BaseOpinion)
                    and obj is not base_opinion.BaseOpinion
                ):
                    logger.debug(f"Found class {name} in {file}")

                    # Install required packages for the opinion.
                    # We do it like this because:
                    #  - there are different ways of defining packages in python projects
                    #  - if an opinion is ignored and not loaded, we don't want to install the packages
                    try:
                        logger.debug(f"Checking required packages for opinion {name}")
                        for package_name in obj.required_packages:
                            logger.debug(
                                f"Checking if package {package_name} is installed."
                            )
                            try:
                                importlib.metadata.version(package_name)
                            except importlib.metadata.PackageNotFoundError:
                                logger.debug(
                                    f"Package {package_name} not found. Installing."
                                )
                                subprocess.run(
                                    [
                                        sys.executable,
                                        "-m",
                                        "pip",
                                        "install",
                                        package_name,
                                    ]
                                )
                    except AttributeError:
                        logger.debug(f"No required packages for opinion {name}")
                    # Inject the config to the opinion
                    loaded_opinions.append(obj(config=self._config))
        return loaded_opinions

    def _load_opinions_from_git(self):
        git_repo = (
            self._config.get("opinions_config", {})
            .get("custom_opinions", {})
            .get("repository")
        )
        revision = (
            self._config.get("opinions_config", {})
            .get("custom_opinions", {})
            .get("rev")
        )

        if not git_repo:
            logger.critical(
                "Custom opinions source is git but repository is not defined."
            )
            sys.exit(1)

        logger.debug(f"Loading custom opinions from git repository: {git_repo}.")
        temp_dir = git.clone_git_repo_and_checkout_revision(git_repo, revision)
        path = temp_dir / "custom_opinions"
        opinions = self._load_opinions_from_path(path)
        shutil.rmtree(temp_dir)  # Clean up the temporary directory
        return opinions
