import tempfile
import inspect
import pathlib
import subprocess
import importlib.util
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
        self._ignored_opinions = self._config.get("global").get("ignore_opinions", [])

        # Load default opinions
        from dbt_opiner.opinions import opinion_classes

        for opinion_class in opinion_classes:
            if opinion_class.__name__ not in self._ignored_opinions:
                self._opinions.append(opinion_class())

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
        return [
            opinion
            for opinion in self._opinions
            if opinion.applies_to_file_type == file_type
            and opinion.applies_to_node_type == node_type
        ]

    def _load_custom_opinions(self):
        custom_opinions = []

        source = self._config.get("global").get("custom_opinions_source")
        if source == "local":
            path = (
                pathlib.Path(ConfigSingleton().get_config_file_path()).parent
                / "custom_opinions"
            )
            logger.debug(f"Loading custom opinions from local source: {path}")
            custom_opinions.extend(self._load_opinions_from_path(path))

        elif source == "git":
            # TODO figure out how to install dependencies that are in the custom opinions project
            # But not in the dbt_opiner project
            with tempfile.TemporaryDirectory() as temp_dir:
                git_repo = self._config.get("global").get("git_repository")
                subprocess.run(["git", "clone", git_repo, temp_dir], check=True)
                path = pathlib.Path(temp_dir) / "custom_opinions"
                logger.debug(f"Loading custom opinions from git repository: {git_repo}")
                custom_opinions.extend(self._load_opinions_from_path(path))

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
                    loaded_opinions.append(obj())
        return loaded_opinions
