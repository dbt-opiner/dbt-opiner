import yaml
from pathlib import Path
from threading import Lock


class ConfigHandler:
    _instance = None
    _lock = Lock()
    _config_path = None

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                if cls._config_path is None:
                    raise ValueError(
                        "Config path must be set before accessing the instance."
                    )
                cls._instance = super(ConfigHandler, cls).__new__(cls)
                cls._instance._initialize(cls._config_path)
        return cls._instance

    @classmethod
    def set_config_path(cls, config_path: Path):
        with cls._lock:
            if cls._instance is not None:
                raise RuntimeError(
                    "Config path cannot be set after the instance has been created."
                )
            cls._config_path = config_path

    def _initialize(self, config_path: Path):
        self.config_path = config_path
        self._config = None
        self._load_config()

    def _load_config(self):
        try:
            with self.config_path.open("r") as file:
                self._config = yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error reading YAML file: {e}")

    @property
    def config(self):
        return self._config
