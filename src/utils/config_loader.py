"""
Configuration Loader Module

This module is responsible for loading configuration settings for the project.
It provides functions to load settings from YAML files or Python modules and manage environment-specific configurations.
"""

import os
import yaml
import importlib.util
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Union

from config.settings import logger

class ConfigLoader:
    """
    A class for loading and managing configuration settings.

    This class provides methods to load configuration from YAML files,
    with support for environment-specific overrides.
    """

    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the ConfigLoader.

        Args:
            config_dir: Directory containing configuration files. Defaults to None.
        """
        if config_dir is None:
            # Default to the config directory in the project root
            self.config_dir = Path(__file__).parent.parent.parent / "config"
        else:
            self.config_dir = Path(config_dir)

        # Ensure the config directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)

        # Initialize empty configuration
        self.config = {}

        # Get environment
        self.env = os.environ.get("APP_ENV", "development")
        logger.info(f"Environment: {self.env}")

    def load_config(self, filename: str = "settings.py") -> Dict[str, Any]:
        """
        Load configuration from a file.

        Args:
            filename: Name of the configuration file. Defaults to "settings.py".

        Returns:
            Dict[str, Any]: Loaded configuration.
        """
        config_path = self.config_dir / filename

        # Load default configuration
        if config_path.exists():
            try:
                # Determine file type by extension
                if filename.endswith('.py'):
                    self.config = self._load_from_python_module(config_path)
                elif filename.endswith('.yaml') or filename.endswith('.yml'):
                    self.config = self._load_from_yaml(config_path)
                else:
                    logger.warning(f"Unsupported file type for {config_path}, trying to load as YAML")
                    self.config = self._load_from_yaml(config_path)

                logger.info(f"Loaded configuration from {config_path}")
            except Exception as e:
                logger.error(f"Error loading configuration from {config_path}: {e}")
                self.config = {}
        else:
            logger.warning(f"Configuration file {config_path} not found, using empty configuration")
            self.config = {}

        # Load environment-specific configuration if available
        base_name, ext = os.path.splitext(filename)
        env_config_path = self.config_dir / f"{self.env}_{base_name}{ext}"

        if env_config_path.exists():
            try:
                # Load environment-specific config using the same method as the main config
                if filename.endswith('.py'):
                    env_config = self._load_from_python_module(env_config_path)
                elif filename.endswith('.yaml') or filename.endswith('.yml'):
                    env_config = self._load_from_yaml(env_config_path)
                else:
                    env_config = self._load_from_yaml(env_config_path)

                # Merge environment-specific configuration with default configuration
                self._deep_update(self.config, env_config)
                logger.info(f"Loaded environment-specific configuration from {env_config_path}")
            except Exception as e:
                logger.error(f"Error loading environment-specific configuration from {env_config_path}: {e}")

        return self.config

    def _load_from_yaml(self, file_path: Path) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.

        Args:
            file_path: Path to the YAML file.

        Returns:
            Dict[str, Any]: Loaded configuration.
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    def _load_from_python_module(self, file_path: Path) -> Dict[str, Any]:
        """
        Load configuration from a Python module.

        Args:
            file_path: Path to the Python module.

        Returns:
            Dict[str, Any]: Loaded configuration.
        """
        module_name = file_path.stem
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load module from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # Look for CONFIG variable in the module
        if hasattr(module, 'CONFIG'):
            return module.CONFIG
        else:
            logger.warning(f"No CONFIG variable found in {file_path}, using empty configuration")
            return {}

    def _deep_update(self, d: Dict[str, Any], u: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively update a dictionary with another dictionary.

        Args:
            d: Dictionary to update.
            u: Dictionary with updates.

        Returns:
            Dict[str, Any]: Updated dictionary.
        """
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v
        return d

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.

        Args:
            key: Configuration key.
            default: Default value if key is not found. Defaults to None.

        Returns:
            Any: Configuration value.
        """
        # Support nested keys with dot notation (e.g., "database.host")
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def create_default_config(self, file_type: str = "py") -> None:
        """
        Create a default configuration file if it doesn't exist.

        Args:
            file_type: Type of configuration file to create ('py' or 'yaml'). Defaults to 'py'.
        """
        if file_type.lower() == "py":
            default_config_path = self.config_dir / "settings.py"
        else:
            default_config_path = self.config_dir / "settings.yaml"

        if not default_config_path.exists():
            default_config = {
                "database": {
                    "path": "data/job_data.duckdb"
                },
                "crawler": {
                    "output_dir": "data/raw_data",
                    "keywords": ["Python", "Data Analyst", "Data Scientist", "Machine Learning"],
                    "max_pages": 5,
                    "schedule": {
                        "daily_crawl": {
                            "hour": 1,
                            "minute": 0
                        }
                    }
                },
                "analysis": {
                    "trend_days": 90,
                    "min_keyword_count": 10
                },
                "visualization": {
                    "theme": "streamlit",
                    "default_chart_height": 500,
                    "default_chart_width": 800,
                    "color_palette": "default"
                }
            }

            try:
                if file_type.lower() == "py":
                    # Create Python configuration file
                    with open(default_config_path, 'w', encoding='utf-8') as f:
                        f.write('"""\n')
                        f.write('104 Job Data Insight Platform Configuration\n')
                        f.write('"""\n\n')
                        f.write('# Configuration dictionary\n')
                        f.write('CONFIG = ')
                        f.write(str(default_config).replace("'", '"'))
                    logger.info(f"Created default Python configuration file at {default_config_path}")
                else:
                    # Create YAML configuration file
                    with open(default_config_path, 'w', encoding='utf-8') as f:
                        yaml.dump(default_config, f, default_flow_style=False, allow_unicode=True)
                    logger.info(f"Created default YAML configuration file at {default_config_path}")
            except Exception as e:
                logger.error(f"Error creating default configuration file: {e}")

# Example usage
if __name__ == "__main__":
    config_loader = ConfigLoader()
    # Create default Python configuration file
    config_loader.create_default_config(file_type="py")
    # Load configuration from Python file
    config = config_loader.load_config(filename="settings.py")

    # Access configuration values
    db_path = config_loader.get("database.path", "data/job_data.duckdb")
    print(f"Database path: {db_path}")

    keywords = config_loader.get("crawler.keywords", [])
    print(f"Crawler keywords: {keywords}")

    # You can also load from YAML if needed
    # config_loader.create_default_config(file_type="yaml")
    # config = config_loader.load_config(filename="settings.yaml")
