"""Config Reader."""

import yaml


class ConfigReader:
    """Read Config File."""

    _config = None
    _config_path = None

    def set_config_path(self, config_path):
        """
        Set Config File Path.

        :param config_path:string, Config File Path
        """
        self._config_path = config_path

    def _get_config(self):
        """Get Handle to config file."""
        if self._config is None:
            self._config = yaml.load(open(self._config_path))
        return self._config

    def get_config_from_file(self, key):
        """
        Read Config based on key.

        :param key:string, Key to read from config
        :return:dict, Dictionary of key/pair relative to key form config file
        """
        config = self._get_config()

        return config.get(key, None)
