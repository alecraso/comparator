"""
    Convenience methods for loading database configs
"""
import logging
import os
import pathlib
import yaml

_log = logging.getLogger(__name__)


DEFAULT_CONFIG_FILE = pathlib.Path('~/.comparator/config.yaml').expanduser()

env_config_file = os.getenv('COMPARATOR_CONFIG_FILE', None)
if env_config_file is not None:
    env_config_file = pathlib.Path(env_config_file).expanduser().resolve()


class DbConfig(object):
    """
        A class holding database configurations

        Configurations are loaded from a yaml file containing
        the connection details for each database. The file shuld be
        organized in the following way:

        - name: "default"
          username: "postgres"
          password: "mysecretpassword"
          host: "localhost"
          port: "5432"
          database: "postgres"
        - name: "my_other_db"
          username: "dbuser"
          ...

        This allows the config object to be used like so:

        from comparator.config import DbConfig
        from comparator.db import PostgresDb

        dbconfig = DbConfig()
        pg = PostgresDb(**dbconfig.default)


        Kwargs:
            config_file : str - The path of the config file to load

        TODO(aaronbiller): allow config to create config file and write to it
    """
    _config = None
    _dbs = list()

    def __init__(self, config_file=None):
        if config_file is not None:
            _log.info('Setting config from provided path')
            self._set_config_from_file(config_file)

        if self._config is None:
            if env_config_file is not None:
                _log.info('Setting config from environment variable')
                self._set_config_from_file(env_config_file)
            else:
                _log.warn('Environment variable not set, falling back')
                self._set_config_from_file(DEFAULT_CONFIG_FILE)

        if self._config is None:
            raise AttributeError('Could not find valid configuration file')
        else:
            self._load_dbs()

    def _set_config_from_file(self, config_file):
        try:
            config = pathlib.Path(config_file)
        except TypeError:
            _log.warn(
                'Invalid path type provided, falling back : %r',
                config_file.__class__)
        else:
            config = config.expanduser().resolve()
            if not config.exists():
                _log.warn(
                    'Provided path does not exist, falling back : %s',
                    config_file)
            else:
                self._config = config

    def _load_dbs(self):
        try:
            dbs = yaml.safe_load(config.read_text())
        except yaml.YAMLError as exc:
            msg = 'Error parsing config file as yaml'
            if hasattr(exc, 'problem_mark'):
                mark = exc.problem_mark
                msg += f' at line {mark.line}, column {mark.column + 1}'
            _log.warn(msg + ' : %s', self._config)
            return

        if not isinstance(dbs, list):
            dbs = [dbs]
        self._dbs = dbs

        for db in dbs:
            if not isinstance(db, dict):
                _log.warn('Misconfigured db, ignoring : %r', db)
                continue
            if not db.hasattr('name'):
                _log.warn('Db has no name, ignoring : %r', db)

            name = db['name']
            setattr(self, name, db)
