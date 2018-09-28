"""
    Convenience methods for loading database configs
"""
import logging
import os
import re
import yaml

from comparator.util import Path

_log = logging.getLogger(__name__)


DEFAULT_CONFIG_FILE = Path('~/.comparator/config.yaml').expanduser()


class DbConfig(object):
    """
        A class holding database configurations

        Configurations are loaded from a yaml file containing the connection details for each database.
        The file shuld be organized in the following way:

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
        TODO(aaronbiller): allow instantiation of BaseDb right from the config
    """
    _config = None
    _dbs = list()

    def __init__(self, config_file=None):
        if config_file is not None:
            _log.info('Setting config from provided path')
            self._set_config_from_file(config_file)

        if self._config is None:
            env_config_file = os.getenv('COMPARATOR_CONFIG_FILE', None)
            if env_config_file:
                _log.info('Setting config from environment variable')
                env_config_file = Path(env_config_file).expanduser().resolve()
                self._set_config_from_file(env_config_file)
            else:
                _log.warning('Environment variable not set, falling back')
                self._set_config_from_file(DEFAULT_CONFIG_FILE)

        if self._config is None:
            raise AttributeError('Could not find valid configuration file')
        else:
            _log.info('Found configuration file at %s', self._config.as_posix())
            self._load_dbs()

    def __repr__(self):
        return '%s -- %r' % (self.__class__, self._config)

    def __str__(self):
        return '%s: %r' % (self.__class__.__name__,
                           [db['name'] for db in self.dbs
                            if isinstance(db, dict) and
                            db.get('name')])

    @property
    def dbs(self):
        return self._dbs

    def _set_config_from_file(self, config_file):
        try:
            config = Path(config_file)
        except TypeError:
            _log.warning('Invalid path type provided, falling back : %r', config_file.__class__)
        else:
            config = config.expanduser().resolve()
            if not config.exists():
                _log.warning('Provided path does not exist, falling back : %s', config_file)
            else:
                self._config = config

    def _load_dbs(self):
        try:
            dbs = yaml.safe_load(self._config.read_text())
        except yaml.YAMLError as exc:
            msg = 'Error parsing config file as yaml'
            if hasattr(exc, 'problem_mark'):  # pragma: no cover
                mark = exc.problem_mark
                msg += ' at line {}, column {}'.format(mark.line, mark.column + 1)
            _log.warning(msg + ' : %s', self._config)
            return

        if not isinstance(dbs, list):
            dbs = [dbs]
        self._dbs = dbs

        for db in dbs:
            if not isinstance(db, dict):
                _log.warning('Misconfigured db, ignoring : %r', db)
                continue
            if not db.get('name'):
                _log.warning('Db has no name, ignoring : %r', db)
                continue

            cleaned_name = self._clean_db_name(db['name'])

            setattr(self, cleaned_name, db)

    def _clean_db_name(self, name):
        """
            Clean the db name before setting the attribute

            Make sure this name hasn't been set already, or that
            someone isn't being nasty and using something like
            __str__  as their db name. Replace non-alphanumeric
            characters with _.

            Would replace 'my_beAutiful --Db?' with 'my_beautiful_db'
        """
        cleaned_name = re.sub(r'[\W]+', '_', name).strip('_').lower()

        if hasattr(self, cleaned_name):
            i = 1
            cleaned_name += '_{}'.format(i)
            while hasattr(self, cleaned_name):
                i += 1
                cleaned_name = cleaned_name[:-1] + str(i)

        return cleaned_name
