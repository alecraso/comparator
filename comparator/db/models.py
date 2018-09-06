"""Base classes for establishing connections to source databases
"""
import logging
import os

from abc import ABC, abstractmethod
from past.builtins import basestring
import sqlalchemy

_log = logging.getLogger(__name__)


uname = os.uname()[1]

DEFAULT_CONN_KWARGS = {
    'database': uname,
    'username': uname,
    'password': None,
    'host': 'localhost',
    'port': 5432
}


class BaseDb(ABC):
    _connected = False
    _conn = None
    _db_type = None
    _conn_kwargs = DEFAULT_CONN_KWARGS

    @property
    def connected(self):
        return self._connected

    @abstractmethod
    def _connect(self):
        """Connect to the source database

        Should set self._conn with the connection object
        """
        pass

    def connect(self):
        """Connect to the source database

        Returns:
            None
        """
        self._connect()
        if self._conn:
            self._connected = True

    @abstractmethod
    def _close(self):
        """Close any open connection
        """
        pass

    def close(self):
        """Close any open connection
        """
        if self._conn:
            self._close()
        self._conn = None
        self._connected = False

    @abstractmethod
    def query(self, query_string, **qwargs):
        """Runs a query against the source database

        If not connected, shoulc call self.connect() first

        Args:
            query_string : str - The query to run against the database

        Kwargs:
            qwargs : Arbitrary parameters to pass to the query engine

        Returns:
            list of tuples - The records returned from the database
        """
        pass


class PostgresDb(BaseDb):
    _db_type = 'postgres'

    def __init__(
            self,
            name=None, conn_string=None, conn_params={}, **conn_kwargs):
        """A standard Postgresql database client

        Kwargs:
            name : str - The canonical name to use for this instance
            conn_string : str - The connection url used to build the engine.
                                If provided, overrides any conn_kwargs.
            conn_params : dict - Parameters to pass to the connection
        """
        self._name = name

        if conn_string is not None:
            if not isinstance(conn_string, basestring):
                raise ValueError('conn_string kwarg must be a valid string')
            self._engine = sqlalchemy.create_engine(conn_string, **conn_params)
        else:
            for k, v in conn_kwargs.items():
                if k in self._conn_kwargs.keys():
                    self._conn_kwargs[k] = v
            url = sqlalchemy.engine.url.URL(self._db_type, **self._conn_kwargs)
            self._engine = sqlalchemy.create_engine(url, **conn_params)

    def __repr__(self):
        return '%s -- %r' % (self.__class__, self._engine.url)

    def __str__(self):
        if self._name is not None:
            return self._name
        return self._engine.url.host

    def _connect(self):
        self._conn = self._engine.connect()

    def _close(self):
        self._conn.close()

    def query(self, query_string, **qwargs):
        if not self._connected:
            self.connect()
        result = self._conn.execute(query_string, **qwargs)
        return result.fetchall()


class RedshiftDb(PostgresDb):
    _db_type = 'redshift+psycopg2'

    def __init__(self, *args, **kwargs):
        self._conn_kwargs['port'] = 5439
        super(RedshiftDb, self).__init__(*args, **kwargs)
