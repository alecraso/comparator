"""Base class for establishing connections to source databases
"""
import os

from abc import ABC, abstractmethod


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
