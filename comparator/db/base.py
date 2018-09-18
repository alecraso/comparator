"""
    Base class for establishing connections to source databases
"""
from __future__ import unicode_literals

import abc
import os
import sys

if sys.version_info >= (3, 4):  # pragma: no cover
    ABC = abc.ABC
else:  # pragma: no cover
    ABC = abc.ABCMeta(str('ABC'), (), {})


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
    _conn_kwargs = None

    @property
    def connected(self):
        return self._connected

    @abc.abstractmethod
    def _connect(self):
        """
            Connect to the source database

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

    @abc.abstractmethod
    def _close(self):
        """
            Close any open connection
        """
        pass

    def close(self):
        """Close any open connection
        """
        if self._conn:
            self._close()
        self._conn = None
        self._connected = False

    @abc.abstractmethod
    def query(self, query_string, **kwargs):
        """
            Runs a query against the source database

            If not connected, should call self.connect() first

            Args:
                query_string : str - The query to run against the database

            Kwargs:
                kwargs : Arbitrary parameters to pass to the query engine

            Returns:
                list of tuples - The records returned from the database
        """
        pass

    @abc.abstractmethod
    def query_df(self, query_string, **kwargs):
        """
            Runs a query against the source database and
            returns a pandas DataFrame

            If not connected, should call self.connect() first

            Args:
                query_string : str - The query to run against the database

            Kwargs:
                kwargs : Arbitrary parameters to pass to the query engine

            Returns:
                pandas DataFrame
        """
        pass
