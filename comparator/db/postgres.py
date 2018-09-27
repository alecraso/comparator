"""
    Class for using Postgres as a source database
"""
import sqlalchemy

from past.builtins import basestring

from comparator.db.base import BaseDb, DEFAULT_CONN_KWARGS
from comparator.db.query import DbQueryResult


class PostgresDb(BaseDb):
    """
        A standard Postgresql database client

        Kwargs:
            name : str - The canonical name to use for this instance
            conn_string : str - The connection url used to build the engine.
                                If provided, overrides any conn_kwargs.
            conn_params : dict - Parameters to pass to the connection
            conn_kwargs : Use in place of a query string to set individual
                          attributes of the connection defaults
                          (host, user, etc)
    """
    _db_type = 'postgresql'

    def __init__(
            self,
            name=None, conn_string=None, conn_params={}, **conn_kwargs):
        self._name = name

        if conn_string is not None:
            if not isinstance(conn_string, basestring):
                raise ValueError('conn_string kwarg must be a valid string')
            self._engine = sqlalchemy.create_engine(
                conn_string, connect_args=conn_params)
        else:
            self._conn_kwargs = dict(**DEFAULT_CONN_KWARGS)
            for k, v in conn_kwargs.items():
                if k in self._conn_kwargs.keys():
                    self._conn_kwargs[k] = v
            url = sqlalchemy.engine.url.URL(self._db_type, **self._conn_kwargs)
            self._engine = sqlalchemy.create_engine(
                url, connect_args=conn_params)

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

    def query(self, query_string, **kwargs):
        if not self._connected:
            self.connect()
        result = self._conn.execute(query_string, **kwargs)
        return DbQueryResult(result)
