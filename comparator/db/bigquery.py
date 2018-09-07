"""Class for using Google BigQuery as a source database
"""
import os

from google.cloud.bigquery import Client

from comparator.db.base import BaseDb

BIGQUERY_CREDS_FILE = os.getenv('BIGQUERY_CREDS_FILE', None)
BIGQUERY_DEFAULT_CONN_KWARGS = {
    'project': None,
    'credentials': None,
    'location': None
}


class BigQueryDb(BaseDb):
    """A Google BigQuery database client

    Kwargs:
        name : str - The canonical name to use for this instance
        conn_kwargs : Use in place of a query string to set individual
                      attributes of the connection defaults (project, etc)
    """
    _conn_kwargs = BIGQUERY_DEFAULT_CONN_KWARGS
    _db_type = None

    def __init__(self, name=None, **conn_kwargs):
        self._name = name
        for k, v in conn_kwargs.items():
            if k in self._conn_kwargs.keys():
                self._conn_kwargs[k] = v

    def __repr__(self):
        return '%s -- %r' % (self.__class__, self._conn_kwargs['project'])

    def __str__(self):
        if self._name is not None:
            return self._name
        return self._conn_kwargs.get('project', self.__class__.__name__)

    @property
    def project(self):
        return self._conn_kwargs['project']

    @project.setter
    def project(self, value):
        self._conn_kwargs['project'] = value

    def _connect(self):
        if BIGQUERY_CREDS_FILE:
            os.environ.setdefault(
                'GOOGLE_APPLICATION_CREDENTIALS', BIGQUERY_CREDS_FILE)
        self._conn = Client(**self._conn_kwargs)
        self._connected = True

    def _close(self):
        return

    def query(self, query_string, **qwargs):
        if not self._connected:
            self.connect()
        query_job = self._conn.query(query_string)
        return [
            tuple([col for col in row])
            for row in query_job.result()]