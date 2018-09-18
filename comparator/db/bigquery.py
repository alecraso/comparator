"""
    Class for using Google BigQuery as a source database
"""
import logging
import os
import pandas as pd

try:  # pragma: no cover
    from pathlib import Path
    Path().expanduser()
except (ImportError, AttributeError):  # pragma: no cover
    from pathlib2 import Path

from google.cloud.bigquery import Client

from comparator.db.base import BaseDb

_log = logging.getLogger(__name__)


BIGQUERY_DEFAULT_CONN_KWARGS = {
    'project': None,
    'credentials': None,
    'location': None
}


class BigQueryDb(BaseDb):
    """
        A Google BigQuery database client

        Kwargs:
            name : str - The canonical name to use for this instance
            conn_kwargs : Use in place of a query string to set individual
                          attributes of the connection defaults (project, etc)
    """

    def __init__(self, name=None, **conn_kwargs):
        self._bq_creds_file = os.getenv('BIGQUERY_CREDS_FILE', None)
        self._conn_kwargs = dict(**BIGQUERY_DEFAULT_CONN_KWARGS)
        self._name = name
        for k, v in conn_kwargs.items():
            if k in self._conn_kwargs.keys():
                self._conn_kwargs[k] = v

    def __repr__(self):
        return '%s -- %r' % (self.__class__, self._conn_kwargs['project'])

    def __str__(self):
        if self._name is not None:
            return self._name
        elif self.project is not None:
            return self.project
        return self.__class__.__name__

    @property
    def project(self):
        return self._conn_kwargs['project']

    @project.setter
    def project(self, value):
        self._conn_kwargs['project'] = value

    def _connect(self):
        if self._bq_creds_file:
            if Path(self._bq_creds_file).exists():
                os.environ.setdefault(
                    'GOOGLE_APPLICATION_CREDENTIALS', self._bq_creds_file)
            else:
                _log.warning(
                    'Path set by BIGQUERY_CREDS_FILE does not exist: %s',
                    self._bq_creds_file)
        self._conn = Client(**self._conn_kwargs)
        self._connected = True

    def _close(self):
        """
            This is a no-op because the bigquery Client doesn't
            have a close method. The BaseDb close method will handle
            setting self._conn to None and self._connected to False.
        """
        return

    def _query(self, query_string):
        if not self._connected:
            self.connect()
        query_job = self._conn.query(query_string)
        return query_job.result()

    def query(self, query_string):
        results = self._query(query_string)
        return [
            tuple([col for col in row])
            for row in results]

    def query_df(self, query_string):
        results = self._query(query_string)

        columns = list(results[0].keys())
        data = [list(x.values()) for x in results]

        return pd.DataFrame(data=data, columns=columns)
