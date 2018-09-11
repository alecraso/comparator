"""
    Class for using Google BigQuery as a source database
"""
import logging
import os

try:
    from pathlib import Path
    Path().expanduser()  # pragma: no cover
except (ImportError, AttributeError):
    from pathlib2 import Path  # pragma: no cover

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

    def query(self, query_string, **qwargs):
        if not self._connected:
            self.connect()
        query_job = self._conn.query(query_string)
        return [
            tuple([col for col in row])
            for row in query_job.result()]
