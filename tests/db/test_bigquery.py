import mock
import os

try:
    from pathlib import Path
    Path().expanduser()
except (ImportError, AttributeError):
    from pathlib2 import Path

from comparator.db import BaseDb, BigQueryDb
from comparator.db.bigquery import BIGQUERY_DEFAULT_CONN_KWARGS

uname = os.uname()[1]
project = 'my-project'
expected_conn_kwargs = {'project': 'my-project', 'credentials': None, 'location': None}
query = 'select * from nowhere'
test_creds_path = Path.cwd().as_posix() + '/tests/configs/bq_creds.json'
test_creds_not_exist_path = Path.cwd().as_posix() + '/tests/configs/bq_creds_nope.json'


class MockBigQueryClient(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def set_creds_var(self):
        self.creds_var = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)

    def query(self, sql, **kwargs):
        self.query = query
        return self

    def result(self):
        return [[self.query]]


def test_bigquery():
    bq1 = BigQueryDb()

    assert isinstance(bq1, BaseDb)
    assert bq1._name is None
    assert bq1._db_type is None
    assert bq1.project is None

    assert repr(bq1) == "<class 'comparator.db.bigquery.BigQueryDb'> -- None"
    assert str(bq1) == 'BigQueryDb'


def test_with_kwargs():
    name = 'Alphabet'
    bq1 = BigQueryDb(name)
    assert str(bq1) == name

    bq2 = BigQueryDb(project=project)
    assert repr(bq2) == "<class 'comparator.db.bigquery.BigQueryDb'> -- '{}'".format(project)
    assert str(bq2) == project
    assert bq2.project == project
    assert bq2._conn_kwargs == expected_conn_kwargs

    bq3 = BigQueryDb()
    assert str(bq3) == 'BigQueryDb'
    bq3.project = project
    assert str(bq3) == project

    bq4 = BigQueryDb(genuflect='the-vatican-rag', locution='DE')
    assert bq4._conn_kwargs == BIGQUERY_DEFAULT_CONN_KWARGS


def test_connection():
    with mock.patch.dict('comparator.db.bigquery.os.environ', {'BIGQUERY_CREDS_FILE': ''}):
        bq = BigQueryDb(project=project)

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.connect()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.project == project

    results = bq.query(query)
    assert results == [(query,)]

    bq.close()
    assert not bq._conn
    assert bq.connected is False


def test_connection_with_env():
    try:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        pass

    with mock.patch.dict('comparator.db.bigquery.os.environ', {'BIGQUERY_CREDS_FILE': test_creds_path}):
        bq = BigQueryDb()

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
        bq._conn.set_creds_var()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.creds_var == test_creds_path


def test_connection_with_bad_env():
    try:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        pass

    with mock.patch.dict('comparator.db.bigquery.os.environ', {'BIGQUERY_CREDS_FILE': test_creds_not_exist_path}):
        bq = BigQueryDb()

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
        bq._conn.set_creds_var()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.creds_var is None


def test_query_without_connection():
    bq = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.query(query)
    assert bq._conn
    assert bq.connected is True


def test_df(mock_bq_query_result):
    bq = BigQueryDb()

    with mock.patch.object(bq, '_query', return_value=mock_bq_query_result):
        df = bq.query_df(query)

    assert len(df) == 3
    for i, col in enumerate(df.columns):
        assert list(mock_bq_query_result[0].keys())[i] == col
    for i, row in enumerate(df.itertuples(index=False)):
        assert list(mock_bq_query_result[i].values()) == list(row)
