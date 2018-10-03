import mock
import os

from google.cloud.bigquery.table import Row, RowIterator

from comparator.db.base import BaseDb
from comparator.db.bigquery import BigQueryDb, BIGQUERY_DEFAULT_CONN_KWARGS
from comparator.util import Path

uname = os.uname()[1]
project = 'my-project'
expected_conn_kwargs = {'project': 'my-project', 'credentials': None, 'location': None}
query = 'select * from nowhere'
test_creds_path = Path.cwd().as_posix() + '/tests/configs/bq_creds.json'
test_creds_not_exist_path = Path.cwd().as_posix() + '/tests/configs/bq_creds_nope.json'
expected_query_results = [{'first': 'a', 'second': 'b', 'third': 'c'},
                          {'first': 'd', 'second': 'e', 'third': 'f'},
                          {'first': 'g', 'second': 'h', 'third': 'i'}]


class MockBigQueryTable(object):
    def __init__(self, dataset):
        self.dataset = dataset

    @property
    def table_id(self):
        return '{}.table'.format(self.dataset)


class MockBigQueryClient(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def set_creds_var(self):
        self.creds_var = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)

    def query(self, sql, **kwargs):
        self._query = query
        return self

    def result(self):
        result = mock.MagicMock(spec=RowIterator)
        result.__iter__.return_value = [
            Row(['a', 'b', 'c'], {'first': 0, 'second': 1, 'third': 2}),
            Row(['d', 'e', 'f'], {'first': 0, 'second': 1, 'third': 2}),
            Row(['g', 'h', 'i'], {'first': 0, 'second': 1, 'third': 2}),
        ]
        return result

    def dataset(self, dataset):
        self._dataset = dataset
        return self

    def list_tables(self, mbqc):
        return [MockBigQueryTable(mbqc._dataset) for i in range(3)]

    def table(self, table_id):
        self._table = table_id
        return table_id

    def delete_table(self, table_id):
        return None


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
    assert results.result == expected_query_results

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


def test_connection_with_passed():
    try:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        pass

    bq = BigQueryDb(creds_file=test_creds_path)
    assert bq._bq_creds_file == test_creds_path
    assert bq._conn_kwargs == BIGQUERY_DEFAULT_CONN_KWARGS

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
        bq._conn.set_creds_var()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.creds_var == test_creds_path


def test_query_without_connection():
    bq = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.query(query)
    assert bq._conn
    assert bq.connected is True


def test_query_vs_execute():
    bq = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        res = bq.query(query)
    assert res is not None

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        res = bq.execute(query)
    assert res is None


def test_list_tables():
    bq = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
    tables = bq.list_tables('my_dataset')
    assert tables == ['my_dataset.table'] * 3

    bq1 = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        tables1 = bq1.list_tables('my_dataset')
    assert tables1 == ['my_dataset.table'] * 3
    assert bq1.connected is True


def test_delete_table():
    bq = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
    res = bq.delete_table('my_dataset', 'my_table')
    assert res is None
    assert bq._conn._dataset == 'my_dataset'
    assert bq._conn._table == 'my_table'

    bq1 = BigQueryDb()

    with mock.patch('comparator.db.bigquery.Client', MockBigQueryClient):
        bq1.delete_table('my_dataset', 'my_table')
    assert bq1.connected is True
