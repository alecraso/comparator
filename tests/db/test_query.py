import datetime
import decimal
import json
import mock
import pandas as pd
import pytest

from collections import OrderedDict
from google.cloud.bigquery.table import RowIterator
from sqlalchemy.engine import ResultProxy

from comparator.db.query import QueryResult, QueryResultRow, DtDecEncoder

results = [OrderedDict([('a', 1), ('b', decimal.Decimal(2.0)), ('c', datetime.date(2018, 8, 1))]),
           OrderedDict([('a', 4), ('b', decimal.Decimal(5.0)), ('c', datetime.date(2018, 9, 1))]),
           OrderedDict([('a', 7), ('b', decimal.Decimal(8.0)), ('c', datetime.datetime(2018, 10, 1))])]
malformed_results = [OrderedDict([('a', 1), ('b', 2)]),
                     OrderedDict([('a', 3), ('c', 4)])]


def get_mock_iterator(spec, values):
    mock_result = mock.MagicMock(spec=spec)
    mock_result.__iter__.return_value = values
    return mock_result


def test_queryresult():
    with pytest.raises(TypeError):
        QueryResult('nope')

    itr = get_mock_iterator(RowIterator, malformed_results)
    with pytest.raises(AttributeError):
        QueryResult(itr)

    itr = get_mock_iterator(RowIterator, list())
    qr = QueryResult(itr)
    assert qr.keys() == list()
    assert bool(qr) is False

    itr = get_mock_iterator(ResultProxy, results)
    qr = QueryResult(itr)

    assert bool(qr) is True
    assert not qr == 3
    assert qr != 3
    assert len(qr) == 3
    for row in qr:
        assert isinstance(row, QueryResultRow)

    assert qr.a == (1, 4, 7)
    assert qr['a'] == (1, 4, 7)
    assert isinstance(qr[0], QueryResultRow)
    with pytest.raises(TypeError):
        qr[get_mock_iterator]

    expected_list = [(1, decimal.Decimal('2'), datetime.date(2018, 8, 1)),
                     (4, decimal.Decimal('5'), datetime.date(2018, 9, 1)),
                     (7, decimal.Decimal('8'), datetime.datetime(2018, 10, 1, 0, 0))]
    expected_json = json.dumps(results, cls=DtDecEncoder)
    expected_df = pd.DataFrame(results)
    expected_str = (
        "[OrderedDict([('a', 1), ('b', Decimal('2')), ('c', datetime.date(2018, 8, 1))]), "
        "OrderedDict([('a', 4), ('b', Decimal('5')), ('c', datetime.date(2018, 9, 1))]), "
        "OrderedDict([('a', 7), ('b', Decimal('8')), ('c', datetime.datetime(2018, 10, 1, 0, 0))])]")

    assert qr.list() == expected_list
    assert qr.values() == expected_list
    assert qr.json() == expected_json
    assert qr.df().equals(expected_df)
    assert str(qr) == expected_str

    expected_items = {'a': (1, 4, 7),
                      'b': (decimal.Decimal(2.0), decimal.Decimal(5.0), decimal.Decimal(8.0)),
                      'c': (datetime.date(2018, 8, 1), datetime.date(2018, 9, 1), datetime.datetime(2018, 10, 1))}
    for k, v in qr.items():
        assert expected_items[k] == v

    assert qr.get('a') == (1, 4, 7)
    assert qr.get(1) is None
    assert qr.get('d') is None


def test_queryresultrow():
    itr = get_mock_iterator(ResultProxy, results)
    qr = QueryResult(itr)
    qrr = qr[0]

    assert bool(qrr) is True

    assert qrr[0] == qrr.a == qrr['a'] == 1
    assert qrr[1] == qrr.b == qrr['b'] == decimal.Decimal(2.0)
    assert qrr[2] == qrr.c == qrr['c'] == datetime.date(2018, 8, 1)
    with pytest.raises(TypeError):
        qrr[get_mock_iterator]
    with pytest.raises(KeyError):
        qrr['d']

    assert not qrr == 3
    assert qrr != 3

    assert qrr.keys() == ['a', 'b', 'c']
    assert qrr.values() == (1, decimal.Decimal('2'), datetime.date(2018, 8, 1))
    assert str(qrr) == "(1, Decimal('2'), datetime.date(2018, 8, 1))"

    expected_items = {'a': 1, 'b': decimal.Decimal(2.0), 'c': datetime.date(2018, 8, 1)}
    for k, v in qrr.items():
        assert expected_items[k] == v

    assert qrr.get('a') == 1
    assert qrr.get(1) is None
    assert qrr.get('d') is None


def test_dtdecencoder():
    with pytest.raises(TypeError):
        json.dumps(results)

    expected_json = (
        '[{"a": 1, "b": 2.0, "c": "2018-08-01"}, '
        '{"a": 4, "b": 5.0, "c": "2018-09-01"}, '
        '{"a": 7, "b": 8.0, "c": "2018-10-01T00:00:00"}]')
    data = json.dumps(results, cls=DtDecEncoder, sort_keys=True)
    assert data == expected_json

    bad_json = {'a': 7, 'b': decimal.Decimal(8.0), 'c': datetime.datetime(2018, 10, 1), 'd': get_mock_iterator}
    with pytest.raises(TypeError):
        json.dumps(bad_json, cls=DtDecEncoder, sort_keys=True)
