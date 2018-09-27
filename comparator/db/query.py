"""
    Base classes to house the results of queries against a source databse
"""
from __future__ import unicode_literals

import copy
import datetime
import decimal
import json
import pandas as pd
import six

from google.cloud.bigquery.table import RowIterator
from sqlalchemy.engine import ResultProxy

from comparator.util import ABC


class DtDecEncoder(json.JSONEncoder):
    def default(self, obj):
        if (
                isinstance(obj, datetime.date) or
                isinstance(obj, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return super(DtDecEncoder, self).default(obj)


class QueryResultRow(object):
    def __init__(self, keys, row):
        self._keys = keys
        self._row = row

    def __getattr__(self, name):
        value = self._row.get(name)
        if value is None:
            raise AttributeError('Not found : %r' % name)
        return value

    def __getitem__(self, key):
        if isinstance(key, six.string_types):
            if key not in self._keys:
                raise KeyError('Not found : %r' % key)
            value = self._row[key]
        elif isinstance(key, int):
            k = self._keys[key]
            value = self._row[k]
        return value

    def __eq__(self, other):
        if not isinstance(other, QueryResultRow):
            return NotImplemented
        return self._row == other._row

    def __ne__(self, other):
        return not self == other

    def values(self):
        return self._row.values()

    def keys(self):
        return self._row.keys()

    def items(self):
        for key, value in six.iteritems(self._row):
            yield (key, value)

    def get(self, key, default=None):
        value = self._row.get(key)
        if value is None:
            return default
        return value


class BaseQueryResult(ABC):
    def __init__(self, result, keys):
        if (
                not isinstance(result, (list, tuple)) or
                not all(isinstance(x, dict) for x in result)):
            raise TypeError('result arg must be a list or tuple of dicts')
        self._result = result

        if not isinstance(keys, (list, tuple)):
            raise TypeError('keys arg must be a list or tuple')
        elif not all(keys == list(x.keys()) for x in result):
            raise AttributeError('keys arg does not match all result keys')
        self._keys = keys

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        self._index += 1
        try:
            row = self._result[self._index - 1]
        except IndexError:
            raise StopIteration
        return QueryResultRow(self._keys, row)

    next = __next__

    def __getattr__(self, name):
        if name not in self._keys:
            raise AttributeError('Not found : %r' % name)
        return self.dict()[name]

    def __getitem__(self, key):
        if isinstance(key, six.string_types):
            if key not in self._keys:
                raise KeyError('Not found : %r' % key)
            value = self.dict()[key]
        elif isinstance(key, int):
            k = self._keys[key]
            value = self.dict()[k]
        return value

    def __len__(self):
        return len(self._result)

    def __eq__(self, other):
        if not isinstance(other, BaseQueryResult):
            return NotImplemented
        return self._result == other._result

    def __ne__(self, other):
        return not self == other

    @property
    def result(self):
        """
            Get the full results of the query as a list of dicts

            Returns:
                list of dicts - [{column: value, ... }, ... ]
        """
        return copy.deepcopy(self._result)

    def dict(self):
        """
            Get the full results of the query as a columnar dict

            Returns:
                dict - {column: (value, ... ), ... }
        """
        return {k: tuple([row[k] for row in self._result]) for k in self._keys}

    def json(self):
        """
            Get the full results of the query as a json string

            Returns:
                string
        """
        return json.dumps(self._result, cls=DtDecEncoder)

    def list(self):
        """
            Get the full results of the query as a row-based list of tuples

            Returns:
                list of tuples - [(value, ... ), ... ]
        """
        return [tuple([v for v in row.values()]) for row in self._result]

    def df(self, *args, **kwargs):
        """
            Get the full results of the query as a dataframe

            Returns:
                pandas.DataFrame
        """
        return pd.DataFrame(self._result, *args, **kwargs)

    def first(self):
        """
            Get the first row of the result

            Returns:
                QueryResultRow
        """
        return QueryResultRow(self._keys, self._result[0])

    def values(self):
        return self.list()

    def keys(self):
        return copy.deepcopy(self._keys)

    def items(self):
        for key, value in six.iteritems(self.dict()):
            yield (key, value)

    def get(self, key, default=None):
        value = self.dict().get(key)
        if value is None:
            return default
        return value


class DbQueryResult(BaseQueryResult):
    def __init__(self, result_proxy):
        if not isinstance(result_proxy, ResultProxy):
            raise TypeError(
                'DbQueryResult instantiated with invalid result type : %s. '
                'Must be a sqlalchemy.engine.ResultProxy, returned by a call '
                'to sqlalchemy.engine.Connection.execute()'
                % type(result_proxy))

        result = [dict(row) for row in result_proxy]
        keys = result_proxy.keys()

        super(DbQueryResult, self).__init__(result, keys)


class BigQueryResult(BaseQueryResult):
    def __init__(self, row_iterator):
        if not isinstance(row_iterator, RowIterator):
            raise TypeError(
                'BigQueryResult instantiated with invalid result type : %s. '
                'Must be a google.cloud.bigquery.table.RowIterator, returned '
                'by a call to google.cloud.bigquery.Client.query().result()'
                % type(row_iterator))

        result = [dict(row) for row in row_iterator]
        keys = result[0].keys()

        super(BigQueryResult, self).__init__(result, keys)
