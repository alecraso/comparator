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

from collections import OrderedDict
from google.cloud.bigquery.table import RowIterator
from sqlalchemy.engine import ResultProxy


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

    def __repr__(self):
        return str(tuple([v for v in self.values()]))

    def __str__(self):
        return self.__repr__()

    def __bool__(self):
        return bool(self._row)

    __nonzero__ = __bool__

    def __getattr__(self, name):
        return self.__getitem__(str(name))

    def __getitem__(self, key):
        if isinstance(key, six.string_types):
            # Return the column corresponding with this key
            if key not in self._keys:
                raise KeyError('Not found : %r' % key)
            value = self._row[key]
        elif isinstance(key, six.integer_types):
            # Return the column corresponding with this index
            k = self._keys[key]
            value = self._row[k]
        else:
            raise TypeError('Lookups must be done with integers or strings, not %s' % type(key))
        return value

    def __eq__(self, other):
        if not isinstance(other, QueryResultRow):
            return NotImplemented
        return self._row == other._row

    def __ne__(self, other):
        return not self == other

    def values(self):
        return tuple(six.itervalues(self._row))

    def keys(self):
        return list(six.iterkeys(self._row))

    def items(self):
        for key, value in six.iteritems(self._row):
            yield (key, value)

    def get(self, key, default=None):
        value = self._row.get(key)
        if value is None:
            return default
        return value


class QueryResult(object):
    def __init__(self, query_iterator=None):
        if query_iterator is not None:
            if not isinstance(query_iterator, (RowIterator, ResultProxy)):
                raise TypeError(
                    'DbQueryResult instantiated with invalid result type : %s. Must be a sqlalchemy.engine.ResultProxy,'
                    ' returned by a call to sqlalchemy.engine.Connection.execute(), or a google.cloud.bigquery.table.'
                    'RowIterator, returned by a call to google.cloud.bigquery.Client.query().result()'
                    % type(query_iterator))

            self._result = [OrderedDict(row) for row in query_iterator]
        else:
            self._result = list()

        if self._result:
            keys = list(six.iterkeys(self._result[0]))
            if not all(sorted(keys) == sorted(list(six.iterkeys(x))) for x in self._result):
                raise AttributeError('keys arg does not match all result keys')
        else:
            keys = list()
        self._keys = keys

    def __repr__(self):
        return '<QueryResult: {qr._result}>'.format(qr=self)

    def __bool__(self):
        return bool(self._result)

    __nonzero__ = __bool__

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
        return self.__getitem__(str(name))

    def __getitem__(self, key):
        if isinstance(key, six.string_types):
            # Return the column corresponding with this key
            if key not in self._keys:
                raise KeyError('Not found : %r' % key)
            value = self.dict()[key]
        elif isinstance(key, six.integer_types):
            # Return the row corresponding with this index
            row = self._result[key]
            value = QueryResultRow(self._keys, row)
        elif isinstance(key, slice):
            # Return the rows corresponding with this slice
            sliced = [self._result[ii] for ii in range(*key.indices(len(self._result)))]
            value = self._from_part(self._keys, sliced)
        else:
            raise TypeError('Lookups must be done with integers or strings, not %s' % type(key))
        return value

    def __len__(self):
        return len(self._result)

    def __eq__(self, other):
        if not isinstance(other, QueryResult):
            return NotImplemented
        return self._result == other._result

    def __ne__(self, other):
        return not self == other

    @classmethod
    def _from_part(cls, keys, result):
        """
            Get a new QueryResult from an existing sliced or filtered result

            Returns:
                QueryResult
        """
        qr = cls()
        qr._keys = keys
        qr._result = result
        return qr

    @property
    def empty(self):
        """
            Check if both the result and keys are empty

            Returns:
                bool
        """
        return bool(not self._keys and not self._result)

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
        return [tuple([v for v in six.itervalues(row)]) for row in self._result]

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
        for item in self.list():
            yield item

    def keys(self):
        for key in self._keys:
            yield key

    def items(self):
        for key, value in six.iteritems(self.dict()):
            yield (key, value)

    def get(self, key, default=None):
        try:
            value = self.__getitem__(str(key))
        except KeyError:
            return default
        return value

    def pop(self, index=-1):
        """
            Remove and return the row at the given index (default last)

            Args:
                index : int - The index of the row to remove and return

            Returns:
                QueryResultRow
        """
        return QueryResultRow(self._keys, self._result.pop(index))

    def append(self, other):
        """
            Append a QueryResultRow with matching keys to the current result

            Args:
                other : QueryResultRow - The row to append
        """
        if not isinstance(other, QueryResultRow):
            raise NotImplementedError('Appending object must be a QueryResultRow')
        if self.empty:
            self._keys = other._keys
        elif self._keys != other._keys:
            raise ValueError('Keys in appending row do not match, cannot append')
        self._result.append(other._row)

    def extend(self, other):
        """
            Extend the current result with another QueryResult with matching keys

            Args:
                other : QueryResult - The row to append
        """
        if not isinstance(other, QueryResult):
            raise NotImplementedError('Extending object must be a QueryResult')
        if self.empty:
            self._keys = other._keys
        elif self._keys != other._keys:
            raise ValueError('Keys in other QueryResult to not match, cannot extend')
        self._result.extend(other._result)

    def filter(self, predicate, inplace=False):
        """
            Filter the query result rows

            Args:
                predicate : callable - The function to apply to each result row, should return a boolean

            Kwargs:
                inplace : boolean - Whether to alter the results in-place or return a new QueryResult object

            Returns:
                QueryResult with filtered results
        """
        filtered = [row for row in self._result if predicate(QueryResultRow(self._keys, row))]
        if inplace:
            self._result = filtered
            return None
        else:
            return self._from_part(self._keys, filtered)
