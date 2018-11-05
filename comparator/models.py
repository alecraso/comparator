"""
    Base classes for running comparisons between two databases
"""
from __future__ import unicode_literals

import copy
import inspect
import logging
import re
import six

from comparator.comps import COMPS, DEFAULT_COMP
from comparator.db.base import BaseDb
from comparator.exceptions import QueryFormatError, InvalidCompSetException

_log = logging.getLogger(__name__)


class QueryPair(object):
    """
        A container object to hold comparison queries and their results

        Args:
            left : BaseDb - The "left" source database, against which the "left" query will run
            right : BaseDb - The "right" source database, against which the "right" query will run
            lquery : string - The query to run against the "left" database

        Kwargs:
            rquery : string - The query to run against the "right" database.
                              If not provided, lquery will be run against both.
    """
    def __init__(self, left, right, lquery, rquery=None):
        if not (isinstance(left, BaseDb) and isinstance(right, BaseDb)):
            raise TypeError('left and right both must be BaseDb instances')
        self._left = left
        self._right = right

        self._set_queries(lquery, rquery)
        self._set_empty()

    def __repr__(self):
        return '<QueryPair({qp._left} || {qp._right})>'.format(qp=self)

    @property
    def lresult(self):
        return self._lresult

    @property
    def rresult(self):
        return self._rresult

    @property
    def query_results(self):
        """
            Get the full results of the two queries
        """
        return (self._lresult, self._rresult)

    @property
    def empty(self):
        """
            Returns True if the query results are empty
        """
        if self._lresult is None and self._rresult is None:
            return True
        return False

    def _set_queries(self, lquery, rquery):
        """
            Sets the queries to use in each source database
        """
        if rquery is None:
            rquery = lquery

        for q in (lquery, rquery):
            if not isinstance(q, six.string_types):
                raise TypeError('Queries must be valid strings')

        self._lquery = lquery
        self._rquery = rquery

    def _set_empty(self):
        """
            Reset the query results
        """
        self._lresult = None
        self._rresult = None

    def _format_rquery(self):
        """
            This method allows the right query to reference the output of the left

            Adding {{ column_name }} to the rquery will format the query with the results of the
            lquery, which is run first. For example, the rquery...

                SELECT *
                  FROM table
                 WHERE uuid IN {{ uuid }}
                   AND id NOT IN {{ id }}

            ...will be automatically formatted  to become...

                SELECT *
                  FROM table
                 WHERE uuid IN ('uuid_1', 'uuid_2', 'uuid_3')
                   AND id NOT IN (1, 2, 3)
        """
        qfmt = re.compile(r'\{\{[\s]?[a-zA-Z0-9\_]+[\s]?\}\}')
        formatting = qfmt.findall(self._rquery)

        if not formatting:
            return self._rquery
        else:
            rquery = self._rquery
            keys = [key.strip('{ }') for key in formatting]

            _log.info('Found result formatting for keys : %r', keys)

            for fmt, key in zip(formatting, keys):
                try:
                    repl = str(self._lresult[key])
                except KeyError:
                    raise QueryFormatError('Key not found in lquery result : ' + key)
                rquery = re.sub(fmt, repl, rquery)

            return rquery

    def get_query_results(self):
        """
            Runs each query against its source database
        """
        self._lresult = self._left.query(self._lquery)

        rquery = self._format_rquery()
        self._rresult = self._right.query(rquery)

    def clear(self):
        """
            Clear the query results to allow for a refresh
        """
        self._set_empty()


class ComparatorResult(object):
    """
        A container object to hold the results of a comparison

        This primarily provides syntactic sugar on what is really just a (name, result) tuple. The "truthiness" of a
        result can be checked easily, as well as other standard comparison operators if the particular comparison
        returns a value other than a boolean.

        Args:
            comparator_name : str - The name of the calling Comparator
            name : str - The name of the comparison
            result - The result of the comparison
    """
    def __init__(self, comparator_name, name, result):
        self._cname = comparator_name
        self._name = str(name)
        self._result = result

    def __repr__(self):
        return '<ComparatorResult({cr._name}, {cr._result})>'.format(cr=self)

    def __str__(self):
        return '{cr._name} : {cr._result}'.format(cr=self)

    def __bool__(self):
        return bool(self._result)

    __nonzero__ = __bool__

    def __getitem__(self, key):
        if isinstance(key, six.integer_types):
            key = {0: 'name', 1: 'result'}.get(key, None)
            if key is None:
                raise IndexError('list index out of range')
        if key == 'name':
            return self._name
        elif key == 'result':
            return self._result
        else:
            raise KeyError(key)

    def __eq__(self, other):
        return self._result == other

    def __ne__(self, other):
        return self._result != other

    def __gt__(self, other):
        return self._result > other

    def __ge__(self, other):
        return self._result >= other

    def __lt__(self, other):
        return self._result < other

    def __le__(self, other):
        return self._result <= other

    @property
    def comparator_name(self):
        return self._cname

    @property
    def name(self):
        return self._name

    @property
    def result(self):
        return self._result


class Comparator(object):
    """
        The primary comparison operator for programmatically comparing the results of queries against two databases

        Kwrgs:
            left : BaseDb - The "left" source database, against which the "left" query will run
            right : BaseDb - The "right" source database, against which the "right" query will run
            lquery : string - The query to run against the "left" database
            rquery : string - The query to run against the "right" database.
                              If not provided, lquery will be run against both.
            qp : QueryPair - Provide an instantiated QueryPair object instead of databases/queries
            comps : string/callable or list of strings/callables
                        - String or strings must be one of the comstants in the comps module.
                          Otherwise, the callable or list of callables can be any function that accepts two
                          QueryResult classes, performs arbitrary checks, and returns a boolean.
            name : string - A name to give this particular Comparator instance, useful for checking results when
                            instantiating multiple as part of a ComparatorSet.
    """
    def __init__(self, left=None, right=None, lquery=None, rquery=None, qp=None, comps=None, name=None):
        if qp is not None:
            self._qp = qp
        else:
            self._qp = QueryPair(left, right, lquery, rquery)

        # Set the list of comparisons
        if comps is None:
            comps = [DEFAULT_COMP]

        if not isinstance(comps, list):
            comps = [comps]

        self._comps = list()
        for comp in comps:
            if callable(comp):
                self._comps.append(comp)
            elif COMPS.get(comp, None):
                self._comps.append(COMPS[comp])

        if not self._comps:
            _log.warning('No valid comparisons found, falling back to default')
            self._comps.append(COMPS[DEFAULT_COMP])

        self._name = name

        # Set an empty result
        self._set_empty()

    def __repr__(self):
        if self._name is None:
            return '<{c.__class__}>'.format(c=self)
        return '<Comparator({c._name})>'.format(c=self)

    @property
    def name(self):
        return self._name

    @property
    def results(self):
        return self._results

    def _set_empty(self):
        """
            Reset all results
        """
        self._qp.clear()
        self._results = list()
        self._complete = False

    def get_query_results(self, run=True):
        """
            Get the results of the two queries

            Kwargs:
                run : bool - Whether to run the queries if
                             self._qp.query_results is empty

            Returns:
                tuple - The results of the two queries (left, right)
        """
        if run and self._qp.empty:
            self._qp.get_query_results()
        return self._qp.query_results

    def compare(self):
        """
            Generator that yields the results of each comparison

            Yields:
                tuple : (function name, comparison result)


            Usage examples (assumtion is each comp returns a bool):

            c = Comparator(...)
            failed_comps = [res[0] for res in c.compare() if res[1] is False]

            c = Comparator(...)
            for comp, result in c.compare():
                if result is False:
                    raise Exception('Failed comparison: {}'.format(comp))
        """
        if self._qp.empty:
            self._qp.get_query_results()

        if not self._complete:
            for comp in self._comps:
                name = comp.__name__
                # Try to surface a more useful name if lambda is used
                if name == '<lambda>':
                    source = inspect.getsource(comp)
                    name = 'lambda' + re.split('lambda', source)[1].strip()

                result = ComparatorResult(
                    self._name, name, comp(*self._qp.query_results))
                self._results.append(result)

                yield result

            self._complete = True

        else:
            for result in self._results:
                yield result

    def run_comparisons(self):
        """
            Run all comparisons and return the results

            Returns:
                list of tuples
        """
        if not self._complete:
            for _ in self.compare():
                pass
        return copy.deepcopy(self._results)

    def clear(self):
        """
            Clear all results to allow a refresh
        """
        self._set_empty()


class ComparatorSet(object):
    """
        A convenience wrapper around Comparators

        This object is generally intended to be used with one of the two classmethods, from_dict or from_list.

        It is expected that users will want to run multiple queries and comparisons against a particular database or
        databases. The ComparatorSet can be used to bundle various queries and comparisons against source databases.
        The resulting set of Comparator objects can be iterated upon and run/checked in a variety of ways.

        Args:
            left : BaseDb - The "left" source database, against which the "left" query will run
            right : BaseDb - The "right" source database, against which the "right" query will run
            queries : list - The set of two-member tuples each containing the lquery and rquery strings

        Kwargs:
            comps : list - The set of comparisons that correspond to each query pair. If None, the default_comp or
                           global DEFAULT_COMP will be used for each. If passed, must be the same length as the list
                           of queries.
            names : list - The set of Comparator names that correspond to each query pair. If None, each Comparator
                           will self-name using the left and right databases. If passed, must be the same length as
                           the list of queries.
            default_comp : callable - The default comparison to use if no comps are passed. Ignored if comps is passed.
    """
    def __init__(self, query_pairs=None, comps=None, names=None, default_comp=None):
        self._set_query_pairs(query_pairs)
        self._set_comps(comps, default_comp)
        self._set_names(names)

        self._comparisons = [
            Comparator(qp=qp, comps=c, name=n)
            for qp, c, n in zip(self._query_pairs, self._comps, self._names)
        ]

    def __repr__(self):
        return '<ComparatorSet: {cs._comparisons}>'.format(cs=self)

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        self._index += 1
        try:
            c = self._comparisons[self._index - 1]
        except IndexError:
            raise StopIteration
        return c

    next = __next__

    def __getitem__(self, key):
        return self._comparisons[key]

    def _set_query_pairs(self, query_pairs):
        if not isinstance(query_pairs, list):
            query_pairs = [query_pairs]
        for qp in query_pairs:
            if not isinstance(qp, QueryPair):
                raise InvalidCompSetException(
                    'Each query pair must be a valid QueryPair object. Problem at : %r' % qp)

        self._query_pairs = query_pairs

    def _set_comps(self, comps, default_comp):
        if comps is None:
            default = default_comp or DEFAULT_COMP
            # Use the default comparison for each query pair
            comps = [default for i in range(len(self._query_pairs))]
        if not isinstance(comps, list):
            comps = [comps]

        for comp_list in comps:
            if not isinstance(comp_list, list):
                comp_list = [comp_list]
            for comp in comp_list:
                if not callable(comp):
                    if COMPS.get(comp, None) is None:
                        raise InvalidCompSetException(
                            'Each comp must be a callable. Problem at : %r' % comp)

        if len(self._query_pairs) != len(comps):
            raise InvalidCompSetException(
                'Queries and comparison mapping is mismatched. There are %d query pairs and %d comparisons' % (
                    len(self._query_pairs), len(comps)))

        self._comps = comps

    def _set_names(self, names):
        if names is None:
            names = [None for i in range(len(self._query_pairs))]
        if not isinstance(names, list):
            names = [names]

        names = [str(name) if name is not None else None for name in names]

        if len(self._query_pairs) != len(names):
            raise InvalidCompSetException(
                'Queries and name mapping is mismatched. There are %d query pairs and %d names' % (
                    len(self._query_pairs), len(names)))

        self._names = names

    @classmethod
    def from_dict(cls, dict_or_dicts, left=None, right=None, default_comp=None):
        """
            Build a ComparatorSet from a dict or list of dicts of query pairs and comparisons

            A single dict or a list of dicts are both valid. Each dict can be constructed in the following way:
            {
                'name': str - A name for this Comparator, useful when viewing the results of the comparisons
                'lquery': str - The query to run against the "left" source
                'rquery': str - The query to run against the "right" source
                'qp' : QueryPair - An instantiated QueryPair object
                'comps': callable or list of callables - The comparison(s) to run against the result
            }
            The 'lquery' and 'rquery' values are required, unless a QueryPair is provided.
            The 'comps' value is optional, and the 'name' value is optional but recommended.

            The 'default_comp' value can be used to set a fallback for missing 'comps' values. These will only be
            used in the case that 'comps' is not set. If no default is passed, the global DEFAULT_COMP will be used.

            Args:
                dict_or_dicts : dict or list - The queries and comps to use to construct a new ComparatorSet

            Kwargs:
                left : BaseDb - The "left" source database, against which the "left" query will run
                right : BaseDb - The "right" source database, against which the "right" query will run
                default_comp : callable or list - The fallback comps to use if comps is not set for a set of queries

            Returns:
                instantiated ComparatorSet
        """
        if not isinstance(dict_or_dicts, (list, tuple)):
            dict_or_dicts = [dict_or_dicts]

        for d in dict_or_dicts:
            if (
                    not isinstance(d, dict)
                    or ((d.get('lquery') is None or d.get('rquery') is None) and d.get('qp') is None)):
                raise InvalidCompSetException(
                    'Each value must be a dict with lquery, rquery or query pair. Problem with : %r' % d)

        all_names = []
        all_query_pairs = []
        all_comps = []

        for d in dict_or_dicts:
            all_names.append(d.get('name', None))
            qp = d.get('qp', None)
            if qp is None:
                qp = QueryPair(left, right, d['lquery'], d['rquery'])

            all_query_pairs.append(qp)
            all_comps.append(d.get('comps', default_comp or DEFAULT_COMP))

        return cls(all_query_pairs, all_comps, all_names)
