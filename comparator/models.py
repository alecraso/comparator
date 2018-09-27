"""
    Base classes for running comparisons between two databases
"""
from __future__ import unicode_literals

import copy
import inspect
import logging
import re

from past.builtins import basestring

from comparator.comps import COMPS, DEFAULT_COMP
from comparator.db.base import BaseDb
from comparator.exceptions import InvalidCompSetException

_log = logging.getLogger(__name__)


class Comparator(object):
    """
        The primary comparison operator for programmatically comparing
        the results of queries against two databases (or just one)

        Args:
            left : BaseDb - The "left" source database, against which the
                            "left" query will run
            right : BaseDb - The "right" source database, against which the
                             "right" query will run
            lquery : string - The query to run against the "left" database

        Kwargs:
            rquery : string - The query to run against the "right" database.
                              If not provided, lquery will be run against both.
            comps : string/callable or list of strings/callables
                        - String or strings must be one of the comstants in the
                          comps module. Otherwise, the callable or list of
                          callables can be any function that accepts two
                          QueryResult classes, performs arbitrary checks, and
                          returns a boolean.
            name : string - A name to give this particular Comparator instance,
                            useful for checking results when instantiating
                            multiple as part of a ComparatorSet.
    """
    def __init__(
            self, left, right, lquery,
            rquery=None, comps=None, name=None):
        if not (isinstance(left, BaseDb) and isinstance(right, BaseDb)):
            raise TypeError('left and right both must be BaseDb instances')
        self._left = left
        self._right = right

        self._set_queries(lquery, rquery)

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

        if name is None:
            name = 'Comparator: %s | %s' % (left, right)
        self._name = str(name)

        # Set an empty result
        self._query_results = tuple()
        self._results = {self._name: list()}

    @property
    def query_results(self):
        return tuple([x.result for x in self._query_results])

    @property
    def results(self):
        return self._results

    def _set_queries(self, lquery, rquery):
        """
            Sets the queries to use in each source database
        """
        if rquery is None:
            rquery = lquery

        for q in (lquery, rquery):
            if not isinstance(q, basestring):
                raise TypeError('Queries must be valid strings')

        self._lquery = lquery
        self._rquery = rquery

    def _get_query_results(self):
        """
            Runs each query against its source database
        """
        left_result = self._left.query(self._lquery)
        right_result = self._right.query(self._rquery)
        self._query_results = (left_result, right_result)

    def get_query_results(self, run=True):
        """
            Get the results of the two queries

            Kwargs:
                run : bool - Whether to run the queries if
                             self._query_results is empty

            Returns:
                tuple - The results of the two queries (left, right)
        """
        if run and not self._query_results:
            self._get_query_results()
        return self._query_results

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
        if not self._query_results:
            self._get_query_results()
        for comp in self._comps:
            name = comp.__name__
            # Try to surface a more useful name if lambda is used
            if name == '<lambda>':
                source = inspect.getsource(comp)
                name = 'lambda' + re.split('lambda', source)[1].trim()

            result = (name, comp(*self._query_results))
            self._results[self._name].append(result)

            yield result

    def run_comparisons(self, results_only=True):
        """
            Run all comparisons and return the results

            Kwargs:
                results_only : - Whether to return the full dict with the
                                 comparator name, or just the results list

            Returns:
                dict or list of tuples
        """
        for _ in self.compare():
            pass
        results = copy.deepcopy(self._results)
        if results_only:
            return results[self._name]
        return results


class ComparatorSet(object):
    def __init__(
            self, left, right, queries,
            comps=None, names=None, default_comp=None):
        if not (isinstance(left, BaseDb) and isinstance(right, BaseDb)):
            raise TypeError('left and right both must be BaseDb instances')
        self._left = left
        self._right = right

        self._set_queries(queries)
        self._set_comps(comps, default_comp)
        self._set_names(names)

        self._comparisons = [
            Comparator(left, right, q[0], q[1], c, n)
            for q, c, n in zip(self._queries, self._comps, self._names)
        ]

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

    def _set_queries(self, queries):
        if not isinstance(queries, list):
            queries = [queries]
        for pair in queries:
            if (
                    not isinstance(pair, tuple) or
                    len(pair) != 2 or
                    not isinstance(pair[0], basestring) or
                    not isinstance(pair[1], basestring)):
                raise InvalidCompSetException(
                    'Each query pair must only be a two-member tuple '
                    'of strings. Problem at : %r' % pair)

        self._queries = queries

    def _set_comps(self, comps, default_comp):
        if comps is None:
            default = default_comp or DEFAULT_COMP
            # Use the default comparison for each query pair
            comps = [[default] for i in xrange(len(self._queries))]
        if not isinstance(comps, list):
            comps = [comps]

        for comp in comps:
            if not callable(comp):
                if COMPS.get(comp, None) is None:
                    raise InvalidCompSetException(
                        'Each comp must be a callable. '
                        'Problem at : %r' % comp)

        if len(self._queries) != len(comps):
            raise InvalidCompSetException(
                'Queries and comparison mapping is mismatched. '
                'There are %d query pairs and %d comparisons' % (
                    len(self._queries), len(comps)))

        self._comps = comps

    def _set_names(self, names):
        if names is None:
            names = [None for i in xrange(len(self._queries))]
        if not isinstance(names, list):
            names = [names]

        names = [str(name) if name is not None else None for name in names]

        if len(self._queries) != len(names):
            raise InvalidCompSetException(
                'Queries and name mapping is mismatched. '
                'There are %d query pairs and %d names' % (
                    len(self._queries), len(names)))

        self._names = names

    @classmethod
    def from_dict(cls, left, right, dict_or_dicts, default_comp=None):
        """
            Build a CompSet from dict(s) of query pairs and comparisons

            A single dict or a list of dicts are both valid. Each dict can
            be constructed in the following way:
            {
                'name': str - A name for this Comparator, useful when viewing
                              the results of the comparisons.
                'lquery': str - The query to run against the "left" source,
                'rquery': str - The query to run against the "right" source,
                'comps': callable or list of callables - The comparison(s) to
                                                         run against the result
            }
            The 'lquery' and 'rquery' values are required. The 'comps' value is
            optional, and the 'name' value is optional but recommended.

            The 'default_comp' value can be used to set a fallback for missing
            'comps' values. These will only be used in the case that 'comps'
            is not set. If no default is passed, the global DEFAULT_COMP
            will be used.

            Args:
                dict_or_dicts : dict or list - The queries and comps to use to
                                               construct a new CompSet
                default_comp : callable or list - The fallback comps to use if
                                                  comps is not set for a set
                                                  of queries

            Returns:
                instantiated CompSet
        """
        if not isinstance(dict_or_dicts, (list, tuple)):
            dict_or_dicts = [dict_or_dicts]

        for d in dict_or_dicts:
            if (
                    not isinstance(d, dict) or
                    d.get('lquery') is None or
                    d.get('rquery') is None):
                raise InvalidCompSetException(
                    'Each value must be a dict with lquery, rquery. '
                    'Problem with : %r' % d)

        all_names = []
        all_queries = []
        all_comps = []

        for d in dict_or_dicts:
            all_names.append(d.get('name', None))
            all_queries.append((d['lquery'], d['rquery']))
            all_comps.append(d.get('comps', default_comp or DEFAULT_COMP))

        return cls(left, right, all_queries, all_comps, all_names)

    @classmethod
    def from_list(cls, left, right, list_or_lists, default_comp=None):
        if not isinstance(list_or_lists, (list, tuple)):
            raise TypeError('list_or_lists must be a list or tuple')

        if not all(isinstance(x, (list, tuple)) for x in list_or_lists):
            # We can assume this is a single comparison
            # Why are you using ComparatorSet for this then?
            list_or_lists = [list_or_lists]

        all_names = []
        all_queries = []
        all_comps = []

        for l in list_or_lists:
            if len(l) == 2:
                # Elements are assumed to be queries only
                all_names.append(None)
                all_queries.append((l[0], l[1]))
                all_comps.append(default_comp or DEFAULT_COMP)
            elif len(l) == 3:
                if isinstance(l[2], basestring):
                    if l[2] in COMPS:
                        # Elements are assumed to be queries and comp constant
                        all_names.append(None)
                        all_queries.append((l[0], l[1]))
                        all_comps.append(l[2])
                    else:
                        # Elements are assumed to be name and queries
                        all_names.append(l[0])
                        all_queries.append((l[1], l[2]))
                        all_comps.append(default_comp or DEFAULT_COMP)
                else:
                    # Elements are assumed to be queries and comp or comps
                    all_names.append(None)
                    all_queries.append((l[0], l[1]))
                    all_comps.append(l[2])
            elif len(l) == 4:
                # All elements are assumed to be present
                all_names.append(l[0])
                all_queries.append((l[1], l[2]))
                all_comps.append(l[3])
            else:
                raise InvalidCompSetException(
                    'Too many or too few elements passed to properly build '
                    'a comparison. Problem with : %r' % l)

        return cls(left, right, all_queries, all_comps, all_names)

    def compare(self):
        """
            Generator that yields the results of each comparison

            Yields:
                tuple : (function name, comparison result)
        """
        for c in self._comparisons:
            for result in c.compare():
                yield result

    def run_comparisons(self):
        """
            Run all comparisons for all provided sets

            Returns:
                dict : {name: [(comparison, result), ... ], ... }
        """
        all_results = []
        for c in self._comparisons:
            all_results.append(c.run_comparisons(results_only=False))
        return all_results
