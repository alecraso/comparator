"""Base classes for running comparisons between two databases
"""
import logging

from past.builtins import basestring

from comparator.comps import COMPS, DEFAULT_COMP
from comparator.db import BaseDb

_log = logging.getLogger(__name__)


class Comparator(object):
    _comps = list()
    _results = tuple()

    def __init__(
            self,
            left, right,
            query=None, left_query=None, right_query=None,
            comparisons=None):
        """Runs comparisons on the results of two queries

        Args:
            left : BaseDb - The "left" database source
            right : BaseDb - The "right" database source

        Kwargs:
            query : str - The query string to run. If this kwarg is set,
                          the same query will be run against both DB sources.
            left_query : str - The query to run against the "left" DB source
            right_query : str - The query to run against the "right" DB source
            comparisons : list - The comparison functions to run against the
                                 results of the two queries. Strings that
                                 correspond to the comparison constants can
                                 also be passed.
        """
        if not (isinstance(left, BaseDb) and isinstance(right, BaseDb)):
            raise ValueError('left and right both must be BaseDb instances')
        self._left = left
        self._right = right

        self._set_queries(query, left_query, right_query)

        if comparisons is None:
            comparisons = [DEFAULT_COMP]

        if not isinstance(comparisons, list):
            comparisons = [comparisons]

        for comp in comparisons:
            if callable(comp):
                self._comps.append(comp)
            elif COMPS.get(comp, None):
                self._comps.append(COMPS[comp])

    @property
    def results(self, run=False):
        """Get the results of the two queries

        Kwargs:
            run : bool - Whether to run the queries if self._results is empty

        Returns:
            tuple - The results of the two queries (left, right)
        """
        if run and not self._results:
            self._get_results()
        return self._results

    def _set_queries(self, q, lq, rq):
        """Sets the queries to use in each source database"""
        if not any((q, lq, rq)):
            raise AttributeError('No queries provided')

        if q is not None:
            if not isinstance(q, basestring):
                raise ValueError('query must be a valid string')
            if lq or rq:
                _log.warn('query is set, overriding left_ and right_ kwargs')
            self._left_query = q
            self._right_query = q
        else:
            if lq is None or rq is None:
                raise AttributeError(
                    'Both left_ and right_ queries must be provided')
            if not (
                    isinstance(lq, basestring) and
                    isinstance(rq, basestring)):
                raise ValueError(
                    'Both left_ and right_ queries must be strings')
            self._left_query = lq
            self._right_query = rq

    def _get_results(self):
        """Runs each query against its source database"""
        left_result = self._left.query(self._left_query)
        right_result = self._right.query(self._right_query)
        self._results = (left_result, right_result)

    def compare(self):
        """Generator that yields the results of each comparison

        Returns:
            tuple : (function name, comparison result)


        Usage examples (assumtion is each comp returns a bool):

        c = Comparator(...)
        failed_comps = [res[0] for res in c.compare() if res[1] is False]

        c = Comparator(...)
        for comp, result in c.compare():
            if result is False:
                raise Exception('Failed comparison: {}'.format(comp))
        """
        if not self._results:
            self._get_results()
        for comp in self._comps:
            yield comp.__name__, comp(*self._results)

    def run_comparisons(self):
        """Run all comparisons and return the results"""
        return [r for r in self.compare()]
