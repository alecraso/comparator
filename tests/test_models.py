import mock
import pytest
import types

from comparator import comps
from comparator import Comparator
from comparator.db import PostgresDb
from comparator.db.query import BaseQueryResult
from comparator.models import ComparatorResult

query = 'select * from nowhere'
other_query = 'select count(*) from somewhere'
left_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
right_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
mismatch_right_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}, {'a': 7, 'b': 8, 'c': 9}]
keys = ['a', 'b', 'c']

left_results = BaseQueryResult(left_query_results, keys)
right_results = BaseQueryResult(right_query_results, keys)
mismatch_right_results = BaseQueryResult(mismatch_right_query_results, keys)

expected_default_result = ('basic_comp', True)
expected_multiple_result = [
    ComparatorResult('test', 'first_eq_comp', True),
    ComparatorResult('test', 'len_comp', True)]
expected_mismatch_result = [
    ComparatorResult('test', 'first_eq_comp', True),
    ComparatorResult('test', 'len_comp', False)]


def test_comparator():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(TypeError):
        Comparator(l, r)

    with pytest.raises(TypeError):
        Comparator(l, r, 1234)

    with pytest.raises(TypeError):
        Comparator(l, 'derp', query, query)

    c = Comparator(l, r, query)
    assert c._lquery == query
    assert c._rquery == query
    assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]

    c = Comparator(l, r, query, other_query, comps.FIRST_COMP)
    assert c._lquery == query
    assert c._rquery == other_query
    assert c._comps == [comps.COMPS.get(comps.FIRST_COMP)]

    assert c.query_results == tuple()
    assert c.results == list()


def test_compare_defaults():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=right_results):
            assert c.get_query_results() == (left_results, right_results)

    assert isinstance(c.compare(), types.GeneratorType)
    res = c.run_comparisons()[0]
    assert (res.name, res.result) == expected_default_result


def test_compare_multiple():
    l, r = PostgresDb(), PostgresDb()
    comparisons = [comps.FIRST_COMP, comps.LEN_COMP]
    c = Comparator(l, r, query, comps=comparisons)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=right_results):
            assert c.get_query_results() == (left_results, right_results)

    for i, result in enumerate(c.compare()):
        assert result == expected_multiple_result[i]


def test_compare_mismatch():
    l, r = PostgresDb(), PostgresDb()
    comparisons = [comps.FIRST_COMP, comps.LEN_COMP]
    c = Comparator(l, r, query, comps=comparisons)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()
    assert c._query_results == (left_results, mismatch_right_results)
    assert res == expected_mismatch_result


def test_left_right_queries():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(TypeError):
        Comparator(l, r, lquery=query, rquery=1234)

    c1 = Comparator(l, r, lquery=query)
    assert c1._lquery == query
    assert c1._rquery == query

    c = Comparator(l, r, query, other_query)
    assert c._lquery == query
    assert c._rquery == other_query


def test_results_run():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query)

    with mock.patch.object(c._left, 'query', return_value=left_results) as lq:
        with mock.patch.object(c._right, 'query', return_value=right_results) as rq:
            res = c.get_query_results(run=True)
    c.get_query_results(run=True)

    assert lq.call_count == 1
    assert rq.call_count == 1
    assert res == (left_results, right_results)


def test_no_comps():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query, comps='notarealcomparison')
    assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]


def test_custom_comparison():
    def custom_comp(left, right):
        return len(left) < len(right)
    expected_result = [ComparatorResult('test', 'custom_comp', True)]

    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query, comps=custom_comp)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()

    assert res == expected_result
