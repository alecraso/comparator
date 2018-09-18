import mock
import pytest
import types

from comparator import comps
from comparator import Comparator
from comparator.db import PostgresDb

query = 'select * from nowhere'
other_query = 'select count(*) from somewhere'
left_results = [(1, 2, 3), (4, 5, 6)]
right_results = [(1, 2, 3), (4, 5, 6)]
mismatch_right_results = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]

expected_default_result = [('first_eq_comp', True)]
expected_multiple_result = [('basic_comp', True), ('len_comp', True)]
expected_mismatch_result = [('basic_comp', False), ('len_comp', False)]


def test_comparator():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(AttributeError):
        Comparator(l, r)

    with pytest.raises(TypeError):
        Comparator(l, r, 1234)

    with pytest.raises(TypeError):
        Comparator(l, 'derp', query)

    c = Comparator(l, r, query)
    assert c._left_query == query
    assert c._right_query == query
    assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]

    assert c.raw_results == tuple()


def test_compare_defaults():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=right_results):
            c._get_results()
    assert c.raw_results == (left_results, right_results)

    assert isinstance(c.compare(), types.GeneratorType)
    res = c.run_comparisons()
    assert res == expected_default_result


def test_compare_multiple():
    l, r = PostgresDb(), PostgresDb()
    comparisons = [comps.BASIC_COMP, comps.LEN_COMP]
    c = Comparator(l, r, query, comparisons=comparisons)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=right_results):
            c._get_results()
    assert c.raw_results == (left_results, right_results)

    for i, result in enumerate(c.compare()):
        assert result == expected_multiple_result[i]


def test_compare_mismatch():
    l, r = PostgresDb(), PostgresDb()
    comparisons = [comps.BASIC_COMP, comps.LEN_COMP]
    c = Comparator(l, r, query, comparisons=comparisons)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()
    assert c.raw_results == (left_results, mismatch_right_results)
    assert res == expected_mismatch_result


def test_left_right_queries():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(AttributeError):
        Comparator(l, r, left_query=query)

    with pytest.raises(TypeError):
        Comparator(l, r, left_query=query, right_query=1234)

    c1 = Comparator(l, r, query=query, right_query=other_query)
    assert c1._left_query == query
    assert c1._right_query == query

    c = Comparator(l, r, left_query=query, right_query=other_query)
    assert c._left_query == query
    assert c._right_query == other_query


def test_results_run():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query)

    with mock.patch.object(c._left, 'query', return_value=left_results) as lq:
        with mock.patch.object(c._right, 'query', return_value=right_results) as rq:
            res = c.results(run=True)
    c.results(run=True)

    assert lq.call_count == 1
    assert rq.call_count == 1
    assert res == str((left_results, right_results))


def test_no_comps():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query, comparisons='notarealcomparison')
    assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]


def test_custom_comparison():
    def custom_comp(left, right):
        return len(left) < len(right)
    expected_result = [('custom_comp', True)]

    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query, comparisons=custom_comp)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()

    assert res == expected_result


def test_bad_output():
    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query)

    with pytest.raises(ValueError):
        c.results(run=True, output='lithograph')

    with mock.patch.object(c._left, 'query_df') as left_mock_df:
        with mock.patch.object(c._right, 'query_df') as right_mock_df:
            c.run_comparisons(output='df')

    assert left_mock_df.call_count == 1
    assert right_mock_df.call_count == 1
    left_mock_df.assert_called_with(query)
    right_mock_df.assert_called_with(query)
