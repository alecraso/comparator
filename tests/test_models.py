import mock
import pytest
import types

from sqlalchemy.engine import ResultProxy

from comparator import comps
from comparator import Comparator, ComparatorSet
from comparator.exceptions import InvalidCompSetException
from comparator.db import PostgresDb
from comparator.db.query import QueryResult
from comparator.models import ComparatorResult

query = 'select * from nowhere'
other_query = 'select count(*) from somewhere'
left_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
right_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
mismatch_right_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}, {'a': 7, 'b': 8, 'c': 9}]


def get_mock_query_result(values):
    mock_result = mock.MagicMock(spec=ResultProxy)
    mock_result.__iter__.return_value = values
    return QueryResult(mock_result)


left_results = get_mock_query_result(left_query_results)
right_results = get_mock_query_result(right_query_results)
mismatch_right_results = get_mock_query_result(mismatch_right_query_results)

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
    assert c.name == 'Comparator: localhost | localhost'

    c = Comparator(l, r, query, other_query, comps.FIRST_COMP, name='Shirley')
    assert c._lquery == query
    assert c._rquery == other_query
    assert c._comps == [comps.COMPS.get(comps.FIRST_COMP)]
    assert c.name == 'Shirley'

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
    assert c._complete is True

    ln = len(c.results)
    c.run_comparisons()
    assert len(c.results) == ln

    for _ in c.compare():
        pass
    assert len(c.results) == ln

    c._complete = False
    c.run_comparisons()
    assert len(c.results) == ln * 2
    c._complete = True

    c.clear()
    assert c._complete is False
    assert c.query_results == tuple()
    assert c.results == list()


def test_comarison_result():
    pass


def test_compare_multiple():
    l, r = PostgresDb(), PostgresDb()
    comparisons = [comps.FIRST_COMP, comps.LEN_COMP]
    c = Comparator(l, r, query, comps=comparisons)

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=right_results):
            assert c.get_query_results() == (left_results, right_results)

    assert isinstance(c.lresult, QueryResult)
    assert c.lresult.a == (1, 4)
    assert isinstance(c.rresult, QueryResult)
    assert c.lresult.b == (2, 5)

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
    expected_result = [ComparatorResult('test', 'custom_comp', True),
                       ComparatorResult('test', 'lambda x, y: len(x) < len(y)', True)]

    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query, comps=[custom_comp, lambda x, y: len(x) < len(y)])

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()

    assert res == expected_result


def test_non_bool_comparison():
    def custom_comp(left, right):
        return len(right) - len(left)
    expected_result = [ComparatorResult('test', 'custom_comp', 1)]

    l, r = PostgresDb(), PostgresDb()
    c = Comparator(l, r, query, comps=custom_comp, name='test')

    with mock.patch.object(c._left, 'query', return_value=left_results):
        with mock.patch.object(c._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()

    assert res == expected_result
    comp = res[0]
    assert str(comp) == "('custom_comp', 1)"
    assert comp.comparator_name == 'test'
    assert comp.name == 'custom_comp'
    assert comp['name'] == 'custom_comp'
    assert bool(comp)
    assert comp > 0
    assert comp >= 0
    assert comp < 2
    assert comp <= 2
    assert comp == 1
    assert comp != 0
    assert comp.result == 1
    assert comp['result'] == 1
    assert comp[0] == 'custom_comp'
    assert comp[1] == 1
    with pytest.raises(IndexError):
        comp[2]
    with pytest.raises(KeyError):
        comp['cheesecake']


def test_comparatorset():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(TypeError):
        ComparatorSet(l, r)

    with pytest.raises(InvalidCompSetException):
        ComparatorSet(l, r, 1234)

    with pytest.raises(TypeError):
        ComparatorSet(l, 'derp', (query, query))

    cs = ComparatorSet(l, r, (query, query))
    for c in cs:
        assert isinstance(c, Comparator)
        assert c._left is l
        assert c._right is r
        assert c._lquery == query
        assert c._rquery == query
        assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]
        assert c.name == 'Comparator: localhost | localhost'

    with pytest.raises(InvalidCompSetException):
        ComparatorSet(
            l, r,
            [(query, other_query), (query, other_query)],
            comps=[comps.LEN_COMP, comps.FIRST_COMP],
            names='Shirley')

    names = ['Shirley', 'Eugene']
    with pytest.raises(InvalidCompSetException):
        ComparatorSet(
            l, r,
            [(query, other_query), (query, other_query)],
            comps=comps.COMPS.get(comps.LEN_COMP),
            names=names)

    with pytest.raises(InvalidCompSetException):
        ComparatorSet(
            l, r,
            [(query, other_query), (query, other_query)],
            comps=[comps.LEN_COMP, 'nope'],
            names=names)

    cmps = [comps.LEN_COMP, comps.FIRST_COMP]
    cs = ComparatorSet(
        l, r,
        [(query, other_query), (query, other_query)],
        comps=cmps,
        names=names)
    for i, c in enumerate(cs):
        assert c._left is l
        assert c._right is r
        assert c._lquery == query
        assert c._rquery == other_query
        assert c._comps == [comps.COMPS.get(cmps[i])]
        assert c.name == names[i]

    assert cs[0]
    assert cs[1]
    with pytest.raises(IndexError):
        cs[2]
    with pytest.raises(TypeError):
        cs['what']


def test_comparatorset_from_dict():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(TypeError):
        ComparatorSet.from_dict(l, r)

    with pytest.raises(InvalidCompSetException):
        ComparatorSet.from_dict(l, r, 'what')

    with pytest.raises(InvalidCompSetException):
        ComparatorSet.from_dict(l, r, {'lquery': query})

    cs = ComparatorSet.from_dict(l, r, {'lquery': query, 'rquery': other_query})
    assert isinstance(cs, ComparatorSet)

    d1 = {'lquery': query, 'rquery': other_query}
    d2 = {'name': 'test', 'lquery': query, 'rquery': other_query, 'comps': comps.LEN_COMP}
    cs = ComparatorSet.from_dict(l, r, [d1, d2], default_comp=comps.FIRST_COMP)
    for c in cs:
        assert c._left is l
        assert c._right is r
        assert c._lquery == query
        assert c._rquery == other_query
    assert cs[0].name == 'Comparator: localhost | localhost'
    assert cs[1].name == 'test'
    assert cs[0]._comps == [comps.COMPS.get(comps.FIRST_COMP)]
    assert cs[1]._comps == [comps.COMPS.get(comps.LEN_COMP)]

    with pytest.raises(IndexError):
        cs[2]


def test_comaratorset_from_list():
    l, r = PostgresDb(), PostgresDb()

    with pytest.raises(TypeError):
        ComparatorSet.from_list(l, r)

    with pytest.raises(TypeError):
        ComparatorSet.from_list(l, r, 'what')

    with pytest.raises(InvalidCompSetException):
        ComparatorSet.from_list(l, r, (query, ))

    with pytest.raises(InvalidCompSetException):
        ComparatorSet.from_list(l, r, ('name', query, query, 'nope'))

    with pytest.raises(InvalidCompSetException):
        ComparatorSet.from_list(l, r, ('name', query, query, comps.LEN_COMP, 'extra'))

    cs = ComparatorSet.from_list(l, r, (query, other_query))
    assert isinstance(cs, ComparatorSet)

    t1 = (query, other_query)
    t2 = ('test', query, other_query, comps.LEN_COMP)
    t3 = (query, other_query, comps.COMPS.get(comps.BASIC_COMP))
    t4 = (query, other_query, comps.BASIC_COMP)
    t5 = ('testing', query, other_query)
    t6 = ('running', query, other_query, comps.COMPS.get(comps.BASIC_COMP))
    cs = ComparatorSet.from_list(l, r, [t1, t2, t3, t4, t5, t6], default_comp=comps.FIRST_COMP)
    for c in cs:
        assert c._left is l
        assert c._right is r
        assert c._lquery == query
        assert c._rquery == other_query

    assert cs[0].name == 'Comparator: localhost | localhost'
    assert cs[1].name == 'test'
    assert cs[2].name == 'Comparator: localhost | localhost'
    assert cs[3].name == 'Comparator: localhost | localhost'
    assert cs[4].name == 'testing'
    assert cs[5].name == 'running'

    assert cs[0]._comps == [comps.COMPS.get(comps.FIRST_COMP)]
    assert cs[1]._comps == [comps.COMPS.get(comps.LEN_COMP)]
    assert cs[2]._comps == [comps.COMPS.get(comps.BASIC_COMP)]
    assert cs[3]._comps == [comps.COMPS.get(comps.BASIC_COMP)]
    assert cs[4]._comps == [comps.COMPS.get(comps.FIRST_COMP)]
    assert cs[5]._comps == [comps.COMPS.get(comps.BASIC_COMP)]

    with pytest.raises(IndexError):
        cs[6]
