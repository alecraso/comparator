import mock
import pytest
import types

from sqlalchemy.engine import ResultProxy

from comparator import comps
from comparator import QueryPair, Comparator, ComparatorSet
from comparator.exceptions import InvalidCompSetException, QueryFormatError
from comparator.db import PostgresDb
from comparator.db.query import QueryResult
from comparator.models import ComparatorResult

query = 'select * from nowhere'
other_query = 'select count(*) from somewhere'
left_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
right_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}]
mismatch_right_query_results = [{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}, {'a': 7, 'b': 8, 'c': 9}]
string_query_results = [{'a': 'one', 'b': 'two', 'c': 'three'}, {'a': 'four', 'b': 'five', 'c': 'six'}]
unicode_query_results = [{u'a': u'one', u'b': u'two', u'c': u'three'}, {u'a': u'four', u'b': u'five', u'c': u'six'}]


def get_mock_query_result(values):
    mock_result = mock.MagicMock(spec=ResultProxy)
    mock_result.__iter__.return_value = values
    return QueryResult(mock_result)


left_results = get_mock_query_result(left_query_results)
right_results = get_mock_query_result(right_query_results)
mismatch_right_results = get_mock_query_result(mismatch_right_query_results)
string_results = get_mock_query_result(string_query_results)
unicode_results = get_mock_query_result(unicode_query_results)

expected_default_result = ('basic_comp', True)
expected_multiple_result = [
    ComparatorResult('test', 'first_eq_comp', True),
    ComparatorResult('test', 'len_comp', True)]
expected_mismatch_result = [
    ComparatorResult('test', 'first_eq_comp', True),
    ComparatorResult('test', 'len_comp', False)]


def test_query_pair():
    l, r = PostgresDb(), PostgresDb()
    qp = QueryPair(l, query, r)
    assert qp._lquery == qp._rquery
    assert qp.empty
    assert qp.query_results == (None, None)
    assert qp.lresult is None
    assert qp.rresult is None

    qp2 = QueryPair(l, query)
    assert qp2._lquery == query
    assert qp2._right is None
    assert qp2._rquery is None
    assert qp2.query_results == (None, )

    with pytest.raises(TypeError):
        QueryPair(l, r, query)

    with pytest.raises(TypeError):
        QueryPair(l, query, r, 1234)


def test_query_pair_queries():
    l, r = PostgresDb(), PostgresDb()
    rquery = 'select * from somewhere where id in {{ a }}'
    qp = QueryPair(l, query, r, rquery)

    with mock.patch.object(qp._left, 'query', return_value=left_results):
        with mock.patch.object(qp._right, 'query', return_value=right_results):
            with mock.patch.object(qp, '_format_rquery') as mock_fmt:
                qp.get_query_results()
    assert mock_fmt.call_count == 1

    qp._lresult = left_results
    formatted = qp._format_rquery()
    assert formatted == 'select * from somewhere where id in (1, 4)'

    qp._rquery = 'select * from somewhere where id in {{ notreal }}'
    with pytest.raises(QueryFormatError):
        qp._format_rquery()

    qp._rquery = rquery
    qp._lresult = string_results
    formatted = qp._format_rquery()
    assert formatted == "select * from somewhere where id in ('one', 'four')"

    qp._lresult = unicode_results
    formatted = qp._format_rquery()
    assert formatted == "select * from somewhere where id in ('one', 'four')"


def test_comparator():
    qp1 = QueryPair(PostgresDb(), query, PostgresDb())
    qp2 = QueryPair(PostgresDb(), query, PostgresDb(), other_query)

    with pytest.raises(TypeError):
        Comparator(qp1)

    c = Comparator(qp=qp1)
    assert c._qp._lquery == query
    assert c._qp._rquery == query
    assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]
    assert c.name is None

    c = Comparator(qp=qp2, comps=comps.FIRST_COMP, name='Shirley')
    assert c._qp._lquery == query
    assert c._qp._rquery == other_query
    assert c._comps == [comps.COMPS.get(comps.FIRST_COMP)]
    assert c.name == 'Shirley'

    assert c.query_results == (None, None)
    assert c.results == list()


def test_compare_defaults():
    qp = QueryPair(PostgresDb(), query, PostgresDb())
    c = Comparator(qp=qp)

    with mock.patch.object(c._qp._left, 'query', return_value=left_results):
        with mock.patch.object(c._qp._right, 'query', return_value=right_results):
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
    assert c.query_results == (None, None)
    assert c.results == list()


def test_left_only_compare():
    qp = QueryPair(PostgresDb(), query)
    c = Comparator(qp=qp, comps=lambda x: bool(x[0]))

    with mock.patch.object(c._qp._left, 'query', return_value=left_results):
        assert c.get_query_results() == (left_results, )

    expected_comp_result = ('lambda x: bool(x[0]))', True)
    res = c.run_comparisons()[0]
    assert (res.name, res.result) == expected_comp_result
    assert c._complete is True


def test_comarison_result():
    pass


def test_compare_multiple():
    qp = QueryPair(PostgresDb(), query, PostgresDb())
    comparisons = [comps.FIRST_COMP, comps.LEN_COMP]
    c = Comparator(qp=qp, comps=comparisons)

    with mock.patch.object(c._qp._left, 'query', return_value=left_results):
        with mock.patch.object(c._qp._right, 'query', return_value=right_results):
            assert c.get_query_results() == (left_results, right_results)

    assert isinstance(c.lresult, QueryResult)
    assert str(c.lresult.a) == '(1, 4)'
    assert isinstance(c.rresult, QueryResult)
    assert str(c.lresult.b) == '(2, 5)'

    for i, result in enumerate(c.compare()):
        assert result == expected_multiple_result[i]


def test_compare_mismatch():
    qp = QueryPair(PostgresDb(), query, PostgresDb())
    comparisons = [comps.FIRST_COMP, comps.LEN_COMP]
    c = Comparator(qp=qp, comps=comparisons)

    with mock.patch.object(c._qp._left, 'query', return_value=left_results):
        with mock.patch.object(c._qp._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()
    assert c.query_results == (left_results, mismatch_right_results)
    assert res == expected_mismatch_result


def test_left_right_queries():
    qp1 = QueryPair(PostgresDb(), query, PostgresDb())
    qp2 = QueryPair(PostgresDb(), query, PostgresDb(), other_query)

    c1 = Comparator(qp=qp1)
    assert c1._qp._lquery == query
    assert c1._qp._rquery == query

    c = Comparator(qp=qp2)
    assert c._qp._lquery == query
    assert c._qp._rquery == other_query


def test_results_run():
    qp = QueryPair(PostgresDb(), query, PostgresDb())
    c = Comparator(qp=qp)

    with mock.patch.object(c._qp._left, 'query', return_value=left_results) as lq:
        with mock.patch.object(c._qp._right, 'query', return_value=right_results) as rq:
            res = c.get_query_results(run=True)
    c.get_query_results(run=True)

    assert lq.call_count == 1
    assert rq.call_count == 1
    assert res == (left_results, right_results)


def test_no_comps():
    qp = QueryPair(PostgresDb(), query, PostgresDb())
    c = Comparator(qp=qp, comps='notarealcomparison')
    assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]


def test_custom_comparison():
    def custom_comp(left, right):
        return len(left) < len(right)
    expected_result = [ComparatorResult('test', 'custom_comp', True),
                       ComparatorResult('test', 'lambda x, y: len(x) < len(y)', True)]

    qp = QueryPair(PostgresDb(), query, PostgresDb())
    c = Comparator(qp=qp, comps=[custom_comp, lambda x, y: len(x) < len(y)])

    with mock.patch.object(c._qp._left, 'query', return_value=left_results):
        with mock.patch.object(c._qp._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()

    assert res == expected_result


def test_non_bool_comparison():
    def custom_comp(left, right):
        return len(right) - len(left)
    expected_result = [ComparatorResult('test', 'custom_comp', 1)]

    qp = QueryPair(PostgresDb(), query, PostgresDb())
    c = Comparator(qp=qp, comps=custom_comp, name='test')

    with mock.patch.object(c._qp._left, 'query', return_value=left_results):
        with mock.patch.object(c._qp._right, 'query', return_value=mismatch_right_results):
            res = c.run_comparisons()

    assert res == expected_result
    comp = res[0]
    assert str(comp) == 'custom_comp : 1'
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
    qp1 = QueryPair(l, query, r)
    qp2 = QueryPair(l, query, r, other_query)

    with pytest.raises(TypeError):
        ComparatorSet()

    with pytest.raises(InvalidCompSetException):
        ComparatorSet('bananas')

    with pytest.raises(InvalidCompSetException):
        ComparatorSet(qp1, qp1)

    with pytest.raises(InvalidCompSetException):
        ComparatorSet([qp1, qp1], comps='not_a_callable')

    with pytest.raises(InvalidCompSetException):
        ComparatorSet([qp1, qp1], names='too_short')

    cs = ComparatorSet([qp1, qp1])
    for c in cs:
        assert isinstance(c, Comparator)
        assert c._qp._left is l
        assert c._qp._right is r
        assert c._qp._lquery == query
        assert c._qp._rquery == query
        assert c._comps == [comps.COMPS.get(comps.DEFAULT_COMP)]
        assert c.name is None

    with pytest.raises(InvalidCompSetException):
        ComparatorSet(
            [qp2, qp2],
            comps=[comps.LEN_COMP, comps.FIRST_COMP],
            names='Shirley')

    names = ['Shirley', 'Eugene']
    with pytest.raises(InvalidCompSetException):
        ComparatorSet(
            [qp2, qp2],
            comps=comps.COMPS.get(comps.LEN_COMP),
            names=names)

    with pytest.raises(InvalidCompSetException):
        ComparatorSet(
            [qp2, qp2],
            comps=[comps.LEN_COMP, 'nope'],
            names=names)

    cmps = [[comps.LEN_COMP], [comps.FIRST_COMP]]
    cs = ComparatorSet(
        [qp2, qp2],
        comps=cmps,
        names=names)
    for i, c in enumerate(cs):
        assert c._qp._left is l
        assert c._qp._right is r
        assert c._qp._lquery == query
        assert c._qp._rquery == other_query
        assert c._comps == [comps.COMPS.get(cmps[i][0])]
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
        ComparatorSet.from_dict()

    with pytest.raises(InvalidCompSetException):
        ComparatorSet.from_dict('what', l, r)

    cs = ComparatorSet.from_dict({'lquery': query, 'rquery': other_query}, l, r)
    assert isinstance(cs, ComparatorSet)

    qp = QueryPair(l, query, r, other_query)
    d1 = {'qp': qp}
    d2 = {'name': 'test', 'lquery': query, 'rquery': other_query, 'comps': comps.LEN_COMP}
    cs = ComparatorSet.from_dict([d1, d2], l, r, default_comp=comps.FIRST_COMP)
    for c in cs:
        assert c._qp._left is l
        assert c._qp._right is r
        assert c._qp._lquery == query
        assert c._qp._rquery == other_query
    assert cs[0].name is None
    assert cs[1].name == 'test'
    assert cs[0]._comps == [comps.COMPS.get(comps.FIRST_COMP)]
    assert cs[1]._comps == [comps.COMPS.get(comps.LEN_COMP)]

    with pytest.raises(IndexError):
        cs[2]
