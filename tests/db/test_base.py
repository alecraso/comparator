import mock
import pytest

import comparator.db


def test_abc():
    with pytest.raises(TypeError):
        comparator.db.base.BaseDb()


def test_base_db():
    db = comparator.db.base.BaseDb
    db.__abstractmethods__ = set()
    db = db()

    assert db._conn is None
    assert db.connected is False

    with pytest.raises(NotImplementedError):
        db.connect()
    assert db._conn is None
    assert db.connected is False

    db._conn = 'immaconnection'
    with mock.patch('comparator.db.base.BaseDb._connect'):
        db.connect()
    assert db.connected is True

    with mock.patch('comparator.db.base.BaseDb._close'):
        db.close()
    assert db._conn is None
    assert db.connected is False

    with mock.patch('comparator.db.base.BaseDb._connect'):
        db.connect()
    assert db.connected is False

    with mock.patch('comparator.db.base.BaseDb._close'):
        db.close()

    with pytest.raises(TypeError):
        db.query()

    with pytest.raises(TypeError):
        db.execute()

    with pytest.raises(NotImplementedError):
        db.query('select 1')

    with pytest.raises(NotImplementedError):
        db.execute('insert 1')

    with pytest.raises(NotImplementedError):
        db._close()
