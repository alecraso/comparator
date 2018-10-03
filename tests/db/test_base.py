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

    db.connect()
    assert db._conn is None
    assert db.connected is False

    db._conn = 'immaconnection'
    db.connect()
    assert db.connected is True

    db.close()
    assert db._conn is None
    assert db.connected is False

    db.close()

    with pytest.raises(TypeError):
        db.query()
    db.query('select * from nowhere')

    with pytest.raises(TypeError):
        db.execute()
    db.execute("insert into nowhere select 'nothing'")
