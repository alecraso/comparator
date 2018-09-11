import os

from comparator.db import BaseDb, PostgresDb, RedshiftDb

uname = os.uname()[1]
expected_db_type = 'redshift+psycopg2'
expected_default_url = '{0}://{1}@localhost:5439/{1}'.format(expected_db_type, uname)


def test_redshift():
    rs = RedshiftDb()

    assert isinstance(rs, BaseDb)
    assert isinstance(rs, PostgresDb)
    assert rs._conn_kwargs
    assert rs._conn_kwargs['port'] == 5439
    assert rs._db_type == expected_db_type

    assert repr(rs) == "<class 'comparator.db.redshift.RedshiftDb'> -- {}".format(expected_default_url)
    assert str(rs) == 'localhost'


def test_postgres_and_redshift():
    rs = RedshiftDb(name='red')
    pg = PostgresDb(name='blue')

    assert str(rs) == 'red'
    assert str(pg) == 'blue'

    assert rs._conn_kwargs
    assert rs._conn_kwargs['port'] == 5439
    assert rs._db_type == expected_db_type

    assert pg._conn_kwargs
    assert pg._conn_kwargs['port'] == 5432
    assert pg._db_type == 'postgresql'
