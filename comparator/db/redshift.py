"""
    Class for using Redshift as a source database
"""
from comparator.db.postgres import PostgresDb


class RedshiftDb(PostgresDb):

    def __init__(self, *args, **kwargs):
        self._db_type = 'redshift+psycopg2'
        self._conn_kwargs['port'] = 5439
        super(RedshiftDb, self).__init__(*args, **kwargs)
