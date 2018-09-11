"""
    Class for using Redshift as a source database
"""
from comparator.db.postgres import PostgresDb


class RedshiftDb(PostgresDb):
    _db_type = 'redshift+psycopg2'

    def __init__(self, *args, **kwargs):
        super(RedshiftDb, self).__init__(*args, port=5439, **kwargs)
