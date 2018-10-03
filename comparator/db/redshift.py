"""
    Class for using Redshift as a source database
"""
from comparator.db.postgres import PostgresDb


class RedshiftDb(PostgresDb):
    _db_type = 'redshift+psycopg2'

    def __init__(self, *args, **kwargs):
        port = 5439
        if 'port' in kwargs:
            port = kwargs.pop('port')

        conn_params = {'sslmode': 'prefer'}

        super(RedshiftDb, self).__init__(
            *args, port=port, conn_params=conn_params, **kwargs)

    def _query(self, query_string, **kwargs):
        result = super(RedshiftDb, self)._query(query_string, **kwargs)
        self.close()  # Close the connection each time since we're using SSL
        return result
