import pytest


class mock_result:
    def __init__(self, sql):
        self.sql = sql

    def fetchall(self):
        return [self.sql]


class mock_engine:
    def __init__(self, url, **kwargs):
        self.url = str(url)

    def connect(self):
        return self

    def close(self):
        pass

    def execute(self, sql, **kwargs):
        return mock_result(sql)


@pytest.fixture(scope='module')
def mock_create_engine():
    def _create_engine(url, **kwargs):
        return mock_engine(url, **kwargs)

    return _create_engine
