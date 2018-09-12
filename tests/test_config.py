import mock
import pytest
import os

try:
    from pathlib import Path
    Path().expanduser()
except (ImportError, AttributeError):
    from pathlib2 import Path

import comparator.config
import comparator.db


test_config_file = Path('./tests/configs/test_config.yaml').resolve()
test_not_exist_config_file = Path('./im_not_real.yaml').resolve()
test_config_path = Path.cwd().as_posix() + '/tests/configs/test_config.yaml'
test_bad_config_path = Path.cwd().as_posix() + '/tests/configs/test_bad_config.yaml'
test_nolist_config_path = Path.cwd().as_posix() + '/tests/configs/test_nolist_config.yaml'
test_nonexistent_config_path = 'tests/configs/test_nothere_config.yaml'

expected_db_list = [
    {
        'name': 'default',
        'database': 'mydb',
        'host': 'localhost',
        'password': 'sUp3rs3cReTz',
        'port': 5432,
        'username': 'postgres',
    },
    {
        'name': 'th1s is annoy;ing butwecan figureIt-out!',
        'project': 'my-gewgle-proyekt',
    },
    {
        'database': 'mydb',
        'host': 'this.hostpath.toatlly.exists.com',
        'password': 'm0ArSeCr1tZ',
        'port': 5432,
        'username': 'postgres',
        'whatsthisfor': 'idunno',
    },
    {
        'name': 'default',
        'project': 'ohnoitsadupe!',
    },
    {
        'name': 'default',
        'project': 'ANOTHERONE',
    },
    'whatamievendoinghere',
]
expected_db_names = [
    'default',
    'th1s_is_annoy_ing_butwecan_figureit_out',
    'default_1',
    'default_2',
]
expected_nolist_db_list = [
    {
        'name': 'thisaintalist',
        'database': 'mydb',
    },
]


def test_default_config():
    os.environ.setdefault('COMPARATOR_CONFIG_FILE', '')
    with mock.patch('comparator.config.DEFAULT_CONFIG_FILE', test_config_file):
        conf = comparator.config.DbConfig()
    del os.environ['COMPARATOR_CONFIG_FILE']

    assert conf._config == test_config_file
    assert conf.dbs == expected_db_list

    assert repr(conf) == "<class 'comparator.config.DbConfig'> -- %r" % test_config_file
    assert str(conf) == 'DbConfig: %r' % [db['name'] for db in expected_db_list
                                          if isinstance(db, dict) and
                                          db.get('name')]


def test_env_variable():
    os.environ.setdefault('COMPARATOR_CONFIG_FILE', test_config_path)
    conf = comparator.config.DbConfig()
    del os.environ['COMPARATOR_CONFIG_FILE']

    assert conf._config == test_config_file
    assert conf.dbs == expected_db_list


def test_passed_path():
    conf = comparator.config.DbConfig(test_config_path)
    assert conf._config == test_config_file
    assert conf.dbs == expected_db_list


def test_file_not_exists():
    with mock.patch('comparator.config.DEFAULT_CONFIG_FILE', None):
        with pytest.raises(AttributeError):
            comparator.config.DbConfig()

    with mock.patch('comparator.config.DEFAULT_CONFIG_FILE', test_config_file):
        conf = comparator.config.DbConfig(test_nonexistent_config_path)
    assert conf._config == test_config_file


def test_config_attributes():
    conf = comparator.config.DbConfig(test_config_path)
    for name in expected_db_names:
        assert hasattr(conf, name)
        assert isinstance(getattr(conf, name), dict)

    assert conf.default['host'] == 'localhost'
    assert conf.th1s_is_annoy_ing_butwecan_figureit_out['project'] == 'my-gewgle-proyekt'


def test_bad_yaml():
    conf = comparator.config.DbConfig(test_bad_config_path)
    assert conf.dbs == list()

    nolist_conf = comparator.config.DbConfig(test_nolist_config_path)
    assert nolist_conf.dbs == expected_nolist_db_list
