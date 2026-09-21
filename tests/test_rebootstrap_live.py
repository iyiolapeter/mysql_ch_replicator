"""End-to-end tests for the live per-table control plane (skip / unskip /
rebootstrap) driven through the runner's HTTP endpoints. Needs the docker
MySQL + ClickHouse from docker-compose-tests.yaml, like the other e2e tests."""
from common import *
import requests
import time
from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api

CONTROL_URL = 'http://localhost:9128'
SHADOW = f'{TEST_TABLE_NAME}__rb'


def _status():
    return requests.get(f'{CONTROL_URL}/db/{TEST_DB_NAME}/status').json()


def _post(path):
    r = requests.post(f'{CONTROL_URL}/db/{TEST_DB_NAME}/{path}')
    assert r.status_code == 200, r.text
    return r.json()


def _ch_rows(ch, table=TEST_TABLE_NAME):
    return {r['id']: r['name'] for r in ch.select(table, final=True)}


def _mysql_rows(mysql, table=TEST_TABLE_NAME):
    mysql.set_database(TEST_DB_NAME)
    mysql.execute(f'SELECT id, name FROM `{table}` ORDER BY id')  # execute() returns None; read the cursor
    return {row[0]: row[1] for row in mysql.cursor.fetchall()}


def _setup(n_rows):
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)
    mysql = mysql_api.MySQLApi(database=None, mysql_settings=cfg.mysql)
    ch = clickhouse_api.ClickhouseApi(database=TEST_DB_NAME, clickhouse_settings=cfg.clickhouse)
    prepare_env(cfg, mysql, ch)
    mysql.execute(f"CREATE TABLE `{TEST_TABLE_NAME}` (id int NOT NULL, name varchar(255), PRIMARY KEY (id));")
    mysql.execute(f"CREATE TABLE `{TEST_TABLE_NAME_2}` (id int NOT NULL, PRIMARY KEY (id));")
    values = ', '.join(f"({i}, 'name_{i}')" for i in range(1, n_rows + 1))
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES {values}", commit=True)

    runner = RunAllRunner()
    runner.run()
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(_ch_rows(ch)) == n_rows, max_wait_time=60)
    assert_wait(lambda: 'skipped_tables' in _status(), max_wait_time=30)  # control plane up
    return cfg, mysql, ch, runner


def _marker(mysql, ch, marker_id):
    """Insert into the second table and wait for it: proves the replicator has
    processed everything issued before this point."""
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME_2}` (id) VALUES ({marker_id})", commit=True)
    assert_wait(lambda: marker_id in {r['id'] for r in ch.select(TEST_TABLE_NAME_2, final=True)}, max_wait_time=30)


def test_skip_unskip_and_rebootstrap_rebuilds_the_table():
    cfg, mysql, ch, runner = _setup(n_rows=5)
    try:
        # --- skip: DML for the table is no longer applied (no restart needed)
        _post(f'skip/{TEST_TABLE_NAME}')
        assert_wait(lambda: TEST_TABLE_NAME in _status()['skipped_tables'], max_wait_time=15)
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (6, 'name_6')", commit=True)
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE id = 1", commit=True)
        _marker(mysql, ch, 1)
        assert set(_ch_rows(ch)) == {1, 2, 3, 4, 5}  # unchanged: 6 not added, 1 not removed

        # --- unskip: new DML applies again; the skipped window stays stale
        _post(f'unskip/{TEST_TABLE_NAME}')
        assert_wait(lambda: TEST_TABLE_NAME not in _status()['skipped_tables'], max_wait_time=15)
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (7, 'name_7')", commit=True)
        assert_wait(lambda: 7 in _ch_rows(ch), max_wait_time=30)
        assert set(_ch_rows(ch)) == {1, 2, 3, 4, 5, 7}

        # --- rebootstrap: the table is rebuilt from MySQL truth, live
        _post(f'rebootstrap/{TEST_TABLE_NAME}')
        assert_wait(lambda: _status()['rebootstrap']['in_progress'], max_wait_time=15)
        assert_wait(lambda: not _status()['rebootstrap']['in_progress'], max_wait_time=120)
        assert _status()['rebootstrap']['last_error'] is None, _status()
        assert _ch_rows(ch) == _mysql_rows(mysql) == {i: f'name_{i}' for i in (2, 3, 4, 5, 6, 7)}
        assert SHADOW not in ch.get_tables()

        # --- realtime keeps working on the rebuilt table
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (8, 'name_8')", commit=True)
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE id = 2", commit=True)
        _marker(mysql, ch, 2)
        assert _ch_rows(ch) == _mysql_rows(mysql)
    finally:
        runner.stop()


def test_rebootstrap_under_concurrent_writes():
    cfg, mysql, ch, runner = _setup(n_rows=3000)
    try:
        _post(f'rebootstrap/{TEST_TABLE_NAME}')
        assert_wait(lambda: _status()['rebootstrap']['in_progress'], max_wait_time=15)
        # writes that overlap the load window: insert, update, delete,
        # and delete-then-reinsert (the case the window reconciliation must not over-delete)
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (5001, 'new')", commit=True)
        mysql.execute(f"UPDATE `{TEST_TABLE_NAME}` SET name = 'updated' WHERE id = 10", commit=True)
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE id IN (20, 21)", commit=True)
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE id = 30", commit=True)
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (30, 'reinserted')", commit=True)
        assert_wait(lambda: not _status()['rebootstrap']['in_progress'], max_wait_time=180)
        assert _status()['rebootstrap']['last_error'] is None, _status()
        _marker(mysql, ch, 1)

        expected = _mysql_rows(mysql)
        assert expected[10] == 'updated' and 20 not in expected and expected[30] == 'reinserted' and expected[5001] == 'new'
        assert_wait(lambda: _ch_rows(ch) == expected, max_wait_time=60)
        assert SHADOW not in ch.get_tables()
        assert 'state_worker' not in ' '.join(os.listdir(os.path.join(cfg.binlog_replicator.data_dir, TEST_DB_NAME)))
    finally:
        runner.stop()


def test_abort_leaves_live_table_intact():
    cfg, mysql, ch, runner = _setup(n_rows=2000)
    try:
        before = _ch_rows(ch)
        _post(f'rebootstrap/{TEST_TABLE_NAME}')
        assert_wait(lambda: _status()['rebootstrap']['in_progress'], max_wait_time=15)
        _post('rebootstrap/abort')
        assert_wait(lambda: not _status()['rebootstrap']['in_progress'], max_wait_time=60)
        assert 'requested by operator' in (_status()['rebootstrap']['last_error'] or '')
        assert SHADOW not in ch.get_tables()
        assert _ch_rows(ch) == before  # mirroring means an abort never loses live data
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (9001, 'after_abort')", commit=True)
        assert_wait(lambda: 9001 in _ch_rows(ch), max_wait_time=30)
    finally:
        runner.stop()
