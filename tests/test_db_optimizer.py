from common import *
import os

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api
from mysql_ch_replicator.db_optimizer import State as DbOptimizerState


def test_db_optimizer_runs_with_target_database_mapping():
    """Regression: db_optimizer must OPTIMIZE the renamed TARGET database.

    Bug: select_db_to_optimize() compared the MySQL *source* database name
    against the ClickHouse database list. With a target_databases mapping
    (source -> renamed target), the source name never matches a ClickHouse db,
    so the optimizer silently no-op'd — no table was ever OPTIMIZEd, and the
    failure was invisible because data correctness is unaffected (FINAL dedups
    at query time). This caused real part-bloat in production on a mapped DB.

    This test runs the full stack via run_all (which spawns db_optimizer) against
    the db-mapping config and asserts the optimizer actually processes the mapped
    database. On the buggy code the optimizer never records the db / never logs
    an OPTIMIZE, so the assertions below time out.
    """
    config_file = "tests/tests_config_db_mapping.yaml"  # maps replication-test_db -> mapped_target_db, optimize_interval=3
    cfg = config.Settings()
    cfg.load(config_file)

    mysql = mysql_api.MySQLApi(database=None, mysql_settings=cfg.mysql)
    ch = clickhouse_api.ClickhouseApi(database="mapped_target_db", clickhouse_settings=cfg.clickhouse)

    ch.drop_database("mapped_target_db")
    assert_wait(lambda: "mapped_target_db" not in ch.get_databases())

    prepare_env(cfg, mysql, ch, db_name=TEST_DB_NAME)

    mysql.execute(f'''
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`));
    ''')
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name) VALUES (1, 'one')",
        commit=True,
    )

    run = RunAllRunner(cfg_file=config_file)
    run.run()
    try:
        # 1. Initial replication into the mapped target database works.
        assert_wait(lambda: "mapped_target_db" in ch.get_databases())
        ch.execute_command('USE `mapped_target_db`')
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

        # 2. The db_optimizer (optimize_interval=3s) must actually process the
        #    source db -> mapped target. Its state records last_process_time for
        #    a db ONLY if select_db_to_optimize() returned it and
        #    optimize_database() ran — i.e. the mapping was honoured.
        opt_state_path = os.path.join(cfg.binlog_replicator.data_dir, 'db_optimizer.bin')

        def optimizer_processed_db():
            if not os.path.exists(opt_state_path):
                return False
            return TEST_DB_NAME in DbOptimizerState(opt_state_path).last_process_time

        assert_wait(optimizer_processed_db, max_wait_time=60)

        # 3. And it must have OPTIMIZEd the MAPPED target table (not the source
        #    name) — proves the source->target mapping is applied to CH ops.
        opt_log_path = os.path.join(cfg.binlog_replicator.data_dir, 'db_optimizer.log')
        if os.path.exists(opt_log_path):
            log = open(opt_log_path).read()
            assert f'Optimizing table mapped_target_db.{TEST_TABLE_NAME}' in log, (
                f'optimizer did not OPTIMIZE the mapped target table; log:\n{log}'
            )
    finally:
        run.stop()
