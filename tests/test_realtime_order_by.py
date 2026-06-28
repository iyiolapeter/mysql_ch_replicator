from common import *

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api


def test_realtime_create_table_applies_configured_order_by():
    """Regression: a table created AFTER replication starts (via the realtime
    binlog CREATE event) must get the configured order_bys.

    Bug: handle_create_table_query passed additional_indexes and
    additional_partition_bys to create_table but OMITTED additional_order_bys,
    so a realtime-created table silently got ORDER BY = primary key instead of
    the configured order_bys. The initial-replication path was unaffected, and
    the existing db-mapping test only exercises the initial path (table created
    BEFORE the replicator starts) — so this gap was uncovered.
    """
    # order_bys maps test_table -> 'name, id'; target_databases maps the db to mapped_target_db.
    config_file = "tests/tests_config_db_mapping.yaml"
    cfg = config.Settings()
    cfg.load(config_file)

    mysql = mysql_api.MySQLApi(database=None, mysql_settings=cfg.mysql)
    ch = clickhouse_api.ClickhouseApi(database="mapped_target_db", clickhouse_settings=cfg.clickhouse)

    ch.drop_database("mapped_target_db")
    assert_wait(lambda: "mapped_target_db" not in ch.get_databases())
    prepare_env(cfg, mysql, ch, db_name=TEST_DB_NAME)

    # Warm-up table created BEFORE the replicator starts, so initial replication
    # completes, the mapped target db exists, and the replicator enters realtime
    # BEFORE we create the table under test.
    mysql.execute(f'''
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME_2}` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`));
    ''')
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME_2}` (id, name) VALUES (1, 'warmup')",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()
    try:
        # Initial replication done -> replicator is now in realtime.
        assert_wait(lambda: "mapped_target_db" in ch.get_databases())
        ch.execute_command('USE `mapped_target_db`')
        assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

        # Create the table-under-test NOW -> goes through the realtime
        # handle_create_table_query path (NOT initial replication).
        mysql.execute(f'''
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`));
        ''')

        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())

        # The configured order_by must be applied by the realtime CREATE path.
        create_query = ch.show_create_table(TEST_TABLE_NAME)
        assert 'ORDER BY (name, id)' in create_query, (
            f"realtime CREATE did not apply configured order_by; got:\n{create_query}"
        )
    finally:
        db_replicator_runner.stop()
        binlog_replicator_runner.stop()
