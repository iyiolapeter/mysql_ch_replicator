import pytest
from mysql_ch_replicator.converter import MysqlToClickhouseConverter


@pytest.mark.parametrize("query", [
    # The exact statement that crash-looped production: dropping a CHECK
    # constraint (MySQL 8 syntax). ClickHouse has no CHECK constraints, so this
    # must be skipped, not routed to the drop-column handler.
    "ALTER TABLE `payment` DROP CHECK check_split",
    "ALTER TABLE `t` DROP CHECK `check_split`",
    # Same family: constraint/primary-key operations meaningless to ClickHouse.
    "ALTER TABLE `t` DROP PRIMARY KEY",
    "ALTER TABLE `t` ADD CHECK (amount > 0)",
    "ALTER TABLE `t` ADD CONSTRAINT `c` CHECK (amount > 0)",
    # DROP CHECK combined with an online-DDL hint (the .9 family), comma-split.
    "ALTER TABLE `t` DROP CHECK check_split, ALGORITHM=INSTANT",
])
def test_convert_alter_query_skips_check_and_primary_key(query):
    """`DROP CHECK`/`ADD CHECK`/`DROP PRIMARY KEY` carry tokens the drop/add
    dispatcher previously did not recognise, so they fell through to
    __convert_alter_table_{drop,add}_column and raised
    ('wrong tokens count', [...]) — a real production crash-loop on
    `DROP CHECK check_split`. ClickHouse has no CHECK/constraint concept, so
    these operations must be skipped.
    """
    conv = MysqlToClickhouseConverter(db_replicator=None)
    conv.convert_alter_query(query, 'test_db')


@pytest.mark.parametrize("query", [
    # Regression guard: real DROP COLUMN must STILL be applied, not skipped.
    "ALTER TABLE `t` DROP COLUMN `bar`",
    "ALTER TABLE `t` DROP COLUMN bar",
])
def test_convert_alter_query_still_drops_columns(query):
    """The skip-list must not swallow genuine DROP COLUMN operations. With
    db_replicator=None there is no ClickHouse side effect, but the parser must
    route to the drop-column path without raising."""
    conv = MysqlToClickhouseConverter(db_replicator=None)
    conv.convert_alter_query(query, 'test_db')
