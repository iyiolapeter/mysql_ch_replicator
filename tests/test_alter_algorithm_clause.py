import pytest
from mysql_ch_replicator.converter import MysqlToClickhouseConverter


@pytest.mark.parametrize("query", [
    # The exact statement that crash-looped production: ADD COLUMN with an
    # ALGORITHM=INSTANT online-DDL hint (emitted by MySQL 8 / Sequelize).
    "ALTER TABLE `payment` ADD COLUMN `paid_at` DATETIME NULL, ALGORITHM=INSTANT",
    # ALGORITHM + LOCK together, across different operations.
    "ALTER TABLE `t` ADD COLUMN `x` INT, ALGORITHM=INSTANT, LOCK=NONE",
    "ALTER TABLE `t` MODIFY COLUMN `c` DECIMAL(10,2), ALGORITHM=INPLACE, LOCK=NONE",
    "ALTER TABLE `t` DROP COLUMN `c`, ALGORITHM=INPLACE",
    # spacing / case variants
    "ALTER TABLE `t` ADD COLUMN `x` INT, ALGORITHM = INSTANT",
    "ALTER TABLE `t` ADD COLUMN `x` INT, algorithm=copy, lock=shared",
])
def test_convert_alter_query_ignores_algorithm_and_lock_clauses(query):
    """MySQL online-DDL hints (ALGORITHM=.../LOCK=...) are appended as comma-
    separated clauses, and split_high_level turns each into its own "subquery".
    They mean nothing to ClickHouse and must be skipped.

    Regression: previously an ALGORITHM=/LOCK= subquery was a single token, so
    `tokens = tokens[1:]` emptied the list and the next `tokens[0]` raised
    IndexError — crashing mid-ALTER *after* the ADD/MODIFY had already been
    applied to ClickHouse, which then forced a DUPLICATE_COLUMN replay loop
    (a real production incident on `payment.paid_at`).
    """
    conv = MysqlToClickhouseConverter(db_replicator=None)
    # db_replicator=None => no ClickHouse side effects; this exercises only the
    # alter-query parsing / clause handling, which must not raise.
    conv.convert_alter_query(query, 'test_db')
