import pytest
from mysql_ch_replicator.converter import MysqlToClickhouseConverter


@pytest.mark.parametrize("query", [
    # Production crash: MODIFY/CHANGE of an ENUM whose values are separated by
    # ", " (comma-space). The raw whitespace split fragmented enum('a', 'b')
    # across tokens, handing the enum parser a truncated "enum('a'," ->
    # ValueError('Unbalanced parentheses in enum definition'). ADD already
    # re-tokenized; MODIFY and CHANGE did not.
    "ALTER TABLE `__nudge_execution` CHANGE `status` `status` ENUM('pending', 'materializing', 'processing', 'completed', 'failed') NOT NULL DEFAULT 'pending'",
    "ALTER TABLE `t` MODIFY `status` ENUM('a', 'b', 'c') NOT NULL",
    "ALTER TABLE `t` MODIFY COLUMN `status` ENUM('a', 'b') NOT NULL",
    "ALTER TABLE `t` MODIFY `amount` DECIMAL(19, 8) NOT NULL",   # decimal with space too
    "ALTER TABLE `t` CHANGE `old` `new` ENUM('a', 'b', 'c') NOT NULL",
    # no-space forms must still work
    "ALTER TABLE `t` CHANGE `s` `s` ENUM('a','b','c') NOT NULL",
    "ALTER TABLE `t` MODIFY `x` INT",
])
def test_spaced_parenthesized_types_parse(query):
    conv = MysqlToClickhouseConverter(db_replicator=None)
    conv.convert_alter_query(query, 'test_db')


def test_modify_decimal_with_space_maps_to_decimal():
    """Regression: MODIFY `a` DECIMAL(19, 8) used to hand convert_type the
    truncated token 'DECIMAL(19,' and silently fall back to Int64."""
    conv = MysqlToClickhouseConverter(db_replicator=None)
    assert conv.convert_field_type("DECIMAL(19, 8)", "NOT NULL") == "Decimal(19, 8)"


@pytest.mark.parametrize("current,new_mysql,expected", [
    # Inserting a value mid-list must NOT renumber existing elements — CH stores
    # the integer and rejects renumbering (code 70). Existing numbers are kept;
    # the new label is appended with a fresh number.
    (
        "Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4)",
        "ENUM('pending', 'materializing', 'processing', 'completed', 'failed')",
        "Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4, 'materializing' = 5)",
    ),
    # Appending at the end is likewise preserved.
    (
        "Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4)",
        "ENUM('pending', 'processing', 'completed', 'failed', 'abandoned')",
        "Enum8('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4, 'abandoned' = 5)",
    ),
])
def test_merge_ch_enum_preserves_existing_numbers(current, new_mysql, expected):
    conv = MysqlToClickhouseConverter(db_replicator=None)
    new_ch = conv.convert_field_type(new_mysql, "NOT NULL")
    merged = conv._merge_ch_enum(current, new_ch)
    assert merged == expected
    # idempotent: replaying the ALTER after it applied yields no further change
    assert conv._merge_ch_enum(merged, new_ch) == expected


def test_merge_ch_enum_passthrough_for_non_enum():
    conv = MysqlToClickhouseConverter(db_replicator=None)
    assert conv._merge_ch_enum("Int32", "Int64") == "Int64"
    assert conv._merge_ch_enum("Decimal(19, 4)", "Decimal(19, 8)") == "Decimal(19, 8)"
