"""Enum columns that reach the replicator through a realtime ALTER (ADD / MODIFY /
CHANGE) were stored as TableField(name, field_type) with additional_data=None -
only parse_mysql_table_structure filled the allowed values. The first non-NULL
value then raised "TypeError: object of type 'NoneType' has no len()" in
EnumConverter and crash-looped db_replicator (__nudge_execution.status,
2026-11-15, ten weeks after its .12-era MODIFY; 20 such fields existed in prod).

Also: the enum branch matched `startswith('enum(')` case-sensitively, so
uppercase `ENUM(...)` columns (what Sequelize emits) bypassed label conversion
and pushed MySQL's raw index to ClickHouse - wrong whenever CH's numbering
diverged from MySQL's after an enum evolution.
"""
from mysql_ch_replicator.converter import MysqlToClickhouseConverter
from mysql_ch_replicator.table_structure import TableField, TableStructure


def _structs(mysql_type, ch_type, additional_data=None):
    my = TableStructure(); ch = TableStructure()
    my.fields = [TableField(name='id', field_type='int', parameters='NOT NULL'),
                 TableField(name='status', field_type=mysql_type, additional_data=additional_data)]
    ch.fields = [TableField(name='id', field_type='Int32'), TableField(name='status', field_type=ch_type)]
    for s in (my, ch):
        s.primary_keys = ['id']; s.preprocess()
    return my, ch


def test_missing_additional_data_is_healed_and_value_converted_by_label():
    my, ch = _structs("enum('pending','materializing','processing')",
                      "Enum8('pending' = 1, 'processing' = 2, 'materializing' = 3)")
    conv = MysqlToClickhouseConverter(db_replicator=None)
    out = conv.convert_records([[1, 2]], my, ch)[0]
    assert out[1] == 'materializing'                      # MySQL index 2, NOT CH's 'processing' = 2
    assert my.fields[1].additional_data == ['pending', 'materializing', 'processing']  # persisted on the field


def test_uppercase_enum_type_is_converted_too():
    my, ch = _structs("ENUM('owner','sponsor')", "Nullable(Enum8('owner' = 1, 'sponsor' = 2))")
    conv = MysqlToClickhouseConverter(db_replicator=None)
    assert conv.convert_records([[7, 2]], my, ch)[0][1] == 'sponsor'
    assert conv.convert_records([[8, None]], my, ch)[0][1] is None


def test_alter_built_field_carries_enum_values():
    f = MysqlToClickhouseConverter._mysql_table_field('kind', "ENUM('tenant', 'network') NOT NULL")
    assert f.additional_data == ['tenant', 'network']
    assert MysqlToClickhouseConverter._mysql_table_field('n', 'int').additional_data is None
