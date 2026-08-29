import pytest
from mysql_ch_replicator.converter import MysqlToClickhouseConverter


@pytest.mark.parametrize("mysql_type,expected", [
    # DECIMAL and NUMERIC are MySQL synonyms and must both map to CH Decimal.
    ("decimal(19,8)", "Decimal(19, 8)"),
    ("decimal(19,4)", "Decimal(19, 4)"),
    ("decimal(17,2)", "Decimal(17, 2)"),
    ("decimal(6,4)", "Decimal(6, 4)"),
    ("decimal(33,16)", "Decimal(33, 16)"),
    ("numeric(19,8)", "Decimal(19, 8)"),
    # unsigned decimals with a fractional part still map to signed CH Decimal.
    ("decimal(19,8) unsigned", "Decimal(19, 8)"),
])
def test_decimal_maps_to_ch_decimal(mysql_type, expected):
    """Regression: a bare `decimal` used to fall through to a Float64 fallback,
    silently losing precision on money columns whenever a decimal(p, s)
    precision appeared that was not enumerated in the config types_mapping
    (production incident: unit_cost widened decimal(19,4) -> decimal(19,8)
    became Float64). DECIMAL must map to Decimal(p, s) like NUMERIC.
    db_replicator=None => empty config types_mapping, so the hardcoded mapping
    path is exercised directly.
    """
    conv = MysqlToClickhouseConverter(db_replicator=None)
    assert conv.convert_type(mysql_type, mysql_type) == expected


@pytest.mark.parametrize("mysql_type,expected", [
    # scale == 0 => integer type (mirrors the numeric branch), not Float64.
    ("decimal(9,0)", "Int32"),
    ("decimal(18,0)", "Int64"),
    ("decimal(9,0) unsigned", "UInt32"),
])
def test_zero_scale_decimal_maps_to_integer(mysql_type, expected):
    conv = MysqlToClickhouseConverter(db_replicator=None)
    assert conv.convert_type(mysql_type, mysql_type) == expected
