"""JSONB OPAQUE scalars (a DECIMAL / DATETIME value that MySQL stores *typed*
inside a JSON document, e.g. JSON_SET(params, '$.amount', amount) with a
DECIMAL column) crashed binlog_replicator: the C++ accelerator
(cpp_accelerated -> libmysqljsonparse via ctypes) has no OPAQUE case and
throws std::runtime_error("invalid scalar type"); a C++ exception crossing a
ctypes boundary cannot be caught in Python, so the whole process died with
SIGABRT and crash-looped production for ~32h (2026-11-07, payment 555952).
packet.read_binary_json now uses the pure-Python parser, which handles OPAQUE.

The payload below is hand-encoded (no production data) in MySQL's JSONB
small-object layout with a single key "amount" -> OPAQUE(NEWDECIMAL 46511627.91).
"""
import json
import struct
from decimal import Decimal

from mysql_ch_replicator.pymysqlreplication import packet
from mysql_ch_replicator.pymysqlreplication.packet import decode_binary_json, _normalize_json_value

JSONB_TYPE_SMALL_OBJECT = 0x00
JSONB_TYPE_OPAQUE = 0x0F
MYSQL_TYPE_NEWDECIMAL = 0xF6


def _opaque_newdecimal_46511627_91() -> bytes:
    # MySQL binary decimal for DECIMAL(10,2) 46511627.91: precision, scale,
    # then integer part 46511627 (8 digits -> 4 bytes big-endian, sign bit of
    # the first byte flipped for a positive number) and fraction 91 (1 byte).
    int_part = struct.pack('>I', 46511627)
    payload = bytes([10, 2]) + bytes([int_part[0] | 0x80]) + int_part[1:] + bytes([91])
    return bytes([MYSQL_TYPE_NEWDECIMAL, len(payload)]) + payload   # type, varlen length, value


def _small_object_with_opaque_amount() -> bytes:
    key = b'amount'
    value = _opaque_newdecimal_46511627_91()
    # header: element_count, size | key entry: key_offset, key_len | value entry: type, offset
    header_len = 2 + 2 + 4 + 3
    key_offset = header_len
    value_offset = key_offset + len(key)
    size = value_offset + len(value)
    body = (struct.pack('<HH', 1, size)
            + struct.pack('<HH', key_offset, len(key))
            + bytes([JSONB_TYPE_OPAQUE]) + struct.pack('<H', value_offset)
            + key + value)
    assert len(body) == size
    return bytes([JSONB_TYPE_SMALL_OBJECT]) + body


def test_opaque_decimal_document_decodes_to_a_json_number():
    blob = _small_object_with_opaque_amount()
    assert blob[1 + 8:1 + 8 + 1] == bytes([JSONB_TYPE_OPAQUE])   # the value entry is the crashing scalar type
    text = decode_binary_json(blob)
    assert json.loads(text) == {'amount': 46511627.91}


def test_normalize_turns_bytes_keys_and_values_into_str():
    assert _normalize_json_value({b'k': [b'v', 1, {b'n': None}]}) == {'k': ['v', 1, {'n': None}]}


def test_decimal_serialisation():
    assert json.loads(json.dumps(Decimal('5.00'), default=packet._json_default)) == 5
    assert json.loads(json.dumps(Decimal('5.25'), default=packet._json_default)) == 5.25


def test_ctypes_accelerator_is_no_longer_on_the_decode_path():
    assert 'cpp_mysql_to_json' not in open(packet.__file__).read()
