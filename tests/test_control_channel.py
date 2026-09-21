"""Unit tests for the file-based runner <-> db_replicator control channel."""
import json
import os

from mysql_ch_replicator.control import ControlChannel


def test_submit_take_roundtrip(tmp_path):
    ch = ControlChannel(str(tmp_path))
    r1 = ch.submit('skip', table='a')
    r2 = ch.submit('rebootstrap', table='b')
    assert r1['op'] == 'skip' and r1['table'] == 'a' and r1['id'] != r2['id']

    taken = ch.take_requests()
    assert [r['op'] for r in taken] == ['skip', 'rebootstrap']
    # drained: a second take returns nothing, and the file is an empty list
    assert ch.take_requests() == []
    assert json.load(open(os.path.join(str(tmp_path), 'control.json'))) == []


def test_status_roundtrip_and_defaults(tmp_path):
    ch = ControlChannel(str(tmp_path))
    assert ch.read_status() == {}
    ch.write_status({'skipped_tables': ['x'], 'position': ('mysql-bin.000001', 4)})
    status = ch.read_status()
    assert status['skipped_tables'] == ['x']
    assert status['position'] == ['mysql-bin.000001', 4]  # JSON turns the tuple into a list
    assert 'updated' in status


def test_corrupt_files_are_tolerated(tmp_path):
    ch = ControlChannel(str(tmp_path))
    open(ch.requests_path, 'w').write('{not json')
    open(ch.status_path, 'w').write('')
    assert ch.take_requests() == []
    assert ch.read_status() == {}
