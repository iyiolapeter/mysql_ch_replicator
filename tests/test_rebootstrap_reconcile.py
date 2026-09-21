"""Pure unit tests for the live single-table rebootstrap's window
reconciliation - no MySQL / ClickHouse needed."""
from mysql_ch_replicator.binlog_replicator import LogEvent, EventType, FileReader
from mysql_ch_replicator.rebootstrap import compute_window_deletes, iter_window_events

T = 'gift_request_recipient'
ADD, REM, QRY = EventType.ADD_EVENT.value, EventType.REMOVE_EVENT.value, EventType.QUERY.value


def ev(event_type, table=T, records=None, pos=0):
    return LogEvent(transaction_id=('mysql-bin.000001', pos), db_name='db', table_name=table,
                    records=records, event_type=event_type)


def key_fn(event):
    # records are already primary-key ids in these tests
    return list(event.records)


def test_only_keys_whose_last_event_is_a_delete_are_deleted():
    events = [
        ev(REM, records=['1', '2']),   # 1, 2 deleted
        ev(ADD, records=['2']),        # 2 re-inserted -> must NOT be deleted
        ev(REM, records=['3']),        # 3 deleted
        ev(ADD, records=['4']),        # 4 only inserted
    ]
    ids, saw_ddl = compute_window_deletes(events, T, key_fn)
    assert ids == {'1', '3'}
    assert saw_ddl is False


def test_other_tables_are_ignored():
    events = [ev(REM, table='other', records=['1']), ev(REM, records=['9'])]
    ids, _ = compute_window_deletes(events, T, key_fn)
    assert ids == {'9'}


def test_ddl_for_the_table_is_flagged():
    events = [ev(QRY, table='', records=f'ALTER TABLE `{T}` ADD COLUMN x INT'), ev(REM, records=['1'])]
    ids, saw_ddl = compute_window_deletes(events, T, key_fn)
    assert saw_ddl is True
    assert ids == {'1'}


def test_ddl_for_other_tables_is_not_flagged():
    events = [ev(QRY, table='', records='ALTER TABLE `payment` ADD COLUMN x INT')]
    _, saw_ddl = compute_window_deletes(events, T, key_fn)
    assert saw_ddl is False


def test_empty_window():
    ids, saw_ddl = compute_window_deletes([], T, key_fn)
    assert ids == set() and saw_ddl is False


class _FakeReader:
    def __init__(self, events):
        self.events, self.i, self.position = events, 0, None

    def set_position(self, position):
        self.position = position

    def read_next_event(self):
        if self.i >= len(self.events):
            return None
        e = self.events[self.i]
        self.i += 1
        return e


def test_iter_window_events_stops_at_end_position():
    events = [ev(ADD, records=['1'], pos=10), ev(ADD, records=['2'], pos=20), ev(ADD, records=['3'], pos=30)]
    reader = _FakeReader(events)
    out = list(iter_window_events(reader, ('mysql-bin.000001', 5), ('mysql-bin.000001', 20)))
    assert reader.position == ('mysql-bin.000001', 5)
    assert [e.transaction_id[1] for e in out] == [10, 20]


def test_iter_window_events_without_end_reads_to_eof():
    events = [ev(ADD, records=['1'], pos=10), ev(ADD, records=['2'], pos=20)]
    out = list(iter_window_events(_FakeReader(events), ('mysql-bin.000001', 1)))
    assert len(out) == 2


# ---- open_reader_at: exact-offset window reader over real .bin files -------
from mysql_ch_replicator.binlog_replicator import FileWriter
from mysql_ch_replicator.rebootstrap import open_reader_at


class _Settings:
    def __init__(self, data_dir):
        self.data_dir = data_dir


def _write_bin(path, events):
    w = FileWriter(path)
    for e in events:
        w.write_event(e)
    w.close()


def test_open_reader_at_resumes_after_offset_and_rolls_over(tmp_path):
    db = 'db'
    (tmp_path / db).mkdir()
    settings = _Settings(str(tmp_path))
    f1 = [ev(ADD, records=['1'], pos=10), ev(ADD, records=['2'], pos=20), ev(REM, records=['2'], pos=30)]
    f2 = [ev(ADD, records=['3'], pos=40)]
    _write_bin(str(tmp_path / db / '7.bin'), f1)
    _write_bin(str(tmp_path / db / '8.bin'), f2)

    # Simulate the realtime reader having just processed event pos=10 in 7.bin:
    # its file.tell() is the offset right after that event.
    probe = FileReader(str(tmp_path / db / '7.bin'))
    probe.read_next_event()
    offset_after_first = probe.file.tell()
    probe.close()

    reader = open_reader_at(settings, db, 7, offset_after_first)
    out = list(iter_window_events(reader))  # no start_position: already positioned
    assert [(e.event_type, e.transaction_id[1]) for e in out] == [(ADD, 20), (REM, 30), (ADD, 40)]

    # and with an end position, it stops inclusively
    reader = open_reader_at(settings, db, 7, offset_after_first)
    out = list(iter_window_events(reader, None, ('mysql-bin.000001', 30)))
    assert [e.transaction_id[1] for e in out] == [20, 30]

    ids, saw_ddl = compute_window_deletes(out, T, key_fn)
    assert ids == {'2'} and saw_ddl is False
