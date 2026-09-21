"""Live single-table rebootstrap.

Rebuild ONE table from MySQL while realtime replication keeps running for every
other table - without the whole-database ``_tmp`` + ``RENAME DATABASE`` swap that
``DbReplicatorInitial`` performs (which would replace the entire target DB).

Flow, driven from the realtime loop through ``Rebootstrap.poll()``:

1. ``start(table)``
   - load the table's CURRENT structure from MySQL and create a shadow table
     ``<table>__rb`` with the same ORDER BY / PARTITION BY / indexes a fresh
     initial load would use;
   - flush realtime's pending records, record the binlog position ``P``;
   - MIRROR realtime writes for the table into the shadow (live AND shadow), so
     an abort at any point is lossless - the live table never stops being kept
     current;
   - spawn the bulk-load workers (``--table X --target_table X__rb
     --fixed_version 1``), reusing the parallel initial-replication path.
2. ``poll()``  - non-blocking check on the workers.
3. ``finish()``
   - flush pending records, take the end position ``Q``;
   - reconcile deletes that raced the bulk load (``compute_window_deletes``);
   - ``EXCHANGE TABLES live AND shadow`` (atomic on an Atomic database);
   - drop the shadow (now holding the old data), stop mirroring, clear state.

Version semantics: snapshot rows carry a fixed ``_version`` of 1; mirrored
realtime rows use the LIVE table's counter (much higher). ReplacingMergeTree
keeps the max ``_version`` per key, so a realtime write always wins over a stale
snapshot row regardless of the order in which they physically landed.

Why the delete reconciliation: ClickHouse deletes are mutations, not versioned
rows. A snapshot chunk read *before* a MySQL DELETE but inserted *after* the
delete event was applied to the shadow resurrects the row. Re-applying every
DELETE from the window would over-delete rows that were deleted and then
re-inserted, so only keys whose LAST event in the window is a REMOVE are
deleted.

Crash behaviour: the load is not resumable across a db_replicator restart (the
worker handles are lost), so on restart an in-progress rebootstrap is aborted
(shadow dropped, mirroring cleared). The live table is untouched; re-run.
"""
import copy
import glob
import hashlib
import os
import subprocess
import sys
import time
from logging import getLogger

from .binlog_replicator import DataReader, FileReader, EventType, get_file_name_by_num
from .converter import strip_sql_comments

logger = getLogger(__name__)


def compute_window_deletes(events, table_name: str, key_fn):
    """Return ``(ids_to_delete, saw_ddl)`` for the events of one table.

    ``events``  - iterable of LogEvent, in binlog order, from position P.
    ``key_fn``  - ``key_fn(event) -> list[str]`` giving the primary-key id of
                  every record in the event (same encoding realtime uses for
                  its erase batches).
    ``saw_ddl`` - True if a QUERY (DDL) event mentioning the table was seen;
                  the snapshot's structure may then be stale and the caller
                  should abort instead of swapping.
    """
    last_event_type = {}
    saw_ddl = False
    for event in events:
        if event.event_type == EventType.QUERY.value:
            query = event.records if isinstance(event.records, str) else ''
            if table_name in strip_sql_comments(query):
                saw_ddl = True
            continue
        if event.table_name != table_name:
            continue
        for key in key_fn(event):
            last_event_type[key] = event.event_type
    ids = {key for key, event_type in last_event_type.items() if event_type == EventType.REMOVE_EVENT.value}
    return ids, saw_ddl


def open_reader_at(replicator_settings, db_name: str, file_num: int, offset: int) -> DataReader:
    """A DataReader positioned at an exact byte offset of ``<file_num>.bin``.

    ``DataReader.set_position(P)`` locates P by scanning the cached binlog
    files linearly - minutes for multi-GB files, and it would block the
    realtime loop. The realtime reader's own ``(file_num, file.tell())`` taken
    right after it processed P is an O(1) resume point (FileReader keeps no
    partial buffer once a full event has been read). File rollover is handled
    by ``DataReader.read_next_event`` as usual.
    """
    reader = DataReader(replicator_settings, db_name)
    file_reader = FileReader(get_file_name_by_num(replicator_settings.data_dir, db_name, file_num))
    file_reader.file.seek(offset)
    reader.current_file_reader = file_reader
    return reader


def iter_window_events(reader: DataReader, start_position=None, end_position=None):
    """Yield events after ``start_position`` up to and including
    ``end_position`` (None = until the end of the cached binlog).

    If ``start_position`` is None the reader is assumed to be positioned
    already (see ``open_reader_at``); otherwise it is located with
    ``set_position`` (slow on large binlog caches - fallback only).
    """
    if start_position is not None:
        reader.set_position(start_position)
    while True:
        event = reader.read_next_event()
        if event is None:
            return
        if end_position is not None and event.transaction_id > end_position:
            return
        yield event


class Rebootstrap:
    SHADOW_SUFFIX = '__rb'
    SNAPSHOT_VERSION = 1
    POLL_INTERVAL = 2.0

    def __init__(self, replicator):
        self.replicator = replicator
        self.processes: list[subprocess.Popen] = []
        self.last_poll_time = 0.0

    # ---- persisted phase -----------------------------------------------------
    @property
    def info(self) -> dict | None:
        return self.replicator.state.rebootstrap

    def in_progress(self) -> bool:
        return bool(self.replicator.state.rebootstrap)

    def _save(self, **fields):
        info = dict(self.replicator.state.rebootstrap or {})
        info.update(fields)
        self.replicator.state.rebootstrap = info
        self.replicator.state.save()

    def status(self) -> dict:
        info = self.info
        return {
            'in_progress': bool(info),
            'info': info,
            'workers_alive': sum(1 for p in self.processes if p.poll() is None),
            'last_error': self.replicator.rebootstrap_last_error,
        }

    # ---- lifecycle -------------------------------------------------------------
    def recover_after_restart(self):
        """Called once at realtime start: a rebootstrap that was in progress
        when the process died cannot be resumed - roll it back."""
        if self.in_progress():
            self.abort('db_replicator restarted during the load; re-run the rebootstrap')

    def start(self, table_name: str):
        replicator = self.replicator
        if self.in_progress():
            raise Exception(f'rebootstrap already in progress: {self.info}')
        if not replicator.config.is_table_matches(table_name):
            raise Exception(f'table {table_name} does not match the configured tables')

        live = replicator.get_live_target_table_name(table_name)
        shadow = live + self.SHADOW_SUFFIX
        logger.info(f'rebootstrap {table_name}: live={live} shadow={shadow}')

        # Current MySQL schema - also refreshes what realtime uses for this table.
        mysql_structure, ch_structure = replicator.load_table_structure_from_mysql(table_name)
        replicator.state.tables_structure[table_name] = (mysql_structure, ch_structure)
        if table_name not in replicator.state.tables:
            replicator.state.tables.append(table_name)

        shadow_structure = copy.deepcopy(ch_structure)
        shadow_structure.table_name = shadow
        shadow_structure.if_not_exists = False
        replicator.clickhouse_api.drop_table(shadow)  # leftover of a previous failed attempt
        replicator.clickhouse_api.create_table(
            shadow_structure,
            additional_indexes=replicator.config.get_indexes(replicator.database, table_name),
            additional_partition_bys=replicator.config.get_partition_bys(replicator.database, table_name),
            additional_order_bys=replicator.config.get_order_bys(replicator.database, table_name),
        )

        # Everything up to P is in the live table; from P on, writes are mirrored.
        replicator.realtime_replicator.upload_records()
        start_position = replicator.state.last_processed_transaction
        # Exact resume point of the realtime reader (== "just after P"): lets
        # finish() reconcile the window without a linear seek through the cache.
        file_reader = replicator.data_reader.current_file_reader
        start_file_num = file_reader.file_num if file_reader else None
        start_offset = file_reader.file.tell() if file_reader else 0
        replicator.table_mirrors[table_name] = shadow
        replicator.skipped_tables.discard(table_name)  # a rebootstrap re-includes the table
        replicator.state.skipped_tables = list(replicator.skipped_tables)
        replicator.rebootstrap_last_error = None
        self._save(table=table_name, live=live, shadow=shadow,
                   start_position=start_position, start_file_num=start_file_num, start_offset=start_offset,
                   phase='loading', started=time.time())
        self._spawn_workers(table_name)

    def _spawn_workers(self, table_name: str):
        replicator = self.replicator
        total_workers = max(1, replicator.config.initial_replication_threads)
        shadow = self.info['shadow']
        # A worker that died mid-load leaves state_worker_*.pckl behind; a new
        # worker would RESUME from its saved max primary key and silently skip
        # the range before it. Always start the load from scratch.
        table_hash = hashlib.sha256(table_name.encode('utf-8')).hexdigest()[:16]
        for stale in glob.glob(os.path.join(os.path.dirname(replicator.state_path), f'state_worker_*_{table_hash}.pckl*')):
            logger.info(f'rebootstrap {table_name}: removing stale worker state {stale}')
            os.remove(stale)
        self.processes = []
        for worker_id in range(total_workers):
            cmd = [
                sys.executable, '-m', 'mysql_ch_replicator.main',
                'db_replicator',
                '--config', replicator.settings_file,
                '--db', replicator.database,
                '--worker_id', str(worker_id),
                '--total_workers', str(total_workers),
                '--table', table_name,
                '--target_db', replicator.target_database,
                '--target_table', shadow,
                '--fixed_version', str(self.SNAPSHOT_VERSION),
                '--initial_only=True',
            ]
            logger.info(f'rebootstrap {table_name}: launching worker {worker_id}: {" ".join(cmd)}')
            self.processes.append(subprocess.Popen(cmd))

    def poll(self):
        """Non-blocking; called from the realtime loop."""
        if not self.in_progress():
            return
        now = time.time()
        if now - self.last_poll_time < self.POLL_INTERVAL:
            return
        self.last_poll_time = now
        if self.info.get('phase') != 'loading':
            return
        if not self.processes:
            self.abort('no worker handles (process restarted during the load)')
            return
        failed = [p.returncode for p in self.processes if p.poll() not in (None, 0)]
        if failed:
            self.abort(f'bulk-load worker failed, exit codes {failed}')
            return
        if any(p.poll() is None for p in self.processes):
            return
        self.finish()

    def finish(self):
        replicator = self.replicator
        info = self.info
        table_name, live, shadow = info['table'], info['live'], info['shadow']
        realtime = replicator.realtime_replicator
        logger.info(f'rebootstrap {table_name}: workers done, reconciling')
        self._save(phase='reconciling')

        realtime.upload_records()  # mirrored rows up to Q are in the shadow
        end_position = replicator.state.last_processed_transaction

        mysql_structure, ch_structure = replicator.state.tables_structure[table_name]
        converter = replicator.converter

        def key_fn(event):
            only_primary = event.event_type == EventType.REMOVE_EVENT.value
            records = converter.convert_records(event.records, mysql_structure, ch_structure, only_primary=only_primary)
            return [realtime._get_record_id(ch_structure, record) for record in records]

        if info.get('start_file_num') is not None:
            reader = open_reader_at(replicator.config.binlog_replicator, replicator.database,
                                    info['start_file_num'], info['start_offset'])
            events = iter_window_events(reader, None, end_position)
        else:  # no binlog file existed at start: locate P the slow way
            reader = DataReader(replicator.config.binlog_replicator, replicator.database)
            events = iter_window_events(reader, info['start_position'], end_position)
        ids, saw_ddl = compute_window_deletes(events, table_name, key_fn)
        if saw_ddl:
            self.abort('DDL for the table was observed during the load; structure may be stale - re-run')
            return
        if ids:
            logger.info(f'rebootstrap {table_name}: re-applying {len(ids)} window deletes to the shadow')
            replicator.clickhouse_api.erase(table_name=shadow, field_name=mysql_structure.primary_keys, field_values=ids)

        live_rows = replicator.clickhouse_api.count_rows(live)
        shadow_rows = replicator.clickhouse_api.count_rows(shadow)
        logger.info(f'rebootstrap {table_name}: rows live={live_rows} shadow={shadow_rows} (pre-merge parts, informational)')

        replicator.clickhouse_api.exchange_tables(live, shadow)
        replicator.table_mirrors.pop(table_name, None)
        replicator.clickhouse_api.drop_table(shadow)  # the old data
        self.processes = []
        replicator.state.rebootstrap = None
        replicator.state.save()
        logger.info(f'rebootstrap {table_name}: done (swapped in {time.time() - info["started"]:.0f}s)')

    def abort(self, reason: str):
        replicator = self.replicator
        info = self.info or {}
        logger.error(f'rebootstrap {info.get("table")}: aborted - {reason}')
        for process in self.processes:
            try:
                process.terminate()
            except OSError:
                pass
        self.processes = []
        if info.get('table'):
            replicator.table_mirrors.pop(info['table'], None)
        if info.get('shadow'):
            try:
                replicator.clickhouse_api.drop_table(info['shadow'])
            except Exception as e:  # keep the live table safe; leftovers are dropped on the next start
                logger.error(f'could not drop shadow {info["shadow"]}: {e}')
        replicator.rebootstrap_last_error = reason
        replicator.state.rebootstrap = None
        replicator.state.save()
