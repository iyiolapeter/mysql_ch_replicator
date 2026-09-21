import os.path
import time
import pickle
import hashlib
from logging import getLogger
from dataclasses import dataclass

from .config import Settings
from .mysql_api import MySQLApi
from .clickhouse_api import ClickhouseApi
from .converter import MysqlToClickhouseConverter
from .binlog_replicator import DataReader
from .db_replicator_initial import DbReplicatorInitial
from .db_replicator_realtime import DbReplicatorRealtime
from .rebootstrap import Rebootstrap
from .control import ControlChannel
from .common import Status


logger = getLogger(__name__)


@dataclass
class Statistics:
    last_transaction: tuple = None
    events_count: int = 0
    insert_events_count: int = 0
    insert_records_count: int = 0
    erase_events_count: int = 0
    erase_records_count: int = 0
    no_events_count: int = 0
    skipped_events_count: int = 0
    cpu_load: float = 0.0


class State:

    def __init__(self, file_name):
        self.file_name = file_name
        self.last_processed_transaction = None
        self.last_processed_transaction_non_uploaded = None
        self.status = Status.NONE
        self.tables_last_record_version = {}
        self.initial_replication_table = None
        self.initial_replication_max_primary_key = None
        self.tables_structure: dict = {}
        self.tables = []
        self.pid = None
        # Tables whose binlog events are read but NOT applied (live skip).
        self.skipped_tables = []
        # In-progress live single-table rebootstrap (see rebootstrap.py), or None.
        self.rebootstrap = None
        self.load()

    def load(self):
        file_name = self.file_name
        if not os.path.exists(file_name):
            return
        data = open(file_name, 'rb').read()
        data = pickle.loads(data)
        self.last_processed_transaction = data['last_processed_transaction']
        self.last_processed_transaction_non_uploaded = data['last_processed_transaction']
        self.status = Status(data['status'])
        self.tables_last_record_version = data['tables_last_record_version']
        self.initial_replication_table = data['initial_replication_table']
        self.initial_replication_max_primary_key = data['initial_replication_max_primary_key']
        self.tables_structure = data['tables_structure']
        self.tables = data['tables']
        self.pid = data.get('pid', None)
        self.skipped_tables = data.get('skipped_tables', [])
        self.rebootstrap = data.get('rebootstrap', None)

    def save(self):
        file_name = self.file_name
        data = pickle.dumps({
            'last_processed_transaction': self.last_processed_transaction,
            'status': self.status.value,
            'tables_last_record_version': self.tables_last_record_version,
            'initial_replication_table': self.initial_replication_table,
            'initial_replication_max_primary_key': self.initial_replication_max_primary_key,
            'tables_structure': self.tables_structure,
            'tables': self.tables,
            'skipped_tables': list(self.skipped_tables),
            'rebootstrap': self.rebootstrap,
            'pid': os.getpid(),
            'save_time': time.time(),
        })
        with open(file_name + '.tmp', 'wb') as f:
            f.write(data)
        os.rename(file_name + '.tmp', file_name)

    def remove(self):
        file_name = self.file_name
        if os.path.exists(file_name):
            os.remove(file_name)
        if os.path.exists(file_name + '.tmp'):
            os.remove(file_name + '.tmp')


class DbReplicator:
    def __init__(self, config: Settings, database: str, target_database: str = None, initial_only: bool = False, 
                 worker_id: int = None, total_workers: int = None, table: str = None, 
                 initial_replication_test_fail_records: int = None, skip_initial_replication: bool = False,
                 target_table: str = None, fixed_version: int = None):
        self.config = config
        self.database = database
        self.worker_id = worker_id
        self.total_workers = total_workers
        self.settings_file = config.settings_file
        self.single_table = table  # Store the single table to process
        self.initial_replication_test_fail_records = initial_replication_test_fail_records  # Test flag for early exit
        self.skip_initial_replication = skip_initial_replication or config.skip_initial_replication
        # Live single-table rebootstrap plumbing:
        #  - target_table_override: a bulk-load worker (--table X) writes X's
        #    rows into this ClickHouse table (the shadow) instead of X.
        #  - fixed_version: that worker stamps every row with this _version.
        #  - table_mirrors: in the main process, realtime writes for a source
        #    table are ALSO applied to the mapped shadow table.
        self.target_table_override = target_table
        self.fixed_version = fixed_version
        self.table_mirrors: dict[str, str] = {}
        self.rebootstrap_last_error = None
        
        # use same as source database by default
        self.target_database = database

        # use target database from config file if exists
        target_database_from_config = config.target_databases.get(database)
        if target_database_from_config:
            self.target_database = target_database_from_config

        # use command line argument if exists
        if target_database:
            self.target_database = target_database

        self.initial_only = initial_only

        # Handle state file differently for parallel workers
        if self.worker_id is not None and self.total_workers is not None:
            # For worker processes in parallel mode, use a different state file with a deterministic name
            self.is_parallel_worker = True
            
            # Determine table name for the state file
            table_identifier = self.single_table if self.single_table else "all_tables"
            
            # Create a hash of the table name to ensure it's filesystem-safe
            if self.single_table:
                # Use a hex digest of the table name to ensure it's filesystem-safe
                table_identifier = hashlib.sha256(self.single_table.encode('utf-8')).hexdigest()[:16]
            else:
                table_identifier = "all_tables"
            
            # Create a deterministic state file path that includes worker_id, total_workers, and table hash
            self.state_path = os.path.join(
                self.config.binlog_replicator.data_dir, 
                self.database, 
                f'state_worker_{self.worker_id}_of_{self.total_workers}_{table_identifier}.pckl'
            )
            
            logger.info(f"Worker {self.worker_id}/{self.total_workers} using state file: {self.state_path}")
            
            if self.single_table:
                logger.info(f"Worker {self.worker_id} focusing only on table: {self.single_table}")
        else:
            self.state_path = os.path.join(self.config.binlog_replicator.data_dir, self.database, 'state.pckl')
            self.is_parallel_worker = False

        # Check if multiple MySQL databases are being replicated to the same ClickHouse database
        self.is_multi_mysql_to_single_ch = self.config.is_multiple_mysql_dbs_to_single_ch_db(
            self.database, self.target_database
        )
        
        self.target_database_tmp = self.target_database + '_tmp'
        if self.is_parallel_worker:
            self.target_database_tmp = self.target_database
        
        # If ignore_deletes is enabled, we replicate directly into the target DB
        # This must be set here to ensure consistency between first run and resume
        if self.config.ignore_deletes:
            self.target_database_tmp = self.target_database
        
        # because its a pain in the neck to shift distributed tables from *_tmp db to new one
        if self.config.cluster_mode:
            self.target_database_tmp = self.target_database

        # If multiple MySQL databases map to same ClickHouse database, replicate directly
        if self.is_multi_mysql_to_single_ch:
            self.target_database_tmp = self.target_database
            logger.info(f'detected multiple MySQL databases mapping to {self.target_database} - using direct replication')

        # If skip_initial_replication is enabled, replicate directly to target
        if self.skip_initial_replication:
            self.target_database_tmp = self.target_database

        self.mysql_api = MySQLApi(
            database=self.database,
            mysql_settings=config.mysql,
            mysql_timezone=config.mysql_timezone,
        )
        self.clickhouse_api = ClickhouseApi(
            database=self.target_database,
            clickhouse_settings=config.clickhouse,
            version_initial_value=config.version_initial_value,
        )
        self.converter = MysqlToClickhouseConverter(self)
        self.data_reader = DataReader(config.binlog_replicator, database)
        self.state = self.create_state()
        self.skipped_tables: set[str] = set(self.state.skipped_tables or [])
        self.clickhouse_api.tables_last_record_version = self.state.tables_last_record_version
        self.stats = Statistics()
        self.start_time = time.time()
        
        # Create the initial replicator instance
        self.initial_replicator = DbReplicatorInitial(self)
        
        # Create the realtime replicator instance
        self.realtime_replicator = DbReplicatorRealtime(self)

        # Live control plane (skip/unskip/rebootstrap) + the rebootstrap driver.
        self.control = ControlChannel(os.path.dirname(self.state_path))
        self.rebootstrap = Rebootstrap(self)

    def create_state(self):
        return State(self.state_path)

    def get_live_target_table_name(self, source_table: str) -> str:
        """Configured target name only - never the shadow override."""
        return self.config.get_target_table_name(self.database, source_table)

    def get_target_table_name(self, source_table: str) -> str:
        if self.target_table_override and self.single_table == source_table:
            return self.target_table_override
        return self.get_live_target_table_name(source_table)

    def get_mysql_api_lazy(self) -> MySQLApi:
        """Realtime replication closes mysql_api; re-open on demand (used to
        load a table's structure when it is (re)included at runtime and by
        the live rebootstrap)."""
        if self.mysql_api is None:
            self.mysql_api = MySQLApi(
                database=self.database,
                mysql_settings=self.config.mysql,
                mysql_timezone=self.config.mysql_timezone,
            )
        return self.mysql_api

    def load_table_structure_from_mysql(self, table_name: str):
        """(mysql_structure, clickhouse_structure) for table_name from the
        CURRENT MySQL schema; the CH structure carries the live target name."""
        create_statement = self.get_mysql_api_lazy().get_table_create_statement(table_name)
        mysql_structure = self.converter.parse_mysql_table_structure(
            create_statement, required_table_name=table_name,
        )
        clickhouse_structure = self.converter.convert_table_structure(mysql_structure)
        clickhouse_structure.table_name = self.get_live_target_table_name(table_name)
        return mysql_structure, clickhouse_structure

    def ensure_table_structure(self, table_name: str):
        """Make sure state.tables_structure knows table_name. Realtime handlers
        index tables_structure directly, which raised KeyError for a table
        re-included after being skipped (or added to the whitelist while the
        replicator was already in realtime mode)."""
        if table_name in self.state.tables_structure:
            return
        logger.info(f'loading structure for table {table_name} from MySQL (not in state)')
        self.state.tables_structure[table_name] = self.load_table_structure_from_mysql(table_name)
        if table_name not in self.state.tables:
            self.state.tables.append(table_name)
        self.state.save()

    def validate_database_settings(self):
        if not self.initial_only:
            final_setting = self.clickhouse_api.get_system_setting('final')
            if final_setting != '1':
                logger.warning('settings validation failed')
                logger.warning(
                    '\n\n\n    !!!  WARNING - MISSING REQUIRED CLICKHOUSE SETTING  (final)  !!!\n\n'
                    'You need to set <final>1</final> in clickhouse config file\n'
                    'Otherwise you will get DUPLICATES in your SELECT queries\n\n\n'
                )

    def run(self):
        try:
            logger.info('launched db_replicator')
            self.validate_database_settings()

            if self.state.status != Status.NONE:
                # ensure target database still exists
                if self.target_database not in self.clickhouse_api.get_databases() and f"{self.target_database}_tmp" not in self.clickhouse_api.get_databases():
                    logger.warning(f'database {self.target_database} missing in CH')
                    logger.warning('will run replication from scratch')
                    self.state.remove()
                    self.state = self.create_state()

            if self.state.status == Status.RUNNING_REALTIME_REPLICATION:
                self.run_realtime_replication()
                return
            if self.state.status == Status.PERFORMING_INITIAL_REPLICATION:
                self.initial_replicator.perform_initial_replication()
                self.run_realtime_replication()
                return

            if self.skip_initial_replication:
                logger.info('skipping initial replication')
                self.clickhouse_api.database = self.target_database
                self.state.tables = self.mysql_api.get_tables()
                self.state.tables = [
                    table for table in self.state.tables if self.config.is_table_matches(table)
                ]
                self.state.last_processed_transaction = self.data_reader.get_last_transaction_id()
                self.initial_replicator.parse_tables_structure()
                self.state.save()
                logger.info(f'last known transaction {self.state.last_processed_transaction}')
                self.run_realtime_replication()
                return

            # If ignore_deletes is enabled OR multiple MySQL databases map to same ClickHouse database,
            # we don't create a temporary DB and don't swap DBs
            # We replicate directly into the target DB
            if self.config.ignore_deletes or self.is_multi_mysql_to_single_ch:
                if self.config.ignore_deletes:
                    logger.info(f'using existing database (ignore_deletes=True)')
                if self.is_multi_mysql_to_single_ch:
                    logger.info(f'using existing database (multi-mysql-to-single-ch mode)')
                    
                self.clickhouse_api.database = self.target_database
                
                # Create database if it doesn't exist (use IF NOT EXISTS to avoid race condition)
                if self.target_database not in self.clickhouse_api.get_databases():
                    logger.info(f'creating database {self.target_database}')
                    self.clickhouse_api.create_database(db_name=self.target_database, if_not_exists=True)
            else:
                logger.info('recreating database')
                self.clickhouse_api.database = self.target_database_tmp
                if not self.is_parallel_worker:
                    self.clickhouse_api.recreate_database()

            self.state.tables = self.mysql_api.get_tables()
            self.state.tables = [
                table for table in self.state.tables if self.config.is_table_matches(table)
            ]
            self.state.last_processed_transaction = self.data_reader.get_last_transaction_id()
            self.state.save()
            logger.info(f'last known transaction {self.state.last_processed_transaction}')
            self.initial_replicator.create_initial_structure()
            self.initial_replicator.perform_initial_replication()
            self.run_realtime_replication()
        except Exception:
            logger.error(f'unhandled exception', exc_info=True)
            raise

    def run_realtime_replication(self):
        # Delegate to the realtime replicator
        self.realtime_replicator.run_realtime_replication()
