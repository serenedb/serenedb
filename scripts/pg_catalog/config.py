SCHEMAS = {
    'pg_catalog': 'kPgCatalogSchema',
    'information_schema': 'kPgInformationSchema',
    'public': 'kPgPublicSchema',
}

DATABASES = {'postgres': 'kPgPostgresDatabase'}

TABLESPACES = {
    'pg_default': 'kPgDefaultTablespace',
    'pg_global': 'kPgGlobalTablespace',
}

ACCESS_METHODS = {'heap': 'kPgAmHeap'}

LANGUAGES = {
    'internal': 'kPgInternalLanguage',
    'sql': 'kPgSqlLanguage',
}

PROCEDURES = {
    'array_subscript_handler': 'kArraySubscriptHandler',
    'enum_in': 'kEnumIn',
    'enum_out': 'kEnumOut',
    'enum_recv': 'kEnumRecv',
    'enum_send': 'kEnumSend',
}

PSEUDO_TYPES = ('any', 'anyelement', 'event_trigger', 'fdw_handler',
                'index_am_handler', 'internal', 'language_handler',
                'pg_ddl_command', 'record', 'table_am_handler', 'trigger',
                'tsm_handler')

COLLATIONS = {
    'default': 'kDefaultCollation',
    'C': 'kCCollation',
    'POSIX': 'kPosixCollation',
}

STUB_FUNCTIONS = (
    'pg_lock_status',
    'pg_cursor',
    'pg_available_extensions',
    'pg_available_extension_versions',
    'pg_prepared_xact',
    'pg_show_all_file_settings',
    'pg_ident_file_mappings',
    'pg_timezone_abbrevs_zone',
    'pg_timezone_abbrevs_abbrevs',
    'pg_config',
    'pg_get_shmem_allocations',
    'pg_get_shmem_allocations_numa',
    'pg_get_backend_memory_contexts',
    'pg_stat_get_wal_senders',
    'pg_stat_get_slru',
    'pg_stat_get_wal_receiver',
    'pg_get_replication_slots',
    'pg_stat_get_replication_slot',
    'pg_stat_get_io',
    'pg_get_wait_events',
    'pg_get_aios',
    'pg_mcv_list_items',
    'pg_get_publication_tables',
)

ZERO_STUB_FUNCTIONS = (
    'pg_stat_get_analyze_count',
    'pg_stat_get_autoanalyze_count',
    'pg_stat_get_autovacuum_count',
    'pg_stat_get_bgwriter_buf_written_clean',
    'pg_stat_get_bgwriter_maxwritten_clean',
    'pg_stat_get_blocks_fetched',
    'pg_stat_get_blocks_hit',
    'pg_stat_get_buf_alloc',
    'pg_stat_get_checkpointer_buffers_written',
    'pg_stat_get_checkpointer_num_performed',
    'pg_stat_get_checkpointer_num_requested',
    'pg_stat_get_checkpointer_num_timed',
    'pg_stat_get_checkpointer_restartpoints_performed',
    'pg_stat_get_checkpointer_restartpoints_requested',
    'pg_stat_get_checkpointer_restartpoints_timed',
    'pg_stat_get_checkpointer_slru_written',
    'pg_stat_get_checkpointer_sync_time',
    'pg_stat_get_checkpointer_write_time',
    'pg_stat_get_db_blk_read_time',
    'pg_stat_get_db_blk_write_time',
    'pg_stat_get_db_blocks_fetched',
    'pg_stat_get_db_blocks_hit',
    'pg_stat_get_db_checksum_failures',
    'pg_stat_get_db_conflict_all',
    'pg_stat_get_db_conflict_bufferpin',
    'pg_stat_get_db_conflict_lock',
    'pg_stat_get_db_conflict_logicalslot',
    'pg_stat_get_db_conflict_snapshot',
    'pg_stat_get_db_conflict_startup_deadlock',
    'pg_stat_get_db_conflict_tablespace',
    'pg_stat_get_db_deadlocks',
    'pg_stat_get_db_parallel_workers_launched',
    'pg_stat_get_db_parallel_workers_to_launch',
    'pg_stat_get_db_sessions_abandoned',
    'pg_stat_get_db_sessions_fatal',
    'pg_stat_get_db_sessions_killed',
    'pg_stat_get_db_temp_bytes',
    'pg_stat_get_db_temp_files',
    'pg_stat_get_db_tuples_deleted',
    'pg_stat_get_db_tuples_fetched',
    'pg_stat_get_db_tuples_inserted',
    'pg_stat_get_db_tuples_returned',
    'pg_stat_get_db_tuples_updated',
    'pg_stat_get_dead_tuples',
    'pg_stat_get_ins_since_vacuum',
    'pg_stat_get_mod_since_analyze',
    'pg_stat_get_numscans',
    'pg_stat_get_total_analyze_time',
    'pg_stat_get_total_autoanalyze_time',
    'pg_stat_get_total_autovacuum_time',
    'pg_stat_get_total_vacuum_time',
    'pg_stat_get_tuples_deleted',
    'pg_stat_get_tuples_fetched',
    'pg_stat_get_tuples_hot_updated',
    'pg_stat_get_tuples_inserted',
    'pg_stat_get_tuples_newpage_updated',
    'pg_stat_get_tuples_returned',
    'pg_stat_get_tuples_updated',
    'pg_stat_get_vacuum_count',
    'pg_stat_get_xact_numscans',
    'pg_stat_get_xact_tuples_deleted',
    'pg_stat_get_xact_tuples_fetched',
    'pg_stat_get_xact_tuples_hot_updated',
    'pg_stat_get_xact_tuples_inserted',
    'pg_stat_get_xact_tuples_newpage_updated',
    'pg_stat_get_xact_tuples_returned',
    'pg_stat_get_xact_tuples_updated',
)

NULL_STUB_FUNCTIONS = (
    'pg_get_partkeydef',
    'pg_get_statisticsobjdef',
    'pg_get_statisticsobjdef_columns',
    'pg_get_statisticsobjdef_expressions',
    'pg_stat_get_bgwriter_stat_reset_time',
    'pg_stat_get_checkpointer_stat_reset_time',
    'pg_stat_get_db_checksum_last_failure',
    'pg_stat_get_db_stat_reset_time',
    'pg_stat_get_function_calls',
    'pg_stat_get_function_self_time',
    'pg_stat_get_function_total_time',
    'pg_stat_get_last_analyze_time',
    'pg_stat_get_last_autoanalyze_time',
    'pg_stat_get_last_autovacuum_time',
    'pg_stat_get_last_vacuum_time',
    'pg_stat_get_lastscan',
    'pg_stat_get_xact_function_calls',
    'pg_stat_get_xact_function_self_time',
    'pg_stat_get_xact_function_total_time',
)

STUB_TYPES = {'pg_lsn': 'text', 'pg_mcv_list': 'text'}
