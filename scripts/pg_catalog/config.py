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

PROCEDURES = {'array_subscript_handler': 'kArraySubscriptHandler'}

SDB_OWNED_TYPES = ('tsquery', '_tsquery')

PSEUDO_TYPES = ('any', 'internal', 'record')

ENUM_PROCS = ('enum_in', 'enum_out', 'enum_recv', 'enum_send')

COLLATIONS = {
    'default': 'kDefaultCollation',
    'C': 'kCCollation',
    'POSIX': 'kPosixCollation',
}

FIRST_INITDB_OID = 10000

NATIVE_VIEWS = {
    ('pg_catalog', 'pg_settings'): [('name', True)],
    ('pg_catalog', 'pg_hba_file_rules'): [],
    ('information_schema', 'columns'): [('table_name', False),
                                        ('table_schema', False)],
}

COLUMN_DEFAULTS = {
    ('pg_catalog', 'pg_class'): {'relfrozenxid': '0', 'relminmxid': '0'},
    ('pg_catalog', 'pg_collation'): {'collencoding': '-1'},
    ('pg_catalog', 'pg_constraint'): {
        'conenforced': 'true',
        'convalidated': 'true',
        'conislocal': 'true',
        'confupdtype': ' ',
        'confdeltype': ' ',
        'confmatchtype': ' ',
    },
    ('pg_catalog', 'pg_database'): {
        'encoding': '6',
        'datlocprovider': 'c',
        'datallowconn': 'true',
        'datconnlimit': '-1',
        'dattablespace': '1663',
        'datcollate': 'C.UTF-8',
        'datctype': 'C.UTF-8',
    },
    ('pg_catalog', 'pg_index'): {
        'indimmediate': 'true',
        'indisvalid': 'true',
        'indisready': 'true',
        'indislive': 'true',
    },
    ('pg_catalog', 'pg_opclass'): {'opcdefault': 'false'},
    ('pg_catalog', 'pg_rewrite'): {
        'ev_type': '1',
        'ev_enabled': 'O',
        'is_instead': 'true',
        'ev_qual': '<>',
    },
    ('pg_catalog', 'pg_trigger'): {'tgenabled': 'O'},
    ('information_schema', 'columns'): {
        'is_self_referencing': 'NO',
        'is_identity': 'NO',
        'identity_cycle': 'NO',
        'is_generated': 'NEVER',
    },
}

PHYSICAL_TYPES = {
    'bool': 'BOOL',
    'int2': 'INT16',
    'int4': 'INT32',
    'cardinal_number': 'INT32',
    'int8': 'INT64',
    'oid': 'INT64',
    'regproc': 'INT64',
    'regtype': 'INT64',
    'xid': 'INT64',
    'timestamptz': 'INT64',
    'time_stamp': 'INT64',
    'pg_lsn': 'UINT64',
    'float4': 'FLOAT',
    'float8': 'DOUBLE',
    'text': 'VARCHAR',
    'name': 'VARCHAR',
    'char': 'VARCHAR',
    'aclitem': 'VARCHAR',
    'bytea': 'VARCHAR',
    'pg_node_tree': 'VARCHAR',
    'character_data': 'VARCHAR',
    'sql_identifier': 'VARCHAR',
    'yes_or_no': 'VARCHAR',
    'anyarray': 'VARCHAR',
    'pg_ndistinct': 'VARCHAR',
    'pg_dependencies': 'VARCHAR',
    'pg_mcv_list': 'VARCHAR',
}

DEFAULT_ESTIMATED_ROWS = 1000

ESTIMATED_ROWS = {
    ('pg_catalog', 'pg_attribute'): 100000,
    ('pg_catalog', 'pg_depend'): 100000,
    ('pg_catalog', 'pg_attrdef'): 10000,
    ('pg_catalog', 'pg_class'): 10000,
    ('pg_catalog', 'pg_constraint'): 10000,
    ('pg_catalog', 'pg_index'): 10000,
    ('pg_catalog', 'pg_proc'): 10000,
    ('pg_catalog', 'pg_type'): 10000,
    ('information_schema', 'columns'): 10000,
    ('pg_catalog', 'pg_am'): 10,
    ('pg_catalog', 'pg_authid'): 10,
    ('pg_catalog', 'pg_collation'): 10,
    ('pg_catalog', 'pg_database'): 10,
    ('pg_catalog', 'pg_language'): 10,
    ('pg_catalog', 'pg_namespace'): 10,
    ('pg_catalog', 'pg_tablespace'): 10,
}

SDB_TABLES = {
    'sdb_settings': (400, (
        ('name', 'text'),
        ('setting', 'text'),
        ('unit', 'text'),
        ('category', 'text'),
        ('short_desc', 'text'),
        ('context', 'text'),
        ('vartype', 'text'),
        ('source', 'text'),
        ('min_val', 'text'),
        ('max_val', 'text'),
        ('boot_val', 'text'),
        ('reset_val', 'text'),
        ('pending_restart', 'bool'),
    )),
    'sdb_metrics': (401, (
        ('metric', 'text'),
        ('value', 'int8'),
        ('description', 'text'),
        ('relation_id', 'oid'),
    )),
    'sdb_progress': (402, (
        ('pid', 'int4'),
        ('datid', 'oid'),
        ('usename', 'text'),
        ('datname', 'text'),
        ('state', 'text'),
        ('query', 'text'),
        ('backend_start_us', 'int8'),
        ('query_start_us', 'int8'),
        ('percent', 'float8'),
        ('rows_processed', 'int8'),
        ('rows_total', 'int8'),
        ('command', 'text'),
        ('io_type', 'text'),
        ('relid', 'oid'),
        ('current_relid', 'oid'),
        ('phase', 'text'),
        ('bytes_processed', 'int8'),
        ('bytes_total', 'int8'),
        ('tuples_processed', 'int8'),
        ('tuples_total', 'int8'),
        ('stage', 'int8'),
        ('stages_total', 'int8'),
        ('step', 'int8'),
        ('steps_total', 'int8'),
        ('items_processed', 'int8'),
        ('items_total', 'int8'),
    )),
}

SUPERUSER_ONLY = {('pg_catalog', 'pg_foreign_server')}

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
    'pg_stat_get_subscription',
    'pg_get_replication_slots',
    'pg_stat_get_replication_slot',
    'pg_stat_get_io',
    'pg_show_replication_origin_status',
    'pg_stat_get_subscription_stats',
    'pg_get_wait_events',
    'pg_get_aios',
    'pg_mcv_list_items',
    'pg_get_publication_tables',
)

STUB_TYPES = {'pg_lsn': 'text', 'pg_mcv_list': 'text'}
