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

STUB_TYPES = {'pg_lsn': 'text', 'pg_mcv_list': 'text'}
