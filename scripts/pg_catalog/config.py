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

COLLATIONS = {
    'default': 'kDefaultCollation',
    'C': 'kCCollation',
    'POSIX': 'kPosixCollation',
}

ROLES = {
    'pg_checkpoint': 'kPgCheckpointRole',
    'pg_create_subscription': 'kPgCreateSubscriptionRole',
    'pg_database_owner': 'kPgDatabaseOwnerRole',
    'pg_maintain': 'kPgMaintainRole',
    'pg_read_all_data': 'kPgReadAllDataRole',
    'pg_read_all_settings': 'kPgReadAllSettingsRole',
    'pg_read_all_stats': 'kPgReadAllStatsRole',
    'pg_read_server_files': 'kPgReadServerFilesRole',
    'pg_signal_backend': 'kPgSignalBackendRole',
    'pg_write_all_data': 'kPgWriteAllDataRole',
    'pg_write_server_files': 'kPgWriteServerFilesRole',
}
