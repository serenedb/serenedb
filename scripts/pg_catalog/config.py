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
