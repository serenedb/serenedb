import SqlLogicTest from "@site/src/components/SqlLogicTest";

# System Table Compatibility

Besides compatibility with the PostgreSQL SQL dialect and protocol, SereneDB also supports a large part of the PostgreSQL system table catalog in the pg_catalog schema. These system tables contain information about the system state in the form of metadata. This metadata is often used by external tools and clients to interact with the database system for introspection and reflection, e.g., to show which tables exist.

This page provides an overview of the currently supported system tables and views.

- 🟢 Supported
- 🟡 Stubbed for compatibility: These do not contain actual content and exist solely for compatibility purposes, as SereneDB does not share PostgreSQL’s codebase or internal architecture
- 🔴 Not yet supported

## System Tables

System tables provide a raw view into the state of the database system. In contrast to PostgreSQL, system tables in SereneDB are read-only, and can only be indirectly influenced through DDL statements.

System tables often contain many low-level details. For more accessible and friendly access to the same information, consider using the built-in system views, or the SQL-standard information schema.

| Feature                     | Support State | Details |
|-----------------------------|---------------|---------|
| pg_aggregate                | 🟡            | Stores information about aggregate functions. |
| pg_am                       | 🟡            | Contains information about access methods. |
| pg_amop                     | 🟡            | Stores information about operators associated with access methods. |
| pg_amproc                   | 🟡            | Contains information about support procedures associated with access methods. |
| pg_attrdef                  | 🟡            | Stores column default values. |
| pg_attribute                | 🟢            | Contains information about table columns. |
| pg_authid                   | 🟡            | Stores information about database roles. |
| pg_auth_members             | 🟡            | Tracks role memberships. |
| pg_cast                     | 🟡            | Contains information about type casts. |
| pg_class                    | 🟢            | Stores information about tables, indexes, sequences and other relations. |
| pg_collation                | 🟡            | Contains information about collations. |
| pg_constraint               | 🟢            | Stores information about table constraints. |
| pg_conversion               | 🟡            | Contains information about encoding conversions. |
| pg_database                 | 🟡            | Stores information about databases. |
| pg_db_role_setting          | 🟡            | Contains per-role and per-database configuration settings. |
| pg_default_acl              | 🟡            | Stores default access privileges. |
| pg_depend                   | 🟡            | Tracks dependencies between database objects. |
| pg_description              | 🟡            | Stores optional descriptions (comments) for database objects. |
| pg_enum                     | 🟡            | Contains information about enum types. |
| pg_event_trigger            | 🟡            | Stores information about event triggers. |
| pg_extension                | 🟡            | Contains information about installed extensions. |
| pg_foreign_data_wrapper     | 🟡            | Stores information about foreign-data wrappers. |
| pg_foreign_server           | 🟢            | Contains information about foreign servers created with [`CREATE SERVER`](../sql/statements/create_server/index.md). **Superuser-only** — its `srvoptions` carry credentials and are shown unredacted. `srvfdw` is always `0`. |
| pg_foreign_table            | 🟡            | Stores information about foreign tables. |
| pg_index                    | 🟡            | Contains information about indexes. |
| pg_inherits                 | 🟡            | Tracks table inheritance hierarchies. |
| pg_init_privs               | 🟡            | Stores initial privileges of database objects. |
| pg_language                 | 🟡            | Contains information about procedural languages. |
| pg_largeobject              | 🟡            | Stores large object data. |
| pg_largeobject_metadata     | 🟡            | Contains metadata for large objects. |
| pg_namespace                | 🟢            | Stores information about schemas. |
| pg_opclass                  | 🟡            | Contains information about operator classes. |
| pg_operator                 | 🟡            | Stores information about operators. |
| pg_opfamily                 | 🟡            | Contains information about operator families. |
| pg_parameter_acl            | 🟡            | Stores access privileges for server parameters. |
| pg_partitioned_table        | 🟡            | Contains information about partitioned tables. |
| pg_policy                   | 🟡            | Stores information about row-level security policies. |
| pg_proc                     | 🟡            | Contains information about functions and procedures. |
| pg_publication              | 🟡            | Contains all publications created in the database. |
| pg_publication_namespace    | 🟡            | Maps schemas to publications (many-to-many). |
| pg_publication_rel          | 🟡            | Maps relations (tables) to publications (many-to-many). |
| pg_range                    | 🟡            | Stores information about range types. |
| pg_replication_origin       | 🟡            | Contains replication origins shared across the cluster. |
| pg_rewrite                  | 🟡            | Stores rewrite rules for tables and views. |
| pg_seclabel                 | 🟡            | Stores security labels on database objects. |
| pg_sequence                 | 🟡            | Contains information about sequences. |
| pg_shdepend                 | 🟡            | Records dependencies between database objects and shared objects. |
| pg_shdescription            | 🟡            | Stores optional descriptions for shared database objects. |
| pg_shseclabel               | 🟡            | Stores security labels for shared database objects. |
| pg_statistic                | 🟡            | Stores planner statistics. |
| pg_statistic_ext            | 🟡            | Stores extended statistics definitions. |
| pg_statistic_ext_data       | 🟡            | Contains data for extended statistics. |
| pg_subscription             | 🟡            | Stores logical replication subscriptions. |
| pg_subscription_rel         | 🟡            | Tracks per-relation subscription state. |
| pg_tablespace               | 🟡            | Stores information about tablespaces. |
| pg_transform                | 🟡            | Stores transforms between data types and languages. |
| pg_trigger                  | 🟡            | Contains information about table triggers. |
| pg_ts_config                | 🟡            | Stores text search configurations. |
| pg_ts_config_map            | 🟡            | Maps text search configurations to dictionaries. |
| pg_ts_dict                  | 🟡            | Stores text search dictionaries. |
| pg_ts_parser                | 🟡            | Contains text search parsers. |
| pg_ts_template              | 🟡            | Stores text search templates. |
| pg_type                     | 🟢            | Stores information about data types. |
| pg_user_mapping             | 🟡            | Contains user mappings for foreign data access. |

## System Views
System views provide convenient access to system information. System tables often contain numeric identifiers, e.g., for the owner of tables. The views instead use more human-readable symbolic names.

| Feature | Support State | Details |
|--------|---------------|---------|
| pg_available_extensions | 🟡 | Lists available extensions. |
| pg_available_extension_versions | 🟡 | Shows available versions of extensions. |
| pg_backend_memory_contexts | 🟡 | Displays memory contexts of the backend. |
| pg_config | 🟡 | Provides access to compile-time configuration parameters. |
| pg_cursors | 🟡 | Lists open cursors. |
| pg_file_settings | 🟡 | Summarizes contents of configuration files. |
| pg_group | 🟡 | Displays groups of database users. |
| pg_hba_file_rules | 🟡 | Summarizes client authentication configuration. |
| pg_ident_file_mappings | 🟡 | Summarizes client user name mapping configuration. |
| pg_indexes | 🟢 | Shows information about indexes. |
| pg_locks | 🟡 | Displays locks currently held or awaited. |
| pg_matviews | 🟡 | Lists materialized views. |
| pg_policies | 🟡 | Displays information about policies. |
| pg_prepared_statements | 🟡 | Lists prepared statements. |
| pg_prepared_xacts | 🟡 | Shows prepared transactions. |
| pg_publication_tables | 🟡 | Displays publications and their associated tables. |
| pg_replication_origin_status | 🟡 | Provides information about replication origins, including replication progress. |
| pg_replication_slots | 🟡 | Displays replication slot information. |
| pg_roles | 🟡 | Lists database roles. |
| pg_rules | 🟡 | Shows information about rules. |
| pg_seclabels | 🟡 | Displays security labels. |
| pg_sequences | 🟡 | Lists sequences. |
| pg_settings | 🟢 | Provides access to parameter settings. |
| pg_shadow | 🟡 | Displays database users. |
| pg_shmem_allocations | 🟡 | Shows shared memory allocations. |
| pg_stats | 🟡 | Provides planner statistics. |
| pg_stats_ext | 🟡 | Displays extended planner statistics. |
| pg_stats_ext_exprs | 🟡 | Shows extended planner statistics for expressions. |
| pg_tables | 🟢 | Lists tables. |
| pg_timezone_abbrevs | 🟡 | Displays time zone abbreviations. |
| pg_timezone_names | 🟡 | Lists time zone names. |
| pg_user | 🟡 | Shows database users. |
| pg_user_mappings | 🟡 | Displays user mappings. |
| pg_views | 🟢 | Lists views. |

### SereneDB System Views

SereneDB does not currently expose additional system views beyond the PostgreSQL-compatible ones listed above.

## Information Schema

The `information_schema` is a standardized, cross-database schema that allows portable system introspection. For compatibility, SereneDB follows the PostgreSQL Information Schema, which matches the ISO/IEC 9075-11 standard.

>The information schema views are not included in the default search path, so queries on it need to use the fully qualified
> name:
><SqlLogicTest id="compatibility/system-table-compatibility/example_003" />

| Table name | Support State | Details |
|-----------|---------------|---------|
| information_schema_catalog_name | 🟡 |  |
| administrable_role_authorizations | 🟡 |  |
| applicable_roles | 🟡 |  |
| attributes | 🟡 |  |
| character_sets | 🟡 |  |
| check_constraint_routine_usage | 🟡 |  |
| check_constraints | 🟢 |  |
| collations | 🟡 |  |
| collation_character_set_applicability | 🟡 |  |
| column_column_usage | 🟡 |  |
| column_domain_usage | 🟡 |  |
| column_options | 🟡 |  |
| column_privileges | 🟢 |  |
| column_udt_usage | 🟢 |  |
| columns | 🟢 |  |
| constraint_column_usage | 🟢 |  |
| constraint_table_usage | 🟢 |  |
| data_type_privileges | 🟢 |  |
| domain_constraints | 🟡 |  |
| domain_udt_usage | 🟡 |  |
| domains | 🟡 |  |
| element_types | 🟡 |  |
| enabled_roles | 🟡 |  |
| foreign_data_wrapper_options | 🟡 |  |
| foreign_data_wrappers | 🟡 |  |
| foreign_server_options | 🟡 |  |
| foreign_servers | 🟡 |  |
| foreign_table_options | 🟡 |  |
| foreign_tables | 🟡 |  |
| key_column_usage | 🟢 |  |
| parameters | 🟡 |  |
| referential_constraints | 🟡 |  |
| role_column_grants | 🟢 |  |
| role_routine_grants | 🟡 |  |
| role_table_grants | 🟢 |  |
| role_udt_grants | 🟡 |  |
| role_usage_grants | 🟡 |  |
| routine_column_usage | 🟡 |  |
| routine_privileges | 🟡 |  |
| routine_routine_usage | 🟡 |  |
| routine_sequence_usage | 🟡 |  |
| routine_table_usage | 🟡 |  |
| routines | 🟡 |  |
| schemata | 🟢 |  |
| sequences | 🟡 |  |
| sql_features | 🟡 |  |
| sql_implementation_info | 🟡 |  |
| sql_parts | 🟡 |  |
| sql_sizing | 🟡 |  |
| table_constraints | 🟢 |  |
| table_privileges | 🟢 |  |
| tables | 🟢 |  |
| transforms | 🟡 |  |
| triggered_update_columns | 🟡 |  |
| triggers | 🟡 |  |
| udt_privileges | 🟡 |  |
| usage_privileges | 🟡 |  |
| user_defined_types | 🟡 |  |
| user_mapping_options | 🟡 |  |
| user_mappings | 🟡 |  |
| view_column_usage | 🟡 |  |
| view_routine_usage | 🟡 |  |
| view_table_usage | 🟡 |  |
| views | 🟢 |  |
