////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <duckdb/common/constants.hpp>
#include <string_view>

namespace sdb::pg {

struct SystemMacro {
  std::string_view schema;
  std::string_view name;
  std::string_view macro_definition;
};

inline constexpr SystemMacro kExternalMacros[] = {
  // clang-format off
  {"pg_catalog", "pg_show_all_settings",
   R"(()
  RETURNS TABLE( name TEXT,
                 setting TEXT,
                 unit TEXT,
                 category TEXT,
                 short_desc TEXT,
                 extra_desc TEXT,
                 context TEXT,
                 vartype TEXT,
                 source TEXT,
                 min_val TEXT,
                 max_val TEXT,
                 enumvals TEXT[],
                 boot_val TEXT,
                 reset_val TEXT,
                 sourcefile TEXT,
                 sourceline INTEGER,
                 pending_restart BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT
        name,
        value as setting,
        NULL::TEXT as unit,
        NULL::TEXT as category,
        description as short_desc,
        NULL::TEXT as extra_desc,
        NULL::TEXT as context,
        NULL::TEXT as vartype,
        NULL::TEXT as source,
        NULL::TEXT as min_val,
        NULL::TEXT as max_val,
        NULL::TEXT[] as enumvals,
        NULL::TEXT as boot_val,
        NULL::TEXT as reset_val,
        NULL::TEXT as sourcefile,
        NULL::INT as sourceline,
        NULL::BOOL as pending_restart
      FROM duckdb_settings();
  END;)"},

    // A few supporting functions first ...

  // Expand any 1-D array into a set with integers 1..N

  // TODO(mbkkt): rewrite once parser supports PG-style OUT params
  {"information_schema", "_pg_expandarray",
   R"((arr) AS TABLE SELECT unnest AS x, ordinality AS n FROM unnest(arr) WITH ORDINALITY)"},

  // Given an index's OID and an underlying-table column number, return the
  // column's position in the index (NULL if not there)

  // Rewritten: PG (ss.a).n / (ss.a).x -> DuckDB table macro columns directly
  {"information_schema", "_pg_index_position",
   R"((oid, smallint) RETURNS int
      LANGUAGE sql STRICT STABLE
  BEGIN ATOMIC
  SELECT ss.n FROM
    (SELECT ea.x, ea.n
     FROM pg_catalog.pg_index, information_schema._pg_expandarray(indkey) AS ea
     WHERE indexrelid = $1) ss
    WHERE ss.x = $2;
  END;)"},

  {"information_schema", "_pg_truetypid",
   R"((pg_attribute, pg_type) RETURNS oid
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END;)"},

  {"information_schema", "_pg_truetypmod",
   R"((pg_attribute, pg_type) RETURNS int4
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN CASE WHEN $2.typtype = 'd' THEN $2.typtypmod ELSE $1.atttypmod END;)"},

  // these functions encapsulate knowledge about the encoding of typmod:

  {"information_schema", "_pg_char_max_length",
   R"((typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $2 = -1 /* default typmod */
         THEN null
         WHEN $1 IN (1042, 1043) /* char, varchar */
         THEN $2 - 4
         WHEN $1 IN (1560, 1562) /* bit, varbit */
         THEN $2
         ELSE null
    END;)"},

  {"information_schema", "_pg_char_octet_length",
   R"((typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (25, 1042, 1043) /* text, char, varchar */
         THEN CASE WHEN $2 = -1 /* default typmod */
                   THEN CAST(2^30 AS integer)
                   ELSE information_schema._pg_char_max_length($1, $2) *
                        pg_catalog.pg_encoding_max_length((SELECT encoding FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()))
              END
         ELSE null
    END;)"},

  {"information_schema", "_pg_numeric_precision",
   R"((typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE $1
           WHEN 21 /*int2*/ THEN 16
           WHEN 23 /*int4*/ THEN 32
           WHEN 20 /*int8*/ THEN 64
           WHEN 1700 /*numeric*/ THEN
                CASE WHEN $2 = -1
                     THEN null
                     ELSE (($2 - 4) >> 16) & 0xFFFF
                     END
           WHEN 700 /*float4*/ THEN 24 /*FLT_MANT_DIG*/
           WHEN 701 /*float8*/ THEN 53 /*DBL_MANT_DIG*/
           ELSE null
    END;)"},

  {"information_schema", "_pg_numeric_precision_radix",
   R"((typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (21, 23, 20, 700, 701) THEN 2
         WHEN $1 IN (1700) THEN 10
         ELSE null
    END;)"},

  {"information_schema", "_pg_numeric_scale",
   R"((typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (21, 23, 20) THEN 0
         WHEN $1 IN (1700) THEN
              CASE WHEN $2 = -1
                   THEN null
                   ELSE ($2 - 4) & 0xFFFF
                   END
         ELSE null
    END;)"},

  {"information_schema", "_pg_datetime_precision",
   R"((typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (1082) /* date */
             THEN 0
         WHEN $1 IN (1083, 1114, 1184, 1266) /* time, timestamp, same + tz */
             THEN CASE WHEN $2 < 0 THEN 6 ELSE $2 END
         WHEN $1 IN (1186) /* interval */
             THEN CASE WHEN $2 < 0 OR $2 & 0xFFFF = 0xFFFF THEN 6 ELSE $2 & 0xFFFF END
         ELSE null
    END;)"},

  {"information_schema", "_pg_interval_type",
   R"((typid oid, mod int4) RETURNS text
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (1186) /* interval */
             -- upper and format_type is in system.main, not pg_catalog
             THEN upper(substring(format_type($1, $2) similar 'interval[()0-9]* #"%#"' escape '#'))
         ELSE null
    END;)"},

  // Stub set-returning functions (return empty tables)
  // Used by pg_catalog system views that reference these functions.

  {"pg_catalog", "pg_lock_status",
   R"(()
  RETURNS TABLE( locktype TEXT,
                 database BIGINT,
                 relation BIGINT,
                 page INTEGER,
                 tuple SMALLINT,
                 virtualxid TEXT,
                 transactionid BIGINT,
                 classid BIGINT,
                 objid BIGINT,
                 objsubid SMALLINT,
                 virtualtransaction TEXT,
                 pid INTEGER,
                 mode TEXT,
                 granted BOOLEAN,
                 fastpath BOOLEAN,
                 waitstart TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::BIGINT, NULL::BIGINT,
             NULL::INTEGER, NULL::SMALLINT, NULL::TEXT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::SMALLINT,
             NULL::TEXT, NULL::INTEGER, NULL::TEXT,
             NULL::BOOLEAN, NULL::BOOLEAN, NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_cursor",
   R"(()
  RETURNS TABLE( name TEXT,
                 statement TEXT,
                 is_holdable BOOLEAN,
                 is_binary BOOLEAN,
                 is_scrollable BOOLEAN,
                 creation_time TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT,
             NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN,
             NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_available_extensions",
   R"(()
  RETURNS TABLE( name TEXT,
                 default_version TEXT,
                 comment TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_available_extension_versions",
   R"(()
  RETURNS TABLE( name TEXT,
                 version TEXT,
                 superuser BOOLEAN,
                 trusted BOOLEAN,
                 relocatable BOOLEAN,
                 schema TEXT,
                 requires TEXT[],
                 comment TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT,
             NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN,
             NULL::TEXT, NULL::TEXT[], NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_prepared_xact",
   R"(()
  RETURNS TABLE( transaction BIGINT,
                 gid TEXT,
                 prepared TIMESTAMP,
                 ownerid BIGINT,
                 dbid BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::TEXT, NULL::TIMESTAMP,
             NULL::BIGINT, NULL::BIGINT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_prepared_statement",
   R"(()
  RETURNS TABLE( name TEXT,
                 statement TEXT,
                 prepare_time TIMESTAMP,
                 parameter_types BIGINT[],
                 result_types BIGINT[],
                 from_sql BOOLEAN,
                 generic_plans BIGINT,
                 custom_plans BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TIMESTAMP,
             NULL::BIGINT[], NULL::BIGINT[],
             NULL::BOOLEAN, NULL::BIGINT, NULL::BIGINT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_show_all_file_settings",
   R"(()
  RETURNS TABLE( sourcefile TEXT,
                 sourceline INTEGER,
                 seqno INTEGER,
                 name TEXT,
                 setting TEXT,
                 applied BOOLEAN,
                 error TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::INTEGER, NULL::INTEGER,
             NULL::TEXT, NULL::TEXT, NULL::BOOLEAN, NULL::TEXT
      WHERE false;
  END;)"},

  // Function form of the pg_hba_file_rules relation (PG exposes both); serves
  // the same live HBA ruleset.
  {"pg_catalog", "pg_hba_file_rules",
   R"(()
  RETURNS TABLE( rule_number INTEGER,
                 file_name TEXT,
                 line_number INTEGER,
                 type TEXT,
                 database TEXT[],
                 user_name TEXT[],
                 address TEXT,
                 netmask TEXT,
                 auth_method TEXT,
                 options TEXT[],
                 error TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT rule_number, file_name, line_number::INTEGER, type, database,
             user_name, address, netmask, auth_method, options, error::TEXT
      FROM pg_catalog.pg_hba_file_rules;
  END;)"},

  {"pg_catalog", "pg_ident_file_mappings",
   R"(()
  RETURNS TABLE( map_number INTEGER,
                 file_name TEXT,
                 line_number INTEGER,
                 map_name TEXT,
                 sys_name TEXT,
                 pg_username TEXT,
                 error TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::INTEGER, NULL::TEXT, NULL::INTEGER,
             NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_timezone_abbrevs_zone",
   R"(()
  RETURNS TABLE( abbrev TEXT,
                 utc_offset TEXT,
                 is_dst BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::BOOLEAN
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_timezone_abbrevs_abbrevs",
   R"(()
  RETURNS TABLE( abbrev TEXT,
                 utc_offset TEXT,
                 is_dst BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::BOOLEAN
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_timezone_names",
   R"(()
  RETURNS TABLE( name TEXT,
                 abbrev TEXT,
                 utc_offset TEXT,
                 is_dst BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::BOOLEAN
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_config",
   R"(()
  RETURNS TABLE( name TEXT,
                 setting TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_get_shmem_allocations",
   R"(()
  RETURNS TABLE( name TEXT,
                 off BIGINT,
                 size BIGINT,
                 allocated_size BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_get_shmem_allocations_numa",
   R"(()
  RETURNS TABLE( name TEXT,
                 numa_node INTEGER,
                 size BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::INTEGER, NULL::BIGINT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_get_backend_memory_contexts",
   R"(()
  RETURNS TABLE( name TEXT,
                 ident TEXT,
                 type TEXT,
                 level INTEGER,
                 path TEXT,
                 total_bytes BIGINT,
                 total_nblocks BIGINT,
                 free_bytes BIGINT,
                 free_chunks BIGINT,
                 used_bytes BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT,
             NULL::INTEGER, NULL::TEXT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_activity",
   R"((pid INTEGER)
  RETURNS TABLE( datid BIGINT,
                 pid BIGINT,
                 usesysid BIGINT,
                 application_name TEXT,
                 state TEXT,
                 query TEXT,
                 wait_event_type TEXT,
                 wait_event TEXT,
                 xact_start TIMESTAMP,
                 query_start TIMESTAMP,
                 backend_start TIMESTAMP,
                 state_change TIMESTAMP,
                 client_addr TEXT,
                 client_hostname TEXT,
                 client_port INTEGER,
                 backend_xid BIGINT,
                 backend_xmin BIGINT,
                 backend_type TEXT,
                 ssl BOOLEAN,
                 sslversion TEXT,
                 sslcipher TEXT,
                 sslbits INTEGER,
                 ssl_client_dn TEXT,
                 ssl_client_serial BIGINT,
                 ssl_issuer_dn TEXT,
                 gss_auth BOOLEAN,
                 gss_princ TEXT,
                 gss_enc BOOLEAN,
                 gss_delegation BOOLEAN,
                 leader_pid BIGINT,
                 query_id BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT,
             NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::TIMESTAMP,
             NULL::TEXT, NULL::TEXT, NULL::INTEGER,
             NULL::BIGINT, NULL::BIGINT, NULL::TEXT,
             NULL::BOOLEAN, NULL::TEXT, NULL::TEXT, NULL::INTEGER,
             NULL::TEXT, NULL::BIGINT, NULL::TEXT,
             NULL::BOOLEAN, NULL::TEXT, NULL::BOOLEAN, NULL::BOOLEAN,
             NULL::BIGINT, NULL::BIGINT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_wal_senders",
   R"(()
  RETURNS TABLE( pid INTEGER,
                 state TEXT,
                 sent_lsn TEXT,
                 write_lsn TEXT,
                 flush_lsn TEXT,
                 replay_lsn TEXT,
                 write_lag TEXT,
                 flush_lag TEXT,
                 replay_lag TEXT,
                 sync_priority INTEGER,
                 sync_state TEXT,
                 reply_time TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::INTEGER, NULL::TEXT, NULL::TEXT, NULL::TEXT,
             NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::TEXT,
             NULL::TEXT, NULL::INTEGER, NULL::TEXT, NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_slru",
   R"(()
  RETURNS TABLE( name TEXT,
                 blks_zeroed BIGINT,
                 blks_hit BIGINT,
                 blks_read BIGINT,
                 blks_written BIGINT,
                 blks_exists BIGINT,
                 flushes BIGINT,
                 truncates BIGINT,
                 stats_reset TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_wal_receiver",
   R"(()
  RETURNS TABLE( pid INTEGER,
                 status TEXT,
                 receive_start_lsn TEXT,
                 receive_start_tli INTEGER,
                 written_lsn TEXT,
                 flushed_lsn TEXT,
                 received_tli INTEGER,
                 last_msg_send_time TIMESTAMP,
                 last_msg_receipt_time TIMESTAMP,
                 latest_end_lsn TEXT,
                 latest_end_time TIMESTAMP,
                 slot_name TEXT,
                 sender_host TEXT,
                 sender_port INTEGER,
                 conninfo TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::INTEGER, NULL::TEXT, NULL::TEXT, NULL::INTEGER,
             NULL::TEXT, NULL::TEXT, NULL::INTEGER,
             NULL::TIMESTAMP, NULL::TIMESTAMP,
             NULL::TEXT, NULL::TIMESTAMP,
             NULL::TEXT, NULL::TEXT, NULL::INTEGER, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_recovery_prefetch",
   R"(()
  RETURNS TABLE( stats_reset TIMESTAMP,
                 prefetch BIGINT,
                 hit BIGINT,
                 skip_init BIGINT,
                 skip_new BIGINT,
                 skip_fpw BIGINT,
                 skip_rep BIGINT,
                 wal_distance INTEGER,
                 block_distance INTEGER,
                 io_depth INTEGER)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TIMESTAMP, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::INTEGER, NULL::INTEGER, NULL::INTEGER
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_subscription",
   R"((subid OID)
  RETURNS TABLE( subid BIGINT,
                 relid BIGINT,
                 pid INTEGER,
                 leader_pid INTEGER,
                 received_lsn TEXT,
                 last_msg_send_time TIMESTAMP,
                 last_msg_receipt_time TIMESTAMP,
                 latest_end_lsn TEXT,
                 latest_end_time TIMESTAMP,
                 worker_type TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::BIGINT, NULL::INTEGER, NULL::INTEGER,
             NULL::TEXT, NULL::TIMESTAMP, NULL::TIMESTAMP,
             NULL::TEXT, NULL::TIMESTAMP, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_get_replication_slots",
   R"(()
  RETURNS TABLE( slot_name TEXT,
                 plugin TEXT,
                 slot_type TEXT,
                 datoid BIGINT,
                 temporary BOOLEAN,
                 active BOOLEAN,
                 active_pid INTEGER,
                 xmin BIGINT,
                 catalog_xmin BIGINT,
                 restart_lsn TEXT,
                 confirmed_flush_lsn TEXT,
                 wal_status TEXT,
                 safe_wal_size BIGINT,
                 two_phase BOOLEAN,
                 two_phase_at TEXT,
                 inactive_since TIMESTAMP,
                 conflicting BOOLEAN,
                 invalidation_reason TEXT,
                 failover BOOLEAN,
                 synced BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::BIGINT,
             NULL::BOOLEAN, NULL::BOOLEAN, NULL::INTEGER,
             NULL::BIGINT, NULL::BIGINT, NULL::TEXT, NULL::TEXT,
             NULL::TEXT, NULL::BIGINT, NULL::BOOLEAN, NULL::TEXT,
             NULL::TIMESTAMP, NULL::BOOLEAN, NULL::TEXT,
             NULL::BOOLEAN, NULL::BOOLEAN
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_replication_slot",
   R"((slot_name TEXT)
  RETURNS TABLE( slot_name TEXT,
                 spill_txns BIGINT,
                 spill_count BIGINT,
                 spill_bytes BIGINT,
                 stream_txns BIGINT,
                 stream_count BIGINT,
                 stream_bytes BIGINT,
                 total_txns BIGINT,
                 total_bytes BIGINT,
                 stats_reset TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_archiver",
   R"(()
  RETURNS TABLE( archived_count BIGINT,
                 last_archived_wal TEXT,
                 last_archived_time TIMESTAMP,
                 failed_count BIGINT,
                 last_failed_wal TEXT,
                 last_failed_time TIMESTAMP,
                 stats_reset TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::TEXT, NULL::TIMESTAMP,
             NULL::BIGINT, NULL::TEXT, NULL::TIMESTAMP,
             NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_io",
   R"(()
  RETURNS TABLE( backend_type TEXT,
                 object TEXT,
                 context TEXT,
                 reads BIGINT,
                 read_bytes BIGINT,
                 read_time DOUBLE PRECISION,
                 writes BIGINT,
                 write_bytes BIGINT,
                 write_time DOUBLE PRECISION,
                 writebacks BIGINT,
                 writeback_time DOUBLE PRECISION,
                 extends BIGINT,
                 extend_bytes BIGINT,
                 extend_time DOUBLE PRECISION,
                 hits BIGINT,
                 evictions BIGINT,
                 reuses BIGINT,
                 fsyncs BIGINT,
                 fsync_time DOUBLE PRECISION,
                 stats_reset TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT,
             NULL::BIGINT, NULL::BIGINT, NULL::DOUBLE PRECISION,
             NULL::BIGINT, NULL::BIGINT, NULL::DOUBLE PRECISION,
             NULL::BIGINT, NULL::DOUBLE PRECISION,
             NULL::BIGINT, NULL::BIGINT, NULL::DOUBLE PRECISION,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::DOUBLE PRECISION, NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_wal",
   R"(()
  RETURNS TABLE( wal_records BIGINT,
                 wal_fpi BIGINT,
                 wal_bytes BIGINT,
                 wal_buffers_full BIGINT,
                 stats_reset TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_show_replication_origin_status",
   R"(()
  RETURNS TABLE( local_id BIGINT,
                 external_id TEXT,
                 remote_lsn TEXT,
                 local_lsn TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_stat_get_subscription_stats",
   R"((subid OID)
  RETURNS TABLE( subid BIGINT,
                 apply_error_count BIGINT,
                 sync_error_count BIGINT,
                 confl_insert_exists BIGINT,
                 confl_update_origin_differs BIGINT,
                 confl_update_exists BIGINT,
                 confl_update_missing BIGINT,
                 confl_delete_origin_differs BIGINT,
                 confl_delete_missing BIGINT,
                 confl_multiple_unique_conflicts BIGINT,
                 stats_reset TIMESTAMP)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::BIGINT,
             NULL::TIMESTAMP
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_get_wait_events",
   R"(()
  RETURNS TABLE( type TEXT,
                 name TEXT,
                 description TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)"},

  {"pg_catalog", "pg_get_aios",
   R"(()
  RETURNS TABLE( pid INTEGER,
                 io_id BIGINT,
                 io_generation BIGINT,
                 state TEXT,
                 operation TEXT,
                 off BIGINT,
                 length BIGINT,
                 target TEXT,
                 handle_data_len BIGINT,
                 raw_result BIGINT,
                 result BIGINT,
                 target_desc TEXT,
                 f_sync BOOLEAN,
                 f_localmem BOOLEAN,
                 f_buffered BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::INTEGER, NULL::BIGINT, NULL::BIGINT,
             NULL::TEXT, NULL::TEXT, NULL::BIGINT,
             NULL::BIGINT, NULL::TEXT, NULL::BIGINT,
             NULL::BIGINT, NULL::BIGINT, NULL::TEXT,
             NULL::BOOLEAN, NULL::BOOLEAN, NULL::BOOLEAN
      WHERE false;
  END;)"},
  // PG regexp functions wrapping DuckDB builtins

  {DEFAULT_SCHEMA, "regexp_count",
   R"((text, pattern) AS len(regexp_extract_all(text, pattern)),
      (text, pattern, start) AS len(regexp_extract_all(text[start:], pattern)))"},

  {DEFAULT_SCHEMA, "regexp_substr",
   R"((text, pattern) AS CASE WHEN regexp_matches(text, pattern) THEN regexp_extract(text, pattern) END,
      (text, pattern, start) AS CASE WHEN regexp_matches(text[start:], pattern) THEN regexp_extract(text[start:], pattern) END)"},

  // PG array functions missing from DuckDB

  {DEFAULT_SCHEMA, "array_remove",
   R"((arr, elem) AS list_filter(arr, x -> x IS DISTINCT FROM elem))"},

  {DEFAULT_SCHEMA, "trim_array",
   R"((arr, n) AS arr[:len(arr) - n])"},

  {DEFAULT_SCHEMA, "array_positions",
   R"((arr, elem) AS list_filter(list_transform(arr, (x, i) -> CASE WHEN x IS NOT DISTINCT FROM elem THEN i ELSE NULL END), x -> x IS NOT NULL))"},

  {DEFAULT_SCHEMA, "array_replace",
   R"((arr, old_elem, new_elem) AS list_transform(arr, x -> CASE WHEN x IS NOT DISTINCT FROM old_elem THEN new_elem ELSE x END))"},

  {DEFAULT_SCHEMA, "array_lower",
   R"((arr, dim) AS CASE WHEN arr IS NULL OR len(arr) = 0 THEN NULL ELSE 1 END)"},

  {DEFAULT_SCHEMA, "array_upper",
   R"((arr, dim) AS CASE WHEN arr IS NULL OR len(arr) = 0 THEN NULL ELSE len(arr) END)"},

  // regexp_like: alias registered in duckdb functions.json -> regexp_matches

  // overlay(string placing string from int for int) -> string
  // Parser transforms: overlay(s PLACING r FROM p FOR n) -> overlay(s, r, p, n)
  {DEFAULT_SCHEMA, "overlay",
   R"((s, repl, start, count) AS substr(s, 1, start - 1) || repl || substr(s, start + count))"},
  // 3-arg form: overlay(string placing string from int) -- count defaults to length of replacement
  {DEFAULT_SCHEMA, "overlay",
   R"((s, repl, start) AS substr(s, 1, start - 1) || repl || substr(s, start + length(repl)))"},

  // PG datetime functions missing from DuckDB
  // clock_timestamp: real wall-clock time (not statement time).
  // DuckDB's now() is transaction-scoped, but close enough for most uses.
  {DEFAULT_SCHEMA, "clock_timestamp", R"(() AS now())"},
  // timeofday: wall clock as formatted text
  {DEFAULT_SCHEMA, "timeofday",
   R"(() AS strftime(now()::timestamp, '%a %b %d %H:%M:%S %Y UTC'))"},

  // PG math functions missing from DuckDB

  // div: registered as C++ function in connector/functions/math.cpp

  // Degree-based trigonometric functions
  {DEFAULT_SCHEMA, "sind",
   R"((x) AS sin(radians(x)))"},

  {DEFAULT_SCHEMA, "cosd",
   R"((x) AS cos(radians(x)))"},

  {DEFAULT_SCHEMA, "tand",
   R"((x) AS tan(radians(x)))"},

  // cotd is registered as a scalar function in RegisterPgMathFunctions
  // with PG-compatible division-by-zero error handling.

  {DEFAULT_SCHEMA, "asind",
   R"((x) AS degrees(asin(x)))"},

  {DEFAULT_SCHEMA, "acosd",
   R"((x) AS degrees(acos(x)))"},

  {DEFAULT_SCHEMA, "atand",
   R"((x) AS degrees(atan(x)))"},

  {DEFAULT_SCHEMA, "atan2d",
   R"((y, x) AS degrees(atan2(y, x)))"},

  // set_config: registered as C++ function in connector/functions/system.cpp


  {"pg_catalog", "aclexplode",
   R"((acl) AS TABLE
    SELECT
      COALESCE((SELECT a.oid::INTEGER FROM pg_catalog.pg_authid a
                WHERE a.rolname = SUBSTRING(item, STRPOS(item, '/') + 1)), 0) AS grantor,
      CASE WHEN STRPOS(item, '=') = 1 THEN 0
           ELSE COALESCE((SELECT a.oid::INTEGER FROM pg_catalog.pg_authid a
                          WHERE a.rolname = SUBSTRING(item, 1, STRPOS(item, '=') - 1)), 0)
      END AS grantee,
      pc.priv AS privilege_type,
      (STRPOS(SUBSTRING(item, STRPOS(item, '=') + 1, STRPOS(item, '/') - STRPOS(item, '=') - 1), pc.chr || '*') > 0) AS is_grantable
    FROM (SELECT UNNEST(acl)::TEXT AS item) t
    CROSS JOIN (SELECT UNNEST(['a','r','w','d','D','x','t','m','X','U','C','T','c']) AS chr,
                       UNNEST(['INSERT','SELECT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN','EXECUTE','USAGE','CREATE','TEMPORARY','CONNECT']) AS priv) pc
    WHERE STRPOS(SUBSTRING(item, STRPOS(item, '=') + 1, STRPOS(item, '/') - STRPOS(item, '=') - 1), pc.chr) > 0)"},

  {"pg_catalog", "acldefault",
   R"((objtype, owner) AS CASE objtype
        WHEN 'r' THEN ARRAY[(COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=arwdDxtm/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 's' THEN ARRAY[(COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=rwU/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 'd' THEN ARRAY[('=Tc/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text)), (COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=CTc/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 'n' THEN ARRAY[(COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=UC/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 'f' THEN ARRAY[('=X/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text)), (COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=X/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 'T' THEN ARRAY[('=U/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text)), (COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=U/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 'l' THEN ARRAY[('=U/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text)), (COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text) || '=U/' || COALESCE((SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = owner), owner::text))]
        WHEN 'c' THEN CAST(ARRAY[] AS TEXT[])
        ELSE CAST(NULL AS TEXT[])
      END)"},

  // makeaclitem(grantee, grantor, privileges, is_grantable): one aclitem in
  // PG's `grantee=privchars[*]/grantor` text form. Chars emit in canonical
  // order (not input order); the unrecognized-privilege error preserves the
  // input token's case, like PG.
  {"pg_catalog", "makeaclitem",
   R"((grantee, grantor, privileges, is_grantable) AS (
      WITH canon(ord,chr,nm) AS (VALUES
          (1,'a','INSERT'),(2,'r','SELECT'),(3,'w','UPDATE'),(4,'d','DELETE'),
          (5,'D','TRUNCATE'),(6,'x','REFERENCES'),(7,'t','TRIGGER'),(8,'m','MAINTAIN'),
          (9,'X','EXECUTE'),(10,'U','USAGE'),(11,'C','CREATE'),(12,'T','TEMPORARY'),
          (13,'c','CONNECT'),(12,'T','TEMP')),
      toks AS (SELECT trim(t) AS tok FROM unnest(string_split(privileges, ',')) AS s(t)),
      bad AS (SELECT tok FROM toks WHERE upper(tok) NOT IN (SELECT nm FROM canon) LIMIT 1)
      SELECT CASE WHEN EXISTS (SELECT 1 FROM bad)
        THEN error('unrecognized privilege type: "' || (SELECT tok FROM bad) || '"')
        ELSE
          CASE WHEN grantee = 0 THEN ''
               ELSE COALESCE((SELECT rolname FROM pg_catalog.pg_authid WHERE oid=grantee), grantee::text) END
          || '='
          || COALESCE((SELECT string_agg(chr || CASE WHEN is_grantable THEN '*' ELSE '' END, '' ORDER BY ord)
              FROM (SELECT DISTINCT ord,chr FROM canon WHERE nm IN (SELECT upper(tok) FROM toks)) d), '')
          || '/'
          || COALESCE((SELECT rolname FROM pg_catalog.pg_authid WHERE oid=grantor), grantor::text)
        END))"},

  // Stubs for PG C built-in functions from ruleutils.c / misc.
  // These take OIDs and return text representations of database objects.
  // TODO(mbkkt): implement properly -- currently return NULL.
  {"pg_catalog", "pg_get_ruledef", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_ruledef", "(oid, pretty_bool) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_viewdef",
   "(relid) AS (SELECT regexp_replace(vd_view.sql, '^CREATE VIEW [^ ]+ "
   "([(][^)]*[)] )?AS ', '') FROM pg_catalog.pg_class vd_class JOIN "
   "duckdb_views() vd_view ON vd_view.view_name = vd_class.relname AND "
   "vd_view.database_name = current_database() JOIN pg_catalog.pg_namespace "
   "vd_ns ON vd_ns.oid = vd_class.relnamespace AND vd_ns.nspname = "
   "vd_view.schema_name WHERE vd_class.oid = relid)"},
  {"pg_catalog", "pg_get_viewdef",
   "(relid, flag) AS (SELECT regexp_replace(vd_view.sql, '^CREATE VIEW "
   "[^ ]+ ([(][^)]*[)] )?AS ', '') FROM pg_catalog.pg_class vd_class JOIN "
   "duckdb_views() vd_view ON vd_view.view_name = vd_class.relname AND "
   "vd_view.database_name = current_database() JOIN pg_catalog.pg_namespace "
   "vd_ns ON vd_ns.oid = vd_class.relnamespace AND vd_ns.nspname = "
   "vd_view.schema_name WHERE vd_class.oid = relid)"},
  {"pg_catalog", "pg_get_indexdef", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_indexdef", "(oid, col, pretty_bool) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_triggerdef", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_triggerdef", "(oid, pretty_bool) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_constraintdef", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_constraintdef", "(oid, pretty_bool) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_expr", "(node_text, rel_oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_expr", "(node_text, rel_oid, pretty_bool) AS CAST(NULL AS TEXT)"},
  // pg_get_userbyid(oid): role name for a role oid, or PG's
  {"pg_catalog", "pg_get_userbyid",
   R"((role_oid) AS COALESCE(
        (SELECT a.rolname FROM pg_catalog.pg_authid a WHERE a.oid = role_oid),
        ('unknown (OID=' || role_oid || ')')))"},
  {"pg_catalog", "pg_get_function_result",
   "(function_oid) AS (SELECT format_type(prorettype, NULL) "
   "FROM pg_catalog.pg_proc WHERE oid = function_oid)"},
  {"pg_catalog", "pg_get_function_arguments",
   "(function_oid) AS (SELECT string_agg("
   "  CASE WHEN proargnames IS NOT NULL "
   "         AND array_length(proargnames, 1) >= i "
   "         AND proargnames[i] IS NOT NULL "
   "         AND proargnames[i] <> '' "
   "       THEN proargnames[i] || ' ' ELSE '' END "
   "  || format_type(proargtypes[i], NULL), ', ' ORDER BY i) "
   "FROM pg_catalog.pg_proc, unnest(proargtypes) WITH ORDINALITY AS u(t, i) "
   "WHERE oid = function_oid GROUP BY proargnames)"},
  {"pg_catalog", "pg_get_function_arg_default", "(oid, n) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_function_identity_arguments",
   "(function_oid) AS (SELECT string_agg(format_type(proargtypes[i], NULL), ', ' ORDER BY i) "
   "FROM pg_catalog.pg_proc, unnest(proargtypes) WITH ORDINALITY AS u(t, i) "
   "WHERE oid = function_oid)"},
  {"pg_catalog", "pg_get_functiondef", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_statisticsobjdef_expressions", "(oid) AS CAST(NULL AS TEXT[])"},
  {"pg_catalog", "pg_get_statisticsobjdef_columns", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_partkeydef", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_get_serial_sequence", "(tbl, col) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_tablespace_location", "(oid) AS CAST(NULL AS TEXT)"},

  // Recovery status: SereneDB is always a primary (no WAL-replay standby
  // mode), so both are constant false. pgAdmin calls these on every connect.
  {"pg_catalog", "pg_is_in_recovery", "() AS false"},
  {"pg_catalog", "pg_is_wal_replay_paused", "() AS false"},
  {"pg_catalog", "obj_description", "(oid, catalog) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "obj_description", "(oid) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "shobj_description", "(oid, catalog) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "col_description", "(oid, col) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_function_is_visible",
   "(function_oid) AS ((SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE n.oid = "
   "(SELECT pronamespace FROM pg_catalog.pg_proc WHERE oid = function_oid)) = "
   "ANY(current_schemas(true)))"},
  {"pg_catalog", "pg_table_is_visible",
   "(table_oid) AS ((SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE n.oid = "
   "(SELECT relnamespace FROM pg_catalog.pg_class WHERE oid = table_oid)) = "
   "ANY(current_schemas(true)))"},
  {"pg_catalog", "pg_type_is_visible",
   "(type_oid) AS ((SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE n.oid = "
   "(SELECT typnamespace FROM pg_catalog.pg_type WHERE oid = type_oid)) = "
   "ANY(current_schemas(true)))"},

  // Stub scalar functions returning 0/NULL -- PG C built-ins not yet implemented.
  // TODO(mbkkt): implement properly.
  {"pg_catalog", "pg_column_is_updatable", "(a, b, c) AS true"},
  {"pg_catalog", "pg_relation_is_updatable", "(a, b) AS 0"},
  {"pg_catalog", "pg_relation_is_publishable", "(a) AS false"},
  {"pg_catalog", "pg_sequence_last_value", "(a) AS CAST(NULL AS BIGINT)"},
  {"pg_catalog", "pg_indexam_progress_phasename", "(a, b) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_statistics_obj_is_visible", "(a) AS true"},

  // Table-returning function stubs (return empty)
  {"pg_catalog", "pg_options_to_table",
   R"((opts) AS TABLE SELECT NULL::TEXT AS option_name, NULL::TEXT AS option_value WHERE false)"},
  {"pg_catalog", "pg_mcv_list_items",
   R"((mcv) AS TABLE SELECT NULL::INTEGER AS index, NULL::TEXT[] AS values, NULL::BOOLEAN[] AS nulls, NULL::DOUBLE AS frequency, NULL::DOUBLE AS base_frequency WHERE false)"},
  {"pg_catalog", "pg_get_publication_tables",
   R"((pubname) AS TABLE SELECT NULL::INTEGER AS pubid, NULL::INTEGER AS relid, NULL::SMALLINT[] AS attrs, NULL::TEXT AS qual WHERE false)"},

  // pg_stat_get_* stubs -- statistics functions, all return 0 or NULL
  {"pg_catalog", "pg_stat_get_analyze_count", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_autoanalyze_count", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_autovacuum_count", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_bgwriter_buf_written_clean", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_bgwriter_maxwritten_clean", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_bgwriter_stat_reset_time", "() AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_blocks_fetched", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_blocks_hit", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_buf_alloc", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_buffers_written", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_num_performed", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_num_requested", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_num_timed", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_restartpoints_performed", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_restartpoints_requested", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_restartpoints_timed", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_slru_written", "() AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_checkpointer_stat_reset_time", "() AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_checkpointer_sync_time", "() AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_checkpointer_write_time", "() AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_db_active_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_db_blk_read_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_db_blk_write_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_db_blocks_fetched", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_blocks_hit", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_checksum_failures", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_checksum_last_failure", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_db_conflict_all", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_conflict_bufferpin", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_conflict_lock", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_conflict_logicalslot", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_conflict_snapshot", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_conflict_startup_deadlock", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_conflict_tablespace", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_deadlocks", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_idle_in_transaction_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_db_numbackends", "(a) AS CAST(0 AS INTEGER)"},
  {"pg_catalog", "pg_stat_get_db_parallel_workers_launched", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_parallel_workers_to_launch", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_session_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_db_sessions", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_sessions_abandoned", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_sessions_fatal", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_sessions_killed", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_stat_reset_time", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_db_temp_bytes", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_temp_files", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_tuples_deleted", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_tuples_fetched", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_tuples_inserted", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_tuples_returned", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_tuples_updated", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_xact_commit", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_db_xact_rollback", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_dead_tuples", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_function_calls", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_function_self_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_function_total_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_ins_since_vacuum", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_last_analyze_time", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_last_autoanalyze_time", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_last_autovacuum_time", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_last_vacuum_time", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_lastscan", "(a) AS CAST(NULL AS TIMESTAMP)"},
  {"pg_catalog", "pg_stat_get_live_tuples", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_mod_since_analyze", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_numscans", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_total_analyze_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_total_autoanalyze_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_total_autovacuum_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_total_vacuum_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_tuples_deleted", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_tuples_fetched", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_tuples_hot_updated", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_tuples_inserted", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_tuples_newpage_updated", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_tuples_returned", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_tuples_updated", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_vacuum_count", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_function_calls", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_function_self_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_xact_function_total_time", "(a) AS CAST(0 AS DOUBLE)"},
  {"pg_catalog", "pg_stat_get_xact_numscans", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_deleted", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_fetched", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_hot_updated", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_inserted", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_newpage_updated", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_returned", "(a) AS CAST(0 AS BIGINT)"},
  {"pg_catalog", "pg_stat_get_xact_tuples_updated", "(a) AS CAST(0 AS BIGINT)"},

  // --- Real, PG-faithful privilege / row-security oracles. These return PG's
  // actual answer for every reachable input (not permissive placeholders). ---

  // SereneDB has no RLS (no CREATE POLICY, relrowsecurity always false), so
  // row security is never active -- PG also returns false for every reachable
  // input (no-policy table, system oid, non-owner). Matches PG.
  {"pg_catalog", "row_security_active", "(a) AS false"},

  // has_{foreign_data_wrapper,server}_privilege: SereneDB has no FDWs or foreign
  // servers, so every object oid is nonexistent. PG returns true for a
  // superuser (the superuser check precedes the object lookup) and NULL for any
  // other role on a missing object (is_missing). Matches PG for the reachable
  // (oid) forms; the role arg may be a name or an oid. `a` is the role,
  // `current_user` for the 2-arg form.
  {"pg_catalog", "has_foreign_data_wrapper_privilege",
   "(a, b) AS CASE WHEN (SELECT rolsuper FROM pg_catalog.pg_authid WHERE rolname = current_user) THEN true ELSE CAST(NULL AS BOOLEAN) END, "
   "(a, b, c) AS CASE WHEN (SELECT rolsuper FROM pg_catalog.pg_authid WHERE rolname = a::text OR oid::text = a::text) THEN true ELSE CAST(NULL AS BOOLEAN) END"},
  {"pg_catalog", "has_server_privilege",
   "(a, b) AS CASE WHEN (SELECT rolsuper FROM pg_catalog.pg_authid WHERE rolname = current_user) THEN true ELSE CAST(NULL AS BOOLEAN) END, "
   "(a, b, c) AS CASE WHEN (SELECT rolsuper FROM pg_catalog.pg_authid WHERE rolname = a::text OR oid::text = a::text) THEN true ELSE CAST(NULL AS BOOLEAN) END"},

  // has_language_privilege: a language grants USAGE to PUBLIC by default
  // (acldefault world_default = USAGE), so every role holds USAGE on SereneDB's
  // built-in languages -- PG returns true too. (A nonexistent language oid would
  // be NULL in PG, but SereneDB has no CREATE LANGUAGE so that is unreachable.)
  {"pg_catalog", "has_language_privilege", "(a, b) AS true, (a, b, c) AS true"},

  // has_tablespace_privilege: SereneDB has only the hardcoded pg_default (oid
  // 1663) and pg_global (1664), whose default ACLs grant nothing to PUBLIC
  // (world_default = NO_RIGHTS). PG: superuser -> true; non-superuser on either
  // -> false (no PUBLIC create); non-superuser on a nonexistent oid -> NULL.
  {"pg_catalog", "has_tablespace_privilege",
   "(a, b) AS CASE WHEN (SELECT rolsuper FROM pg_catalog.pg_authid WHERE rolname = current_user) THEN true WHEN a::text IN ('1663', 'pg_default', '1664', 'pg_global') THEN false ELSE CAST(NULL AS BOOLEAN) END, "
   "(a, b, c) AS CASE WHEN (SELECT rolsuper FROM pg_catalog.pg_authid WHERE rolname = a::text OR oid::text = a::text) THEN true WHEN b::text IN ('1663', 'pg_default', '1664', 'pg_global') THEN false ELSE CAST(NULL AS BOOLEAN) END"},

  // Real: name||'_'||oid, matching PG's nameconcatoid.
  {"pg_catalog", "nameconcatoid", "(a, b) AS CAST(a || '_' || CAST(b AS TEXT) AS TEXT)"},

  {"pg_catalog", "getdatabaseencoding", "() AS 'UTF8'"},
  // Always UTF-8 -> max 4 bytes per character. Argument ignored.
  {"pg_catalog", "pg_encoding_max_length", "(encoding int4) AS 4"},
  // Always UTF-8 -> encoding 6. Stub: only UTF8 supported.
  {"pg_catalog", "pg_encoding_to_char", "(encoding int4) AS 'UTF8'::name"},
  {"pg_catalog", "pg_char_to_encoding", "(enc_name) AS 6::int4"},
  // No temp schemas supported yet.
  {"pg_catalog", "pg_my_temp_schema", "() AS 0::oid"},
  // pg_backend_pid() is a C++ scalar (PgBackendPidFunction) -- it reads the
  // per-connection backend PID from the ConnectionContext.
  // clang-format on
};

}  // namespace sdb::pg
