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

#include <array>
#include <string_view>

namespace sdb::pg {

// TODO(mkornaukhov) write queries in separate sql file
inline constexpr auto kSystemFunctionsQueries = std::to_array<
  std::string_view>({
  // clang-format off
  R"(CREATE FUNCTION pg_show_all_settings()
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
      FROM sdb_show_all_settings;
  END;)",

  R"(CREATE FUNCTION pg_stat_get_progress_info(cmd TEXT)
  RETURNS TABLE( pid BIGINT,
                 datid OID,
                 relid OID,
                 param1 BIGINT,
                 param2 BIGINT,
                 param3 BIGINT,
                 param4 BIGINT,
                 param5 BIGINT,
                 param6 BIGINT,
                 param7 BIGINT,
                 param8 BIGINT,
                 param9 BIGINT,
                 param10 BIGINT,
                 param11 BIGINT,
                 param12 BIGINT,
                 param13 BIGINT,
                 param14 BIGINT,
                 param15 BIGINT,
                 param16 BIGINT,
                 param17 BIGINT,
                 param18 BIGINT,
                 param19 BIGINT,
                 param20 BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT
        pid, datid, relid,
        param1, param2, param3, param4, param5,
        param6, param7, param8, param9, param10,
        param11, param12, param13, param14, param15,
        param16, param17, param18, param19, param20
      FROM sdb_stat_progress
      WHERE command = cmd;
  END;)",

  // A few supporting functions first ...

  // TODO(mbkkt) Enable when implement proper SETOF functions support.

  // Expand any 1-D array into a set with integers 1..N
  // R"(CREATE FUNCTION _pg_expandarray(IN anyarray, OUT x anyelement, OUT n int)
  //     RETURNS SETOF RECORD
  //     LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE
  //     ROWS 100 SUPPORT pg_catalog.array_unnest_support
  //     AS 'SELECT * FROM pg_catalog.unnest($1) WITH ORDINALITY';)",

  // Given an index's OID and an underlying-table column number, return the
  // column's position in the index (NULL if not there)
  // R"(CREATE FUNCTION _pg_index_position(oid, smallint) RETURNS int
  //     LANGUAGE sql STRICT STABLE
  // BEGIN ATOMIC
  // SELECT (ss.a).n FROM
  //   (SELECT information_schema._pg_expandarray(indkey) AS a
  //    FROM pg_catalog.pg_index WHERE indexrelid = $1) ss
  //   WHERE (ss.a).x = $2;
  // END;)",

  R"(CREATE FUNCTION _pg_truetypid(pg_attribute, pg_type) RETURNS oid
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END;)",

  R"(CREATE FUNCTION _pg_truetypmod(pg_attribute, pg_type) RETURNS int4
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN CASE WHEN $2.typtype = 'd' THEN $2.typtypmod ELSE $1.atttypmod END;)",

  // these functions encapsulate knowledge about the encoding of typmod:

  R"(CREATE FUNCTION _pg_char_max_length(typid oid, typmod int4) RETURNS integer
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
    END;)",

  R"(CREATE FUNCTION _pg_char_octet_length(typid oid, typmod int4) RETURNS integer
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
    END;)",

  R"(CREATE FUNCTION _pg_numeric_precision(typid oid, typmod int4) RETURNS integer
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
    END;)",

  R"(CREATE FUNCTION _pg_numeric_precision_radix(typid oid, typmod int4) RETURNS integer
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (21, 23, 20, 700, 701) THEN 2
         WHEN $1 IN (1700) THEN 10
         ELSE null
    END;)",

  R"(CREATE FUNCTION _pg_numeric_scale(typid oid, typmod int4) RETURNS integer
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
    END;)",

  R"(CREATE FUNCTION _pg_datetime_precision(typid oid, typmod int4) RETURNS integer
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
    END;)",

  R"(CREATE FUNCTION _pg_interval_type(typid oid, mod int4) RETURNS text
      LANGUAGE sql
      IMMUTABLE
      PARALLEL SAFE
      RETURNS NULL ON NULL INPUT
  RETURN
    CASE WHEN $1 IN (1186) /* interval */
             THEN pg_catalog.upper(substring(pg_catalog.format_type($1, $2) similar 'interval[()0-9]* #"%#"' escape '#'))
         ELSE null
    END;)",

  // Stub set-returning functions (return empty tables)
  // Used by pg_catalog system views that reference these functions.

  R"(CREATE FUNCTION pg_lock_status()
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
  END;)",

  R"(CREATE FUNCTION pg_cursor()
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
  END;)",

  R"(CREATE FUNCTION pg_available_extensions()
  RETURNS TABLE( name TEXT,
                 default_version TEXT,
                 comment TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_available_extension_versions()
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
  END;)",

  R"(CREATE FUNCTION pg_prepared_xact()
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
  END;)",

  R"(CREATE FUNCTION pg_prepared_statement()
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
  END;)",

  R"(CREATE FUNCTION pg_show_all_file_settings()
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
  END;)",

  R"(CREATE FUNCTION pg_hba_file_rules()
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
      SELECT NULL::INTEGER, NULL::TEXT, NULL::INTEGER,
             NULL::TEXT, NULL::TEXT[], NULL::TEXT[],
             NULL::TEXT, NULL::TEXT, NULL::TEXT,
             NULL::TEXT[], NULL::TEXT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_ident_file_mappings()
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
  END;)",

  R"(CREATE FUNCTION pg_timezone_abbrevs_zone()
  RETURNS TABLE( abbrev TEXT,
                 utc_offset TEXT,
                 is_dst BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::BOOLEAN
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_timezone_abbrevs_abbrevs()
  RETURNS TABLE( abbrev TEXT,
                 utc_offset TEXT,
                 is_dst BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::BOOLEAN
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_timezone_names()
  RETURNS TABLE( name TEXT,
                 abbrev TEXT,
                 utc_offset TEXT,
                 is_dst BOOLEAN)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::BOOLEAN
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_config()
  RETURNS TABLE( name TEXT,
                 setting TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_get_shmem_allocations()
  RETURNS TABLE( name TEXT,
                 off BIGINT,
                 size BIGINT,
                 allocated_size BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_get_shmem_allocations_numa()
  RETURNS TABLE( name TEXT,
                 numa_node INTEGER,
                 size BIGINT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::INTEGER, NULL::BIGINT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_get_backend_memory_contexts()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_activity(pid INTEGER)
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_wal_senders()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_slru()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_wal_receiver()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_recovery_prefetch()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_subscription(subid OID)
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
  END;)",

  R"(CREATE FUNCTION pg_get_replication_slots()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_replication_slot(slot_name TEXT)
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_archiver()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_io()
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
  END;)",

  R"(CREATE FUNCTION pg_stat_get_wal()
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
  END;)",

  R"(CREATE FUNCTION pg_show_replication_origin_status()
  RETURNS TABLE( local_id BIGINT,
                 external_id TEXT,
                 remote_lsn TEXT,
                 local_lsn TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::BIGINT, NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_stat_get_subscription_stats(subid OID)
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
  END;)",

  R"(CREATE FUNCTION pg_get_wait_events()
  RETURNS TABLE( type TEXT,
                 name TEXT,
                 description TEXT)
  LANGUAGE SQL
  BEGIN ATOMIC
      SELECT NULL::TEXT, NULL::TEXT, NULL::TEXT
      WHERE false;
  END;)",

  R"(CREATE FUNCTION pg_get_aios()
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
  END;)",
  // clang-format on
});

}  // namespace sdb::pg
