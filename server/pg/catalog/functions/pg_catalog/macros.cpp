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

#include "pg/catalog/functions/functions.h"

namespace sdb::pg {
namespace {

constexpr SystemMacro kMacros[] = {
#include "pg/catalog/generated/function_stubs.gen.inc"
  // clang-format off
  {"pg_catalog", "pg_get_keywords",
   R"(() AS TABLE
      SELECT word,
             catcode,
             NULL::BOOLEAN AS barelabel,
             CASE catcode
               WHEN 'R' THEN 'reserved'
               WHEN 'T' THEN 'reserved (can be function or type name)'
               WHEN 'C' THEN 'unreserved (cannot be function or type name)'
               ELSE 'unreserved'
             END AS catdesc,
             NULL::TEXT AS baredesc
      FROM (SELECT keyword_name AS word,
                   CASE
                     WHEN bool_or(keyword_category = 'reserved') THEN 'R'
                     WHEN bool_or(keyword_category = 'unreserved') THEN 'U'
                     WHEN bool_or(keyword_category = 'type_function') AND
                          bool_or(keyword_category = 'column_name') THEN 'U'
                     WHEN bool_or(keyword_category = 'type_function') THEN 'T'
                     ELSE 'C'
                   END::"char" AS catcode
            FROM duckdb_keywords()
            GROUP BY keyword_name)
      ORDER BY word)"},

  {"pg_catalog", "pg_prepared_statement",
   R"(() AS TABLE
      SELECT P.name AS name,
             P.statement AS statement,
             NULL::TIMESTAMPTZ AS prepare_time,
             NULL::BIGINT[] AS parameter_types,
             NULL::BIGINT[] AS result_types,
             true AS from_sql,
             0::BIGINT AS generic_plans,
             0::BIGINT AS custom_plans
      FROM sdb_prepared_statements() AS P)"},

  {"pg_catalog", "pg_stat_get_activity",
   R"((target_pid) AS TABLE
      SELECT S.datid AS datid,
             S.pid AS pid,
             U.oid AS usesysid,
             NULL::TEXT AS application_name,
             S.state AS state,
             S.query AS query,
             NULL::TEXT AS wait_event_type,
             NULL::TEXT AS wait_event,
             NULL::TIMESTAMPTZ AS xact_start,
             CASE WHEN S.query_start_us <> 0
                  THEN to_timestamp(S.query_start_us / 1000000.0) END AS query_start,
             to_timestamp(S.backend_start_us / 1000000.0) AS backend_start,
             NULL::TIMESTAMPTZ AS state_change,
             NULL::INET AS client_addr,
             NULL::TEXT AS client_hostname,
             NULL::INTEGER AS client_port,
             NULL::XID AS backend_xid,
             NULL::XID AS backend_xmin,
             'client backend' AS backend_type,
             NULL::BOOLEAN AS ssl,
             NULL::TEXT AS sslversion,
             NULL::TEXT AS sslcipher,
             NULL::INTEGER AS sslbits,
             NULL::TEXT AS ssl_client_dn,
             NULL::BIGINT AS ssl_client_serial,
             NULL::TEXT AS ssl_issuer_dn,
             NULL::BOOLEAN AS gss_auth,
             NULL::TEXT AS gss_princ,
             NULL::BOOLEAN AS gss_enc,
             NULL::BOOLEAN AS gss_delegation,
             NULL::INTEGER AS leader_pid,
             NULL::BIGINT AS query_id
      FROM pg_catalog.sdb_progress AS S
           LEFT JOIN pg_catalog.pg_authid AS U ON (S.usename = U.rolname)
      WHERE target_pid IS NULL OR S.pid = target_pid)"},

  {"pg_catalog", "pg_stat_get_progress_info",
   R"((cmdtype) AS TABLE
      SELECT S.pid AS pid,
             S.datid AS datid,
             S.relid AS relid,
             CAST(CASE upper(cmdtype)
                    WHEN 'VACUUM' THEN CASE S.phase
                                         WHEN 'initializing' THEN 0
                                         WHEN 'scanning heap' THEN 1
                                         WHEN 'vacuuming indexes' THEN 2
                                         WHEN 'vacuuming heap' THEN 3
                                         WHEN 'cleaning up indexes' THEN 4
                                         WHEN 'truncating heap' THEN 5
                                         WHEN 'performing final cleanup' THEN 6
                                       END
                    WHEN 'ANALYZE' THEN CASE S.phase
                                          WHEN 'initializing' THEN 0
                                          WHEN 'acquiring sample rows' THEN 1
                                          WHEN 'acquiring inherited sample rows' THEN 2
                                          WHEN 'computing statistics' THEN 3
                                          WHEN 'computing extended statistics' THEN 4
                                          WHEN 'finalizing analyze' THEN 5
                                        END
                    WHEN 'CLUSTER' THEN 1
                    WHEN 'CREATE INDEX' THEN CASE S.command
                                               WHEN 'CREATE INDEX' THEN 1
                                               WHEN 'REINDEX' THEN 3
                                             END
                    WHEN 'BASEBACKUP' THEN CASE S.phase
                                             WHEN 'initializing' THEN 0
                                             WHEN 'waiting for checkpoint to finish' THEN 1
                                             WHEN 'estimating backup size' THEN 2
                                             WHEN 'streaming database files' THEN 3
                                             WHEN 'waiting for wal archiving to finish' THEN 4
                                             WHEN 'transferring wal files' THEN 5
                                           END
                    WHEN 'COPY' THEN S.bytes_processed
                    ELSE 0
                  END AS BIGINT) AS param1,
             CAST(CASE upper(cmdtype)
                    WHEN 'VACUUM' THEN S.steps_total
                    WHEN 'CLUSTER' THEN CASE S.phase
                                          WHEN 'initializing' THEN 0
                                          WHEN 'seq scanning heap' THEN 1
                                          WHEN 'index scanning heap' THEN 2
                                          WHEN 'sorting tuples' THEN 3
                                          WHEN 'writing new heap' THEN 4
                                          WHEN 'swapping relation files' THEN 5
                                          WHEN 'rebuilding index' THEN 6
                                          WHEN 'performing final cleanup' THEN 7
                                        END
                    WHEN 'BASEBACKUP' THEN S.bytes_total
                    WHEN 'COPY' THEN S.bytes_total
                    ELSE 0
                  END AS BIGINT) AS param2,
             CAST(CASE upper(cmdtype)
                    WHEN 'VACUUM' THEN S.step
                    WHEN 'CLUSTER' THEN S.current_relid
                    WHEN 'BASEBACKUP' THEN S.bytes_processed
                    WHEN 'COPY' THEN S.tuples_processed
                    ELSE 0
                  END AS BIGINT) AS param3,
             CAST(CASE upper(cmdtype)
                    WHEN 'CLUSTER' THEN S.tuples_processed
                    WHEN 'BASEBACKUP' THEN S.items_total
                    ELSE 0
                  END AS BIGINT) AS param4,
             CAST(CASE upper(cmdtype)
                    WHEN 'BASEBACKUP' THEN S.items_processed
                    WHEN 'COPY' THEN CASE S.command
                                       WHEN 'COPY FROM' THEN 1
                                       WHEN 'COPY TO' THEN 2
                                     END
                    ELSE 0
                  END AS BIGINT) AS param5,
             CAST(CASE upper(cmdtype)
                    WHEN 'ANALYZE' THEN S.items_total
                    WHEN 'CLUSTER' THEN S.steps_total
                    WHEN 'COPY' THEN CASE S.io_type
                                       WHEN 'FILE' THEN 1
                                       WHEN 'PROGRAM' THEN 2
                                       WHEN 'PIPE' THEN 3
                                       WHEN 'CALLBACK' THEN 4
                                     END
                    ELSE 0
                  END AS BIGINT) AS param6,
             CAST(CASE upper(cmdtype)
                    WHEN 'ANALYZE' THEN S.items_processed
                    WHEN 'CLUSTER' THEN S.step
                    WHEN 'CREATE INDEX' THEN S.current_relid
                    ELSE 0
                  END AS BIGINT) AS param7,
             CAST(CASE upper(cmdtype)
                    WHEN 'ANALYZE' THEN S.current_relid
                    ELSE 0
                  END AS BIGINT) AS param8,
             CAST(CASE upper(cmdtype)
                    WHEN 'VACUUM' THEN S.items_total
                    ELSE 0
                  END AS BIGINT) AS param9,
             CAST(CASE upper(cmdtype)
                    WHEN 'VACUUM' THEN S.items_processed
                    WHEN 'CREATE INDEX' THEN CASE S.phase
                                               WHEN 'initializing' THEN 0
                                               WHEN 'building index' THEN 2
                                             END
                    ELSE 0
                  END AS BIGINT) AS param10,
             0::BIGINT AS param11,
             CAST(CASE upper(cmdtype)
                    WHEN 'CREATE INDEX' THEN S.tuples_total
                    ELSE 0
                  END AS BIGINT) AS param12,
             CAST(CASE upper(cmdtype)
                    WHEN 'CREATE INDEX' THEN S.tuples_processed
                    ELSE 0
                  END AS BIGINT) AS param13,
             0::BIGINT AS param14,
             0::BIGINT AS param15,
             0::BIGINT AS param16,
             0::BIGINT AS param17,
             0::BIGINT AS param18,
             0::BIGINT AS param19,
             0::BIGINT AS param20
      FROM pg_catalog.sdb_progress AS S
      WHERE CASE upper(cmdtype)
              WHEN 'COPY' THEN S.command IN ('COPY FROM', 'COPY TO')
              WHEN 'CREATE INDEX' THEN S.command IN ('CREATE INDEX', 'REINDEX')
              ELSE S.command = upper(cmdtype)
            END)"},

  {"pg_catalog", "pg_stat_get_recovery_prefetch",
   R"(() AS TABLE
      SELECT NULL::TIMESTAMPTZ AS stats_reset,
             0::BIGINT AS prefetch,
             0::BIGINT AS hit,
             0::BIGINT AS skip_init,
             0::BIGINT AS skip_new,
             0::BIGINT AS skip_fpw,
             0::BIGINT AS skip_rep,
             0::INTEGER AS wal_distance,
             0::INTEGER AS block_distance,
             0::INTEGER AS io_depth)"},

  {"pg_catalog", "pg_stat_get_archiver",
   R"(() AS TABLE
      SELECT 0::BIGINT AS archived_count,
             NULL::TEXT AS last_archived_wal,
             NULL::TIMESTAMPTZ AS last_archived_time,
             0::BIGINT AS failed_count,
             NULL::TEXT AS last_failed_wal,
             NULL::TIMESTAMPTZ AS last_failed_time,
             NULL::TIMESTAMPTZ AS stats_reset)"},

  {"pg_catalog", "pg_stat_get_wal",
   R"(() AS TABLE
      SELECT 0::BIGINT AS wal_records,
             0::BIGINT AS wal_fpi,
             0::BIGINT AS wal_bytes,
             0::BIGINT AS wal_buffers_full,
             NULL::TIMESTAMPTZ AS stats_reset)"},

  // PG regexp functions wrapping DuckDB builtins

  {"pg_catalog","regexp_count",
   R"((text, pattern) AS len(regexp_extract_all(text, pattern)),
      (text, pattern, start) AS len(regexp_extract_all(text[start:], pattern)))"},

  {"pg_catalog","regexp_substr",
   R"((text, pattern) AS CASE WHEN regexp_matches(text, pattern) THEN regexp_extract(text, pattern) END,
      (text, pattern, start) AS CASE WHEN regexp_matches(text[start:], pattern) THEN regexp_extract(text[start:], pattern) END)"},

  // PG array functions missing from DuckDB

  {"pg_catalog","array_remove",
   R"((arr, elem) AS list_filter(arr, x -> x IS DISTINCT FROM elem))"},

  {"pg_catalog","trim_array",
   R"((arr, n) AS arr[:len(arr) - n])"},

  {"pg_catalog","array_positions",
   R"((arr, elem) AS list_filter(list_transform(arr, (x, i) -> CASE WHEN x IS NOT DISTINCT FROM elem THEN i ELSE NULL END), x -> x IS NOT NULL))"},

  {"pg_catalog","array_replace",
   R"((arr, old_elem, new_elem) AS list_transform(arr, x -> CASE WHEN x IS NOT DISTINCT FROM old_elem THEN new_elem ELSE x END))"},

  {"pg_catalog","array_lower",
   R"((arr, dim) AS CASE WHEN arr IS NULL OR len(arr) = 0 THEN NULL ELSE 1 END)"},

  {"pg_catalog","array_upper",
   R"((arr, dim) AS CASE WHEN arr IS NULL OR len(arr) = 0 THEN NULL ELSE len(arr) END)"},

  // regexp_like: alias registered in duckdb functions.json -> regexp_matches

  // overlay(string placing string from int for int) -> string
  // Parser transforms: overlay(s PLACING r FROM p FOR n) -> overlay(s, r, p, n)
  {"pg_catalog","overlay",
   R"((s, repl, start, count) AS substr(s, 1, start - 1) || repl || substr(s, start + count))"},
  // 3-arg form: overlay(string placing string from int) -- count defaults to length of replacement
  {"pg_catalog","overlay",
   R"((s, repl, start) AS substr(s, 1, start - 1) || repl || substr(s, start + length(repl)))"},

  // PG datetime functions missing from DuckDB
  // timeofday: wall clock as formatted text
  {"pg_catalog","timeofday",
   R"(() AS strftime(clock_timestamp()::timestamp, '%a %b %d %H:%M:%S %Y UTC'))"},

  // PG math functions missing from DuckDB

  // div: registered as C++ function in connector/functions/math.cpp

  // Degree-based trigonometric functions
  {"pg_catalog","sind",
   R"((x) AS sin(radians(x)))"},

  {"pg_catalog","cosd",
   R"((x) AS cos(radians(x)))"},

  {"pg_catalog","tand",
   R"((x) AS tan(radians(x)))"},

  // cotd is registered as a scalar function in RegisterPgMathFunctions
  // with PG-compatible division-by-zero error handling.

  {"pg_catalog","asind",
   R"((x) AS degrees(asin(x)))"},

  {"pg_catalog","acosd",
   R"((x) AS degrees(acos(x)))"},

  {"pg_catalog","atand",
   R"((x) AS degrees(atan(x)))"},

  {"pg_catalog","atan2d",
   R"((y, x) AS degrees(atan2(y, x)))"},

  // adbin / conbin already hold the deparsed expression rather than a node
  // tree, so deparsing it is handing it back.
  {"pg_catalog", "pg_get_expr", "(node_text, rel_oid) AS CAST(node_text AS TEXT)"},
  {"pg_catalog", "pg_get_expr",
   "(node_text, rel_oid, pretty_bool) AS CAST(node_text AS TEXT)"},
  {"pg_catalog", "pg_tablespace_location", "(oid) AS CAST('' AS TEXT)"},

  {"pg_catalog", "oidvectortypes",
   R"((v) AS array_to_string(
        list_transform(v, x -> pg_catalog.format_type(x, NULL)), ', '))"},

  {"pg_catalog", "pg_indexam_has_property",
   R"((am, property) AS
      CASE (SELECT amname FROM pg_catalog.pg_am WHERE oid = am AND amtype = 'i')
        WHEN 'secondary' THEN CASE lower(property)
          WHEN 'can_order' THEN false WHEN 'can_unique' THEN true
          WHEN 'can_multi_col' THEN true WHEN 'can_exclude' THEN false
          WHEN 'can_include' THEN false END
        WHEN 'inverted' THEN CASE lower(property)
          WHEN 'can_order' THEN false WHEN 'can_unique' THEN false
          WHEN 'can_multi_col' THEN true WHEN 'can_exclude' THEN false
          WHEN 'can_include' THEN true END
      END)"},

  {"pg_catalog", "pg_index_has_property",
   R"((index, property) AS
      CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_index i
                        WHERE i.indexrelid = index)
        THEN CASE lower(property)
          WHEN 'clusterable' THEN false WHEN 'index_scan' THEN true
          WHEN 'bitmap_scan' THEN false WHEN 'backward_scan' THEN false END
      END)"},

  {"pg_catalog", "pg_index_column_has_property",
   R"((index, attno, property) AS
      CASE WHEN attno >= 1 AND attno <= (SELECT i.indnatts FROM pg_catalog.pg_index i
                                         WHERE i.indexrelid = index)
        THEN CASE lower(property)
          WHEN 'orderable' THEN false WHEN 'distance_orderable' THEN false
          WHEN 'returnable' THEN false WHEN 'search_array' THEN false
          WHEN 'search_nulls' THEN false END
      END)"},

  // Recovery status: SereneDB is always a primary (no WAL-replay standby
  // mode), so both are constant false. pgAdmin calls these on every connect.
  {"pg_catalog", "pg_is_in_recovery", "() AS false"},
  {"pg_catalog", "pg_is_wal_replay_paused", "() AS false"},
  {"pg_catalog", "shobj_description", "(oid, catalog) AS CAST(NULL AS TEXT)"},

  {"pg_catalog", "pg_relation_is_publishable", "(a) AS false"},
  {"pg_catalog", "pg_partition_ancestors", "(relid) AS CAST(relid AS regclass)"},
  {"pg_catalog", "pg_partition_ancestors",
   "(relid) AS TABLE SELECT CAST(relid AS regclass) AS relid"},
  {"pg_catalog", "pg_indexam_progress_phasename", "(a, b) AS CAST(NULL AS TEXT)"},
  {"pg_catalog", "pg_statistics_obj_is_visible", "(a) AS true"},

  {"pg_catalog", "pg_options_to_table",
   R"((opts) AS TABLE
      SELECT CAST(CASE WHEN strpos(o, '=') = 0 THEN o
                       ELSE substr(o, 1, strpos(o, '=') - 1) END AS TEXT) AS option_name,
             CAST(CASE WHEN strpos(o, '=') = 0 THEN NULL
                       ELSE substr(o, strpos(o, '=') + 1) END AS TEXT) AS option_value
      FROM unnest(opts) AS t(o))"},

  // --- Real, PG-faithful privilege / row-security oracles. These return PG's
  // actual answer for every reachable input (not permissive placeholders). ---

  // SereneDB has no RLS (no CREATE POLICY, relrowsecurity always false), so
  // row security is never active -- PG also returns false for every reachable
  // input (no-policy table, system oid, non-owner). Matches PG.
  {"pg_catalog", "row_security_active", "(a) AS false"},

  // has_language_privilege: a language grants USAGE to PUBLIC by default
  // (acldefault world_default = USAGE), so every role holds USAGE on SereneDB's
  // built-in languages -- PG returns true too. (A nonexistent language oid would
  // be NULL in PG, but SereneDB has no CREATE LANGUAGE so that is unreachable.)
  {"pg_catalog", "has_language_privilege", "(a, b) AS true, (a, b, c) AS true"},

  // Real: name||'_'||oid, matching PG's nameconcatoid.
  {"pg_catalog", "nameconcatoid", "(a, b) AS CAST(a || '_' || CAST(b AS TEXT) AS TEXT)"},

  {"pg_catalog", "getdatabaseencoding", "() AS 'UTF8'"},
  // Always UTF-8 -> max 4 bytes per character. Argument ignored.
  {"pg_catalog", "pg_encoding_max_length", "(encoding int4) AS 4"},
  // No temp schemas supported yet.
  {"pg_catalog", "pg_my_temp_schema", "() AS 0::oid"},
  {"pg_catalog", "make_interval",
   R"((years INTEGER := 0, months INTEGER := 0, weeks INTEGER := 0,
       days INTEGER := 0, hours INTEGER := 0, mins INTEGER := 0,
       secs DOUBLE := 0.0) AS
      to_months(years::BIGINT * 12 + months) +
      to_days(weeks::BIGINT * 7 + days) +
      to_microseconds(hours::BIGINT * 3600000000 + mins::BIGINT * 60000000 +
                      round(secs * 1000000)::BIGINT))"},
  // pg_backend_pid() is a C++ scalar (PgBackendPidFunction) -- it reads the
  // per-connection backend PID from the ConnectionContext.
  // clang-format on
};

}  // namespace

const std::span<const SystemMacro> kPgCatalogMacros = kMacros;

}  // namespace sdb::pg
