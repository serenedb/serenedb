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
                 datid BIGINT,
                 relid BIGINT,
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

  // Expand any 1-D array into a set with integers 1..N

  // TODO(mbkkt) Enable when implement proper SETOF functions support.
  // R"(CREATE FUNCTION _pg_expandarray(IN anyarray, OUT x anyelement, OUT n int)
  //     RETURNS SETOF RECORD
  //     LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE
  //     ROWS 100 SUPPORT pg_catalog.array_unnest_support
  //     AS 'SELECT * FROM pg_catalog.unnest($1) WITH ORDINALITY';)",

  // Given an index's OID and an underlying-table column number, return the
  // column's position in the index (NULL if not there)

  // TODO(mbkkt) Use original text when implement LATERAL references.
  R"(CREATE FUNCTION _pg_index_position(oid, smallint) RETURNS int
      LANGUAGE sql STRICT STABLE
    RETURN NULLIF(
      (SELECT array_position(indkey, $2) FROM pg_catalog.pg_index WHERE indexrelid = $1),
      0)::int;)",

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
});

}  // namespace sdb::pg
