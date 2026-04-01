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
// TODO revoke, grant, create rules and other stuff?
inline constexpr auto kSystemViewsQueries = std::to_array<std::string_view>({
  // clang-format off
  // PostgreSQL System Views
  //
  // Copyright (c) 1996-2025, PostgreSQL Global Development Group
  //
  // src/backend/catalog/system_views.sql
  //
  // Note: this file is read in single-user -j mode, which means that the
  // command terminator is semicolon-newline-newline; whenever the backend
  // sees that, it stops and executes what it's got.  If you write a lot of
  // statements without empty lines between, they'll all get quoted to you
  // in any error message about one of them, so don't do that.  Also, you
  // cannot write a semicolon immediately followed by an empty line in a
  // string literal (including a function body!) or a multiline comment.

  R"(CREATE VIEW pg_roles AS
      SELECT
          rolname,
          rolsuper,
          rolinherit,
          rolcreaterole,
          rolcreatedb,
          rolcanlogin,
          rolreplication,
          rolconnlimit,
          '********'::text as rolpassword,
          rolvaliduntil,
          rolbypassrls,
          setconfig as rolconfig,
          pg_authid.oid
      FROM pg_authid LEFT JOIN pg_db_role_setting s
      ON (pg_authid.oid = setrole AND setdatabase = 0);)",

  R"(CREATE VIEW pg_shadow AS
      SELECT
          rolname AS usename,
          pg_authid.oid AS usesysid,
          rolcreatedb AS usecreatedb,
          rolsuper AS usesuper,
          rolreplication AS userepl,
          rolbypassrls AS usebypassrls,
          rolpassword AS passwd,
          rolvaliduntil AS valuntil,
          setconfig AS useconfig
      FROM pg_authid LEFT JOIN pg_db_role_setting s
      ON (pg_authid.oid = setrole AND setdatabase = 0)
      WHERE rolcanlogin;)",

  // R"(REVOKE ALL ON pg_shadow FROM public;)",

  R"(CREATE VIEW pg_group AS
      SELECT
          rolname AS groname,
          oid AS grosysid,
          ARRAY(SELECT member FROM pg_auth_members WHERE roleid = pg_authid.oid) AS grolist
      FROM pg_authid
      WHERE NOT rolcanlogin;)",

  R"(CREATE VIEW pg_user AS
      SELECT
          usename,
          usesysid,
          usecreatedb,
          usesuper,
          userepl,
          usebypassrls,
          '********'::text as passwd,
          valuntil,
          useconfig
      FROM pg_shadow;)",

  // TODO(mbkkt) Use original text when implement implicit cast from unknown to bigint[]
  R"(CREATE VIEW pg_policies AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS tablename,
          pol.polname AS policyname,
          CASE
              WHEN pol.polpermissive THEN
                  'PERMISSIVE'
              ELSE
                  'RESTRICTIVE'
          END AS permissive,
          CASE
              WHEN pol.polroles = '{0}'::oid[] THEN
                  string_to_array('public', '')
              ELSE
                  ARRAY
                  (
                      SELECT rolname::varchar
                      FROM pg_catalog.pg_authid
                      WHERE oid = ANY (pol.polroles) ORDER BY 1
                  )
          END AS roles,
          CASE pol.polcmd
              WHEN 'r' THEN 'SELECT'
              WHEN 'a' THEN 'INSERT'
              WHEN 'w' THEN 'UPDATE'
              WHEN 'd' THEN 'DELETE'
              WHEN '*' THEN 'ALL'
          END AS cmd,
          pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS qual,
          pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check
      FROM pg_catalog.pg_policy pol
      JOIN pg_catalog.pg_class C ON (C.oid = pol.polrelid)
      LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace);)",

  R"(CREATE VIEW pg_rules AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS tablename,
          R.rulename AS rulename,
          pg_get_ruledef(R.oid) AS definition
      FROM (pg_rewrite R JOIN pg_class C ON (C.oid = R.ev_class))
          LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE R.rulename != '_RETURN';)",

  R"(CREATE VIEW pg_views AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS viewname,
          pg_get_userbyid(C.relowner) AS viewowner,
          pg_get_viewdef(C.oid) AS definition
      FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind = 'v';)",

  R"(CREATE VIEW pg_tables AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS tablename,
          pg_get_userbyid(C.relowner) AS tableowner,
          T.spcname AS tablespace,
          C.relhasindex AS hasindexes,
          C.relhasrules AS hasrules,
          C.relhastriggers AS hastriggers,
          C.relrowsecurity AS rowsecurity
      FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
           LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
      WHERE C.relkind IN ('r', 'p');)",

  R"(CREATE VIEW pg_matviews AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS matviewname,
          pg_get_userbyid(C.relowner) AS matviewowner,
          T.spcname AS tablespace,
          C.relhasindex AS hasindexes,
          C.relispopulated AS ispopulated,
          pg_get_viewdef(C.oid) AS definition
      FROM pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
           LEFT JOIN pg_tablespace T ON (T.oid = C.reltablespace)
      WHERE C.relkind = 'm';)",

  R"(CREATE VIEW pg_indexes AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS tablename,
          I.relname AS indexname,
          T.spcname AS tablespace,
          pg_get_indexdef(I.oid) AS indexdef
      FROM pg_index X JOIN pg_class C ON (C.oid = X.indrelid)
           JOIN pg_class I ON (I.oid = X.indexrelid)
           LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
           LEFT JOIN pg_tablespace T ON (T.oid = I.reltablespace)
      WHERE C.relkind IN ('r', 'm', 'p') AND I.relkind IN ('i', 'I');)",

  R"(CREATE VIEW pg_sequences AS
      SELECT
          N.nspname AS schemaname,
          C.relname AS sequencename,
          pg_get_userbyid(C.relowner) AS sequenceowner,
          S.seqtypid::regtype AS data_type,
          S.seqstart AS start_value,
          S.seqmin AS min_value,
          S.seqmax AS max_value,
          S.seqincrement AS increment_by,
          S.seqcycle AS cycle,
          S.seqcache AS cache_size,
          pg_sequence_last_value(C.oid) AS last_value
      FROM pg_sequence S JOIN pg_class C ON (C.oid = S.seqrelid)
           LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE NOT pg_is_other_temp_schema(N.oid)
            AND relkind = 'S';)",

  R"(CREATE VIEW pg_stats WITH (security_barrier) AS
      SELECT
          nspname AS schemaname,
          relname AS tablename,
          attname AS attname,
          stainherit AS inherited,
          stanullfrac AS null_frac,
          stawidth AS avg_width,
          stadistinct AS n_distinct,
          CASE
              WHEN stakind1 = 1 THEN stavalues1
              WHEN stakind2 = 1 THEN stavalues2
              WHEN stakind3 = 1 THEN stavalues3
              WHEN stakind4 = 1 THEN stavalues4
              WHEN stakind5 = 1 THEN stavalues5
          END AS most_common_vals,
          CASE
              WHEN stakind1 = 1 THEN stanumbers1
              WHEN stakind2 = 1 THEN stanumbers2
              WHEN stakind3 = 1 THEN stanumbers3
              WHEN stakind4 = 1 THEN stanumbers4
              WHEN stakind5 = 1 THEN stanumbers5
          END AS most_common_freqs,
          CASE
              WHEN stakind1 = 2 THEN stavalues1
              WHEN stakind2 = 2 THEN stavalues2
              WHEN stakind3 = 2 THEN stavalues3
              WHEN stakind4 = 2 THEN stavalues4
              WHEN stakind5 = 2 THEN stavalues5
          END AS histogram_bounds,
          CASE
              WHEN stakind1 = 3 THEN stanumbers1[1]
              WHEN stakind2 = 3 THEN stanumbers2[1]
              WHEN stakind3 = 3 THEN stanumbers3[1]
              WHEN stakind4 = 3 THEN stanumbers4[1]
              WHEN stakind5 = 3 THEN stanumbers5[1]
          END AS correlation,
          CASE
              WHEN stakind1 = 4 THEN stavalues1
              WHEN stakind2 = 4 THEN stavalues2
              WHEN stakind3 = 4 THEN stavalues3
              WHEN stakind4 = 4 THEN stavalues4
              WHEN stakind5 = 4 THEN stavalues5
          END AS most_common_elems,
          CASE
              WHEN stakind1 = 4 THEN stanumbers1
              WHEN stakind2 = 4 THEN stanumbers2
              WHEN stakind3 = 4 THEN stanumbers3
              WHEN stakind4 = 4 THEN stanumbers4
              WHEN stakind5 = 4 THEN stanumbers5
          END AS most_common_elem_freqs,
          CASE
              WHEN stakind1 = 5 THEN stanumbers1
              WHEN stakind2 = 5 THEN stanumbers2
              WHEN stakind3 = 5 THEN stanumbers3
              WHEN stakind4 = 5 THEN stanumbers4
              WHEN stakind5 = 5 THEN stanumbers5
          END AS elem_count_histogram,
          CASE
              WHEN stakind1 = 6 THEN stavalues1
              WHEN stakind2 = 6 THEN stavalues2
              WHEN stakind3 = 6 THEN stavalues3
              WHEN stakind4 = 6 THEN stavalues4
              WHEN stakind5 = 6 THEN stavalues5
          END AS range_length_histogram,
          CASE
              WHEN stakind1 = 6 THEN stanumbers1[1]
              WHEN stakind2 = 6 THEN stanumbers2[1]
              WHEN stakind3 = 6 THEN stanumbers3[1]
              WHEN stakind4 = 6 THEN stanumbers4[1]
              WHEN stakind5 = 6 THEN stanumbers5[1]
          END AS range_empty_frac,
          CASE
              WHEN stakind1 = 7 THEN stavalues1
              WHEN stakind2 = 7 THEN stavalues2
              WHEN stakind3 = 7 THEN stavalues3
              WHEN stakind4 = 7 THEN stavalues4
              WHEN stakind5 = 7 THEN stavalues5
              END AS range_bounds_histogram
      FROM pg_statistic s JOIN pg_class c ON (c.oid = s.starelid)
           JOIN pg_attribute a ON (c.oid = attrelid AND attnum = s.staattnum)
           LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
      WHERE NOT attisdropped
      AND has_column_privilege(c.oid, a.attnum, 'select')
      AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));)",

  // R"(REVOKE ALL ON pg_statistic FROM public;)",

  // TODO(mbkkt) Use original text when implement LATERAL JOIN
  // TODO(mbkkt) attnames: needs correlated unnest(s.stxkeys) subquery
  // TODO(mbkkt) most_common_*: needs LATERAL pg_mcv_list_items(sd.stxdmcv)
  R"(CREATE VIEW pg_stats_ext WITH (security_barrier) AS
      SELECT cn.nspname AS schemaname,
             c.relname AS tablename,
             sn.nspname AS statistics_schemaname,
             s.stxname AS statistics_name,
             pg_get_userbyid(s.stxowner) AS statistics_owner,
             NULL::text[] AS attnames,
             pg_get_statisticsobjdef_expressions(s.oid) as exprs,
             s.stxkind AS kinds,
             sd.stxdinherit AS inherited,
             sd.stxdndistinct AS n_distinct,
             sd.stxddependencies AS dependencies,
             NULL::text[] AS most_common_vals,
             NULL::boolean[] AS most_common_val_nulls,
             NULL::double precision[] AS most_common_freqs,
             NULL::double precision[] AS most_common_base_freqs
      FROM pg_statistic_ext s JOIN pg_class c ON (c.oid = s.stxrelid)
           JOIN pg_statistic_ext_data sd ON (s.oid = sd.stxoid)
           LEFT JOIN pg_namespace cn ON (cn.oid = c.relnamespace)
           LEFT JOIN pg_namespace sn ON (sn.oid = s.stxnamespace)
      WHERE pg_has_role(c.relowner, 'USAGE')
      AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));)",

  // TODO(mbkkt) Use original text when implement LATERAL JOIN + row type field access
  // All stat columns come from LATERAL unnest + ::pg_statistic cast, which
  // requires both LATERAL and composite type field access. The INNER JOIN
  // on the LATERAL means no rows are produced without it, so this view
  // correctly returns no rows until these features are implemented.
  R"(CREATE VIEW pg_stats_ext_exprs WITH (security_barrier) AS
      SELECT cn.nspname AS schemaname,
             c.relname AS tablename,
             sn.nspname AS statistics_schemaname,
             s.stxname AS statistics_name,
             pg_get_userbyid(s.stxowner) AS statistics_owner,
             NULL::text AS expr,
             sd.stxdinherit AS inherited,
             NULL::real AS null_frac,
             NULL::int4 AS avg_width,
             NULL::real AS n_distinct,
             NULL::text AS most_common_vals,
             NULL::real[] AS most_common_freqs,
             NULL::text AS histogram_bounds,
             NULL::real AS correlation,
             NULL::text AS most_common_elems,
             NULL::real[] AS most_common_elem_freqs,
             NULL::real[] AS elem_count_histogram
      FROM pg_statistic_ext s JOIN pg_class c ON (c.oid = s.stxrelid)
           LEFT JOIN pg_statistic_ext_data sd ON (s.oid = sd.stxoid)
           LEFT JOIN pg_namespace cn ON (cn.oid = c.relnamespace)
           LEFT JOIN pg_namespace sn ON (sn.oid = s.stxnamespace)
      WHERE false;)",

  // unprivileged users may read pg_statistic_ext but not pg_statistic_ext_data
  // R"(REVOKE ALL ON pg_statistic_ext_data FROM public;)",

  // TODO(mbkkt) Use original text when implement LATERAL JOIN
  // TODO(mbkkt) attnames depends on decorrelate any non-eq subquery
  R"(CREATE VIEW pg_publication_tables AS
      SELECT
          P.pubname AS pubname,
          N.nspname AS schemaname,
          C.relname AS tablename,
          NULL::text[] AS attnames,
          pg_get_expr(PR.prqual, PR.prrelid) AS rowfilter
      FROM pg_publication P
           JOIN pg_publication_rel PR ON (PR.prpubid = P.oid)
           JOIN pg_class C ON (C.oid = PR.prrelid)
           JOIN pg_namespace N ON (N.oid = C.relnamespace);)",

  R"(CREATE VIEW pg_locks AS
      SELECT * FROM pg_lock_status() AS L;)",

  R"(CREATE VIEW pg_cursors AS
      SELECT * FROM pg_cursor() AS C;)",

  R"(CREATE VIEW pg_available_extensions AS
      SELECT E.name, E.default_version, X.extversion AS installed_version,
             E.comment
        FROM pg_available_extensions() AS E
             LEFT JOIN pg_extension AS X ON E.name = X.extname;)",

  R"(CREATE VIEW pg_available_extension_versions AS
      SELECT E.name, E.version, (X.extname IS NOT NULL) AS installed,
             E.superuser, E.trusted, E.relocatable,
             E.schema, E.requires, E.comment
        FROM pg_available_extension_versions() AS E
             LEFT JOIN pg_extension AS X
               ON E.name = X.extname AND E.version = X.extversion;)",

  R"(CREATE VIEW pg_prepared_xacts AS
      SELECT P.transaction, P.gid, P.prepared,
             U.rolname AS owner, D.datname AS database
      FROM pg_prepared_xact() AS P
           LEFT JOIN pg_authid U ON P.ownerid = U.oid
           LEFT JOIN pg_database D ON P.dbid = D.oid;)",

  R"(CREATE VIEW pg_prepared_statements AS
      SELECT * FROM pg_prepared_statement() AS P;)",

  // TODO(mbkkt) Restore tableoid join conditions when tableoid system column is supported
  R"(CREATE VIEW pg_seclabels AS
  SELECT
      l.objoid, l.classoid, l.objsubid,
      CASE WHEN rel.relkind IN ('r', 'p') THEN 'table'::text
           WHEN rel.relkind = 'v' THEN 'view'::text
           WHEN rel.relkind = 'm' THEN 'materialized view'::text
           WHEN rel.relkind = 'S' THEN 'sequence'::text
           WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
      rel.relnamespace AS objnamespace,
      CASE WHEN pg_table_is_visible(rel.oid)
           THEN quote_ident(rel.relname)
           ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
           END AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_class rel ON l.objoid = rel.oid
      JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      'column'::text AS objtype,
      rel.relnamespace AS objnamespace,
      CASE WHEN pg_table_is_visible(rel.oid)
           THEN quote_ident(rel.relname)
           ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
           END || '.' || att.attname AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_class rel ON l.objoid = rel.oid
      JOIN pg_attribute att
           ON rel.oid = att.attrelid AND l.objsubid = att.attnum
      JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
  WHERE
      l.objsubid != 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      CASE pro.prokind
              WHEN 'a' THEN 'aggregate'::text
              WHEN 'f' THEN 'function'::text
              WHEN 'p' THEN 'procedure'::text
              WHEN 'w' THEN 'window'::text END AS objtype,
      pro.pronamespace AS objnamespace,
      CASE WHEN pg_function_is_visible(pro.oid)
           THEN quote_ident(pro.proname)
           ELSE quote_ident(nsp.nspname) || '.' || quote_ident(pro.proname)
      END || '(' || pg_catalog.pg_get_function_arguments(pro.oid) || ')' AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_proc pro ON l.objoid = pro.oid
      JOIN pg_namespace nsp ON pro.pronamespace = nsp.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      CASE WHEN typ.typtype = 'd' THEN 'domain'::text
      ELSE 'type'::text END AS objtype,
      typ.typnamespace AS objnamespace,
      CASE WHEN pg_type_is_visible(typ.oid)
      THEN quote_ident(typ.typname)
      ELSE quote_ident(nsp.nspname) || '.' || quote_ident(typ.typname)
      END AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_type typ ON l.objoid = typ.oid
      JOIN pg_namespace nsp ON typ.typnamespace = nsp.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      'large object'::text AS objtype,
      NULL::oid AS objnamespace,
      l.objoid::text AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_largeobject_metadata lom ON l.objoid = lom.oid
  WHERE
      l.classoid = 'pg_catalog.pg_largeobject'::regclass AND l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      'language'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(lan.lanname) AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_language lan ON l.objoid = lan.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      'schema'::text AS objtype,
      nsp.oid AS objnamespace,
      quote_ident(nsp.nspname) AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_namespace nsp ON l.objoid = nsp.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      'event trigger'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(evt.evtname) AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_event_trigger evt ON l.objoid = evt.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, l.objsubid,
      'publication'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(p.pubname) AS objname,
      l.provider, l.label
  FROM
      pg_seclabel l
      JOIN pg_publication p ON l.objoid = p.oid
  WHERE
      l.objsubid = 0
  UNION ALL
  SELECT
      l.objoid, l.classoid, 0::int4 AS objsubid,
      'subscription'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(s.subname) AS objname,
      l.provider, l.label
  FROM
      pg_shseclabel l
      JOIN pg_subscription s ON l.objoid = s.oid
  UNION ALL
  SELECT
      l.objoid, l.classoid, 0::int4 AS objsubid,
      'database'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(dat.datname) AS objname,
      l.provider, l.label
  FROM
      pg_shseclabel l
      JOIN pg_database dat ON l.objoid = dat.oid
  UNION ALL
  SELECT
      l.objoid, l.classoid, 0::int4 AS objsubid,
      'tablespace'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(spc.spcname) AS objname,
      l.provider, l.label
  FROM
      pg_shseclabel l
      JOIN pg_tablespace spc ON l.objoid = spc.oid
  UNION ALL
  SELECT
      l.objoid, l.classoid, 0::int4 AS objsubid,
      'role'::text AS objtype,
      NULL::oid AS objnamespace,
      quote_ident(rol.rolname) AS objname,
      l.provider, l.label
  FROM
      pg_shseclabel l
      JOIN pg_authid rol ON l.objoid = rol.oid;)",

  R"(CREATE VIEW pg_settings AS
      SELECT * FROM pg_show_all_settings() AS A;)",

  // R"(CREATE RULE pg_settings_u AS
  //     ON UPDATE TO pg_settings
  //     WHERE new.name = old.name DO
  //     SELECT set_config(old.name, new.setting, 'f');)",

  // R"(CREATE RULE pg_settings_n AS
  //     ON UPDATE TO pg_settings
  //     DO INSTEAD NOTHING;)",

  // R"(GRANT SELECT, UPDATE ON pg_settings TO PUBLIC;)",

  R"(CREATE VIEW pg_file_settings AS
     SELECT * FROM pg_show_all_file_settings() AS A;)",

  // R"(REVOKE ALL ON pg_file_settings FROM PUBLIC;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_show_all_file_settings() FROM PUBLIC;)",

  R"(CREATE VIEW pg_hba_file_rules AS
     SELECT * FROM pg_hba_file_rules() AS A;)",

  // R"(REVOKE ALL ON pg_hba_file_rules FROM PUBLIC;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_hba_file_rules() FROM PUBLIC;)",

  R"(CREATE VIEW pg_ident_file_mappings AS
     SELECT * FROM pg_ident_file_mappings() AS A;)",

  // R"(REVOKE ALL ON pg_ident_file_mappings FROM PUBLIC;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_ident_file_mappings() FROM PUBLIC;)",

  R"(CREATE VIEW pg_timezone_abbrevs AS
      SELECT * FROM pg_timezone_abbrevs_zone() z
      UNION ALL
      (SELECT * FROM pg_timezone_abbrevs_abbrevs() a
       WHERE NOT EXISTS (SELECT 1 FROM pg_timezone_abbrevs_zone() z2
                         WHERE z2.abbrev = a.abbrev))
      ORDER BY abbrev;)",

  R"(CREATE VIEW pg_timezone_names AS
      SELECT * FROM pg_timezone_names();)",

  R"(CREATE VIEW pg_config AS
      SELECT * FROM pg_config();)",

  // R"(REVOKE ALL ON pg_config FROM PUBLIC;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_config() FROM PUBLIC;)",

  R"(CREATE VIEW pg_shmem_allocations AS
      SELECT * FROM pg_get_shmem_allocations();)",

  // R"(REVOKE ALL ON pg_shmem_allocations FROM PUBLIC;)",
  // R"(GRANT SELECT ON pg_shmem_allocations TO pg_read_all_stats;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_get_shmem_allocations() FROM PUBLIC;)",
  // R"(GRANT EXECUTE ON FUNCTION pg_get_shmem_allocations() TO pg_read_all_stats;)",

  R"(CREATE VIEW pg_shmem_allocations_numa AS
      SELECT * FROM pg_get_shmem_allocations_numa();)",

  // R"(REVOKE ALL ON pg_shmem_allocations_numa FROM PUBLIC;)",
  // R"(GRANT SELECT ON pg_shmem_allocations_numa TO pg_read_all_stats;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_get_shmem_allocations_numa() FROM PUBLIC;)",
  // R"(GRANT EXECUTE ON FUNCTION pg_get_shmem_allocations_numa() TO pg_read_all_stats;)",

  R"(CREATE VIEW pg_backend_memory_contexts AS
      SELECT * FROM pg_get_backend_memory_contexts();)",

  // R"(REVOKE ALL ON pg_backend_memory_contexts FROM PUBLIC;)",
  // R"(GRANT SELECT ON pg_backend_memory_contexts TO pg_read_all_stats;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_get_backend_memory_contexts() FROM PUBLIC;)",
  // R"(GRANT EXECUTE ON FUNCTION pg_get_backend_memory_contexts() TO pg_read_all_stats;)",

  // Statistics views

  R"(CREATE VIEW pg_stat_all_tables AS
      SELECT
              C.oid AS relid,
              N.nspname AS schemaname,
              C.relname AS relname,
              pg_stat_get_numscans(C.oid) AS seq_scan,
              pg_stat_get_lastscan(C.oid) AS last_seq_scan,
              pg_stat_get_tuples_returned(C.oid) AS seq_tup_read,
              sum(pg_stat_get_numscans(I.indexrelid))::bigint AS idx_scan,
              max(pg_stat_get_lastscan(I.indexrelid)) AS last_idx_scan,
              sum(pg_stat_get_tuples_fetched(I.indexrelid))::bigint +
              pg_stat_get_tuples_fetched(C.oid) AS idx_tup_fetch,
              pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
              pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
              pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
              pg_stat_get_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
              pg_stat_get_tuples_newpage_updated(C.oid) AS n_tup_newpage_upd,
              pg_stat_get_live_tuples(C.oid) AS n_live_tup,
              pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
              pg_stat_get_mod_since_analyze(C.oid) AS n_mod_since_analyze,
              pg_stat_get_ins_since_vacuum(C.oid) AS n_ins_since_vacuum,
              pg_stat_get_last_vacuum_time(C.oid) as last_vacuum,
              pg_stat_get_last_autovacuum_time(C.oid) as last_autovacuum,
              pg_stat_get_last_analyze_time(C.oid) as last_analyze,
              pg_stat_get_last_autoanalyze_time(C.oid) as last_autoanalyze,
              pg_stat_get_vacuum_count(C.oid) AS vacuum_count,
              pg_stat_get_autovacuum_count(C.oid) AS autovacuum_count,
              pg_stat_get_analyze_count(C.oid) AS analyze_count,
              pg_stat_get_autoanalyze_count(C.oid) AS autoanalyze_count,
              pg_stat_get_total_vacuum_time(C.oid) AS total_vacuum_time,
              pg_stat_get_total_autovacuum_time(C.oid) AS total_autovacuum_time,
              pg_stat_get_total_analyze_time(C.oid) AS total_analyze_time,
              pg_stat_get_total_autoanalyze_time(C.oid) AS total_autoanalyze_time
      FROM pg_class C LEFT JOIN
           pg_index I ON C.oid = I.indrelid
           LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'm', 'p')
      GROUP BY C.oid, N.nspname, C.relname;)",

  R"(CREATE VIEW pg_stat_xact_all_tables AS
      SELECT
              C.oid AS relid,
              N.nspname AS schemaname,
              C.relname AS relname,
              pg_stat_get_xact_numscans(C.oid) AS seq_scan,
              pg_stat_get_xact_tuples_returned(C.oid) AS seq_tup_read,
              sum(pg_stat_get_xact_numscans(I.indexrelid))::bigint AS idx_scan,
              sum(pg_stat_get_xact_tuples_fetched(I.indexrelid))::bigint +
              pg_stat_get_xact_tuples_fetched(C.oid) AS idx_tup_fetch,
              pg_stat_get_xact_tuples_inserted(C.oid) AS n_tup_ins,
              pg_stat_get_xact_tuples_updated(C.oid) AS n_tup_upd,
              pg_stat_get_xact_tuples_deleted(C.oid) AS n_tup_del,
              pg_stat_get_xact_tuples_hot_updated(C.oid) AS n_tup_hot_upd,
              pg_stat_get_xact_tuples_newpage_updated(C.oid) AS n_tup_newpage_upd
      FROM pg_class C LEFT JOIN
           pg_index I ON C.oid = I.indrelid
           LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'm', 'p')
      GROUP BY C.oid, N.nspname, C.relname;)",

  R"(CREATE VIEW pg_stat_sys_tables AS
      SELECT * FROM pg_stat_all_tables
      WHERE schemaname IN ('pg_catalog', 'information_schema') OR
            schemaname ~ '^pg_toast';)",

  R"(CREATE VIEW pg_stat_xact_sys_tables AS
      SELECT * FROM pg_stat_xact_all_tables
      WHERE schemaname IN ('pg_catalog', 'information_schema') OR
            schemaname ~ '^pg_toast';)",

  R"(CREATE VIEW pg_stat_user_tables AS
      SELECT * FROM pg_stat_all_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
            schemaname !~ '^pg_toast';)",

  R"(CREATE VIEW pg_stat_xact_user_tables AS
      SELECT * FROM pg_stat_xact_all_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
            schemaname !~ '^pg_toast';)",

  // TODO(mbkkt) Use original text when implement LATERAL JOIN
  R"(CREATE VIEW pg_statio_all_tables AS
      SELECT
              C.oid AS relid,
              N.nspname AS schemaname,
              C.relname AS relname,
              pg_stat_get_blocks_fetched(C.oid) -
                      pg_stat_get_blocks_hit(C.oid) AS heap_blks_read,
              pg_stat_get_blocks_hit(C.oid) AS heap_blks_hit,
              sum(pg_stat_get_blocks_fetched(I.indexrelid) -
                  pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_read,
              sum(pg_stat_get_blocks_hit(I.indexrelid))::bigint AS idx_blks_hit,
              pg_stat_get_blocks_fetched(T.oid) -
                      pg_stat_get_blocks_hit(T.oid) AS toast_blks_read,
              pg_stat_get_blocks_hit(T.oid) AS toast_blks_hit,
              sum(pg_stat_get_blocks_fetched(TI.indexrelid) -
                  pg_stat_get_blocks_hit(TI.indexrelid))::bigint AS tidx_blks_read,
              sum(pg_stat_get_blocks_hit(TI.indexrelid))::bigint AS tidx_blks_hit
      FROM pg_class C
              LEFT JOIN pg_class T ON C.reltoastrelid = T.oid
              LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
              LEFT JOIN pg_index I ON (I.indrelid = C.oid)
              LEFT JOIN pg_index TI ON (TI.indrelid = T.oid)
      WHERE C.relkind IN ('r', 't', 'm')
      GROUP BY C.oid, N.nspname, C.relname, T.oid;)",

  R"(CREATE VIEW pg_statio_sys_tables AS
      SELECT * FROM pg_statio_all_tables
      WHERE schemaname IN ('pg_catalog', 'information_schema') OR
            schemaname ~ '^pg_toast';)",

  R"(CREATE VIEW pg_statio_user_tables AS
      SELECT * FROM pg_statio_all_tables
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
            schemaname !~ '^pg_toast';)",

  R"(CREATE VIEW pg_stat_all_indexes AS
      SELECT
              C.oid AS relid,
              I.oid AS indexrelid,
              N.nspname AS schemaname,
              C.relname AS relname,
              I.relname AS indexrelname,
              pg_stat_get_numscans(I.oid) AS idx_scan,
              pg_stat_get_lastscan(I.oid) AS last_idx_scan,
              pg_stat_get_tuples_returned(I.oid) AS idx_tup_read,
              pg_stat_get_tuples_fetched(I.oid) AS idx_tup_fetch
      FROM pg_class C JOIN
              pg_index X ON C.oid = X.indrelid JOIN
              pg_class I ON I.oid = X.indexrelid
              LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'm');)",

  R"(CREATE VIEW pg_stat_sys_indexes AS
      SELECT * FROM pg_stat_all_indexes
      WHERE schemaname IN ('pg_catalog', 'information_schema') OR
            schemaname ~ '^pg_toast';)",

  R"(CREATE VIEW pg_stat_user_indexes AS
      SELECT * FROM pg_stat_all_indexes
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
            schemaname !~ '^pg_toast';)",

  R"(CREATE VIEW pg_statio_all_indexes AS
      SELECT
              C.oid AS relid,
              I.oid AS indexrelid,
              N.nspname AS schemaname,
              C.relname AS relname,
              I.relname AS indexrelname,
              pg_stat_get_blocks_fetched(I.oid) -
                      pg_stat_get_blocks_hit(I.oid) AS idx_blks_read,
              pg_stat_get_blocks_hit(I.oid) AS idx_blks_hit
      FROM pg_class C JOIN
              pg_index X ON C.oid = X.indrelid JOIN
              pg_class I ON I.oid = X.indexrelid
              LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind IN ('r', 't', 'm');)",

  R"(CREATE VIEW pg_statio_sys_indexes AS
      SELECT * FROM pg_statio_all_indexes
      WHERE schemaname IN ('pg_catalog', 'information_schema') OR
            schemaname ~ '^pg_toast';)",

  R"(CREATE VIEW pg_statio_user_indexes AS
      SELECT * FROM pg_statio_all_indexes
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
            schemaname !~ '^pg_toast';)",

  R"(CREATE VIEW pg_statio_all_sequences AS
      SELECT
              C.oid AS relid,
              N.nspname AS schemaname,
              C.relname AS relname,
              pg_stat_get_blocks_fetched(C.oid) -
                      pg_stat_get_blocks_hit(C.oid) AS blks_read,
              pg_stat_get_blocks_hit(C.oid) AS blks_hit
      FROM pg_class C
              LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
      WHERE C.relkind = 'S';)",

  R"(CREATE VIEW pg_statio_sys_sequences AS
      SELECT * FROM pg_statio_all_sequences
      WHERE schemaname IN ('pg_catalog', 'information_schema') OR
            schemaname ~ '^pg_toast';)",

  R"(CREATE VIEW pg_statio_user_sequences AS
      SELECT * FROM pg_statio_all_sequences
      WHERE schemaname NOT IN ('pg_catalog', 'information_schema') AND
            schemaname !~ '^pg_toast';)",

  R"(CREATE VIEW pg_stat_activity AS
      SELECT
              S.datid AS datid,
              D.datname AS datname,
              S.pid,
              S.leader_pid,
              S.usesysid,
              U.rolname AS usename,
              S.application_name,
              S.client_addr,
              S.client_hostname,
              S.client_port,
              S.backend_start,
              S.xact_start,
              S.query_start,
              S.state_change,
              S.wait_event_type,
              S.wait_event,
              S.state,
              S.backend_xid,
              s.backend_xmin,
              S.query_id,
              S.query,
              S.backend_type
      FROM pg_stat_get_activity(NULL::integer) AS S
          LEFT JOIN pg_database AS D ON (S.datid = D.oid)
          LEFT JOIN pg_authid AS U ON (S.usesysid = U.oid);)",

  R"(CREATE VIEW pg_stat_replication AS
      SELECT
              S.pid,
              S.usesysid,
              U.rolname AS usename,
              S.application_name,
              S.client_addr,
              S.client_hostname,
              S.client_port,
              S.backend_start,
              S.backend_xmin,
              W.state,
              W.sent_lsn,
              W.write_lsn,
              W.flush_lsn,
              W.replay_lsn,
              W.write_lag,
              W.flush_lag,
              W.replay_lag,
              W.sync_priority,
              W.sync_state,
              W.reply_time
      FROM pg_stat_get_activity(NULL::integer) AS S
          JOIN pg_stat_get_wal_senders() AS W ON (S.pid = W.pid)
          LEFT JOIN pg_authid AS U ON (S.usesysid = U.oid);)",

  R"(CREATE VIEW pg_stat_slru AS
      SELECT
              s.name,
              s.blks_zeroed,
              s.blks_hit,
              s.blks_read,
              s.blks_written,
              s.blks_exists,
              s.flushes,
              s.truncates,
              s.stats_reset
      FROM pg_stat_get_slru() s;)",

  R"(CREATE VIEW pg_stat_wal_receiver AS
      SELECT
              s.pid,
              s.status,
              s.receive_start_lsn,
              s.receive_start_tli,
              s.written_lsn,
              s.flushed_lsn,
              s.received_tli,
              s.last_msg_send_time,
              s.last_msg_receipt_time,
              s.latest_end_lsn,
              s.latest_end_time,
              s.slot_name,
              s.sender_host,
              s.sender_port,
              s.conninfo
      FROM pg_stat_get_wal_receiver() s
      WHERE s.pid IS NOT NULL;)",

  R"(CREATE VIEW pg_stat_recovery_prefetch AS
      SELECT
              s.stats_reset,
              s.prefetch,
              s.hit,
              s.skip_init,
              s.skip_new,
              s.skip_fpw,
              s.skip_rep,
              s.wal_distance,
              s.block_distance,
              s.io_depth
       FROM pg_stat_get_recovery_prefetch() s;)",

  R"(CREATE VIEW pg_stat_subscription AS
      SELECT
              su.oid AS subid,
              su.subname,
              st.worker_type,
              st.pid,
              st.leader_pid,
              st.relid,
              st.received_lsn,
              st.last_msg_send_time,
              st.last_msg_receipt_time,
              st.latest_end_lsn,
              st.latest_end_time
      FROM pg_subscription su
              LEFT JOIN pg_stat_get_subscription(NULL::oid) st
                        ON (st.subid = su.oid);)",

  R"(CREATE VIEW pg_stat_ssl AS
      SELECT
              S.pid,
              S.ssl,
              S.sslversion AS version,
              S.sslcipher AS cipher,
              S.sslbits AS bits,
              S.ssl_client_dn AS client_dn,
              S.ssl_client_serial AS client_serial,
              S.ssl_issuer_dn AS issuer_dn
      FROM pg_stat_get_activity(NULL::integer) AS S
      WHERE S.client_port IS NOT NULL;)",

  R"(CREATE VIEW pg_stat_gssapi AS
      SELECT
              S.pid,
              S.gss_auth AS gss_authenticated,
              S.gss_princ AS principal,
              S.gss_enc AS encrypted,
              S.gss_delegation AS credentials_delegated
      FROM pg_stat_get_activity(NULL::integer) AS S
      WHERE S.client_port IS NOT NULL;)",

  R"(CREATE VIEW pg_replication_slots AS
      SELECT
              L.slot_name,
              L.plugin,
              L.slot_type,
              L.datoid,
              D.datname AS database,
              L.temporary,
              L.active,
              L.active_pid,
              L.xmin,
              L.catalog_xmin,
              L.restart_lsn,
              L.confirmed_flush_lsn,
              L.wal_status,
              L.safe_wal_size,
              L.two_phase,
              L.two_phase_at,
              L.inactive_since,
              L.conflicting,
              L.invalidation_reason,
              L.failover,
              L.synced
      FROM pg_get_replication_slots() AS L
              LEFT JOIN pg_database D ON (L.datoid = D.oid);)",

  // TODO(mbkkt) Use original text when implement LATERAL JOIN
  R"(CREATE VIEW pg_stat_replication_slots AS
      SELECT
              s.slot_name,
              s.spill_txns,
              s.spill_count,
              s.spill_bytes,
              s.stream_txns,
              s.stream_count,
              s.stream_bytes,
              s.total_txns,
              s.total_bytes,
              s.stats_reset
      FROM pg_replication_slots as r,
           pg_stat_get_replication_slot(NULL::TEXT) as s
      WHERE r.slot_name = s.slot_name
            AND r.datoid IS NOT NULL; -- excluding physical slots)",

  // TODO(mbkkt) Use original text when implement implicit casts for UNION
  R"(CREATE VIEW pg_stat_database AS
      SELECT
              D.oid AS datid,
              D.datname AS datname,
                  CASE
                      WHEN (D.oid = (0)::oid) THEN 0
                      ELSE pg_stat_get_db_numbackends(D.oid)
                  END AS numbackends,
              pg_stat_get_db_xact_commit(D.oid) AS xact_commit,
              pg_stat_get_db_xact_rollback(D.oid) AS xact_rollback,
              pg_stat_get_db_blocks_fetched(D.oid) -
                      pg_stat_get_db_blocks_hit(D.oid) AS blks_read,
              pg_stat_get_db_blocks_hit(D.oid) AS blks_hit,
              pg_stat_get_db_tuples_returned(D.oid) AS tup_returned,
              pg_stat_get_db_tuples_fetched(D.oid) AS tup_fetched,
              pg_stat_get_db_tuples_inserted(D.oid) AS tup_inserted,
              pg_stat_get_db_tuples_updated(D.oid) AS tup_updated,
              pg_stat_get_db_tuples_deleted(D.oid) AS tup_deleted,
              pg_stat_get_db_conflict_all(D.oid) AS conflicts,
              pg_stat_get_db_temp_files(D.oid) AS temp_files,
              pg_stat_get_db_temp_bytes(D.oid) AS temp_bytes,
              pg_stat_get_db_deadlocks(D.oid) AS deadlocks,
              pg_stat_get_db_checksum_failures(D.oid) AS checksum_failures,
              pg_stat_get_db_checksum_last_failure(D.oid) AS checksum_last_failure,
              pg_stat_get_db_blk_read_time(D.oid) AS blk_read_time,
              pg_stat_get_db_blk_write_time(D.oid) AS blk_write_time,
              pg_stat_get_db_session_time(D.oid) AS session_time,
              pg_stat_get_db_active_time(D.oid) AS active_time,
              pg_stat_get_db_idle_in_transaction_time(D.oid) AS idle_in_transaction_time,
              pg_stat_get_db_sessions(D.oid) AS sessions,
              pg_stat_get_db_sessions_abandoned(D.oid) AS sessions_abandoned,
              pg_stat_get_db_sessions_fatal(D.oid) AS sessions_fatal,
              pg_stat_get_db_sessions_killed(D.oid) AS sessions_killed,
              pg_stat_get_db_parallel_workers_to_launch(D.oid) as parallel_workers_to_launch,
              pg_stat_get_db_parallel_workers_launched(D.oid) as parallel_workers_launched,
              pg_stat_get_db_stat_reset_time(D.oid) AS stats_reset
      FROM (
          SELECT 0::oid AS oid, NULL::name AS datname
          UNION ALL
          SELECT oid, datname FROM pg_database
      ) D;)",

  R"(CREATE VIEW pg_stat_database_conflicts AS
      SELECT
              D.oid AS datid,
              D.datname AS datname,
              pg_stat_get_db_conflict_tablespace(D.oid) AS confl_tablespace,
              pg_stat_get_db_conflict_lock(D.oid) AS confl_lock,
              pg_stat_get_db_conflict_snapshot(D.oid) AS confl_snapshot,
              pg_stat_get_db_conflict_bufferpin(D.oid) AS confl_bufferpin,
              pg_stat_get_db_conflict_startup_deadlock(D.oid) AS confl_deadlock,
              pg_stat_get_db_conflict_logicalslot(D.oid) AS confl_active_logicalslot
      FROM pg_database D;)",

  R"(CREATE VIEW pg_stat_user_functions AS
      SELECT
              P.oid AS funcid,
              N.nspname AS schemaname,
              P.proname AS funcname,
              pg_stat_get_function_calls(P.oid) AS calls,
              pg_stat_get_function_total_time(P.oid) AS total_time,
              pg_stat_get_function_self_time(P.oid) AS self_time
      FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
      WHERE P.prolang != 12  -- fast check to eliminate built-in functions
            AND pg_stat_get_function_calls(P.oid) IS NOT NULL;)",

  R"(CREATE VIEW pg_stat_xact_user_functions AS
      SELECT
              P.oid AS funcid,
              N.nspname AS schemaname,
              P.proname AS funcname,
              pg_stat_get_xact_function_calls(P.oid) AS calls,
              pg_stat_get_xact_function_total_time(P.oid) AS total_time,
              pg_stat_get_xact_function_self_time(P.oid) AS self_time
      FROM pg_proc P LEFT JOIN pg_namespace N ON (N.oid = P.pronamespace)
      WHERE P.prolang != 12  -- fast check to eliminate built-in functions
            AND pg_stat_get_xact_function_calls(P.oid) IS NOT NULL;)",

  R"(CREATE VIEW pg_stat_archiver AS
      SELECT
          s.archived_count,
          s.last_archived_wal,
          s.last_archived_time,
          s.failed_count,
          s.last_failed_wal,
          s.last_failed_time,
          s.stats_reset
      FROM pg_stat_get_archiver() s;)",

  R"(CREATE VIEW pg_stat_bgwriter AS
      SELECT
          pg_stat_get_bgwriter_buf_written_clean() AS buffers_clean,
          pg_stat_get_bgwriter_maxwritten_clean() AS maxwritten_clean,
          pg_stat_get_buf_alloc() AS buffers_alloc,
          pg_stat_get_bgwriter_stat_reset_time() AS stats_reset;)",

  R"(CREATE VIEW pg_stat_checkpointer AS
      SELECT
          pg_stat_get_checkpointer_num_timed() AS num_timed,
          pg_stat_get_checkpointer_num_requested() AS num_requested,
          pg_stat_get_checkpointer_num_performed() AS num_done,
          pg_stat_get_checkpointer_restartpoints_timed() AS restartpoints_timed,
          pg_stat_get_checkpointer_restartpoints_requested() AS restartpoints_req,
          pg_stat_get_checkpointer_restartpoints_performed() AS restartpoints_done,
          pg_stat_get_checkpointer_write_time() AS write_time,
          pg_stat_get_checkpointer_sync_time() AS sync_time,
          pg_stat_get_checkpointer_buffers_written() AS buffers_written,
          pg_stat_get_checkpointer_slru_written() AS slru_written,
          pg_stat_get_checkpointer_stat_reset_time() AS stats_reset;)",

  R"(CREATE VIEW pg_stat_io AS
  SELECT
         b.backend_type,
         b.object,
         b.context,
         b.reads,
         b.read_bytes,
         b.read_time,
         b.writes,
         b.write_bytes,
         b.write_time,
         b.writebacks,
         b.writeback_time,
         b.extends,
         b.extend_bytes,
         b.extend_time,
         b.hits,
         b.evictions,
         b.reuses,
         b.fsyncs,
         b.fsync_time,
         b.stats_reset
  FROM pg_stat_get_io() b;)",

  R"(CREATE VIEW pg_stat_wal AS
      SELECT
          w.wal_records,
          w.wal_fpi,
          w.wal_bytes,
          w.wal_buffers_full,
          w.stats_reset
      FROM pg_stat_get_wal() w;)",

  R"(CREATE VIEW pg_stat_progress_analyze AS
      SELECT
          S.pid AS pid, S.datid AS datid, D.datname AS datname,
          CAST(S.relid AS oid) AS relid,
          CASE S.param1 WHEN 0 THEN 'initializing'
                        WHEN 1 THEN 'acquiring sample rows'
                        WHEN 2 THEN 'acquiring inherited sample rows'
                        WHEN 3 THEN 'computing statistics'
                        WHEN 4 THEN 'computing extended statistics'
                        WHEN 5 THEN 'finalizing analyze'
                        END AS phase,
          S.param2 AS sample_blks_total,
          S.param3 AS sample_blks_scanned,
          S.param4 AS ext_stats_total,
          S.param5 AS ext_stats_computed,
          S.param6 AS child_tables_total,
          S.param7 AS child_tables_done,
          CAST(S.param8 AS oid) AS current_child_table_relid,
          S.param9 / 1000000::double precision AS delay_time
      FROM pg_stat_get_progress_info('ANALYZE') AS S
          LEFT JOIN pg_database D ON S.datid = D.oid;)",

  R"(CREATE VIEW pg_stat_progress_vacuum AS
      SELECT
          S.pid AS pid, S.datid AS datid, D.datname AS datname,
          S.relid AS relid,
          CASE S.param1 WHEN 0 THEN 'initializing'
                        WHEN 1 THEN 'scanning heap'
                        WHEN 2 THEN 'vacuuming indexes'
                        WHEN 3 THEN 'vacuuming heap'
                        WHEN 4 THEN 'cleaning up indexes'
                        WHEN 5 THEN 'truncating heap'
                        WHEN 6 THEN 'performing final cleanup'
                        END AS phase,
          S.param2 AS heap_blks_total, S.param3 AS heap_blks_scanned,
          S.param4 AS heap_blks_vacuumed, S.param5 AS index_vacuum_count,
          S.param6 AS max_dead_tuple_bytes, S.param7 AS dead_tuple_bytes,
          S.param8 AS num_dead_item_ids, S.param9 AS indexes_total,
          S.param10 AS indexes_processed,
          S.param11 / 1000000::double precision AS delay_time
      FROM pg_stat_get_progress_info('VACUUM') AS S
          LEFT JOIN pg_database D ON S.datid = D.oid;)",

  R"(CREATE VIEW pg_stat_progress_cluster AS
      SELECT
          S.pid AS pid,
          S.datid AS datid,
          D.datname AS datname,
          S.relid AS relid,
          CASE S.param1 WHEN 1 THEN 'CLUSTER'
                        WHEN 2 THEN 'VACUUM FULL'
                        END AS command,
          CASE S.param2 WHEN 0 THEN 'initializing'
                        WHEN 1 THEN 'seq scanning heap'
                        WHEN 2 THEN 'index scanning heap'
                        WHEN 3 THEN 'sorting tuples'
                        WHEN 4 THEN 'writing new heap'
                        WHEN 5 THEN 'swapping relation files'
                        WHEN 6 THEN 'rebuilding index'
                        WHEN 7 THEN 'performing final cleanup'
                        END AS phase,
          CAST(S.param3 AS oid) AS cluster_index_relid,
          S.param4 AS heap_tuples_scanned,
          S.param5 AS heap_tuples_written,
          S.param6 AS heap_blks_total,
          S.param7 AS heap_blks_scanned,
          S.param8 AS index_rebuild_count
      FROM pg_stat_get_progress_info('CLUSTER') AS S
          LEFT JOIN pg_database D ON S.datid = D.oid;)",

  R"(CREATE VIEW pg_stat_progress_create_index AS
      SELECT
          S.pid AS pid, S.datid AS datid, D.datname AS datname,
          S.relid AS relid,
          CAST(S.param7 AS oid) AS index_relid,
          CASE S.param1 WHEN 1 THEN 'CREATE INDEX'
                        WHEN 2 THEN 'CREATE INDEX CONCURRENTLY'
                        WHEN 3 THEN 'REINDEX'
                        WHEN 4 THEN 'REINDEX CONCURRENTLY'
                        END AS command,
       -- CASE S.param10 WHEN 0 THEN 'initializing'
       --                WHEN 1 THEN 'waiting for writers before build'
       --                WHEN 2 THEN 'building index' ||
       --                    COALESCE((': ' || pg_indexam_progress_phasename(S.param9::oid, S.param11)),
       --                             '')
       --                WHEN 3 THEN 'waiting for writers before validation'
       --                WHEN 4 THEN 'index validation: scanning index'
       --                WHEN 5 THEN 'index validation: sorting tuples'
       --                WHEN 6 THEN 'index validation: scanning table'
       --                WHEN 7 THEN 'waiting for old snapshots'
       --                WHEN 8 THEN 'waiting for readers before marking dead'
       --                WHEN 9 THEN 'waiting for readers before dropping'
       --                END as phase,
          CASE S.param10 WHEN 1 THEN 'initializing'
                         WHEN 2 THEN 'building index'
                         WHEN 3 THEN 'committing'
                         WHEN 4 THEN 'finalizing'
                         END AS phase,
          S.param4 AS lockers_total,
          S.param5 AS lockers_done,
          S.param6 AS current_locker_pid,
          S.param16 AS blocks_total,
          S.param17 AS blocks_done,
          S.param12 AS tuples_total,
          S.param13 AS tuples_done,
          S.param14 AS partitions_total,
          S.param15 AS partitions_done
      FROM pg_stat_get_progress_info('CREATE INDEX') AS S
          LEFT JOIN pg_database D ON S.datid = D.oid;)",

  R"(CREATE VIEW pg_stat_progress_basebackup AS
      SELECT
          S.pid AS pid,
          CASE S.param1 WHEN 0 THEN 'initializing'
                        WHEN 1 THEN 'waiting for checkpoint to finish'
                        WHEN 2 THEN 'estimating backup size'
                        WHEN 3 THEN 'streaming database files'
                        WHEN 4 THEN 'waiting for wal archiving to finish'
                        WHEN 5 THEN 'transferring wal files'
                        END AS phase,
          CASE S.param2 WHEN -1 THEN NULL ELSE S.param2 END AS backup_total,
          S.param3 AS backup_streamed,
          S.param4 AS tablespaces_total,
          S.param5 AS tablespaces_streamed
      FROM pg_stat_get_progress_info('BASEBACKUP') AS S;)",

  R"(CREATE VIEW pg_stat_progress_copy AS
      SELECT
          S.pid AS pid, S.datid AS datid, D.datname AS datname,
          S.relid AS relid,
          CASE S.param5 WHEN 1 THEN 'COPY FROM'
                        WHEN 2 THEN 'COPY TO'
                        END AS command,
          CASE S.param6 WHEN 1 THEN 'FILE'
                        WHEN 2 THEN 'PROGRAM'
                        WHEN 3 THEN 'PIPE'
                        WHEN 4 THEN 'CALLBACK'
                        END AS "type",
          S.param1 AS bytes_processed,
          S.param2 AS bytes_total,
          S.param3 AS tuples_processed,
          S.param4 AS tuples_excluded,
          S.param7 AS tuples_skipped
      FROM pg_stat_get_progress_info('COPY') AS S
          LEFT JOIN pg_database D ON S.datid = D.oid;)",

  R"(CREATE VIEW pg_user_mappings AS
      SELECT
          U.oid       AS umid,
          S.oid       AS srvid,
          S.srvname   AS srvname,
          U.umuser    AS umuser,
          CASE WHEN U.umuser = 0 THEN
              'public'
          ELSE
              A.rolname
          END AS usename,
          CASE WHEN (U.umuser <> 0 AND A.rolname = current_user
                       AND (pg_has_role(S.srvowner, 'USAGE')
                            OR has_server_privilege(S.oid, 'USAGE')))
                      OR (U.umuser = 0 AND pg_has_role(S.srvowner, 'USAGE'))
                      OR (SELECT rolsuper FROM pg_authid WHERE rolname = current_user)
                      THEN U.umoptions
                   ELSE NULL END AS umoptions
      FROM pg_user_mapping U
          JOIN pg_foreign_server S ON (U.umserver = S.oid)
          LEFT JOIN pg_authid A ON (A.oid = U.umuser);)",

  // R"(REVOKE ALL ON pg_user_mapping FROM public;)",

  R"(CREATE VIEW pg_replication_origin_status AS
      SELECT *
      FROM pg_show_replication_origin_status();)",

  // R"(REVOKE ALL ON pg_replication_origin_status FROM public;)",

  // All columns of pg_subscription except subconninfo are publicly readable.
  // R"(REVOKE ALL ON pg_subscription FROM public;)",
  // R"(GRANT SELECT (oid, subdbid, subskiplsn, subname, subowner, subenabled,
  //               subbinary, substream, subtwophasestate, subdisableonerr,
  // 			  subpasswordrequired, subrunasowner, subfailover,
  //               subslotname, subsynccommit, subpublications, suborigin)
  //     ON pg_subscription TO public;)",

  R"(CREATE VIEW pg_stat_subscription_stats AS
      SELECT
          ss.subid,
          s.subname,
          ss.apply_error_count,
          ss.sync_error_count,
          ss.confl_insert_exists,
          ss.confl_update_origin_differs,
          ss.confl_update_exists,
          ss.confl_update_missing,
          ss.confl_delete_origin_differs,
          ss.confl_delete_missing,
          ss.confl_multiple_unique_conflicts,
          ss.stats_reset
      FROM pg_subscription as s,
           pg_stat_get_subscription_stats(NULL::oid) as ss
      WHERE ss.subid = s.oid;)",

  R"(CREATE VIEW pg_wait_events AS
      SELECT * FROM pg_get_wait_events();)",

  R"(CREATE VIEW pg_aios AS
      SELECT * FROM pg_get_aios();)",
  // R"(REVOKE ALL ON pg_aios FROM PUBLIC;)",
  // R"(GRANT SELECT ON pg_aios TO pg_read_all_stats;)",
  // R"(REVOKE EXECUTE ON FUNCTION pg_get_aios() FROM PUBLIC;)",
  // R"(GRANT EXECUTE ON FUNCTION pg_get_aios() TO pg_read_all_stats;)",

  // SQL Information Schema
  // as defined in ISO/IEC 9075-11:2023
  //
  // Copyright (c) 2003-2025, PostgreSQL Global Development Group
  //
  // src/backend/catalog/information_schema.sql
  //
  // Note: this file is read in single-user -j mode, which means that the
  // command terminator is semicolon-newline-newline; whenever the backend
  // sees that, it stops and executes what it's got.  If you write a lot of
  // statements without empty lines between, they'll all get quoted to you
  // in any error message about one of them, so don't do that.  Also, you
  // cannot write a semicolon immediately followed by an empty line in a
  // string literal (including a function body!) or a multiline comment.
  //
  // Note: Generally, the definitions in this file should be ordered
  // according to the clause numbers in the SQL standard, which is also the
  // alphabetical order.  In some cases it is convenient or necessary to
  // define one information schema view by using another one; in that case,
  // put the referencing view at the very end and leave a note where it
  // should have been put.

  // 6.2
  // INFORMATION_SCHEMA schema

  // R"(CREATE SCHEMA information_schema;
  // GRANT USAGE ON SCHEMA information_schema TO PUBLIC;
  // SET search_path TO information_schema;)",

  // 6.3 INFORMATION_SCHEMA_CATALOG_NAME view appears later.

  // 6.4
  // CARDINAL_NUMBER domain

  // R"(CREATE DOMAIN cardinal_number AS integer
  //     CONSTRAINT cardinal_number_domain_check CHECK (value >= 0);)",

  // 6.5
  // CHARACTER_DATA domain

  // R"(CREATE DOMAIN character_data AS character varying COLLATE "C";)",

  // 6.6
  // SQL_IDENTIFIER domain

  // R"(CREATE DOMAIN sql_identifier AS name;)",

  // 6.3
  // INFORMATION_SCHEMA_CATALOG_NAME view

  R"(CREATE VIEW information_schema_catalog_name AS
      SELECT CAST(current_database() AS sql_identifier) AS catalog_name;)",

  // R"(GRANT SELECT ON information_schema_catalog_name TO PUBLIC;)",

  // 6.7
  // TIME_STAMP domain

  // R"(CREATE DOMAIN time_stamp AS timestamp(2) with time zone
  //     DEFAULT current_timestamp(2);)",

  // 6.8
  // YES_OR_NO domain

  // R"(CREATE DOMAIN yes_or_no AS character varying(3) COLLATE "C"
  //     CONSTRAINT yes_or_no_check CHECK (value IN ('YES', 'NO'));)",

  // 6.9 ADMINISTRABLE_ROLE_AUTHORIZATIONS view appears later.

  // 6.10
  // APPLICABLE_ROLES view

  R"(CREATE VIEW applicable_roles AS
      SELECT CAST(a.rolname AS sql_identifier) AS grantee,
             CAST(b.rolname AS sql_identifier) AS role_name,
             CAST(CASE WHEN m.admin_option THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable
      FROM (SELECT member, roleid, admin_option FROM pg_auth_members
            -- This UNION could be UNION ALL, but UNION works even if we start
            -- to allow explicit pg_database_owner membership.
            UNION
            SELECT datdba, pg_authid.oid, false
            FROM pg_database, pg_authid
            WHERE datname = current_database() AND rolname = 'pg_database_owner'
           )  m
           JOIN pg_authid a ON (m.member = a.oid)
           JOIN pg_authid b ON (m.roleid = b.oid)
      WHERE pg_has_role(a.oid, 'USAGE');)",

  // R"(GRANT SELECT ON applicable_roles TO PUBLIC;)",

  // 6.9
  // ADMINISTRABLE_ROLE_AUTHORIZATIONS view

  R"(CREATE VIEW administrable_role_authorizations AS
      SELECT *
      FROM information_schema.applicable_roles
      WHERE is_grantable = 'YES';)",

  // R"(GRANT SELECT ON administrable_role_authorizations TO PUBLIC;)",

  // 6.12
  // ATTRIBUTES view

  R"(CREATE VIEW attributes AS
      SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(nc.nspname AS sql_identifier) AS udt_schema,
             CAST(c.relname AS sql_identifier) AS udt_name,
             CAST(a.attname AS sql_identifier) AS attribute_name,
             CAST(a.attnum AS cardinal_number) AS ordinal_position,
             CAST(pg_get_expr(ad.adbin, ad.adrelid) AS character_data) AS attribute_default,
             CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
               AS yes_or_no)
               AS is_nullable, -- This column was apparently removed between SQL:2003 and SQL:2008.

             CAST(
               CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                    ELSE 'USER-DEFINED' END
               AS character_data)
               AS data_type,

             CAST(
               information_schema._pg_char_max_length(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS character_maximum_length,

             CAST(
               information_schema._pg_char_octet_length(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS character_octet_length,

             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,

             CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
             CAST(nco.nspname AS sql_identifier) AS collation_schema,
             CAST(co.collname AS sql_identifier) AS collation_name,

             CAST(
               information_schema._pg_numeric_precision(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS numeric_precision,

             CAST(
               information_schema._pg_numeric_precision_radix(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS numeric_precision_radix,

             CAST(
               information_schema._pg_numeric_scale(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS numeric_scale,

             CAST(
               information_schema._pg_datetime_precision(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS datetime_precision,

             CAST(
               information_schema._pg_interval_type(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS character_data)
               AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,

             CAST(current_database() AS sql_identifier) AS attribute_udt_catalog,
             CAST(nt.nspname AS sql_identifier) AS attribute_udt_schema,
             CAST(t.typname AS sql_identifier) AS attribute_udt_name,

             CAST(null AS sql_identifier) AS scope_catalog,
             CAST(null AS sql_identifier) AS scope_schema,
             CAST(null AS sql_identifier) AS scope_name,

             CAST(null AS cardinal_number) AS maximum_cardinality,
             CAST(a.attnum AS sql_identifier) AS dtd_identifier,
             CAST('NO' AS yes_or_no) AS is_derived_reference_attribute

      FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
           JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
           JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
           LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
             ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

      WHERE a.attnum > 0 AND NOT a.attisdropped
            AND c.relkind IN ('c')
            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_type_privilege(c.reltype, 'USAGE'));)",

  // R"(GRANT SELECT ON attributes TO PUBLIC;)",

  // 6.13
  // CHARACTER_SETS view

  R"(CREATE VIEW character_sets AS
      SELECT CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(getdatabaseencoding() AS sql_identifier) AS character_set_name,
             CAST(CASE WHEN getdatabaseencoding() = 'UTF8' THEN 'UCS' ELSE getdatabaseencoding() END AS sql_identifier) AS character_repertoire,
             CAST(getdatabaseencoding() AS sql_identifier) AS form_of_use,
             CAST(current_database() AS sql_identifier) AS default_collate_catalog,
             CAST(nc.nspname AS sql_identifier) AS default_collate_schema,
             CAST(c.collname AS sql_identifier) AS default_collate_name
      FROM pg_database d
           LEFT JOIN (pg_collation c JOIN pg_namespace nc ON (c.collnamespace = nc.oid))
               ON (datcollate = collcollate AND datctype = collctype)
      WHERE d.datname = current_database()
      ORDER BY char_length(c.collname) DESC, c.collname ASC -- prefer full/canonical name
      LIMIT 1;)",

  // R"(GRANT SELECT ON character_sets TO PUBLIC;)",

  // 6.14
  // CHECK_CONSTRAINT_ROUTINE_USAGE view

  R"(CREATE VIEW check_constraint_routine_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(nc.nspname AS sql_identifier) AS constraint_schema,
             CAST(c.conname AS sql_identifier) AS constraint_name,
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name
      FROM pg_namespace nc, pg_constraint c, pg_depend d, pg_proc p, pg_namespace np
      WHERE nc.oid = c.connamespace
        AND c.contype = 'c'
        AND c.oid = d.objid
        AND d.classid = 'pg_catalog.pg_constraint'::regclass
        AND d.refobjid = p.oid
        AND d.refclassid = 'pg_catalog.pg_proc'::regclass
        AND p.pronamespace = np.oid
        AND pg_has_role(p.proowner, 'USAGE');)",

  // R"(GRANT SELECT ON check_constraint_routine_usage TO PUBLIC;)",

  // 6.15
  // CHECK_CONSTRAINTS view

  // TODO(mbkkt) Use original text when implement proper types
  R"(CREATE VIEW check_constraints AS
      SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(rs.nspname AS sql_identifier) AS constraint_schema,
             CAST(con.conname AS sql_identifier) AS constraint_name,
             CAST(pg_get_expr(con.conbin, coalesce(c.oid, 0)) AS character_data) AS check_clause
      FROM pg_constraint con
             LEFT OUTER JOIN pg_namespace rs ON (rs.oid = con.connamespace)
             LEFT OUTER JOIN pg_class c ON (c.oid = con.conrelid)
             LEFT OUTER JOIN pg_type t ON (t.oid = con.contypid)
      WHERE pg_has_role(coalesce(c.relowner, t.typowner), 'USAGE')
        AND con.contype = 'c'

      UNION ALL
      -- not-null constraints
      SELECT current_database()::sql_identifier AS constraint_catalog,
             rs.nspname::sql_identifier AS constraint_schema,
             con.conname::sql_identifier AS constraint_name,
             (coalesce(at.attname, 'VALUE') || ' IS NOT NULL')::character_data AS check_clause
       FROM pg_constraint con
              LEFT JOIN pg_namespace rs ON rs.oid = con.connamespace
              LEFT JOIN pg_class c ON c.oid = con.conrelid
              LEFT JOIN pg_type t ON t.oid = con.contypid
              LEFT JOIN pg_attribute at ON (con.conrelid = at.attrelid AND con.conkey[1] = at.attnum)
       WHERE pg_has_role(coalesce(c.relowner, t.typowner), 'USAGE'::text)
         AND con.contype = 'n';)",

  // R"(GRANT SELECT ON check_constraints TO PUBLIC;)",

  // 6.16
  // COLLATIONS view

  R"(CREATE VIEW collations AS
      SELECT CAST(current_database() AS sql_identifier) AS collation_catalog,
             CAST(nc.nspname AS sql_identifier) AS collation_schema,
             CAST(c.collname AS sql_identifier) AS collation_name,
             CAST('NO PAD' AS character_data) AS pad_attribute
      FROM pg_collation c, pg_namespace nc
      WHERE c.collnamespace = nc.oid
            AND collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = current_database()));)",

  // R"(GRANT SELECT ON collations TO PUBLIC;)",

  // 6.17
  // COLLATION_CHARACTER_SET_APPLICABILITY view

  R"(CREATE VIEW collation_character_set_applicability AS
      SELECT CAST(current_database() AS sql_identifier) AS collation_catalog,
             CAST(nc.nspname AS sql_identifier) AS collation_schema,
             CAST(c.collname AS sql_identifier) AS collation_name,
             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(getdatabaseencoding() AS sql_identifier) AS character_set_name
      FROM pg_collation c, pg_namespace nc
      WHERE c.collnamespace = nc.oid
            AND collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = current_database()));)",

  // R"(GRANT SELECT ON collation_character_set_applicability TO PUBLIC;)",

  // 6.18
  // COLUMN_COLUMN_USAGE view

  R"(CREATE VIEW column_column_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(n.nspname AS sql_identifier) AS table_schema,
             CAST(c.relname AS sql_identifier) AS table_name,
             CAST(ac.attname AS sql_identifier) AS column_name,
             CAST(ad.attname AS sql_identifier) AS dependent_column

      FROM pg_namespace n, pg_class c, pg_depend d,
           pg_attribute ac, pg_attribute ad, pg_attrdef atd

      WHERE n.oid = c.relnamespace
            AND c.oid = ac.attrelid
            AND c.oid = ad.attrelid
            AND ac.attnum <> ad.attnum
            AND ad.attrelid = atd.adrelid
            AND ad.attnum = atd.adnum
            AND d.classid = 'pg_catalog.pg_attrdef'::regclass
            AND d.refclassid = 'pg_catalog.pg_class'::regclass
            AND d.objid = atd.oid
            AND d.refobjid = ac.attrelid
            AND d.refobjsubid = ac.attnum
            AND ad.attgenerated <> ''
            AND pg_has_role(c.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON column_column_usage TO PUBLIC;)",

  // 6.19
  // COLUMN_DOMAIN_USAGE view

  R"(CREATE VIEW column_domain_usage AS
      SELECT CAST(current_database() AS sql_identifier) AS domain_catalog,
             CAST(nt.nspname AS sql_identifier) AS domain_schema,
             CAST(t.typname AS sql_identifier) AS domain_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nc.nspname AS sql_identifier) AS table_schema,
             CAST(c.relname AS sql_identifier) AS table_name,
             CAST(a.attname AS sql_identifier) AS column_name

      FROM pg_type t, pg_namespace nt, pg_class c, pg_namespace nc,
           pg_attribute a

      WHERE t.typnamespace = nt.oid
            AND c.relnamespace = nc.oid
            AND a.attrelid = c.oid
            AND a.atttypid = t.oid
            AND t.typtype = 'd'
            AND c.relkind IN ('r', 'v', 'f', 'p')
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND pg_has_role(t.typowner, 'USAGE');)",

  // R"(GRANT SELECT ON column_domain_usage TO PUBLIC;)",

  // 6.20
  // COLUMN_PRIVILEGES

  // TODO(mbkkt) Use original text when implement (aclexplode(...)).* row expansion
  R"(CREATE VIEW column_privileges AS
      SELECT CAST(null AS sql_identifier) AS grantor,
             CAST(null AS sql_identifier) AS grantee,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(null AS sql_identifier) AS table_schema,
             CAST(null AS sql_identifier) AS table_name,
             CAST(null AS sql_identifier) AS column_name,
             CAST(null AS character_data) AS privilege_type,
             CAST(null AS yes_or_no) AS is_grantable
      WHERE false;)",

  // R"(GRANT SELECT ON column_privileges TO PUBLIC;)",

  // 6.21
  // COLUMN_UDT_USAGE view

  R"(CREATE VIEW column_udt_usage AS
      SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
             CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nc.nspname AS sql_identifier) AS table_schema,
             CAST(c.relname AS sql_identifier) AS table_name,
             CAST(a.attname AS sql_identifier) AS column_name

      FROM pg_attribute a, pg_class c, pg_namespace nc,
           (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid))
             LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
             ON (t.typtype = 'd' AND t.typbasetype = bt.oid)

      WHERE a.attrelid = c.oid
            AND a.atttypid = t.oid
            AND nc.oid = c.relnamespace
            AND a.attnum > 0 AND NOT a.attisdropped
            AND c.relkind in ('r', 'v', 'f', 'p')
            AND pg_has_role(coalesce(bt.typowner, t.typowner), 'USAGE');)",

  // R"(GRANT SELECT ON column_udt_usage TO PUBLIC;)",

  // 6.22
  // COLUMNS view

  R"(CREATE VIEW columns AS
      SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nc.nspname AS sql_identifier) AS table_schema,
             CAST(c.relname AS sql_identifier) AS table_name,
             CAST(a.attname AS sql_identifier) AS column_name,
             CAST(a.attnum AS cardinal_number) AS ordinal_position,
             CAST(CASE WHEN a.attgenerated = '' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS column_default,
             CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
               AS yes_or_no)
               AS is_nullable,

             CAST(
               CASE WHEN t.typtype = 'd' THEN
                 CASE WHEN bt.typelem <> 0 AND bt.typlen = -1 THEN 'ARRAY'
                      WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                      ELSE 'USER-DEFINED' END
               ELSE
                 CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                      WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
                      ELSE 'USER-DEFINED' END
               END
               AS character_data)
               AS data_type,

             CAST(
               information_schema._pg_char_max_length(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS character_maximum_length,

             CAST(
               information_schema._pg_char_octet_length(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS character_octet_length,

             CAST(
               information_schema._pg_numeric_precision(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS numeric_precision,

             CAST(
               information_schema._pg_numeric_precision_radix(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS numeric_precision_radix,

             CAST(
               information_schema._pg_numeric_scale(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS numeric_scale,

             CAST(
               information_schema._pg_datetime_precision(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS cardinal_number)
               AS datetime_precision,

             CAST(
               information_schema._pg_interval_type(information_schema._pg_truetypid(a, t), information_schema._pg_truetypmod(a, t))
               AS character_data)
               AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,

             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,

             CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
             CAST(nco.nspname AS sql_identifier) AS collation_schema,
             CAST(co.collname AS sql_identifier) AS collation_name,

             CAST(CASE WHEN t.typtype = 'd' THEN current_database() ELSE null END
               AS sql_identifier) AS domain_catalog,
             CAST(CASE WHEN t.typtype = 'd' THEN nt.nspname ELSE null END
               AS sql_identifier) AS domain_schema,
             CAST(CASE WHEN t.typtype = 'd' THEN t.typname ELSE null END
               AS sql_identifier) AS domain_name,

             CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(coalesce(nbt.nspname, nt.nspname) AS sql_identifier) AS udt_schema,
             CAST(coalesce(bt.typname, t.typname) AS sql_identifier) AS udt_name,

             CAST(null AS sql_identifier) AS scope_catalog,
             CAST(null AS sql_identifier) AS scope_schema,
             CAST(null AS sql_identifier) AS scope_name,

             CAST(null AS cardinal_number) AS maximum_cardinality,
             CAST(a.attnum AS sql_identifier) AS dtd_identifier,
             CAST('NO' AS yes_or_no) AS is_self_referencing,

             CAST(CASE WHEN a.attidentity IN ('a', 'd') THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_identity,
             CAST(CASE a.attidentity WHEN 'a' THEN 'ALWAYS' WHEN 'd' THEN 'BY DEFAULT' END AS character_data) AS identity_generation,
             CAST(seq.seqstart AS character_data) AS identity_start,
             CAST(seq.seqincrement AS character_data) AS identity_increment,
             CAST(seq.seqmax AS character_data) AS identity_maximum,
             CAST(seq.seqmin AS character_data) AS identity_minimum,
             CAST(CASE WHEN seq.seqcycle THEN 'YES' ELSE 'NO' END AS yes_or_no) AS identity_cycle,

             CAST(CASE WHEN a.attgenerated <> '' THEN 'ALWAYS' ELSE 'NEVER' END AS character_data) AS is_generated,
             CAST(CASE WHEN a.attgenerated <> '' THEN pg_get_expr(ad.adbin, ad.adrelid) END AS character_data) AS generation_expression,

             CAST(CASE WHEN c.relkind IN ('r', 'p') OR
                            (c.relkind IN ('v', 'f') AND
                             pg_column_is_updatable(c.oid, a.attnum, false))
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_updatable

      FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
           JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
           JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
           LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON (bt.typnamespace = nbt.oid))
             ON (t.typtype = 'd' AND t.typbasetype = bt.oid)
           LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
             ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')
           LEFT JOIN (pg_depend dep JOIN pg_sequence seq ON (dep.classid = 'pg_class'::regclass AND dep.objid = seq.seqrelid AND dep.deptype = 'i'))
             ON (dep.refclassid = 'pg_class'::regclass AND dep.refobjid = c.oid AND dep.refobjsubid = a.attnum)

      WHERE (NOT pg_is_other_temp_schema(nc.oid))

            AND a.attnum > 0 AND NOT a.attisdropped
            AND c.relkind IN ('r', 'v', 'f', 'p')

            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_column_privilege(c.oid, a.attnum,
                                         'SELECT, INSERT, UPDATE, REFERENCES'));)",

  // R"(GRANT SELECT ON columns TO PUBLIC;)",

  // 6.23
  // CONSTRAINT_COLUMN_USAGE view

  R"(CREATE VIEW constraint_column_usage AS
      SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(tblschema AS sql_identifier) AS table_schema,
             CAST(tblname AS sql_identifier) AS table_name,
             CAST(colname AS sql_identifier) AS column_name,
             CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(cstrschema AS sql_identifier) AS constraint_schema,
             CAST(cstrname AS sql_identifier) AS constraint_name

      FROM (
          /* check constraints */
          SELECT DISTINCT nr.nspname, r.relname, r.relowner, a.attname, nc.nspname, c.conname
            FROM pg_namespace nr, pg_class r, pg_attribute a, pg_depend d, pg_namespace nc, pg_constraint c
            WHERE nr.oid = r.relnamespace
              AND r.oid = a.attrelid
              AND d.refclassid = 'pg_catalog.pg_class'::regclass
              AND d.refobjid = r.oid
              AND d.refobjsubid = a.attnum
              AND d.classid = 'pg_catalog.pg_constraint'::regclass
              AND d.objid = c.oid
              AND c.connamespace = nc.oid
              AND c.contype = 'c'
              AND r.relkind IN ('r', 'p')
              AND NOT a.attisdropped

          UNION ALL

          /* not-null constraints */
          SELECT DISTINCT nr.nspname, r.relname, r.relowner, a.attname, nc.nspname, c.conname
            FROM pg_namespace nr, pg_class r, pg_attribute a, pg_namespace nc, pg_constraint c
            WHERE nr.oid = r.relnamespace
              AND r.oid = a.attrelid
              AND r.oid = c.conrelid
              AND a.attnum = c.conkey[1]
              AND c.connamespace = nc.oid
              AND c.contype = 'n'
              AND r.relkind in ('r', 'p')
              AND not a.attisdropped

          UNION ALL

          /* unique/primary key/foreign key constraints */
          SELECT nr.nspname, r.relname, r.relowner, a.attname, nc.nspname, c.conname
            FROM pg_namespace nr, pg_class r, pg_attribute a, pg_namespace nc,
                 pg_constraint c
            WHERE nr.oid = r.relnamespace
              AND r.oid = a.attrelid
              AND nc.oid = c.connamespace
              AND r.oid = CASE c.contype WHEN 'f' THEN c.confrelid ELSE c.conrelid END
              AND a.attnum = ANY (CASE c.contype WHEN 'f' THEN c.confkey ELSE c.conkey END)
              AND NOT a.attisdropped
              AND c.contype IN ('p', 'u', 'f')
              AND r.relkind IN ('r', 'p')

        ) AS x (tblschema, tblname, tblowner, colname, cstrschema, cstrname)

      WHERE pg_has_role(x.tblowner, 'USAGE');)",

  // R"(GRANT SELECT ON constraint_column_usage TO PUBLIC;)",

  // 6.25
  // CONSTRAINT_TABLE_USAGE view

  R"(CREATE VIEW constraint_table_usage AS
      SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nr.nspname AS sql_identifier) AS table_schema,
             CAST(r.relname AS sql_identifier) AS table_name,
             CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(nc.nspname AS sql_identifier) AS constraint_schema,
             CAST(c.conname AS sql_identifier) AS constraint_name

      FROM pg_constraint c, pg_namespace nc,
           pg_class r, pg_namespace nr

      WHERE c.connamespace = nc.oid AND r.relnamespace = nr.oid
            AND ( (c.contype = 'f' AND c.confrelid = r.oid)
               OR (c.contype IN ('p', 'u') AND c.conrelid = r.oid) )
            AND r.relkind IN ('r', 'p')
            AND pg_has_role(r.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON constraint_table_usage TO PUBLIC;)",

  // 6.26 DATA_TYPE_PRIVILEGES view appears later.

  // 6.29
  // DOMAIN_CONSTRAINTS view

  R"(CREATE VIEW domain_constraints AS
      SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(rs.nspname AS sql_identifier) AS constraint_schema,
             CAST(con.conname AS sql_identifier) AS constraint_name,
             CAST(current_database() AS sql_identifier) AS domain_catalog,
             CAST(n.nspname AS sql_identifier) AS domain_schema,
             CAST(t.typname AS sql_identifier) AS domain_name,
             CAST(CASE WHEN condeferrable THEN 'YES' ELSE 'NO' END
               AS yes_or_no) AS is_deferrable,
             CAST(CASE WHEN condeferred THEN 'YES' ELSE 'NO' END
               AS yes_or_no) AS initially_deferred
      FROM pg_namespace rs, pg_namespace n, pg_constraint con, pg_type t
      WHERE rs.oid = con.connamespace
            AND n.oid = t.typnamespace
            AND t.oid = con.contypid
            AND (pg_has_role(t.typowner, 'USAGE')
                 OR has_type_privilege(t.oid, 'USAGE'));)",

  // R"(GRANT SELECT ON domain_constraints TO PUBLIC;)",

  // DOMAIN_UDT_USAGE view
  // apparently removed in SQL:2003

  R"(CREATE VIEW domain_udt_usage AS
      SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(nbt.nspname AS sql_identifier) AS udt_schema,
             CAST(bt.typname AS sql_identifier) AS udt_name,
             CAST(current_database() AS sql_identifier) AS domain_catalog,
             CAST(nt.nspname AS sql_identifier) AS domain_schema,
             CAST(t.typname AS sql_identifier) AS domain_name

      FROM pg_type t, pg_namespace nt,
           pg_type bt, pg_namespace nbt

      WHERE t.typnamespace = nt.oid
            AND t.typbasetype = bt.oid
            AND bt.typnamespace = nbt.oid
            AND t.typtype = 'd'
            AND pg_has_role(bt.typowner, 'USAGE');)",

  // R"(GRANT SELECT ON domain_udt_usage TO PUBLIC;)",

  // 6.30
  // DOMAINS view

  R"(CREATE VIEW domains AS
      SELECT CAST(current_database() AS sql_identifier) AS domain_catalog,
             CAST(nt.nspname AS sql_identifier) AS domain_schema,
             CAST(t.typname AS sql_identifier) AS domain_name,

             CAST(
               CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nbt.nspname = 'pg_catalog' THEN format_type(t.typbasetype, null)
                    ELSE 'USER-DEFINED' END
               AS character_data)
               AS data_type,

             CAST(
               information_schema._pg_char_max_length(t.typbasetype, t.typtypmod)
               AS cardinal_number)
               AS character_maximum_length,

             CAST(
               information_schema._pg_char_octet_length(t.typbasetype, t.typtypmod)
               AS cardinal_number)
               AS character_octet_length,

             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,

             CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
             CAST(nco.nspname AS sql_identifier) AS collation_schema,
             CAST(co.collname AS sql_identifier) AS collation_name,

             CAST(
               information_schema._pg_numeric_precision(t.typbasetype, t.typtypmod)
               AS cardinal_number)
               AS numeric_precision,

             CAST(
               information_schema._pg_numeric_precision_radix(t.typbasetype, t.typtypmod)
               AS cardinal_number)
               AS numeric_precision_radix,

             CAST(
               information_schema._pg_numeric_scale(t.typbasetype, t.typtypmod)
               AS cardinal_number)
               AS numeric_scale,

             CAST(
               information_schema._pg_datetime_precision(t.typbasetype, t.typtypmod)
               AS cardinal_number)
               AS datetime_precision,

             CAST(
               information_schema._pg_interval_type(t.typbasetype, t.typtypmod)
               AS character_data)
               AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,

             CAST(t.typdefault AS character_data) AS domain_default,

             CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(nbt.nspname AS sql_identifier) AS udt_schema,
             CAST(bt.typname AS sql_identifier) AS udt_name,

             CAST(null AS sql_identifier) AS scope_catalog,
             CAST(null AS sql_identifier) AS scope_schema,
             CAST(null AS sql_identifier) AS scope_name,

             CAST(null AS cardinal_number) AS maximum_cardinality,
             CAST(1 AS sql_identifier) AS dtd_identifier

      FROM (pg_type t JOIN pg_namespace nt ON t.typnamespace = nt.oid)
           JOIN (pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid)
             ON (t.typbasetype = bt.oid AND t.typtype = 'd')
           LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
             ON t.typcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

      WHERE (pg_has_role(t.typowner, 'USAGE')
             OR has_type_privilege(t.oid, 'USAGE'));)",

  // R"(GRANT SELECT ON domains TO PUBLIC;)",

  // 6.31 ELEMENT_TYPES view appears later.

  // 6.32
  // ENABLED_ROLES view

  R"(CREATE VIEW enabled_roles AS
      SELECT CAST(a.rolname AS sql_identifier) AS role_name
      FROM pg_authid a
      WHERE pg_has_role(a.oid, 'USAGE');)",

  // R"(GRANT SELECT ON enabled_roles TO PUBLIC;)",

  // 6.34
  // KEY_COLUMN_USAGE view

  R"(CREATE VIEW key_column_usage AS
      SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(nc.nspname AS sql_identifier) AS constraint_schema,
             CAST(c.conname AS sql_identifier) AS constraint_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nr.nspname AS sql_identifier) AS table_schema,
             CAST(r.relname AS sql_identifier) AS table_name,
             CAST(a.attname AS sql_identifier) AS column_name,
             CAST(array_position(c.conkey, a.attnum) AS cardinal_number) AS ordinal_position,
             CAST(CASE WHEN c.contype = 'f' THEN
                    array_position(i.indkey, c.confkey[array_position(c.conkey, a.attnum)])
                  ELSE NULL
                  END AS cardinal_number)
               AS position_in_unique_constraint
      FROM pg_constraint c
      INNER JOIN pg_class r ON r.oid = c.conrelid
      INNER JOIN pg_namespace nr ON nr.oid = r.relnamespace
      INNER JOIN pg_namespace nc ON nc.oid = c.connamespace
      INNER JOIN pg_attribute a ON a.attrelid = r.oid
                                   AND array_position(c.conkey, a.attnum) IS NOT NULL
                                   AND NOT a.attisdropped
      LEFT JOIN pg_index i ON i.indexrelid = c.conindid
      WHERE c.contype IN ('p', 'u', 'f')
            AND r.relkind IN ('r', 'p')
            AND (NOT pg_is_other_temp_schema(nr.oid))
            AND (pg_has_role(r.relowner, 'USAGE')
                 OR has_column_privilege(r.oid, a.attnum,
                                         'SELECT, INSERT, UPDATE, REFERENCES'));)",

  // R"(GRANT SELECT ON key_column_usage TO PUBLIC;)",

  // 6.38
  // PARAMETERS view

  // TODO(mbkkt) Use original text when implement _pg_expandarray / row field access
  R"(CREATE VIEW parameters AS
      SELECT CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(null AS sql_identifier) AS specific_schema,
             CAST(null AS sql_identifier) AS specific_name,
             CAST(null AS cardinal_number) AS ordinal_position,
             CAST(null AS character_data) AS parameter_mode,
             CAST('NO' AS yes_or_no) AS is_result,
             CAST('NO' AS yes_or_no) AS as_locator,
             CAST(null AS sql_identifier) AS parameter_name,
             CAST(null AS character_data) AS data_type,
             CAST(null AS cardinal_number) AS character_maximum_length,
             CAST(null AS cardinal_number) AS character_octet_length,
             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,
             CAST(null AS sql_identifier) AS collation_catalog,
             CAST(null AS sql_identifier) AS collation_schema,
             CAST(null AS sql_identifier) AS collation_name,
             CAST(null AS cardinal_number) AS numeric_precision,
             CAST(null AS cardinal_number) AS numeric_precision_radix,
             CAST(null AS cardinal_number) AS numeric_scale,
             CAST(null AS cardinal_number) AS datetime_precision,
             CAST(null AS character_data) AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,
             CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(null AS sql_identifier) AS udt_schema,
             CAST(null AS sql_identifier) AS udt_name,
             CAST(null AS sql_identifier) AS scope_catalog,
             CAST(null AS sql_identifier) AS scope_schema,
             CAST(null AS sql_identifier) AS scope_name,
             CAST(null AS cardinal_number) AS maximum_cardinality,
             CAST(null AS sql_identifier) AS dtd_identifier,
             CAST(null AS character_data) AS parameter_default
      WHERE false;)",

  // R"(GRANT SELECT ON parameters TO PUBLIC;)",

  // 6.42
  // REFERENTIAL_CONSTRAINTS view

  R"(CREATE VIEW referential_constraints AS
      SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(ncon.nspname AS sql_identifier) AS constraint_schema,
             CAST(con.conname AS sql_identifier) AS constraint_name,
             CAST(
               CASE WHEN npkc.nspname IS NULL THEN NULL
                    ELSE current_database() END
               AS sql_identifier) AS unique_constraint_catalog,
             CAST(npkc.nspname AS sql_identifier) AS unique_constraint_schema,
             CAST(pkc.conname AS sql_identifier) AS unique_constraint_name,

             CAST(
               CASE con.confmatchtype WHEN 'f' THEN 'FULL'
                                      WHEN 'p' THEN 'PARTIAL'
                                      WHEN 's' THEN 'NONE' END
               AS character_data) AS match_option,

             CAST(
               CASE con.confupdtype WHEN 'c' THEN 'CASCADE'
                                    WHEN 'n' THEN 'SET NULL'
                                    WHEN 'd' THEN 'SET DEFAULT'
                                    WHEN 'r' THEN 'RESTRICT'
                                    WHEN 'a' THEN 'NO ACTION' END
               AS character_data) AS update_rule,

             CAST(
               CASE con.confdeltype WHEN 'c' THEN 'CASCADE'
                                    WHEN 'n' THEN 'SET NULL'
                                    WHEN 'd' THEN 'SET DEFAULT'
                                    WHEN 'r' THEN 'RESTRICT'
                                    WHEN 'a' THEN 'NO ACTION' END
               AS character_data) AS delete_rule

      FROM (pg_namespace ncon
            INNER JOIN pg_constraint con ON ncon.oid = con.connamespace
            INNER JOIN pg_class c ON con.conrelid = c.oid AND con.contype = 'f')
           LEFT JOIN pg_depend d1  -- find constraint's dependency on an index
            ON d1.objid = con.oid AND d1.classid = 'pg_constraint'::regclass
               AND d1.refclassid = 'pg_class'::regclass AND d1.refobjsubid = 0
           LEFT JOIN pg_depend d2  -- find pkey/unique constraint for that index
            ON d2.refclassid = 'pg_constraint'::regclass
               AND d2.classid = 'pg_class'::regclass
               AND d2.objid = d1.refobjid AND d2.objsubid = 0
               AND d2.deptype = 'i'
           LEFT JOIN pg_constraint pkc ON pkc.oid = d2.refobjid
              AND pkc.contype IN ('p', 'u')
              AND pkc.conrelid = con.confrelid
           LEFT JOIN pg_namespace npkc ON pkc.connamespace = npkc.oid

      WHERE pg_has_role(c.relowner, 'USAGE')
            -- SELECT privilege omitted, per SQL standard
            OR has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
            OR has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES') ;)",

  // R"(GRANT SELECT ON referential_constraints TO PUBLIC;)",

  // 6.43
  // ROLE_COLUMN_GRANTS view

  R"(CREATE VIEW role_column_grants AS
      SELECT grantor,
             grantee,
             table_catalog,
             table_schema,
             table_name,
             column_name,
             privilege_type,
             is_grantable
      FROM information_schema.column_privileges
      WHERE grantor IN (SELECT role_name FROM information_schema.enabled_roles)
            OR grantee IN (SELECT role_name FROM information_schema.enabled_roles);)",

  // R"(GRANT SELECT ON role_column_grants TO PUBLIC;)",

  // 6.44 ROLE_ROUTINE_GRANTS view is based on 6.51 ROUTINE_PRIVILEGES and is defined there instead.

  // 6.45 ROLE_TABLE_GRANTS view is based on 6.64 TABLE_PRIVILEGES and is defined there instead.

  // 6.46
  // ROLE_TABLE_METHOD_GRANTS view

  // 6.47 ROLE_USAGE_GRANTS view is based on 6.76 USAGE_PRIVILEGES and is defined there instead.

  // 6.48 ROLE_UDT_GRANTS view is based on 6.75 UDT_PRIVILEGES and is defined there instead.

  // 6.49
  // ROUTINE_COLUMN_USAGE view

  R"(CREATE VIEW routine_column_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(current_database() AS sql_identifier) AS routine_catalog,
             CAST(np.nspname AS sql_identifier) AS routine_schema,
             CAST(p.proname AS sql_identifier) AS routine_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nt.nspname AS sql_identifier) AS table_schema,
             CAST(t.relname AS sql_identifier) AS table_name,
             CAST(a.attname AS sql_identifier) AS column_name

      FROM pg_namespace np, pg_proc p, pg_depend d,
           pg_class t, pg_namespace nt, pg_attribute a

      WHERE np.oid = p.pronamespace
            AND p.oid = d.objid
            AND d.classid = 'pg_catalog.pg_proc'::regclass
            AND d.refobjid = t.oid
            AND d.refclassid = 'pg_catalog.pg_class'::regclass
            AND t.relnamespace = nt.oid
            AND t.relkind IN ('r', 'v', 'f', 'p')
            AND t.oid = a.attrelid
            AND d.refobjsubid = a.attnum
            AND pg_has_role(t.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON routine_column_usage TO PUBLIC;)",

  // 6.51
  // ROUTINE_PRIVILEGES view

  // TODO(mbkkt) Use original text when implement (aclexplode(...)).* row expansion
  R"(CREATE VIEW routine_privileges AS
      SELECT CAST(null AS sql_identifier) AS grantor,
             CAST(null AS sql_identifier) AS grantee,
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(null AS sql_identifier) AS specific_schema,
             CAST(null AS sql_identifier) AS specific_name,
             CAST(current_database() AS sql_identifier) AS routine_catalog,
             CAST(null AS sql_identifier) AS routine_schema,
             CAST(null AS sql_identifier) AS routine_name,
             CAST('EXECUTE' AS character_data) AS privilege_type,
             CAST(null AS yes_or_no) AS is_grantable
      WHERE false;)",

  // R"(GRANT SELECT ON routine_privileges TO PUBLIC;)",

  // 6.43
  // ROLE_ROUTINE_GRANTS view

  R"(CREATE VIEW role_routine_grants AS
      SELECT grantor,
             grantee,
             specific_catalog,
             specific_schema,
             specific_name,
             routine_catalog,
             routine_schema,
             routine_name,
             privilege_type,
             is_grantable
      FROM information_schema.routine_privileges
      WHERE grantor IN (SELECT role_name FROM information_schema.enabled_roles)
            OR grantee IN (SELECT role_name FROM information_schema.enabled_roles);)",

  // R"(GRANT SELECT ON role_routine_grants TO PUBLIC;)",

  // 6.52
  // ROUTINE_ROUTINE_USAGE view

  R"(CREATE VIEW routine_routine_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(current_database() AS sql_identifier) AS routine_catalog,
             CAST(np1.nspname AS sql_identifier) AS routine_schema,
             CAST(nameconcatoid(p1.proname, p1.oid) AS sql_identifier) AS routine_name

      FROM pg_namespace np, pg_proc p, pg_depend d,
           pg_proc p1, pg_namespace np1

      WHERE np.oid = p.pronamespace
            AND p.oid = d.objid
            AND d.classid = 'pg_catalog.pg_proc'::regclass
            AND d.refobjid = p1.oid
            AND d.refclassid = 'pg_catalog.pg_proc'::regclass
            AND p1.pronamespace = np1.oid
            AND p.prokind IN ('f', 'p') AND p1.prokind IN ('f', 'p')
            AND pg_has_role(p1.proowner, 'USAGE');)",

  // R"(GRANT SELECT ON routine_routine_usage TO PUBLIC;)",

  // 6.53
  // ROUTINE_SEQUENCE_USAGE view

  R"(CREATE VIEW routine_sequence_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(current_database() AS sql_identifier) AS routine_catalog,
             CAST(np.nspname AS sql_identifier) AS routine_schema,
             CAST(p.proname AS sql_identifier) AS routine_name,
             CAST(current_database() AS sql_identifier) AS sequence_catalog,
             CAST(ns.nspname AS sql_identifier) AS sequence_schema,
             CAST(s.relname AS sql_identifier) AS sequence_name

      FROM pg_namespace np, pg_proc p, pg_depend d,
           pg_class s, pg_namespace ns

      WHERE np.oid = p.pronamespace
            AND p.oid = d.objid
            AND d.classid = 'pg_catalog.pg_proc'::regclass
            AND d.refobjid = s.oid
            AND d.refclassid = 'pg_catalog.pg_class'::regclass
            AND s.relnamespace = ns.oid
            AND s.relkind = 'S'
            AND pg_has_role(s.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON routine_sequence_usage TO PUBLIC;)",

  // 6.54
  // ROUTINE_TABLE_USAGE view

  R"(CREATE VIEW routine_table_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(current_database() AS sql_identifier) AS routine_catalog,
             CAST(np.nspname AS sql_identifier) AS routine_schema,
             CAST(p.proname AS sql_identifier) AS routine_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nt.nspname AS sql_identifier) AS table_schema,
             CAST(t.relname AS sql_identifier) AS table_name

      FROM pg_namespace np, pg_proc p, pg_depend d,
           pg_class t, pg_namespace nt

      WHERE np.oid = p.pronamespace
            AND p.oid = d.objid
            AND d.classid = 'pg_catalog.pg_proc'::regclass
            AND d.refobjid = t.oid
            AND d.refclassid = 'pg_catalog.pg_class'::regclass
            AND t.relnamespace = nt.oid
            AND t.relkind IN ('r', 'v', 'f', 'p')
            AND pg_has_role(t.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON routine_table_usage TO PUBLIC;)",

  // 6.55
  // ROUTINES view

  R"(CREATE VIEW routines AS
      SELECT CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(n.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(current_database() AS sql_identifier) AS routine_catalog,
             CAST(n.nspname AS sql_identifier) AS routine_schema,
             CAST(p.proname AS sql_identifier) AS routine_name,
             CAST(CASE p.prokind WHEN 'f' THEN 'FUNCTION' WHEN 'p' THEN 'PROCEDURE' END
               AS character_data) AS routine_type,
             CAST(null AS sql_identifier) AS module_catalog,
             CAST(null AS sql_identifier) AS module_schema,
             CAST(null AS sql_identifier) AS module_name,
             CAST(null AS sql_identifier) AS udt_catalog,
             CAST(null AS sql_identifier) AS udt_schema,
             CAST(null AS sql_identifier) AS udt_name,

             CAST(
               CASE WHEN p.prokind = 'p' THEN NULL
                    WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                    WHEN nt.nspname = 'pg_catalog' THEN format_type(t.oid, null)
                    ELSE 'USER-DEFINED' END AS character_data)
               AS data_type,
             CAST(null AS cardinal_number) AS character_maximum_length,
             CAST(null AS cardinal_number) AS character_octet_length,
             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,
             CAST(null AS sql_identifier) AS collation_catalog,
             CAST(null AS sql_identifier) AS collation_schema,
             CAST(null AS sql_identifier) AS collation_name,
             CAST(null AS cardinal_number) AS numeric_precision,
             CAST(null AS cardinal_number) AS numeric_precision_radix,
             CAST(null AS cardinal_number) AS numeric_scale,
             CAST(null AS cardinal_number) AS datetime_precision,
             CAST(null AS character_data) AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,
             CAST(CASE WHEN nt.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS type_udt_catalog,
             CAST(nt.nspname AS sql_identifier) AS type_udt_schema,
             CAST(t.typname AS sql_identifier) AS type_udt_name,
             CAST(null AS sql_identifier) AS scope_catalog,
             CAST(null AS sql_identifier) AS scope_schema,
             CAST(null AS sql_identifier) AS scope_name,
             CAST(null AS cardinal_number) AS maximum_cardinality,
             CAST(CASE WHEN p.prokind <> 'p' THEN 0 END AS sql_identifier) AS dtd_identifier,

             CAST(CASE WHEN l.lanname = 'sql' THEN 'SQL' ELSE 'EXTERNAL' END AS character_data)
               AS routine_body,
             CAST(
               CASE WHEN pg_has_role(p.proowner, 'USAGE') THEN p.prosrc ELSE null END
               AS character_data) AS routine_definition,
             CAST(
               CASE WHEN l.lanname = 'c' THEN p.prosrc ELSE null END
               AS character_data) AS external_name,
             CAST(upper(l.lanname) AS character_data) AS external_language,

             CAST('GENERAL' AS character_data) AS parameter_style,
             CAST(CASE WHEN p.provolatile = 'i' THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_deterministic,
             CAST('MODIFIES' AS character_data) AS sql_data_access,
             CAST(CASE WHEN p.prokind <> 'p' THEN
               CASE WHEN p.proisstrict THEN 'YES' ELSE 'NO' END END AS yes_or_no) AS is_null_call,
             CAST(null AS character_data) AS sql_path,
             CAST('YES' AS yes_or_no) AS schema_level_routine,
             CAST(0 AS cardinal_number) AS max_dynamic_result_sets,
             CAST(null AS yes_or_no) AS is_user_defined_cast,
             CAST(null AS yes_or_no) AS is_implicitly_invocable,
             CAST(CASE WHEN p.prosecdef THEN 'DEFINER' ELSE 'INVOKER' END AS character_data) AS security_type,
             CAST(null AS sql_identifier) AS to_sql_specific_catalog,
             CAST(null AS sql_identifier) AS to_sql_specific_schema,
             CAST(null AS sql_identifier) AS to_sql_specific_name,
             CAST('NO' AS yes_or_no) AS as_locator,
             CAST(null AS time_stamp) AS created,
             CAST(null AS time_stamp) AS last_altered,
             CAST(null AS yes_or_no) AS new_savepoint_level,
             CAST('NO' AS yes_or_no) AS is_udt_dependent,

             CAST(null AS character_data) AS result_cast_from_data_type,
             CAST(null AS yes_or_no) AS result_cast_as_locator,
             CAST(null AS cardinal_number) AS result_cast_char_max_length,
             CAST(null AS cardinal_number) AS result_cast_char_octet_length,
             CAST(null AS sql_identifier) AS result_cast_char_set_catalog,
             CAST(null AS sql_identifier) AS result_cast_char_set_schema,
             CAST(null AS sql_identifier) AS result_cast_char_set_name,
             CAST(null AS sql_identifier) AS result_cast_collation_catalog,
             CAST(null AS sql_identifier) AS result_cast_collation_schema,
             CAST(null AS sql_identifier) AS result_cast_collation_name,
             CAST(null AS cardinal_number) AS result_cast_numeric_precision,
             CAST(null AS cardinal_number) AS result_cast_numeric_precision_radix,
             CAST(null AS cardinal_number) AS result_cast_numeric_scale,
             CAST(null AS cardinal_number) AS result_cast_datetime_precision,
             CAST(null AS character_data) AS result_cast_interval_type,
             CAST(null AS cardinal_number) AS result_cast_interval_precision,
             CAST(null AS sql_identifier) AS result_cast_type_udt_catalog,
             CAST(null AS sql_identifier) AS result_cast_type_udt_schema,
             CAST(null AS sql_identifier) AS result_cast_type_udt_name,
             CAST(null AS sql_identifier) AS result_cast_scope_catalog,
             CAST(null AS sql_identifier) AS result_cast_scope_schema,
             CAST(null AS sql_identifier) AS result_cast_scope_name,
             CAST(null AS cardinal_number) AS result_cast_maximum_cardinality,
             CAST(null AS sql_identifier) AS result_cast_dtd_identifier

      FROM (pg_namespace n
            JOIN pg_proc p ON n.oid = p.pronamespace
            JOIN pg_language l ON p.prolang = l.oid)
           LEFT JOIN
           (pg_type t JOIN pg_namespace nt ON t.typnamespace = nt.oid)
           ON p.prorettype = t.oid AND p.prokind <> 'p'

      WHERE (pg_has_role(p.proowner, 'USAGE')
             OR has_function_privilege(p.oid, 'EXECUTE'));)",

  // R"(GRANT SELECT ON routines TO PUBLIC;)",

  // 6.56
  // SCHEMATA view

  R"(CREATE VIEW schemata AS
      SELECT CAST(current_database() AS sql_identifier) AS catalog_name,
             CAST(n.nspname AS sql_identifier) AS schema_name,
             CAST(u.rolname AS sql_identifier) AS schema_owner,
             CAST(null AS sql_identifier) AS default_character_set_catalog,
             CAST(null AS sql_identifier) AS default_character_set_schema,
             CAST(null AS sql_identifier) AS default_character_set_name,
             CAST(null AS character_data) AS sql_path
      FROM pg_namespace n, pg_authid u
      WHERE n.nspowner = u.oid
            AND (pg_has_role(n.nspowner, 'USAGE')
                 OR has_schema_privilege(n.oid, 'CREATE, USAGE'));)",

  // R"(GRANT SELECT ON schemata TO PUBLIC;)",

  // 6.57
  // SEQUENCES view

  R"(CREATE VIEW sequences AS
      SELECT CAST(current_database() AS sql_identifier) AS sequence_catalog,
             CAST(nc.nspname AS sql_identifier) AS sequence_schema,
             CAST(c.relname AS sql_identifier) AS sequence_name,
             CAST(format_type(s.seqtypid, null) AS character_data) AS data_type,
             CAST(information_schema._pg_numeric_precision(s.seqtypid, -1) AS cardinal_number) AS numeric_precision,
             CAST(2 AS cardinal_number) AS numeric_precision_radix,
             CAST(0 AS cardinal_number) AS numeric_scale,
             CAST(s.seqstart AS character_data) AS start_value,
             CAST(s.seqmin AS character_data) AS minimum_value,
             CAST(s.seqmax AS character_data) AS maximum_value,
             CAST(s.seqincrement AS character_data) AS increment,
             CAST(CASE WHEN s.seqcycle THEN 'YES' ELSE 'NO' END AS yes_or_no) AS cycle_option
      FROM pg_namespace nc, pg_class c, pg_sequence s
      WHERE c.relnamespace = nc.oid
            AND c.relkind = 'S'
            AND NOT EXISTS (SELECT 1 FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = c.oid AND deptype = 'i')
            AND (NOT pg_is_other_temp_schema(nc.oid))
            AND c.oid = s.seqrelid
            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_sequence_privilege(c.oid, 'SELECT, UPDATE, USAGE') );)",

  // R"(GRANT SELECT ON sequences TO PUBLIC;)",

  // 6.58
  // SQL_FEATURES table

  // R"(CREATE TABLE sql_features (
  //     feature_id          character_data,
  //     feature_name        character_data,
  //     sub_feature_id      character_data,
  //     sub_feature_name    character_data,
  //     is_supported        yes_or_no,
  //     is_verified_by      character_data,
  //     comments            character_data
  // );)",

  // Will be filled with external data by initdb.

  // R"(GRANT SELECT ON sql_features TO PUBLIC;)",

  // 6.59
  // SQL_IMPLEMENTATION_INFO table

  // R"(CREATE TABLE sql_implementation_info (
  //     implementation_info_id      character_data,
  //     implementation_info_name    character_data,
  //     integer_value               cardinal_number,
  //     character_value             character_data,
  //     comments                    character_data
  // );)",

  // R"(INSERT INTO sql_implementation_info VALUES ('10003', 'CATALOG NAME', NULL, 'Y', NULL);)",
  // R"(INSERT INTO sql_implementation_info VALUES ('10004', 'COLLATING SEQUENCE', NULL, (SELECT default_collate_name FROM character_sets), NULL);)",
  // R"(INSERT INTO sql_implementation_info VALUES ('23',    'CURSOR COMMIT BEHAVIOR', 1, NULL, 'close cursors and retain prepared statements');)",
  // R"(INSERT INTO sql_implementation_info VALUES ('2',     'DATA SOURCE NAME', NULL, '', NULL);)",
  // R"(INSERT INTO sql_implementation_info VALUES ('17',    'DBMS NAME', NULL, (select trim(trailing ' ' from substring(version() from '^[^0-9]*'))), NULL);)",
  // R"(INSERT INTO sql_implementation_info VALUES ('18',    'DBMS VERSION', NULL, '???', NULL); -- filled by initdb)",
  // R"(INSERT INTO sql_implementation_info VALUES ('26',    'DEFAULT TRANSACTION ISOLATION', 2, NULL, 'READ COMMITTED; user-settable');)",
  // R"(INSERT INTO sql_implementation_info VALUES ('28',    'IDENTIFIER CASE', 3, NULL, 'stored in mixed case - case sensitive');)",
  // R"(INSERT INTO sql_implementation_info VALUES ('85',    'NULL COLLATION', 0, NULL, 'nulls higher than non-nulls');)",
  // R"(INSERT INTO sql_implementation_info VALUES ('13',    'SERVER NAME', NULL, '', NULL);)",
  // R"(INSERT INTO sql_implementation_info VALUES ('94',    'SPECIAL CHARACTERS', NULL, '', 'all non-ASCII characters allowed');)",
  // R"(INSERT INTO sql_implementation_info VALUES ('46',    'TRANSACTION CAPABLE', 2, NULL, 'both DML and DDL');)",

  // R"(GRANT SELECT ON sql_implementation_info TO PUBLIC;)",

  // 6.60
  // SQL_PARTS table

  // R"(CREATE TABLE sql_parts (
  //     feature_id      character_data,
  //     feature_name    character_data,
  //     is_supported    yes_or_no,
  //     is_verified_by  character_data,
  //     comments        character_data
  // );)",

  // R"(INSERT INTO sql_parts VALUES ('1', 'Framework (SQL/Framework)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('2', 'Foundation (SQL/Foundation)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('3', 'Call-Level Interface (SQL/CLI)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('4', 'Persistent Stored Modules (SQL/PSM)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('9', 'Management of External Data (SQL/MED)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('10', 'Object Language Bindings (SQL/OLB)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('11', 'Information and Definition Schema (SQL/Schemata)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('13', 'Routines and Types Using the Java Programming Language (SQL/JRT)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('14', 'XML-Related Specifications (SQL/XML)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('15', 'Multi-Dimensional Arrays (SQL/MDA)', 'NO', NULL, '');)",
  // R"(INSERT INTO sql_parts VALUES ('16', 'Property Graph Queries (SQL/PGQ)', 'NO', NULL, '');)",

  // 6.61
  // SQL_SIZING table

  // R"(CREATE TABLE sql_sizing (
  //     sizing_id       cardinal_number,
  //     sizing_name     character_data,
  //     supported_value cardinal_number,
  //     comments        character_data
  // );)",

  // R"(INSERT INTO sql_sizing VALUES (34,    'MAXIMUM CATALOG NAME LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (30,    'MAXIMUM COLUMN NAME LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (97,    'MAXIMUM COLUMNS IN GROUP BY', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (99,    'MAXIMUM COLUMNS IN ORDER BY', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (100,   'MAXIMUM COLUMNS IN SELECT', 1664, NULL); -- match MaxTupleAttributeNumber)",
  // R"(INSERT INTO sql_sizing VALUES (101,   'MAXIMUM COLUMNS IN TABLE', 1600, NULL); -- match MaxHeapAttributeNumber)",
  // R"(INSERT INTO sql_sizing VALUES (1,     'MAXIMUM CONCURRENT ACTIVITIES', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (31,    'MAXIMUM CURSOR NAME LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (0,     'MAXIMUM DRIVER CONNECTIONS', NULL, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (10005, 'MAXIMUM IDENTIFIER LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (32,    'MAXIMUM SCHEMA NAME LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (20000, 'MAXIMUM STATEMENT OCTETS', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (20001, 'MAXIMUM STATEMENT OCTETS DATA', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (20002, 'MAXIMUM STATEMENT OCTETS SCHEMA', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (35,    'MAXIMUM TABLE NAME LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (106,   'MAXIMUM TABLES IN SELECT', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (107,   'MAXIMUM USER NAME LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (25000, 'MAXIMUM CURRENT DEFAULT TRANSFORM GROUP LENGTH', NULL, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (25001, 'MAXIMUM CURRENT TRANSFORM GROUP LENGTH', NULL, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (25002, 'MAXIMUM CURRENT PATH LENGTH', 0, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (25003, 'MAXIMUM CURRENT ROLE LENGTH', NULL, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (25004, 'MAXIMUM SESSION USER LENGTH', 63, NULL);)",
  // R"(INSERT INTO sql_sizing VALUES (25005, 'MAXIMUM SYSTEM USER LENGTH', 63, NULL);)",

  // R"(UPDATE sql_sizing
  //     SET supported_value = (SELECT typlen-1 FROM pg_catalog.pg_type WHERE typname = 'name'),
  //         comments = 'Might be less, depending on character set.'
  //     WHERE supported_value = 63;)",

  // R"(GRANT SELECT ON sql_sizing TO PUBLIC;)",

  // 6.62
  // TABLE_CONSTRAINTS view

  R"(CREATE VIEW table_constraints AS
      SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
             CAST(nc.nspname AS sql_identifier) AS constraint_schema,
             CAST(c.conname AS sql_identifier) AS constraint_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nr.nspname AS sql_identifier) AS table_schema,
             CAST(r.relname AS sql_identifier) AS table_name,
             CAST(
               CASE c.contype WHEN 'c' THEN 'CHECK'
                              WHEN 'n' THEN 'CHECK'
                              WHEN 'f' THEN 'FOREIGN KEY'
                              WHEN 'p' THEN 'PRIMARY KEY'
                              WHEN 'u' THEN 'UNIQUE' END
               AS character_data) AS constraint_type,
             CAST(CASE WHEN c.condeferrable THEN 'YES' ELSE 'NO' END AS yes_or_no)
               AS is_deferrable,
             CAST(CASE WHEN c.condeferred THEN 'YES' ELSE 'NO' END AS yes_or_no)
               AS initially_deferred,
             CAST(CASE WHEN c.conenforced THEN 'YES' ELSE 'NO' END AS yes_or_no) AS enforced,
             CAST(CASE WHEN c.contype = 'u'
                       THEN CASE WHEN (SELECT NOT indnullsnotdistinct FROM pg_index WHERE indexrelid = conindid) THEN 'YES' ELSE 'NO' END
                       END
                  AS yes_or_no) AS nulls_distinct

      FROM pg_namespace nc,
           pg_namespace nr,
           pg_constraint c,
           pg_class r

      WHERE nc.oid = c.connamespace AND nr.oid = r.relnamespace
            AND c.conrelid = r.oid
            AND c.contype NOT IN ('t', 'x')  -- ignore nonstandard constraints
            AND r.relkind IN ('r', 'p')
            AND (NOT pg_is_other_temp_schema(nr.oid))
            AND (pg_has_role(r.relowner, 'USAGE')
                 -- SELECT privilege omitted, per SQL standard
                 OR has_table_privilege(r.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                 OR has_any_column_privilege(r.oid, 'INSERT, UPDATE, REFERENCES') );)",

  // R"(GRANT SELECT ON table_constraints TO PUBLIC;)",

  // 6.64
  // TABLE_PRIVILEGES view

  // TODO(mbkkt) Use original text when implement (aclexplode(...)).* row expansion
  R"(CREATE VIEW table_privileges AS
      SELECT CAST(null AS sql_identifier) AS grantor,
             CAST(null AS sql_identifier) AS grantee,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(null AS sql_identifier) AS table_schema,
             CAST(null AS sql_identifier) AS table_name,
             CAST(null AS character_data) AS privilege_type,
             CAST(null AS yes_or_no) AS is_grantable,
             CAST(null AS yes_or_no) AS with_hierarchy
      WHERE false;)",

  // R"(GRANT SELECT ON table_privileges TO PUBLIC;)",

  // 6.45
  // ROLE_TABLE_GRANTS view

  R"(CREATE VIEW role_table_grants AS
      SELECT grantor,
             grantee,
             table_catalog,
             table_schema,
             table_name,
             privilege_type,
             is_grantable,
             with_hierarchy
      FROM information_schema.table_privileges
      WHERE grantor IN (SELECT role_name FROM information_schema.enabled_roles)
            OR grantee IN (SELECT role_name FROM information_schema.enabled_roles);)",

  // R"(GRANT SELECT ON role_table_grants TO PUBLIC;)",

  // 6.65
  // TABLES view

  R"(CREATE VIEW tables AS
      SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nc.nspname AS sql_identifier) AS table_schema,
             CAST(c.relname AS sql_identifier) AS table_name,

             CAST(
               CASE WHEN nc.oid = pg_my_temp_schema() THEN 'LOCAL TEMPORARY'
                    WHEN c.relkind IN ('r', 'p') THEN 'BASE TABLE'
                    WHEN c.relkind = 'v' THEN 'VIEW'
                    WHEN c.relkind = 'f' THEN 'FOREIGN'
                    ELSE null END
               AS character_data) AS table_type,

             CAST(null AS sql_identifier) AS self_referencing_column_name,
             CAST(null AS character_data) AS reference_generation,

             CAST(CASE WHEN t.typname IS NOT NULL THEN current_database() ELSE null END AS sql_identifier) AS user_defined_type_catalog,
             CAST(nt.nspname AS sql_identifier) AS user_defined_type_schema,
             CAST(t.typname AS sql_identifier) AS user_defined_type_name,

             CAST(CASE WHEN c.relkind IN ('r', 'p') OR
                            (c.relkind IN ('v', 'f') AND
                             -- 1 << CMD_INSERT
                             pg_relation_is_updatable(c.oid, false) & 8 = 8)
                  THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_insertable_into,

             CAST(CASE WHEN t.typname IS NOT NULL THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_typed,
             CAST(null AS character_data) AS commit_action

      FROM pg_namespace nc JOIN pg_class c ON (nc.oid = c.relnamespace)
             LEFT JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON (c.reloftype = t.oid)

      WHERE c.relkind IN ('r', 'v', 'f', 'p')
            AND (NOT pg_is_other_temp_schema(nc.oid))
            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                 OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );)",

  // R"(GRANT SELECT ON tables TO PUBLIC;)",

  // 6.66
  // TRANSFORMS view

  R"(CREATE VIEW transforms AS
      SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(nt.nspname AS sql_identifier) AS udt_schema,
             CAST(t.typname AS sql_identifier) AS udt_name,
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(l.lanname AS sql_identifier) AS group_name,
             CAST('FROM SQL' AS character_data) AS transform_type
      FROM pg_type t JOIN pg_transform x ON t.oid = x.trftype
           JOIN pg_language l ON x.trflang = l.oid
           JOIN pg_proc p ON x.trffromsql = p.oid
           JOIN pg_namespace nt ON t.typnamespace = nt.oid
           JOIN pg_namespace np ON p.pronamespace = np.oid

    UNION

      SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(nt.nspname AS sql_identifier) AS udt_schema,
             CAST(t.typname AS sql_identifier) AS udt_name,
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
             CAST(l.lanname AS sql_identifier) AS group_name,
             CAST('TO SQL' AS character_data) AS transform_type
      FROM pg_type t JOIN pg_transform x ON t.oid = x.trftype
           JOIN pg_language l ON x.trflang = l.oid
           JOIN pg_proc p ON x.trftosql = p.oid
           JOIN pg_namespace nt ON t.typnamespace = nt.oid
           JOIN pg_namespace np ON p.pronamespace = np.oid

    ORDER BY udt_catalog, udt_schema, udt_name, group_name, transform_type  -- some sensible grouping for interactive use
  ;)",

  // 6.68
  // TRIGGERED_UPDATE_COLUMNS view

  // TODO(mbkkt) Use original text when implement _pg_expandarray / row field access
  R"(CREATE VIEW triggered_update_columns AS
      SELECT CAST(current_database() AS sql_identifier) AS trigger_catalog,
             CAST(null AS sql_identifier) AS trigger_schema,
             CAST(null AS sql_identifier) AS trigger_name,
             CAST(current_database() AS sql_identifier) AS event_object_catalog,
             CAST(null AS sql_identifier) AS event_object_schema,
             CAST(null AS sql_identifier) AS event_object_table,
             CAST(null AS sql_identifier) AS event_object_column
      WHERE false;)",

  // R"(GRANT SELECT ON triggered_update_columns TO PUBLIC;)",

  // 6.74
  // TRIGGERS view

  R"(CREATE VIEW triggers AS
      SELECT CAST(current_database() AS sql_identifier) AS trigger_catalog,
             CAST(n.nspname AS sql_identifier) AS trigger_schema,
             CAST(t.tgname AS sql_identifier) AS trigger_name,
             CAST(em.text AS character_data) AS event_manipulation,
             CAST(current_database() AS sql_identifier) AS event_object_catalog,
             CAST(n.nspname AS sql_identifier) AS event_object_schema,
             CAST(c.relname AS sql_identifier) AS event_object_table,
             CAST(
               -- To determine action order, partition by schema, table,
               -- event_manipulation (INSERT/DELETE/UPDATE), ROW/STATEMENT (1),
               -- BEFORE/AFTER (66), then order by trigger name.  It's preferable
               -- to partition by view output columns, so that query constraints
               -- can be pushed down below the window function.
               rank() OVER (PARTITION BY CAST(n.nspname AS sql_identifier),
                                         CAST(c.relname AS sql_identifier),
                                         em.num,
                                         t.tgtype & 1,
                                         t.tgtype & 66
                                         ORDER BY t.tgname)
               AS cardinal_number) AS action_order,
             CAST(
               CASE WHEN pg_has_role(c.relowner, 'USAGE')
                 THEN (regexp_match(pg_get_triggerdef(t.oid), E'.{35,} WHEN \\((.+)\\) EXECUTE FUNCTION'))[1]
                 ELSE null END
               AS character_data) AS action_condition,
             CAST(
               substring(pg_get_triggerdef(t.oid) from
                         position('EXECUTE FUNCTION' in substring(pg_get_triggerdef(t.oid) from 48)) + 47)
               AS character_data) AS action_statement,
             CAST(
               -- hard-wired reference to TRIGGER_TYPE_ROW
               CASE t.tgtype & 1 WHEN 1 THEN 'ROW' ELSE 'STATEMENT' END
               AS character_data) AS action_orientation,
             CAST(
               -- hard-wired refs to TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSTEAD
               CASE t.tgtype & 66 WHEN 2 THEN 'BEFORE' WHEN 64 THEN 'INSTEAD OF' ELSE 'AFTER' END
               AS character_data) AS action_timing,
             CAST(tgoldtable AS sql_identifier) AS action_reference_old_table,
             CAST(tgnewtable AS sql_identifier) AS action_reference_new_table,
             CAST(null AS sql_identifier) AS action_reference_old_row,
             CAST(null AS sql_identifier) AS action_reference_new_row,
             CAST(null AS time_stamp) AS created

      FROM pg_namespace n, pg_class c, pg_trigger t,
           -- hard-wired refs to TRIGGER_TYPE_INSERT, TRIGGER_TYPE_DELETE,
           -- TRIGGER_TYPE_UPDATE; we intentionally omit TRIGGER_TYPE_TRUNCATE
           (VALUES (4, 'INSERT'),
                   (8, 'DELETE'),
                   (16, 'UPDATE')) AS em (num, text)

      WHERE n.oid = c.relnamespace
            AND c.oid = t.tgrelid
            AND t.tgtype & em.num <> 0
            AND NOT t.tgisinternal
            AND (NOT pg_is_other_temp_schema(n.oid))
            AND (pg_has_role(c.relowner, 'USAGE')
                 -- SELECT privilege omitted, per SQL standard
                 OR has_table_privilege(c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                 OR has_any_column_privilege(c.oid, 'INSERT, UPDATE, REFERENCES') );)",

  // R"(GRANT SELECT ON triggers TO PUBLIC;)",

  // 6.75
  // UDT_PRIVILEGES view

  // TODO(mbkkt) Use original text when implement (aclexplode(...)).* row expansion
  R"(CREATE VIEW udt_privileges AS
      SELECT CAST(null AS sql_identifier) AS grantor,
             CAST(null AS sql_identifier) AS grantee,
             CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(null AS sql_identifier) AS udt_schema,
             CAST(null AS sql_identifier) AS udt_name,
             CAST('TYPE USAGE' AS character_data) AS privilege_type,
             CAST(null AS yes_or_no) AS is_grantable
      WHERE false;)",

  // R"(GRANT SELECT ON udt_privileges TO PUBLIC;)",

  // 6.48
  // ROLE_UDT_GRANTS view

  R"(CREATE VIEW role_udt_grants AS
      SELECT grantor,
             grantee,
             udt_catalog,
             udt_schema,
             udt_name,
             privilege_type,
             is_grantable
      FROM information_schema.udt_privileges
      WHERE grantor IN (SELECT role_name FROM information_schema.enabled_roles)
            OR grantee IN (SELECT role_name FROM information_schema.enabled_roles);)",

  // R"(GRANT SELECT ON role_udt_grants TO PUBLIC;)",

  // 6.76
  // USAGE_PRIVILEGES view

  // TODO(mbkkt) Use original text when implement (aclexplode(...)).* row expansion
  R"(CREATE VIEW usage_privileges AS
      SELECT CAST(null AS sql_identifier) AS grantor,
             CAST(null AS sql_identifier) AS grantee,
             CAST(current_database() AS sql_identifier) AS object_catalog,
             CAST(null AS sql_identifier) AS object_schema,
             CAST(null AS sql_identifier) AS object_name,
             CAST(null AS character_data) AS object_type,
             CAST(null AS character_data) AS privilege_type,
             CAST(null AS yes_or_no) AS is_grantable
      WHERE false;)",

  // R"(GRANT SELECT ON usage_privileges TO PUBLIC;)",

  // 6.47
  // ROLE_USAGE_GRANTS view

  R"(CREATE VIEW role_usage_grants AS
      SELECT grantor,
             grantee,
             object_catalog,
             object_schema,
             object_name,
             object_type,
             privilege_type,
             is_grantable
      FROM information_schema.usage_privileges
      WHERE grantor IN (SELECT role_name FROM information_schema.enabled_roles)
            OR grantee IN (SELECT role_name FROM information_schema.enabled_roles);)",

  // R"(GRANT SELECT ON role_usage_grants TO PUBLIC;)",

  // 6.77
  // USER_DEFINED_TYPES view

  R"(CREATE VIEW user_defined_types AS
      SELECT CAST(current_database() AS sql_identifier) AS user_defined_type_catalog,
             CAST(n.nspname AS sql_identifier) AS user_defined_type_schema,
             CAST(c.relname AS sql_identifier) AS user_defined_type_name,
             CAST('STRUCTURED' AS character_data) AS user_defined_type_category,
             CAST('YES' AS yes_or_no) AS is_instantiable,
             CAST(null AS yes_or_no) AS is_final,
             CAST(null AS character_data) AS ordering_form,
             CAST(null AS character_data) AS ordering_category,
             CAST(null AS sql_identifier) AS ordering_routine_catalog,
             CAST(null AS sql_identifier) AS ordering_routine_schema,
             CAST(null AS sql_identifier) AS ordering_routine_name,
             CAST(null AS character_data) AS reference_type,
             CAST(null AS character_data) AS data_type,
             CAST(null AS cardinal_number) AS character_maximum_length,
             CAST(null AS cardinal_number) AS character_octet_length,
             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,
             CAST(null AS sql_identifier) AS collation_catalog,
             CAST(null AS sql_identifier) AS collation_schema,
             CAST(null AS sql_identifier) AS collation_name,
             CAST(null AS cardinal_number) AS numeric_precision,
             CAST(null AS cardinal_number) AS numeric_precision_radix,
             CAST(null AS cardinal_number) AS numeric_scale,
             CAST(null AS cardinal_number) AS datetime_precision,
             CAST(null AS character_data) AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,
             CAST(null AS sql_identifier) AS source_dtd_identifier,
             CAST(null AS sql_identifier) AS ref_dtd_identifier

      FROM pg_namespace n, pg_class c, pg_type t

      WHERE n.oid = c.relnamespace
            AND t.typrelid = c.oid
            AND c.relkind = 'c'
            AND (pg_has_role(t.typowner, 'USAGE')
                 OR has_type_privilege(t.oid, 'USAGE'));)",

  // R"(GRANT SELECT ON user_defined_types TO PUBLIC;)",

  // 6.78
  // VIEW_COLUMN_USAGE

  R"(CREATE VIEW view_column_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS view_catalog,
             CAST(nv.nspname AS sql_identifier) AS view_schema,
             CAST(v.relname AS sql_identifier) AS view_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nt.nspname AS sql_identifier) AS table_schema,
             CAST(t.relname AS sql_identifier) AS table_name,
             CAST(a.attname AS sql_identifier) AS column_name

      FROM pg_namespace nv, pg_class v, pg_depend dv,
           pg_depend dt, pg_class t, pg_namespace nt,
           pg_attribute a

      WHERE nv.oid = v.relnamespace
            AND v.relkind = 'v'
            AND v.oid = dv.refobjid
            AND dv.refclassid = 'pg_catalog.pg_class'::regclass
            AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
            AND dv.deptype = 'i'
            AND dv.objid = dt.objid
            AND dv.refobjid <> dt.refobjid
            AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
            AND dt.refclassid = 'pg_catalog.pg_class'::regclass
            AND dt.refobjid = t.oid
            AND t.relnamespace = nt.oid
            AND t.relkind IN ('r', 'v', 'f', 'p')
            AND t.oid = a.attrelid
            AND dt.refobjsubid = a.attnum
            AND pg_has_role(t.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON view_column_usage TO PUBLIC;)",

  // 6.80
  // VIEW_ROUTINE_USAGE

  R"(CREATE VIEW view_routine_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nv.nspname AS sql_identifier) AS table_schema,
             CAST(v.relname AS sql_identifier) AS table_name,
             CAST(current_database() AS sql_identifier) AS specific_catalog,
             CAST(np.nspname AS sql_identifier) AS specific_schema,
             CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name

      FROM pg_namespace nv, pg_class v, pg_depend dv,
           pg_depend dp, pg_proc p, pg_namespace np

      WHERE nv.oid = v.relnamespace
            AND v.relkind = 'v'
            AND v.oid = dv.refobjid
            AND dv.refclassid = 'pg_catalog.pg_class'::regclass
            AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
            AND dv.deptype = 'i'
            AND dv.objid = dp.objid
            AND dp.classid = 'pg_catalog.pg_rewrite'::regclass
            AND dp.refclassid = 'pg_catalog.pg_proc'::regclass
            AND dp.refobjid = p.oid
            AND p.pronamespace = np.oid
            AND pg_has_role(p.proowner, 'USAGE');)",

  // R"(GRANT SELECT ON view_routine_usage TO PUBLIC;)",

  // 6.81
  // VIEW_TABLE_USAGE

  R"(CREATE VIEW view_table_usage AS
      SELECT DISTINCT
             CAST(current_database() AS sql_identifier) AS view_catalog,
             CAST(nv.nspname AS sql_identifier) AS view_schema,
             CAST(v.relname AS sql_identifier) AS view_name,
             CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nt.nspname AS sql_identifier) AS table_schema,
             CAST(t.relname AS sql_identifier) AS table_name

      FROM pg_namespace nv, pg_class v, pg_depend dv,
           pg_depend dt, pg_class t, pg_namespace nt

      WHERE nv.oid = v.relnamespace
            AND v.relkind = 'v'
            AND v.oid = dv.refobjid
            AND dv.refclassid = 'pg_catalog.pg_class'::regclass
            AND dv.classid = 'pg_catalog.pg_rewrite'::regclass
            AND dv.deptype = 'i'
            AND dv.objid = dt.objid
            AND dv.refobjid <> dt.refobjid
            AND dt.classid = 'pg_catalog.pg_rewrite'::regclass
            AND dt.refclassid = 'pg_catalog.pg_class'::regclass
            AND dt.refobjid = t.oid
            AND t.relnamespace = nt.oid
            AND t.relkind IN ('r', 'v', 'f', 'p')
            AND pg_has_role(t.relowner, 'USAGE');)",

  // R"(GRANT SELECT ON view_table_usage TO PUBLIC;)",

  // 6.82
  // VIEWS view

  R"(CREATE VIEW views AS
      SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(nc.nspname AS sql_identifier) AS table_schema,
             CAST(c.relname AS sql_identifier) AS table_name,

             CAST(
               CASE WHEN pg_has_role(c.relowner, 'USAGE')
                    THEN pg_get_viewdef(c.oid)
                    ELSE null END
               AS character_data) AS view_definition,

             CAST(
               CASE WHEN 'check_option=cascaded' = ANY (c.reloptions)
                    THEN 'CASCADED'
                    WHEN 'check_option=local' = ANY (c.reloptions)
                    THEN 'LOCAL'
                    ELSE 'NONE' END
               AS character_data) AS check_option,

             CAST(
               -- (1 << CMD_UPDATE) + (1 << CMD_DELETE)
               CASE WHEN pg_relation_is_updatable(c.oid, false) & 20 = 20
                    THEN 'YES' ELSE 'NO' END
               AS yes_or_no) AS is_updatable,

             CAST(
               -- 1 << CMD_INSERT
               CASE WHEN pg_relation_is_updatable(c.oid, false) & 8 = 8
                    THEN 'YES' ELSE 'NO' END
               AS yes_or_no) AS is_insertable_into,

             CAST(
               -- TRIGGER_TYPE_ROW + TRIGGER_TYPE_INSTEAD + TRIGGER_TYPE_UPDATE
               CASE WHEN EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = c.oid AND tgtype & 81 = 81)
                    THEN 'YES' ELSE 'NO' END
             AS yes_or_no) AS is_trigger_updatable,

             CAST(
               -- TRIGGER_TYPE_ROW + TRIGGER_TYPE_INSTEAD + TRIGGER_TYPE_DELETE
               CASE WHEN EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = c.oid AND tgtype & 73 = 73)
                    THEN 'YES' ELSE 'NO' END
             AS yes_or_no) AS is_trigger_deletable,

             CAST(
               -- TRIGGER_TYPE_ROW + TRIGGER_TYPE_INSTEAD + TRIGGER_TYPE_INSERT
               CASE WHEN EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = c.oid AND tgtype & 69 = 69)
                    THEN 'YES' ELSE 'NO' END
             AS yes_or_no) AS is_trigger_insertable_into

      FROM pg_namespace nc, pg_class c

      WHERE c.relnamespace = nc.oid
            AND c.relkind = 'v'
            AND (NOT pg_is_other_temp_schema(nc.oid))
            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                 OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') );)",

  // R"(GRANT SELECT ON views TO PUBLIC;)",

  // The following views have dependencies that force them to appear out of order.

  // 6.26
  // DATA_TYPE_PRIVILEGES view

  R"(CREATE VIEW data_type_privileges AS
      SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
             CAST(x.objschema AS sql_identifier) AS object_schema,
             CAST(x.objname AS sql_identifier) AS object_name,
             CAST(x.objtype AS character_data) AS object_type,
             CAST(x.objdtdid AS sql_identifier) AS dtd_identifier

      FROM
        (
          SELECT udt_schema, udt_name, 'USER-DEFINED TYPE'::text, dtd_identifier FROM information_schema.attributes
          UNION ALL
          SELECT table_schema, table_name, 'TABLE'::text, dtd_identifier FROM information_schema.columns
          UNION ALL
          SELECT domain_schema, domain_name, 'DOMAIN'::text, dtd_identifier FROM information_schema.domains
          UNION ALL
          SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM information_schema.parameters
          UNION ALL
          SELECT specific_schema, specific_name, 'ROUTINE'::text, dtd_identifier FROM information_schema.routines
        ) AS x (objschema, objname, objtype, objdtdid);)",

  // R"(GRANT SELECT ON data_type_privileges TO PUBLIC;)",

  // 6.31
  // ELEMENT_TYPES view

  // TODO(mbkkt) Use original text when implement implicit cast for UNION.
  R"(CREATE VIEW element_types AS
      SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
             CAST(n.nspname AS sql_identifier) AS object_schema,
             CAST(x.objname AS sql_identifier) AS object_name,
             CAST(x.objtype AS character_data) AS object_type,
             CAST(x.objdtdid AS sql_identifier) AS collection_type_identifier,
             CAST(
               CASE WHEN nbt.nspname = 'pg_catalog' THEN format_type(bt.oid, null)
                    ELSE 'USER-DEFINED' END AS character_data) AS data_type,

             CAST(null AS cardinal_number) AS character_maximum_length,
             CAST(null AS cardinal_number) AS character_octet_length,
             CAST(null AS sql_identifier) AS character_set_catalog,
             CAST(null AS sql_identifier) AS character_set_schema,
             CAST(null AS sql_identifier) AS character_set_name,
             CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
             CAST(nco.nspname AS sql_identifier) AS collation_schema,
             CAST(co.collname AS sql_identifier) AS collation_name,
             CAST(null AS cardinal_number) AS numeric_precision,
             CAST(null AS cardinal_number) AS numeric_precision_radix,
             CAST(null AS cardinal_number) AS numeric_scale,
             CAST(null AS cardinal_number) AS datetime_precision,
             CAST(null AS character_data) AS interval_type,
             CAST(null AS cardinal_number) AS interval_precision,

             CAST(current_database() AS sql_identifier) AS udt_catalog,
             CAST(nbt.nspname AS sql_identifier) AS udt_schema,
             CAST(bt.typname AS sql_identifier) AS udt_name,

             CAST(null AS sql_identifier) AS scope_catalog,
             CAST(null AS sql_identifier) AS scope_schema,
             CAST(null AS sql_identifier) AS scope_name,

             CAST(null AS cardinal_number) AS maximum_cardinality,
             CAST('a' || CAST(x.objdtdid AS text) AS sql_identifier) AS dtd_identifier

      FROM pg_namespace n, pg_type at, pg_namespace nbt, pg_type bt,
           (
             /* columns, attributes */
             SELECT c.relnamespace, CAST(c.relname AS sql_identifier),
                    CASE WHEN c.relkind = 'c' THEN 'USER-DEFINED TYPE'::text ELSE 'TABLE'::text END,
                    a.attnum, a.atttypid, a.attcollation
             FROM pg_class c, pg_attribute a
             WHERE c.oid = a.attrelid
                   AND c.relkind IN ('r', 'v', 'f', 'c', 'p')
                   AND attnum > 0 AND NOT attisdropped

             UNION ALL

             /* domains */
             SELECT t.typnamespace, CAST(t.typname AS sql_identifier),
                    'DOMAIN'::text, 1::smallint, t.typbasetype, t.typcollation
             FROM pg_type t
             WHERE t.typtype = 'd'

             -- TODO(mbkkt) parameters branch removed: needs _pg_expandarray

             UNION ALL

             /* result types */
             SELECT p.pronamespace,
                    CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier),
                    'ROUTINE'::text, 0::smallint, p.prorettype, 0::oid
             FROM pg_proc p

           ) AS x (objschema, objname, objtype, objdtdid, objtypeid, objcollation)
           LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
             ON x.objcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

      WHERE n.oid = x.objschema
            AND at.oid = x.objtypeid
            AND (at.typelem <> 0 AND at.typlen = -1)
            AND at.typelem = bt.oid
            AND nbt.oid = bt.typnamespace

            AND EXISTS (
                SELECT 1 FROM information_schema.data_type_privileges dtp
                WHERE dtp.object_schema = n.nspname
                  AND dtp.object_name = x.objname
                  AND dtp.object_type = x.objtype
                  AND dtp.dtd_identifier = CAST(x.objdtdid AS sql_identifier)
            );)",

  // R"(GRANT SELECT ON element_types TO PUBLIC;)",

  // SQL/MED views; these use section numbers from part 9 of the standard.

  // Base view for foreign table columns

  R"(CREATE VIEW _pg_foreign_table_columns AS
      SELECT n.nspname,
             c.relname,
             a.attname,
             a.attfdwoptions
      FROM pg_foreign_table t, pg_authid u, pg_namespace n, pg_class c,
           pg_attribute a
      WHERE u.oid = c.relowner
            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_column_privilege(c.oid, a.attnum, 'SELECT, INSERT, UPDATE, REFERENCES'))
            AND n.oid = c.relnamespace
            AND c.oid = t.ftrelid
            AND c.relkind = 'f'
            AND a.attrelid = c.oid
            AND a.attnum > 0;)",

  // 24.3
  // COLUMN_OPTIONS view

  // TODO(mbkkt) Use original text when implement (func(...)).field row field access
  R"(CREATE VIEW column_options AS
      SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
             CAST(null AS sql_identifier) AS table_schema,
             CAST(null AS sql_identifier) AS table_name,
             CAST(null AS sql_identifier) AS column_name,
             CAST(null AS sql_identifier) AS option_name,
             CAST(null AS character_data) AS option_value
      WHERE false;)",

  // R"(GRANT SELECT ON column_options TO PUBLIC;)",

  // Base view for foreign-data wrappers

  R"(CREATE VIEW _pg_foreign_data_wrappers AS
      SELECT w.oid,
             w.fdwowner,
             w.fdwoptions,
             CAST(current_database() AS sql_identifier) AS foreign_data_wrapper_catalog,
             CAST(fdwname AS sql_identifier) AS foreign_data_wrapper_name,
             CAST(u.rolname AS sql_identifier) AS authorization_identifier,
             CAST('c' AS character_data) AS foreign_data_wrapper_language
      FROM pg_foreign_data_wrapper w, pg_authid u
      WHERE u.oid = w.fdwowner
            AND (pg_has_role(fdwowner, 'USAGE')
                 OR has_foreign_data_wrapper_privilege(w.oid, 'USAGE'));)",

  // 24.5
  // FOREIGN_DATA_WRAPPER_OPTIONS view

  // TODO(mbkkt) Use original text when implement (func(...)).field row field access
  R"(CREATE VIEW foreign_data_wrapper_options AS
      SELECT CAST(current_database() AS sql_identifier) AS foreign_data_wrapper_catalog,
             CAST(null AS sql_identifier) AS foreign_data_wrapper_name,
             CAST(null AS sql_identifier) AS option_name,
             CAST(null AS character_data) AS option_value
      WHERE false;)",

  // R"(GRANT SELECT ON foreign_data_wrapper_options TO PUBLIC;)",

  // 24.6
  // FOREIGN_DATA_WRAPPERS view

  R"(CREATE VIEW foreign_data_wrappers AS
      SELECT foreign_data_wrapper_catalog,
             foreign_data_wrapper_name,
             authorization_identifier,
             CAST(NULL AS character_data) AS library_name,
             foreign_data_wrapper_language
      FROM information_schema._pg_foreign_data_wrappers w;)",

  // R"(GRANT SELECT ON foreign_data_wrappers TO PUBLIC;)",

  // Base view for foreign servers

  R"(CREATE VIEW _pg_foreign_servers AS
      SELECT s.oid,
             s.srvoptions,
             CAST(current_database() AS sql_identifier) AS foreign_server_catalog,
             CAST(srvname AS sql_identifier) AS foreign_server_name,
             CAST(current_database() AS sql_identifier) AS foreign_data_wrapper_catalog,
             CAST(w.fdwname AS sql_identifier) AS foreign_data_wrapper_name,
             CAST(srvtype AS character_data) AS foreign_server_type,
             CAST(srvversion AS character_data) AS foreign_server_version,
             CAST(u.rolname AS sql_identifier) AS authorization_identifier
      FROM pg_foreign_server s, pg_foreign_data_wrapper w, pg_authid u
      WHERE w.oid = s.srvfdw
            AND u.oid = s.srvowner
            AND (pg_has_role(s.srvowner, 'USAGE')
                 OR has_server_privilege(s.oid, 'USAGE'));)",

  // 24.7
  // FOREIGN_SERVER_OPTIONS view

  // TODO(mbkkt) Use original text when implement (func(...)).field row field access
  R"(CREATE VIEW foreign_server_options AS
      SELECT CAST(current_database() AS sql_identifier) AS foreign_server_catalog,
             CAST(null AS sql_identifier) AS foreign_server_name,
             CAST(null AS sql_identifier) AS option_name,
             CAST(null AS character_data) AS option_value
      WHERE false;)",

  // R"(GRANT SELECT ON TABLE foreign_server_options TO PUBLIC;)",

  // 24.8
  // FOREIGN_SERVERS view

  R"(CREATE VIEW foreign_servers AS
      SELECT foreign_server_catalog,
             foreign_server_name,
             foreign_data_wrapper_catalog,
             foreign_data_wrapper_name,
             foreign_server_type,
             foreign_server_version,
             authorization_identifier
      FROM information_schema._pg_foreign_servers;)",

  // R"(GRANT SELECT ON foreign_servers TO PUBLIC;)",

  // Base view for foreign tables

  R"(CREATE VIEW _pg_foreign_tables AS
      SELECT
             CAST(current_database() AS sql_identifier) AS foreign_table_catalog,
             CAST(n.nspname AS sql_identifier) AS foreign_table_schema,
             CAST(c.relname AS sql_identifier) AS foreign_table_name,
             t.ftoptions AS ftoptions,
             CAST(current_database() AS sql_identifier) AS foreign_server_catalog,
             CAST(srvname AS sql_identifier) AS foreign_server_name,
             CAST(u.rolname AS sql_identifier) AS authorization_identifier
      FROM pg_foreign_table t, pg_foreign_server s, pg_foreign_data_wrapper w,
           pg_authid u, pg_namespace n, pg_class c
      WHERE w.oid = s.srvfdw
            AND u.oid = c.relowner
            AND (pg_has_role(c.relowner, 'USAGE')
                 OR has_table_privilege(c.oid, 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER')
                 OR has_any_column_privilege(c.oid, 'SELECT, INSERT, UPDATE, REFERENCES'))
            AND n.oid = c.relnamespace
            AND c.oid = t.ftrelid
            AND c.relkind = 'f'
            AND s.oid = t.ftserver;)",

  // 24.9
  // FOREIGN_TABLE_OPTIONS view

  // TODO(mbkkt) Use original text when implement (func(...)).field row field access
  R"(CREATE VIEW foreign_table_options AS
      SELECT CAST(current_database() AS sql_identifier) AS foreign_table_catalog,
             CAST(null AS sql_identifier) AS foreign_table_schema,
             CAST(null AS sql_identifier) AS foreign_table_name,
             CAST(null AS sql_identifier) AS option_name,
             CAST(null AS character_data) AS option_value
      WHERE false;)",

  // R"(GRANT SELECT ON TABLE foreign_table_options TO PUBLIC;)",

  // 24.10
  // FOREIGN_TABLES view

  R"(CREATE VIEW foreign_tables AS
      SELECT foreign_table_catalog,
             foreign_table_schema,
             foreign_table_name,
             foreign_server_catalog,
             foreign_server_name
      FROM information_schema._pg_foreign_tables;)",

  // R"(GRANT SELECT ON foreign_tables TO PUBLIC;)",

  // Base view for user mappings

  R"(CREATE VIEW _pg_user_mappings AS
      SELECT um.oid,
             um.umoptions,
             um.umuser,
             CAST(COALESCE(u.rolname,'PUBLIC') AS sql_identifier ) AS authorization_identifier,
             s.foreign_server_catalog,
             s.foreign_server_name,
             s.authorization_identifier AS srvowner
      FROM pg_user_mapping um LEFT JOIN pg_authid u ON (u.oid = um.umuser),
           information_schema._pg_foreign_servers s
      WHERE s.oid = um.umserver;)",

  // 24.13
  // USER_MAPPING_OPTIONS view

  // TODO(mbkkt) Use original text when implement lateral set-returning function refs
  R"(CREATE VIEW user_mapping_options AS
      SELECT CAST(null AS sql_identifier) AS authorization_identifier,
             CAST(current_database() AS sql_identifier) AS foreign_server_catalog,
             CAST(null AS sql_identifier) AS foreign_server_name,
             CAST(null AS sql_identifier) AS option_name,
             CAST(null AS character_data) AS option_value
      WHERE false;)",

  // R"(GRANT SELECT ON user_mapping_options TO PUBLIC;)",

  // 24.14
  // USER_MAPPINGS view

  R"(CREATE VIEW user_mappings AS
      SELECT authorization_identifier,
             foreign_server_catalog,
             foreign_server_name
      FROM information_schema._pg_user_mappings;)",

  // R"(GRANT SELECT ON user_mappings TO PUBLIC;)",
  // clang-format on
});

}  // namespace sdb::pg
