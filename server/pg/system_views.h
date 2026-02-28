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

  // subquery type is not implemented
  // R"(CREATE VIEW pg_group AS
  //   SELECT
  //       rolname AS groname,
  //       oid AS grosysid,
  //       ARRAY(SELECT member FROM pg_auth_members WHERE roleid =
  //       pg_authid.oid) AS grolist
  //   FROM pg_authid
  //   WHERE NOT rolcanlogin;)",

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

  // subquery type is not implemented
  // R"(CREATE VIEW pg_policies AS
  //   SELECT
  //       N.nspname AS schemaname,
  //       C.relname AS tablename,
  //       pol.polname AS policyname,
  //       CASE
  //           WHEN pol.polpermissive THEN
  //               'PERMISSIVE'
  //           ELSE
  //               'RESTRICTIVE'
  //       END AS permissive,
  //       CASE
  //           WHEN pol.polroles = '{0}' THEN
  //               string_to_array('public', '')
  //           ELSE
  //               ARRAY
  //               (
  //                   SELECT rolname
  //                   FROM pg_catalog.pg_authid
  //                   WHERE oid = ANY (pol.polroles) ORDER BY 1
  //               )
  //       END AS roles,
  //       CASE pol.polcmd
  //           WHEN 'r' THEN 'SELECT'
  //           WHEN 'a' THEN 'INSERT'
  //           WHEN 'w' THEN 'UPDATE'
  //           WHEN 'd' THEN 'DELETE'
  //           WHEN '*' THEN 'ALL'
  //       END AS cmd,
  //       pg_catalog.pg_get_expr(pol.polqual, pol.polrelid) AS qual,
  //       pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check
  //   FROM pg_catalog.pg_policy pol
  //   JOIN pg_catalog.pg_class C ON (C.oid = pol.polrelid)
  //   LEFT JOIN pg_catalog.pg_namespace N ON (N.oid = C.relnamespace);)",

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

  // pg_get_indexdef
  // R"(CREATE VIEW pg_indexes AS
  //   SELECT
  //       N.nspname AS schemaname,
  //       C.relname AS tablename,
  //       I.relname AS indexname,
  //       T.spcname AS tablespace,
  //       pg_get_indexdef(I.oid) AS indexdef
  //   FROM pg_index X JOIN pg_class C ON (C.oid = X.indrelid)
  //        JOIN pg_class I ON (I.oid = X.indexrelid)
  //        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  //        LEFT JOIN pg_tablespace T ON (T.oid = I.reltablespace)
  //   WHERE C.relkind IN ('r', 'm', 'p') AND I.relkind IN ('i', 'I');)",

  // pg_get_userbyid, pg_sequence_last_value
  // R"(CREATE VIEW pg_sequences AS
  //   SELECT
  //       N.nspname AS schemaname,
  //       C.relname AS sequencename,
  //       pg_get_userbyid(C.relowner) AS sequenceowner,
  //       S.seqtypid::regtype AS data_type,
  //       S.seqstart AS start_value,
  //       S.seqmin AS min_value,
  //       S.seqmax AS max_value,
  //       S.seqincrement AS increment_by,
  //       S.seqcycle AS cycle,
  //       S.seqcache AS cache_size,
  //       pg_sequence_last_value(C.oid) AS last_value
  //   FROM pg_sequence S JOIN pg_class C ON (C.oid = S.seqrelid)
  //        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
  //   WHERE NOT pg_is_other_temp_schema(N.oid)
  //         AND relkind = 'S';)",

  // T_A_Indirection
  // R"(CREATE VIEW pg_stats WITH (security_barrier) AS
  //   SELECT
  //       nspname AS schemaname,
  //       relname AS tablename,
  //       attname AS attname,
  //       stainherit AS inherited,
  //       stanullfrac AS null_frac,
  //       stawidth AS avg_width,
  //       stadistinct AS n_distinct,
  //       CASE
  //           WHEN stakind1 = 1 THEN stavalues1
  //           WHEN stakind2 = 1 THEN stavalues2
  //           WHEN stakind3 = 1 THEN stavalues3
  //           WHEN stakind4 = 1 THEN stavalues4
  //           WHEN stakind5 = 1 THEN stavalues5
  //       END AS most_common_vals,
  //       CASE
  //           WHEN stakind1 = 1 THEN stanumbers1
  //           WHEN stakind2 = 1 THEN stanumbers2
  //           WHEN stakind3 = 1 THEN stanumbers3
  //           WHEN stakind4 = 1 THEN stanumbers4
  //           WHEN stakind5 = 1 THEN stanumbers5
  //       END AS most_common_freqs,
  //       CASE
  //           WHEN stakind1 = 2 THEN stavalues1
  //           WHEN stakind2 = 2 THEN stavalues2
  //           WHEN stakind3 = 2 THEN stavalues3
  //           WHEN stakind4 = 2 THEN stavalues4
  //           WHEN stakind5 = 2 THEN stavalues5
  //       END AS histogram_bounds,
  //       CASE
  //           WHEN stakind1 = 3 THEN stanumbers1[1]
  //           WHEN stakind2 = 3 THEN stanumbers2[1]
  //           WHEN stakind3 = 3 THEN stanumbers3[1]
  //           WHEN stakind4 = 3 THEN stanumbers4[1]
  //           WHEN stakind5 = 3 THEN stanumbers5[1]
  //       END AS correlation,
  //       CASE
  //           WHEN stakind1 = 4 THEN stavalues1
  //           WHEN stakind2 = 4 THEN stavalues2
  //           WHEN stakind3 = 4 THEN stavalues3
  //           WHEN stakind4 = 4 THEN stavalues4
  //           WHEN stakind5 = 4 THEN stavalues5
  //       END AS most_common_elems,
  //       CASE
  //           WHEN stakind1 = 4 THEN stanumbers1
  //           WHEN stakind2 = 4 THEN stanumbers2
  //           WHEN stakind3 = 4 THEN stanumbers3
  //           WHEN stakind4 = 4 THEN stanumbers4
  //           WHEN stakind5 = 4 THEN stanumbers5
  //       END AS most_common_elem_freqs,
  //       CASE
  //           WHEN stakind1 = 5 THEN stanumbers1
  //           WHEN stakind2 = 5 THEN stanumbers2
  //           WHEN stakind3 = 5 THEN stanumbers3
  //           WHEN stakind4 = 5 THEN stanumbers4
  //           WHEN stakind5 = 5 THEN stanumbers5
  //       END AS elem_count_histogram,
  //       CASE
  //           WHEN stakind1 = 6 THEN stavalues1
  //           WHEN stakind2 = 6 THEN stavalues2
  //           WHEN stakind3 = 6 THEN stavalues3
  //           WHEN stakind4 = 6 THEN stavalues4
  //           WHEN stakind5 = 6 THEN stavalues5
  //       END AS range_length_histogram,
  //       CASE
  //           WHEN stakind1 = 6 THEN stanumbers1[1]
  //           WHEN stakind2 = 6 THEN stanumbers2[1]
  //           WHEN stakind3 = 6 THEN stanumbers3[1]
  //           WHEN stakind4 = 6 THEN stanumbers4[1]
  //           WHEN stakind5 = 6 THEN stanumbers5[1]
  //       END AS range_empty_frac,
  //       CASE
  //           WHEN stakind1 = 7 THEN stavalues1
  //           WHEN stakind2 = 7 THEN stavalues2
  //           WHEN stakind3 = 7 THEN stavalues3
  //           WHEN stakind4 = 7 THEN stavalues4
  //           WHEN stakind5 = 7 THEN stavalues5
  //           END AS range_bounds_histogram
  //   FROM pg_statistic s JOIN pg_class c ON (c.oid = s.starelid)
  //        JOIN pg_attribute a ON (c.oid = attrelid AND attnum = s.staattnum)
  //        LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
  //   WHERE NOT attisdropped
  //   AND has_column_privilege(c.oid, a.attnum, 'select')
  //   AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));)",

  // T_A_Indirection, subquery type is not implemented
  // R"(CREATE VIEW pg_stats_ext WITH (security_barrier) AS
  //   SELECT cn.nspname AS schemaname,
  //          c.relname AS tablename,
  //          sn.nspname AS statistics_schemaname,
  //          s.stxname AS statistics_name,
  //          pg_get_userbyid(s.stxowner) AS statistics_owner,
  //          ( SELECT array_agg(a.attname ORDER BY a.attnum)
  //            FROM unnest(s.stxkeys) k
  //                 JOIN pg_attribute a
  //                      ON (a.attrelid = s.stxrelid AND a.attnum = k)
  //          ) AS attnames,
  //          pg_get_statisticsobjdef_expressions(s.oid) as exprs,
  //          s.stxkind AS kinds,
  //          sd.stxdinherit AS inherited,
  //          sd.stxdndistinct AS n_distinct,
  //          sd.stxddependencies AS dependencies,
  //          m.most_common_vals,
  //          m.most_common_val_nulls,
  //          m.most_common_freqs,
  //          m.most_common_base_freqs
  //   FROM pg_statistic_ext s JOIN pg_class c ON (c.oid = s.stxrelid)
  //        JOIN pg_statistic_ext_data sd ON (s.oid = sd.stxoid)
  //        LEFT JOIN pg_namespace cn ON (cn.oid = c.relnamespace)
  //        LEFT JOIN pg_namespace sn ON (sn.oid = s.stxnamespace)
  //        LEFT JOIN LATERAL
  //                  ( SELECT array_agg(values) AS most_common_vals,
  //                           array_agg(nulls) AS most_common_val_nulls,
  //                           array_agg(frequency) AS most_common_freqs,
  //                           array_agg(base_frequency) AS
  //                           most_common_base_freqs
  //                    FROM pg_mcv_list_items(sd.stxdmcv)
  //                  ) m ON sd.stxdmcv IS NOT NULL
  //   WHERE pg_has_role(c.relowner, 'USAGE')
  //   AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));)",

  // T_A_Indirection, subquery type is not implemented
  // R"(CREATE VIEW pg_stats_ext_exprs WITH (security_barrier) AS
  //   SELECT cn.nspname AS schemaname,
  //          c.relname AS tablename,
  //          sn.nspname AS statistics_schemaname,
  //          s.stxname AS statistics_name,
  //          pg_get_userbyid(s.stxowner) AS statistics_owner,
  //          stat.expr,
  //          sd.stxdinherit AS inherited,
  //          (stat.a).stanullfrac AS null_frac,
  //          (stat.a).stawidth AS avg_width,
  //          (stat.a).stadistinct AS n_distinct,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 1 THEN (stat.a).stavalues1
  //              WHEN (stat.a).stakind2 = 1 THEN (stat.a).stavalues2
  //              WHEN (stat.a).stakind3 = 1 THEN (stat.a).stavalues3
  //              WHEN (stat.a).stakind4 = 1 THEN (stat.a).stavalues4
  //              WHEN (stat.a).stakind5 = 1 THEN (stat.a).stavalues5
  //          END) AS most_common_vals,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 1 THEN (stat.a).stanumbers1
  //              WHEN (stat.a).stakind2 = 1 THEN (stat.a).stanumbers2
  //              WHEN (stat.a).stakind3 = 1 THEN (stat.a).stanumbers3
  //              WHEN (stat.a).stakind4 = 1 THEN (stat.a).stanumbers4
  //              WHEN (stat.a).stakind5 = 1 THEN (stat.a).stanumbers5
  //          END) AS most_common_freqs,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 2 THEN (stat.a).stavalues1
  //              WHEN (stat.a).stakind2 = 2 THEN (stat.a).stavalues2
  //              WHEN (stat.a).stakind3 = 2 THEN (stat.a).stavalues3
  //              WHEN (stat.a).stakind4 = 2 THEN (stat.a).stavalues4
  //              WHEN (stat.a).stakind5 = 2 THEN (stat.a).stavalues5
  //          END) AS histogram_bounds,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 3 THEN (stat.a).stanumbers1[1]
  //              WHEN (stat.a).stakind2 = 3 THEN (stat.a).stanumbers2[1]
  //              WHEN (stat.a).stakind3 = 3 THEN (stat.a).stanumbers3[1]
  //              WHEN (stat.a).stakind4 = 3 THEN (stat.a).stanumbers4[1]
  //              WHEN (stat.a).stakind5 = 3 THEN (stat.a).stanumbers5[1]
  //          END) correlation,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 4 THEN (stat.a).stavalues1
  //              WHEN (stat.a).stakind2 = 4 THEN (stat.a).stavalues2
  //              WHEN (stat.a).stakind3 = 4 THEN (stat.a).stavalues3
  //              WHEN (stat.a).stakind4 = 4 THEN (stat.a).stavalues4
  //              WHEN (stat.a).stakind5 = 4 THEN (stat.a).stavalues5
  //          END) AS most_common_elems,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 4 THEN (stat.a).stanumbers1
  //              WHEN (stat.a).stakind2 = 4 THEN (stat.a).stanumbers2
  //              WHEN (stat.a).stakind3 = 4 THEN (stat.a).stanumbers3
  //              WHEN (stat.a).stakind4 = 4 THEN (stat.a).stanumbers4
  //              WHEN (stat.a).stakind5 = 4 THEN (stat.a).stanumbers5
  //          END) AS most_common_elem_freqs,
  //          (CASE
  //              WHEN (stat.a).stakind1 = 5 THEN (stat.a).stanumbers1
  //              WHEN (stat.a).stakind2 = 5 THEN (stat.a).stanumbers2
  //              WHEN (stat.a).stakind3 = 5 THEN (stat.a).stanumbers3
  //              WHEN (stat.a).stakind4 = 5 THEN (stat.a).stanumbers4
  //              WHEN (stat.a).stakind5 = 5 THEN (stat.a).stanumbers5
  //          END) AS elem_count_histogram
  //   FROM pg_statistic_ext s JOIN pg_class c ON (c.oid = s.stxrelid)
  //        LEFT JOIN pg_statistic_ext_data sd ON (s.oid = sd.stxoid)
  //        LEFT JOIN pg_namespace cn ON (cn.oid = c.relnamespace)
  //        LEFT JOIN pg_namespace sn ON (sn.oid = s.stxnamespace)
  //        JOIN LATERAL (
  //            SELECT unnest(pg_get_statisticsobjdef_expressions(s.oid)) AS
  //            expr,
  //                   unnest(sd.stxdexpr)::pg_statistic AS a
  //        ) stat ON (stat.expr IS NOT NULL)
  //   WHERE pg_has_role(c.relowner, 'USAGE')
  //   AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));)",

  // pg_get_publication_tables
  // R"(CREATE VIEW pg_publication_tables AS
  //   SELECT
  //       P.pubname AS pubname,
  //       N.nspname AS schemaname,
  //       C.relname AS tablename,
  //       ( SELECT array_agg(a.attname ORDER BY a.attnum)
  //         FROM pg_attribute a
  //         WHERE a.attrelid = GPT.relid AND
  //               a.attnum = ANY(GPT.attrs)
  //       ) AS attnames,
  //       pg_get_expr(GPT.qual, GPT.relid) AS rowfilter
  //   FROM pg_publication P,
  //        LATERAL pg_get_publication_tables(P.pubname) GPT,
  //        pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)
  //   WHERE C.oid = GPT.relid;)",

  // implementation system view: pg_get_publication_sequences

  // pg_lock_status
  // R"(CREATE VIEW pg_locks AS
  //   SELECT * FROM pg_lock_status() AS L;)",

  // pg_cursor
  // R"(CREATE VIEW pg_cursors AS
  //   SELECT * FROM pg_cursor() AS C;)",

  // 2 below -- view has the same name as function

  // pg_available_extensions
  // R"(CREATE VIEW pg_available_extensions AS
  //   SELECT E.name, E.default_version, X.extversion AS installed_version,
  //          E.comment
  //     FROM pg_available_extensions() AS E
  //          LEFT JOIN pg_extension AS X ON E.name = X.extname;)",

  // pg_available_extension_versions
  // R"(CREATE VIEW pg_available_extension_versions AS
  //   SELECT E.name, E.version, (X.extname IS NOT NULL) AS installed,
  //          E.superuser, E.trusted, E.relocatable,
  //          E.schema, E.requires, E.comment
  //     FROM pg_available_extension_versions() AS E
  //          LEFT JOIN pg_extension AS X
  //            ON E.name = X.extname AND E.version = X.extversion;)",

  // pg_prepared_xact
  // R"(CREATE VIEW pg_prepared_xacts AS
  //   SELECT P.transaction, P.gid, P.prepared,
  //          U.rolname AS owner, D.datname AS database
  //   FROM pg_prepared_xact() AS P
  //        LEFT JOIN pg_authid U ON P.ownerid = U.oid
  //        LEFT JOIN pg_database D ON P.dbid = D.oid;)",

  // pg_prepared_statement
  // R"(CREATE VIEW pg_prepared_statements AS
  //   SELECT * FROM pg_prepared_statement() AS P;)",

  // At least pg_table_is_visible
  // R"(CREATE VIEW pg_seclabels AS
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     CASE WHEN rel.relkind IN ('r', 'p') THEN 'table'::text
  //          WHEN rel.relkind = 'v' THEN 'view'::text
  //          WHEN rel.relkind = 'm' THEN 'materialized view'::text
  //          WHEN rel.relkind = 'S' THEN 'sequence'::text
  //          WHEN rel.relkind = 'f' THEN 'foreign table'::text END AS objtype,
  //     rel.relnamespace AS objnamespace,
  //     CASE WHEN pg_table_is_visible(rel.oid)
  //          THEN quote_ident(rel.relname)
  //          ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
  //          END AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
  //     JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     'column'::text AS objtype,
  //     rel.relnamespace AS objnamespace,
  //     CASE WHEN pg_table_is_visible(rel.oid)
  //          THEN quote_ident(rel.relname)
  //          ELSE quote_ident(nsp.nspname) || '.' || quote_ident(rel.relname)
  //          END || '.' || att.attname AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_class rel ON l.classoid = rel.tableoid AND l.objoid = rel.oid
  //     JOIN pg_attribute att
  //          ON rel.oid = att.attrelid AND l.objsubid = att.attnum
  //     JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
  // WHERE
  //     l.objsubid != 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     CASE pro.prokind
  //             WHEN 'a' THEN 'aggregate'::text
  //             WHEN 'f' THEN 'function'::text
  //             WHEN 'p' THEN 'procedure'::text
  //             WHEN 'w' THEN 'window'::text END AS objtype,
  //     pro.pronamespace AS objnamespace,
  //     CASE WHEN pg_function_is_visible(pro.oid)
  //          THEN quote_ident(pro.proname)
  //          ELSE quote_ident(nsp.nspname) || '.' || quote_ident(pro.proname)
  //     END || '(' || pg_catalog.pg_get_function_arguments(pro.oid) || ')' AS
  //     objname, l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_proc pro ON l.classoid = pro.tableoid AND l.objoid = pro.oid
  //     JOIN pg_namespace nsp ON pro.pronamespace = nsp.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     CASE WHEN typ.typtype = 'd' THEN 'domain'::text
  //     ELSE 'type'::text END AS objtype,
  //     typ.typnamespace AS objnamespace,
  //     CASE WHEN pg_type_is_visible(typ.oid)
  //     THEN quote_ident(typ.typname)
  //     ELSE quote_ident(nsp.nspname) || '.' || quote_ident(typ.typname)
  //     END AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_type typ ON l.classoid = typ.tableoid AND l.objoid = typ.oid
  //     JOIN pg_namespace nsp ON typ.typnamespace = nsp.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     'large object'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     l.objoid::text AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_largeobject_metadata lom ON l.objoid = lom.oid
  // WHERE
  //     l.classoid = 'pg_catalog.pg_largeobject'::regclass AND l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     'language'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(lan.lanname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_language lan ON l.classoid = lan.tableoid AND l.objoid =
  //     lan.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     'schema'::text AS objtype,
  //     nsp.oid AS objnamespace,
  //     quote_ident(nsp.nspname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_namespace nsp ON l.classoid = nsp.tableoid AND l.objoid =
  //     nsp.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     'event trigger'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(evt.evtname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_event_trigger evt ON l.classoid = evt.tableoid
  //         AND l.objoid = evt.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, l.objsubid,
  //     'publication'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(p.pubname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_seclabel l
  //     JOIN pg_publication p ON l.classoid = p.tableoid AND l.objoid = p.oid
  // WHERE
  //     l.objsubid = 0
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, 0::int4 AS objsubid,
  //     'subscription'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(s.subname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_shseclabel l
  //     JOIN pg_subscription s ON l.classoid = s.tableoid AND l.objoid = s.oid
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, 0::int4 AS objsubid,
  //     'database'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(dat.datname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_shseclabel l
  //     JOIN pg_database dat ON l.classoid = dat.tableoid AND l.objoid =
  //     dat.oid
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, 0::int4 AS objsubid,
  //     'tablespace'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(spc.spcname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_shseclabel l
  //     JOIN pg_tablespace spc ON l.classoid = spc.tableoid AND l.objoid =
  //     spc.oid
  // UNION ALL
  // SELECT
  //     l.objoid, l.classoid, 0::int4 AS objsubid,
  //     'role'::text AS objtype,
  //     NULL::oid AS objnamespace,
  //     quote_ident(rol.rolname) AS objname,
  //     l.provider, l.label
  // FROM
  //     pg_shseclabel l
  //     JOIN pg_authid rol ON l.classoid = rol.tableoid AND l.objoid =
  //     rol.oid;)",

  // pg_show_all_settings
  R"(CREATE VIEW pg_settings AS
    SELECT * FROM pg_show_all_settings() AS A;)",

  // pg_show_all_file_settings
  // R"(CREATE VIEW pg_file_settings AS
  //  SELECT * FROM pg_show_all_file_settings() AS A;)",

  // pg_hba_file_rules
  // R"(CREATE VIEW pg_hba_file_rules AS
  //  SELECT * FROM pg_hba_file_rules() AS A;)",

  // pg_ident_file_mappings
  // R"(CREATE VIEW pg_ident_file_mappings AS
  //  SELECT * FROM pg_ident_file_mappings() AS A;)",

  // pg_timezone_abbrevs_zone
  // R"(CREATE VIEW pg_timezone_abbrevs AS
  //   SELECT * FROM pg_timezone_abbrevs_zone() z
  //   UNION ALL
  //   (SELECT * FROM pg_timezone_abbrevs_abbrevs() a
  //    WHERE NOT EXISTS (SELECT 1 FROM pg_timezone_abbrevs_zone() z2
  //                      WHERE z2.abbrev = a.abbrev))
  //   ORDER BY abbrev;)",

  // pg_timezone_names
  // R"(CREATE VIEW pg_timezone_names AS
  //   SELECT * FROM pg_timezone_names();)",

  // pg_config
  // R"(CREATE VIEW pg_config AS
  //   SELECT * FROM pg_config();)",

  // pg_get_shmem_allocations
  // R"(CREATE VIEW pg_shmem_allocations AS
  //   SELECT * FROM pg_get_shmem_allocations();)",

  // pg_get_shmem_allocations_numa
  // R"(CREATE VIEW pg_shmem_allocations_numa AS
  //   SELECT * FROM pg_get_shmem_allocations_numa();)",

  // pg_get_dsm_registry_allocations
  // R"(CREATE VIEW pg_dsm_registry_allocations AS
  //   SELECT * FROM pg_get_dsm_registry_allocations();)",

  // pg_get_backend_memory_contexts
  // R"(CREATE VIEW pg_backend_memory_contexts AS
  //   SELECT * FROM pg_get_backend_memory_contexts();)",

  // implementation system views for stats:
  // use tilde operator or specific function, review them later

  // pg_show_replication_origin_status
  // R"(CREATE VIEW pg_replication_origin_status AS
  //   SELECT *
  //   FROM pg_show_replication_origin_status();)",

  // pg_get_replication_slots
  // R"(CREATE VIEW pg_replication_slots AS
  //   SELECT
  //           L.slot_name,
  //           L.plugin,
  //           L.slot_type,
  //           L.datoid,
  //           D.datname AS database,
  //           L.temporary,
  //           L.active,
  //           L.active_pid,
  //           L.xmin,
  //           L.catalog_xmin,
  //           L.restart_lsn,
  //           L.confirmed_flush_lsn,
  //           L.wal_status,
  //           L.safe_wal_size,
  //           L.two_phase,
  //           L.two_phase_at,
  //           L.inactive_since,
  //           L.conflicting,
  //           L.invalidation_reason,
  //           L.failover,
  //           L.synced
  //   FROM pg_get_replication_slots() AS L
  //           LEFT JOIN pg_database D ON (L.datoid = D.oid);)",

  // pg_has_role
  // CREATE VIEW pg_user_mappings AS
  //   SELECT
  //       U.oid       AS umid,
  //       S.oid       AS srvid,
  //       S.srvname   AS srvname,
  //       U.umuser    AS umuser,
  //       CASE WHEN U.umuser = 0 THEN
  //           'public'
  //       ELSE
  //           A.rolname
  //       END AS usename,
  //       CASE WHEN (U.umuser <> 0 AND A.rolname = current_user
  //                    AND (pg_has_role(S.srvowner, 'USAGE')
  //                         OR has_server_privilege(S.oid, 'USAGE')))
  //                   OR (U.umuser = 0 AND pg_has_role(S.srvowner, 'USAGE'))
  //                   OR (SELECT rolsuper FROM pg_authid WHERE rolname =
  //                   current_user) THEN U.umoptions
  //                ELSE NULL END AS umoptions
  //   FROM pg_user_mapping U
  //       JOIN pg_foreign_server S ON (U.umserver = S.oid)
  //       LEFT JOIN pg_authid A ON (A.oid = U.umuser);

  // pg_get_aios
  // R"(CREATE VIEW pg_aios AS
  //   SELECT * FROM pg_get_aios();)",
});

}  // namespace sdb::pg
