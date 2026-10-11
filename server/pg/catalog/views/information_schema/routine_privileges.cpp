////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <string_view>

namespace sdb::pg {

std::string_view gInfoRoutinePrivileges = R"sql(SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS specific_catalog,
       CAST(n.nspname AS sql_identifier) AS specific_schema,
       CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier) AS specific_name,
       CAST(current_database() AS sql_identifier) AS routine_catalog,
       CAST(n.nspname AS sql_identifier) AS routine_schema,
       CAST(p.proname AS sql_identifier) AS routine_name,
       CAST('EXECUTE' AS character_data) AS privilege_type,
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(grantee.oid, p.proowner, 'USAGE')
              OR p.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

FROM (
        -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
        SELECT oid, proname, proowner, pronamespace, acl.* FROM pg_proc, aclexplode(coalesce(proacl, acldefault('f', proowner))) AS acl
     ) p (oid, proname, proowner, pronamespace, grantor, grantee, prtype, grantable),
     pg_namespace n,
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE p.pronamespace = n.oid
      AND grantee.oid = p.grantee
      AND u_grantor.oid = p.grantor
      AND p.prtype IN ('EXECUTE')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC'))sql";

}  // namespace sdb::pg
