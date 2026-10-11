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

std::string_view gInfoTriggeredUpdateColumns = R"sql(SELECT CAST(current_database() AS sql_identifier) AS trigger_catalog,
       CAST(n.nspname AS sql_identifier) AS trigger_schema,
       CAST(t.tgname AS sql_identifier) AS trigger_name,
       CAST(current_database() AS sql_identifier) AS event_object_catalog,
       CAST(n.nspname AS sql_identifier) AS event_object_schema,
       CAST(c.relname AS sql_identifier) AS event_object_table,
       CAST(a.attname AS sql_identifier) AS event_object_column

FROM pg_namespace n, pg_class c, pg_trigger t,
     -- Rewritten: (ta0.tgat).x / .n -> ea.x / ea.n
     (SELECT tgoid, ea.x AS tgattnum, ea.n AS tgattpos
      FROM (SELECT oid AS tgoid, tgattr FROM pg_trigger) AS ta0,
           information_schema._pg_expandarray(ta0.tgattr) AS ea) AS ta,
     pg_attribute a

WHERE n.oid = c.relnamespace
      AND c.oid = t.tgrelid
      AND t.oid = ta.tgoid
      AND (a.attrelid, a.attnum) = (t.tgrelid, ta.tgattnum)
      AND NOT t.tgisinternal
      AND (NOT pg_is_other_temp_schema(n.oid))
      AND (pg_has_role(c.relowner, 'USAGE')
           -- SELECT privilege omitted, per SQL standard
           OR has_column_privilege(c.oid, a.attnum, 'INSERT, UPDATE, REFERENCES') ))sql";

}  // namespace sdb::pg
