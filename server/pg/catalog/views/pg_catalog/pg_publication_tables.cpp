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

std::string_view gPgPublicationTables = R"sql(SELECT
    P.pubname AS pubname,
    N.nspname AS schemaname,
    C.relname AS tablename,
    ( SELECT array_agg(a.attname ORDER BY a.attnum)
      FROM pg_attribute a
      WHERE a.attrelid = GPT.relid AND
            -- TODO(mbkkt): restore ANY() once DuckDB supports correlated UNNEST
            list_has(GPT.attrs, a.attnum)
    ) AS attnames,
    pg_get_expr(GPT.qual, GPT.relid) AS rowfilter
FROM pg_publication P,
     LATERAL pg_get_publication_tables(P.pubname) GPT,
     pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE C.oid = GPT.relid)sql";

}  // namespace sdb::pg
