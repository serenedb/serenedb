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

std::string_view gInfoColumnOptions = R"sql(SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
       CAST(c.nspname AS sql_identifier) AS table_schema,
       CAST(c.relname AS sql_identifier) AS table_name,
       CAST(c.attname AS sql_identifier) AS column_name,
       CAST(opts.option_name AS sql_identifier) AS option_name,
       CAST(opts.option_value AS character_data) AS option_value
FROM information_schema._pg_foreign_table_columns c,
     pg_options_to_table(c.attfdwoptions) opts)sql";

}  // namespace sdb::pg
