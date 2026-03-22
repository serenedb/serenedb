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
inline constexpr auto kSystemFunctionsQueries =
  std::to_array<std::string_view>({
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
                       datname TEXT,
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
              pid, datid, datname, relid,
              param1, param2, param3, param4, param5,
              param6, param7, param8, param9, param10,
              param11, param12, param13, param14, param15,
              param16, param17, param18, param19, param20
            FROM sdb_stat_progress
            WHERE command = cmd;
        END;)",
  });

}  // namespace sdb::pg
