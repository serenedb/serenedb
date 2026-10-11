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

std::string_view gPgStatProgressAnalyze = R"sql(SELECT
    S.pid AS pid, S.datid AS datid, S.datname AS datname,
    S.relid AS relid,
    S.phase AS phase,
    0::BIGINT AS sample_blks_total,
    0::BIGINT AS sample_blks_scanned,
    0::BIGINT AS ext_stats_total,
    0::BIGINT AS ext_stats_computed,
    S.items_total AS child_tables_total,
    S.items_processed AS child_tables_done,
    S.current_relid AS current_child_table_relid,
    0::double precision AS delay_time
FROM sdb_progress S WHERE S.command = 'ANALYZE')sql";

}  // namespace sdb::pg
