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

std::string_view gPgStatProgressCluster = R"sql(SELECT
    S.pid AS pid,
    S.datid AS datid,
    S.datname AS datname,
    S.relid AS relid,
    S.command AS command,
    S.phase AS phase,
    S.current_relid AS cluster_index_relid,
    S.tuples_processed AS heap_tuples_scanned,
    0::BIGINT AS heap_tuples_written,
    S.steps_total AS heap_blks_total,
    S.step AS heap_blks_scanned,
    0::BIGINT AS index_rebuild_count
FROM sdb_progress S WHERE S.command = 'CLUSTER')sql";

}  // namespace sdb::pg
