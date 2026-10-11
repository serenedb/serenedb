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

std::string_view gPgStatProgressCreateIndex = R"sql(SELECT
    S.pid AS pid, S.datid AS datid, S.datname AS datname,
    S.relid AS relid,
    S.current_relid AS index_relid,
    S.command AS command,
    S.phase AS phase,
    0::BIGINT AS lockers_total,
    0::BIGINT AS lockers_done,
    0::BIGINT AS current_locker_pid,
    0::BIGINT AS blocks_total,
    0::BIGINT AS blocks_done,
    S.tuples_total AS tuples_total,
    S.tuples_processed AS tuples_done,
    0::BIGINT AS partitions_total,
    0::BIGINT AS partitions_done
FROM sdb_progress S WHERE S.command IN ('CREATE INDEX', 'REINDEX'))sql";

}  // namespace sdb::pg
