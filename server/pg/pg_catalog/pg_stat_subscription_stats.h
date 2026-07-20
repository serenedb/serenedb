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

#pragma once

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-PG-STAT-SUBSCRIPTION-STATS
// One row per subscription in the current database. apply_error_count is
// tracked live by the SubscriptionEngine (cumulative apply-stream failures /
// disable_on_error events, in-memory, reset when the subscription stops being
// supervised). sync_error_count and the conflict counters are always 0 --
// SereneDB does not run tablesync workers or conflict detection. stats_reset is
// NULL (no pg_stat_reset_subscription_stats support).
// NOLINTBEGIN
struct PgStatSubscriptionStats {
  static constexpr uint64_t kId = 171;
  static constexpr std::string_view kName = "pg_stat_subscription_stats";

  Oid subid;
  Name subname;
  int64_t apply_error_count;
  int64_t sync_error_count;
  int64_t confl_insert_exists;
  int64_t confl_update_origin_differs;
  int64_t confl_update_exists;
  int64_t confl_update_missing;
  int64_t confl_delete_origin_differs;
  int64_t confl_delete_missing;
  int64_t confl_multiple_unique_conflicts;
  Timestamptz stats_reset;
};
// NOLINTEND

template<>
catalog::MaterializedData
SystemTableSnapshot<PgStatSubscriptionStats>::GetTableData();

}  // namespace sdb::pg
