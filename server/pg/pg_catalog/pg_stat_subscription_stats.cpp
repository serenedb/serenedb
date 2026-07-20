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

#include "pg/pg_catalog/pg_stat_subscription_stats.h"

#include <cstdint>
#include <vector>

#include "catalog/subscription.h"
#include "pg/system_table.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {

template<>
catalog::MaterializedData
SystemTableSnapshot<PgStatSubscriptionStats>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  auto db_id = GetDatabaseId();

  auto subs = catalog->GetSubscriptions(db_id);

  std::vector<replication::SubscriptionEngine::SubErrorStat> stats;
  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    stats = engine->ErrorStats(db_id);
  }

  auto result = CreateColumns<PgStatSubscriptionStats>(subs.size());
  duckdb::idx_t row = 0;
  for (const auto& sub : subs) {
    uint64_t apply_errors = 0;
    for (const auto& s : stats) {
      if (s.subscription_id == sub->GetId()) {
        apply_errors = s.apply_error_count;
        break;
      }
    }
    PgStatSubscriptionStats pg_row{
      .subid = static_cast<Oid>(sub->GetId().id()),
      .subname = Name{sub->GetName()},
      .apply_error_count = static_cast<int64_t>(apply_errors),
      .sync_error_count = 0,
      .confl_insert_exists = 0,
      .confl_update_origin_differs = 0,
      .confl_update_exists = 0,
      .confl_update_missing = 0,
      .confl_delete_origin_differs = 0,
      .confl_delete_missing = 0,
      .confl_multiple_unique_conflicts = 0,
      .stats_reset = Timestamptz{},  // NULL
    };
    WriteData(result, pg_row, /*null_mask=*/0, row, *catalog);
    ++row;
  }
  return {std::move(result), row};
}

}  // namespace sdb::pg
