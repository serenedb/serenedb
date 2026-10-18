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

#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "pg/pg_catalog/fwd.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {

template<>
MaterializedData SystemTableSnapshot<PgStatSubscriptionStats>::GetTableData() {
  irs::containers::FlatHashMap<duckdb::idx_t,
                               replication::SubscriptionEngine::SubStats>
    stats;
  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    stats = engine->Stats();
  }
  std::vector<PgStatSubscriptionStats> values;
  auto& database = GetDatabase().Cast<catalog::SereneDBCatalog>();
  database.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
    .Scan(database.GetCatalogTransaction(_context),
          [&](duckdb::CatalogEntry& entry) {
            replication::SubscriptionEngine::SubStats stat;
            if (const auto it = stats.find(entry.oid); it != stats.end()) {
              stat = it->second;
            }
            const auto count = [](uint64_t value) {
              return static_cast<int64_t>(value);
            };
            values.push_back(PgStatSubscriptionStats{
              .subid = entry.oid,
              .subname = entry.name.GetIdentifierName(),
              .apply_error_count = count(stat.apply_error_count),
              .sync_error_count = count(stat.sync_error_count),
              .confl_insert_exists = count(stat.insert_exists),
              .confl_update_origin_differs = 0,
              .confl_update_exists = count(stat.update_exists),
              .confl_update_missing = count(stat.update_missing),
              .confl_delete_origin_differs = 0,
              .confl_delete_missing = count(stat.delete_missing),
              .confl_multiple_unique_conflicts =
                count(stat.multiple_unique_conflicts),
              .stats_reset = {.micros = stat.stats_reset,
                              .is_null = stat.stats_reset == 0},
            });
          });
  auto result = CreateColumns<PgStatSubscriptionStats>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
