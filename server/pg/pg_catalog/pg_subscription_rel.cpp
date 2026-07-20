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

#include "pg/pg_catalog/pg_subscription_rel.h"

#include <vector>

#include "pg/system_table.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {

// Synthesized from the live apply workers: one row per (subscription, replicated
// local table). SereneDB has no separate tablesync worker stage, so a table
// that is being applied is reported as Ready ('r'). srsublsn is NULL -- we do
// not track a per-table sync-completion LSN. Only running subscriptions
// contribute rows (a disabled subscription has no live worker).
template<>
catalog::MaterializedData
SystemTableSnapshot<PgSubscriptionRel>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  auto db_id = GetDatabaseId();

  std::vector<replication::SubscriptionEngine::SubRuntime> runtime;
  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    runtime = engine->RuntimeSnapshot(db_id);
  }

  duckdb::idx_t count = 0;
  for (const auto& rt : runtime) {
    count += rt.tables.size();
  }

  auto result = CreateColumns<PgSubscriptionRel>(count);
  duckdb::idx_t row = 0;
  for (const auto& rt : runtime) {
    for (const ObjectId table_id : rt.tables) {
      PgSubscriptionRel pg_row{
        .srsubid = static_cast<Oid>(rt.subscription_id.id()),
        .srrelid = static_cast<Oid>(table_id.id()),
        .srsubstate = PgSubscriptionRel::Srsubstate::Ready,
        .srsublsn = Empty{},
      };
      WriteData(result, pg_row, /*null_mask=*/0, row, *catalog);
      ++row;
    }
  }
  return {std::move(result), row};
}

}  // namespace sdb::pg
