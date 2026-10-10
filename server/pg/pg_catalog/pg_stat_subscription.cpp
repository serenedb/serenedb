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

#include "pg/pg_catalog/pg_stat_subscription.h"

#include <deque>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "pg/commands/create_subscription.h"
#include "pg/pg_catalog/fwd.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNoWorker = MaskFromNulls({
  GetIndex(&PgStatSubscription::worker_type),
  GetIndex(&PgStatSubscription::pid),
  GetIndex(&PgStatSubscription::leader_pid),
  GetIndex(&PgStatSubscription::relid),
  GetIndex(&PgStatSubscription::received_lsn),
  GetIndex(&PgStatSubscription::last_msg_send_time),
  GetIndex(&PgStatSubscription::last_msg_receipt_time),
  GetIndex(&PgStatSubscription::latest_end_lsn),
  GetIndex(&PgStatSubscription::latest_end_time),
});

constexpr uint64_t kApplyWorker = MaskFromNulls({
  GetIndex(&PgStatSubscription::leader_pid),
  GetIndex(&PgStatSubscription::relid),
});

Timestamptz Time(int64_t micros) {
  return Timestamptz{.micros = micros, .is_null = micros == 0};
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgStatSubscription>::GetTableData() {
  irs::containers::FlatHashMap<duckdb::idx_t,
                               replication::SubscriptionEngine::SubRuntime>
    runtime;
  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    runtime =
      engine->RuntimeSnapshot(GetDatabase().GetName().GetIdentifierName());
  }
  std::deque<std::string> lsns;
  std::vector<PgStatSubscription> values;
  std::vector<uint64_t> null_masks;
  auto& database = GetDatabase().Cast<catalog::SereneDBCatalog>();
  database.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
    .Scan(
      database.GetCatalogTransaction(_context),
      [&](duckdb::CatalogEntry& entry) {
        auto& row = values.emplace_back(PgStatSubscription{
          .subid = entry.oid,
          .subname = entry.name.GetIdentifierName(),
        });
        const auto it = runtime.find(entry.oid);
        if (it == runtime.end()) {
          null_masks.push_back(kNoWorker);
          return;
        }
        const auto& worker = it->second;
        row.worker_type = "apply";
        row.pid = static_cast<int32_t>(entry.oid & 0x7FFFFFFF);
        row.received_lsn = lsns.emplace_back(FormatLsn(worker.received_lsn));
        row.last_msg_send_time = Time(worker.last_send_time);
        row.last_msg_receipt_time = Time(worker.last_receipt_time);
        row.latest_end_lsn = lsns.emplace_back(FormatLsn(worker.flushed_lsn));
        row.latest_end_time = Time(worker.latest_end_time);
        null_masks.push_back(kApplyWorker);
      });
  auto result = CreateColumns<PgStatSubscription>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], null_masks[row], row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
