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

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgStatSubscription::leader_pid),
  GetIndex(&PgStatSubscription::relid),
});

Timestamptz Time(int64_t micros) {
  return Timestamptz{.micros = micros, .is_null = micros == 0};
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgStatSubscription>::GetTableData() {
  std::vector<replication::SubscriptionEngine::SubRuntime> runtime;
  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    runtime =
      engine->RuntimeSnapshot(GetDatabase().GetName().GetIdentifierName());
  }
  auto& database = GetDatabase().Cast<catalog::SereneDBCatalog>();
  const auto transaction = database.GetCatalogTransaction(_context);
  std::deque<std::string> names;
  std::deque<std::string> lsns;
  std::vector<PgStatSubscription> values;
  for (const auto& subscription : runtime) {
    auto entry = database.GetOidIndex().GetVisible(subscription.subscription,
                                                   transaction.view);
    if (!entry || entry->type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
      continue;
    }
    const auto& name = names.emplace_back(entry->name.GetIdentifierName());
    const auto& received =
      lsns.emplace_back(FormatLsn(subscription.received_lsn));
    const auto& flushed =
      lsns.emplace_back(FormatLsn(subscription.flushed_lsn));
    values.push_back(PgStatSubscription{
      .subid = subscription.subscription,
      .subname = name,
      .worker_type = "apply",
      .pid = static_cast<int32_t>(subscription.subscription & 0x7FFFFFFF),
      .leader_pid = 0,
      .relid = 0,
      .received_lsn = received,
      .last_msg_send_time = Time(subscription.last_send_time),
      .last_msg_receipt_time = Time(subscription.last_receipt_time),
      .latest_end_lsn = flushed,
      .latest_end_time = Time(subscription.latest_end_time),
    });
  }
  auto result = CreateColumns<PgStatSubscription>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
