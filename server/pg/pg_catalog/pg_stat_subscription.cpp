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

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/subscription.h"
#include "pg/system_table.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {
namespace {

// PostgreSQL renders an LSN as two 32-bit halves in uppercase hex ("X/Y").
std::string FormatLsn(uint64_t lsn) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "%X/%X", static_cast<uint32_t>(lsn >> 32),
                static_cast<uint32_t>(lsn & 0xFFFFFFFFULL));
  return buf;
}

}  // namespace

template<>
catalog::MaterializedData
SystemTableSnapshot<PgStatSubscription>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  auto db_id = GetDatabaseId();

  auto subs = catalog->GetSubscriptions(db_id);

  std::vector<replication::SubscriptionEngine::SubRuntime> runtime;
  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    runtime = engine->RuntimeSnapshot(db_id);
  }

  auto result = CreateColumns<PgStatSubscription>(runtime.size());
  duckdb::idx_t row = 0;
  for (const auto& rt : runtime) {
    std::string_view subname;
    for (const auto& sub : subs) {
      if (sub->GetId() == rt.subscription_id) {
        subname = sub->GetName();
        break;
      }
    }
    // These strings back the Text (string_view) fields; keep them alive across
    // the WriteData call below.
    const std::string received = FormatLsn(rt.received_lsn);
    const std::string latest = FormatLsn(rt.flushed_lsn);

    PgStatSubscription pg_row{
      .subid = static_cast<Oid>(rt.subscription_id.id()),
      .subname = Name{subname},
      .worker_type = Text{"apply"},
      // No OS process backs the async apply loop; expose a stable synthetic id.
      .pid = static_cast<int32_t>(rt.subscription_id.id() & 0x7FFFFFFFULL),
      .leader_pid = 0,  // NULL (no parallel workers)
      .relid =
        static_cast<Oid>(0),  // NULL (single apply worker, not per-table)
      .received_lsn = Text{received},
      .last_msg_send_time = Timestamptz{},     // unknown (NULL)
      .last_msg_receipt_time = Timestamptz{},  // unknown (NULL)
      .latest_end_lsn = Text{latest},
      .latest_end_time = Timestamptz{},  // unknown (NULL)
    };
    // Null leader_pid (column 4) and relid (column 5).
    const uint64_t null_mask = (uint64_t{1} << 4) | (uint64_t{1} << 5);
    WriteData(result, pg_row, null_mask, row, *catalog);
    ++row;
  }
  return {std::move(result), row};
}

}  // namespace sdb::pg
