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

#include "pg/pg_catalog/pg_replication_origin.h"

#include <absl/strings/str_cat.h>

#include <deque>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "pg/commands/create_subscription.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

struct Origin {
  duckdb::idx_t id = 0;
  std::string name;
  uint64_t remote_lsn = 0;
};

std::vector<Origin> CollectOrigins(duckdb::DatabaseInstance& db) {
  std::vector<Origin> origins;
  for (const auto& database : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    catalog.Cast<duckdb::DuckCatalog>()
      .GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
      .Scan([&](duckdb::CatalogEntry& entry) {
        origins.push_back({
          .id = entry.oid,
          .name = absl::StrCat("pg_", entry.oid),
          .remote_lsn =
            entry.Cast<catalog::SubscriptionCatalogEntry>().RemoteLsn(),
        });
      });
  }
  return origins;
}

struct OriginStatusState final : public duckdb::GlobalTableFunctionState {
  std::vector<Origin> origins;
  size_t offset = 0;
};

duckdb::unique_ptr<duckdb::FunctionData> BindOriginStatus(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  return_types = {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR,
                  duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR};
  names.emplace_back("local_id");
  names.emplace_back("external_id");
  names.emplace_back("remote_lsn");
  names.emplace_back("local_lsn");
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitOriginStatus(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput&) {
  auto state = duckdb::make_uniq<OriginStatusState>();
  state->origins = CollectOrigins(*context.db);
  return state;
}

void ScanOriginStatus(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                      duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<OriginStatusState>();
  duckdb::idx_t row = 0;
  for (; state.offset < state.origins.size() && row < STANDARD_VECTOR_SIZE;
       ++state.offset, ++row) {
    const auto& origin = state.origins[state.offset];
    output.SetValue(0, row,
                    duckdb::Value::BIGINT(static_cast<int64_t>(origin.id)));
    output.SetValue(1, row, duckdb::Value{origin.name});
    output.SetValue(2, row, duckdb::Value{FormatLsn(origin.remote_lsn)});
    output.SetValue(3, row, duckdb::Value{FormatLsn(0)});
  }
  output.SetChildCardinality(row);
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgReplicationOrigin>::GetTableData() {
  auto origins = CollectOrigins(*_context.db);
  std::vector<PgReplicationOrigin> values;
  values.reserve(origins.size());
  for (const auto& origin : origins) {
    values.push_back({.roident = origin.id, .roname = origin.name});
  }
  auto result = CreateColumns<PgReplicationOrigin>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

void RegisterReplicationOriginStatus(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::TableFunction function("pg_show_replication_origin_status",
                                 duckdb::FunctionSignature{}, ScanOriginStatus,
                                 BindOriginStatus, InitOriginStatus);
  loader.RegisterFunction(function);
}

}  // namespace sdb::pg
