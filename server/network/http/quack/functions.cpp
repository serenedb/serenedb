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

#include "network/http/quack/functions.h"

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "network/http/quack/service.h"
#include "network/server.h"
#include "pg/connection_context.h"

namespace sdb::network::http::quack {
namespace {

std::string_view StateName(QueryState state) {
  switch (state) {
    case QueryState::Idle:
      return "idle";
    case QueryState::Active:
      return "active";
    case QueryState::Finished:
      return "finished";
    case QueryState::Cancelled:
      return "cancelled";
  }
  SDB_UNREACHABLE();
}

struct ConnectionsState final : duckdb::GlobalTableFunctionState {
  std::vector<ConnectionSnapshot> snapshots;
  size_t offset = 0;
};

duckdb::unique_ptr<duckdb::FunctionData> Bind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  if (!auth::ClosureFor(&context, ConnectionContext::Get(context).GetRoleId())
         ->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("must be superuser to manage the Quack server"));
  }
  types = {duckdb::LogicalType::VARCHAR,  duckdb::LogicalType::VARCHAR,
           duckdb::LogicalType::VARCHAR,  duckdb::LogicalType::VARCHAR,
           duckdb::LogicalType::VARCHAR,  duckdb::LogicalType::BIGINT,
           duckdb::LogicalType::TIMESTAMP};
  names = {
    duckdb::Identifier{"server_id"},       duckdb::Identifier{"connection_id"},
    duckdb::Identifier{"query"},           duckdb::Identifier{"state"},
    duckdb::Identifier{"client_id_hash"},  duckdb::Identifier{"cached_rows"},
    duckdb::Identifier{"query_started_at"}};
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  auto state = duckdb::make_uniq<ConnectionsState>();
  if (auto* service = Server::instance().Quack()) {
    state->snapshots = service->Connections();
  }
  return state;
}

duckdb::string_t Text(std::string_view text) {
  return duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())};
}

void Scan(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
          duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<ConnectionsState>();
  const auto count = std::min<size_t>(STANDARD_VECTOR_SIZE,
                                      state.snapshots.size() - state.offset);
  auto server_id =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[0], count);
  auto connection_id =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[1], count);
  auto query =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[2], count);
  auto state_name =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[3], count);
  auto client_id_hash =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[4], count);
  auto cached_rows = duckdb::FlatVector::Writer<int64_t>(output.data[5], count);
  auto started_at =
    duckdb::FlatVector::Writer<duckdb::timestamp_t>(output.data[6], count);
  for (size_t i = 0; i < count; ++i) {
    const auto& snapshot = state.snapshots[state.offset + i];
    server_id.WriteValue(Text(snapshot.server_id));
    connection_id.WriteValue(Text(snapshot.connection_id));
    query.WriteValue(Text(snapshot.sql));
    state_name.WriteValue(Text(StateName(snapshot.state)));
    if (snapshot.client_id_hash.empty()) {
      client_id_hash.WriteNull();
    } else {
      client_id_hash.WriteValue(Text(snapshot.client_id_hash));
    }
    if (snapshot.cached_rows) {
      cached_rows.WriteValue(static_cast<int64_t>(*snapshot.cached_rows));
    } else {
      cached_rows.WriteNull();
    }
    if (snapshot.state == QueryState::Idle) {
      started_at.WriteNull();
    } else {
      started_at.WriteValue(snapshot.started_at);
    }
  }
  state.offset += count;
}

}  // namespace

void RegisterFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(
    duckdb::TableFunction{"quack_active_connections", {}, Scan, Bind, Init});
}

}  // namespace sdb::network::http::quack
