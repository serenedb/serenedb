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

#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/table_function.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <span>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/commands/create_subscription.h"
#include "pg/types/logical.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {
namespace {

using Engine = replication::SubscriptionEngine;

struct Worker {
  duckdb::idx_t subid;
  Engine::SubRuntime runtime;
};

struct SubscriptionStats {
  duckdb::idx_t subid;
  Engine::SubStats stats;
};

void WriteLsn(duckdb::VectorWriter<duckdb::string_t>& writer, uint64_t lsn) {
  const auto text = FormatLsn(lsn);
  writer.WriteValue(
    duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())});
}

void WriteTime(duckdb::VectorWriter<duckdb::timestamp_tz_t>& writer,
               int64_t micros) {
  if (micros == 0) {
    writer.WriteNull();
    return;
  }
  writer.WriteValue(duckdb::timestamp_tz_t{micros});
}

void LoadWorkers(duckdb::ClientContext& context, duckdb::DataChunk& input,
                 std::vector<Worker>& rows) {
  auto* engine = Engine::gInstance;
  const auto database = catalog::DefaultSereneDBCatalog(context);
  if (!engine || !database) {
    return;
  }
  const auto runtime =
    engine->RuntimeSnapshot(database->GetName().GetIdentifierName());
  const auto subids = input.data[0].Values<int64_t>();
  for (duckdb::idx_t row = 0; row < input.size(); ++row) {
    const auto subid = subids[row];
    if (!subid.IsValid()) {
      for (const auto& [id, worker] : runtime) {
        rows.emplace_back(Worker{.subid = id, .runtime = worker});
      }
      continue;
    }
    const auto it = runtime.find(static_cast<duckdb::idx_t>(subid.GetValue()));
    if (it != runtime.end()) {
      rows.emplace_back(Worker{.subid = it->first, .runtime = it->second});
    }
  }
}

void WriteWorkers(std::span<const Worker> rows, duckdb::DataChunk& output) {
  const auto count = rows.size();
  auto subids = duckdb::FlatVector::Writer<int64_t>(output.data[0], count);
  auto relids = duckdb::FlatVector::Writer<int64_t>(output.data[1], count);
  auto pids = duckdb::FlatVector::Writer<int32_t>(output.data[2], count);
  auto leaders = duckdb::FlatVector::Writer<int32_t>(output.data[3], count);
  auto received =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[4], count);
  auto sent =
    duckdb::FlatVector::Writer<duckdb::timestamp_tz_t>(output.data[5], count);
  auto receipts =
    duckdb::FlatVector::Writer<duckdb::timestamp_tz_t>(output.data[6], count);
  auto ends =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[7], count);
  auto end_times =
    duckdb::FlatVector::Writer<duckdb::timestamp_tz_t>(output.data[8], count);
  auto types =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[9], count);
  for (const auto& [subid, runtime] : rows) {
    subids.WriteValue(static_cast<int64_t>(subid));
    relids.WriteNull();
    pids.WriteValue(static_cast<int32_t>(subid & 0x7FFFFFFF));
    leaders.WriteNull();
    WriteLsn(received, runtime.received_lsn);
    WriteTime(sent, runtime.last_send_time);
    WriteTime(receipts, runtime.last_receipt_time);
    WriteLsn(ends, runtime.flushed_lsn);
    WriteTime(end_times, runtime.latest_end_time);
    types.WriteValue(duckdb::string_t{"apply"});
  }
}

duckdb::unique_ptr<duckdb::FunctionData> WorkersBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  const auto add = [&](std::string_view name, duckdb::LogicalType type) {
    names.emplace_back(duckdb::Identifier{name});
    types.emplace_back(std::move(type));
  };
  add("subid", OID());
  add("relid", OID());
  add("pid", duckdb::LogicalType::INTEGER);
  add("leader_pid", duckdb::LogicalType::INTEGER);
  add("received_lsn", duckdb::LogicalType::VARCHAR);
  add("last_msg_send_time", duckdb::LogicalType::TIMESTAMP_TZ);
  add("last_msg_receipt_time", duckdb::LogicalType::TIMESTAMP_TZ);
  add("latest_end_lsn", duckdb::LogicalType::VARCHAR);
  add("latest_end_time", duckdb::LogicalType::TIMESTAMP_TZ);
  add("worker_type", duckdb::LogicalType::VARCHAR);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

void LoadStats(duckdb::ClientContext&, duckdb::DataChunk& input,
               std::vector<std::optional<SubscriptionStats>>& rows) {
  irs::containers::FlatHashMap<duckdb::idx_t, Engine::SubStats> stats;
  if (auto* engine = Engine::gInstance) {
    stats = engine->Stats();
  }
  const auto subids = input.data[0].Values<int64_t>();
  for (duckdb::idx_t row = 0; row < input.size(); ++row) {
    const auto subid = subids[row];
    if (!subid.IsValid()) {
      rows.emplace_back();
      continue;
    }
    const auto id = static_cast<duckdb::idx_t>(subid.GetValue());
    const auto it = stats.find(id);
    rows.emplace_back(SubscriptionStats{
      .subid = id,
      .stats = it == stats.end() ? Engine::SubStats{} : it->second});
  }
}

void WriteStats(std::span<const std::optional<SubscriptionStats>> rows,
                duckdb::DataChunk& output) {
  const auto count = rows.size();
  auto subids = duckdb::FlatVector::Writer<int64_t>(output.data[0], count);
  std::vector<duckdb::VectorWriter<int64_t>> counters;
  counters.reserve(9);
  for (duckdb::idx_t column = 1; column <= 9; ++column) {
    counters.emplace_back(
      duckdb::FlatVector::Writer<int64_t>(output.data[column], count));
  }
  auto resets =
    duckdb::FlatVector::Writer<duckdb::timestamp_tz_t>(output.data[10], count);
  for (const auto& row : rows) {
    if (!row) {
      subids.WriteNull();
      for (auto& counter : counters) {
        counter.WriteNull();
      }
      resets.WriteNull();
      continue;
    }
    const auto& [subid, stats] = *row;
    subids.WriteValue(static_cast<int64_t>(subid));
    const uint64_t values[] = {stats.apply_error_count,
                               stats.sync_error_count,
                               stats.insert_exists,
                               0,
                               stats.update_exists,
                               stats.update_missing,
                               0,
                               stats.delete_missing,
                               stats.multiple_unique_conflicts};
    for (size_t i = 0; i < counters.size(); ++i) {
      counters[i].WriteValue(static_cast<int64_t>(values[i]));
    }
    WriteTime(resets, stats.stats_reset);
  }
}

duckdb::unique_ptr<duckdb::FunctionData> StatsBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  names.emplace_back("subid");
  types.emplace_back(OID());
  for (const std::string_view name :
       {"apply_error_count", "sync_error_count", "confl_insert_exists",
        "confl_update_origin_differs", "confl_update_exists",
        "confl_update_missing", "confl_delete_origin_differs",
        "confl_delete_missing", "confl_multiple_unique_conflicts"}) {
    names.emplace_back(duckdb::Identifier{name});
    types.emplace_back(duckdb::LogicalType::BIGINT);
  }
  names.emplace_back("stats_reset");
  types.emplace_back(duckdb::LogicalType::TIMESTAMP_TZ);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct OriginStatusState final : duckdb::GlobalTableFunctionState {
  std::vector<ReplicationOrigin> origins;
  size_t next = 0;
};

duckdb::unique_ptr<duckdb::FunctionData> OriginStatusBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  names.emplace_back("local_id");
  types.emplace_back(OID());
  names.emplace_back("external_id");
  types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("remote_lsn");
  types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("local_lsn");
  types.emplace_back(duckdb::LogicalType::VARCHAR);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> OriginStatusInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput&) {
  auto state = duckdb::make_uniq<OriginStatusState>();
  state->origins = ReplicationOrigins(*context.db);
  return state;
}

void OriginStatusScan(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                      duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<OriginStatusState>();
  const auto count =
    std::min<size_t>(state.origins.size() - state.next, STANDARD_VECTOR_SIZE);
  auto ids = duckdb::FlatVector::Writer<int64_t>(output.data[0], count);
  auto names =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[1], count);
  auto remote =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[2], count);
  auto local =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[3], count);
  for (const auto& origin :
       std::span{state.origins}.subspan(state.next, count)) {
    ids.WriteValue(static_cast<int64_t>(origin.id));
    names.WriteValue(duckdb::string_t{
      origin.name.data(), static_cast<uint32_t>(origin.name.size())});
    WriteLsn(remote, origin.remote_lsn);
    WriteLsn(local, 0);
  }
  state.next += count;
  output.SetCardinality(count);
}

}  // namespace

void RegisterSubscriptionFunctions(duckdb::ExtensionLoader& loader) {
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  duckdb::TableFunction workers{"pg_stat_get_subscription",
                                {OID()},
                                nullptr,
                                WorkersBind,
                                nullptr,
                                InitRows<Worker>};
  workers.in_out_function = RowsInOut<Worker, &LoadWorkers, &WriteWorkers>;
  RegisterTable(loader, pg_catalog, std::move(workers));
  duckdb::TableFunction stats{"pg_stat_get_subscription_stats",
                              {OID()},
                              nullptr,
                              StatsBind,
                              nullptr,
                              InitRows<std::optional<SubscriptionStats>>};
  stats.in_out_function =
    RowsInOut<std::optional<SubscriptionStats>, &LoadStats, &WriteStats>;
  RegisterTable(loader, pg_catalog, std::move(stats));
  RegisterTable(loader, pg_catalog,
                duckdb::TableFunction{"pg_show_replication_origin_status",
                                      {},
                                      OriginStatusScan,
                                      OriginStatusBind,
                                      OriginStatusInit});
}

}  // namespace sdb::pg
