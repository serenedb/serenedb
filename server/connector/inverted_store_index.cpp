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

#include "connector/inverted_store_index.h"

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

ObjectId OptionId(const duckdb::case_insensitive_map_t<duckdb::Value>& options,
                  const char* key) {
  auto it = options.find(key);
  SDB_ENSURE(it != options.end(), ERROR_INTERNAL,
             "store index is missing the ", key, " option");
  return ObjectId{it->second.GetValue<uint64_t>()};
}

struct InvertedStoreBuildGlobalState final : duckdb::IndexBuildGlobalState {
  duckdb::unique_ptr<InvertedStoreIndex> index;
};

struct InvertedStoreBuildLocalState final : duckdb::IndexBuildLocalState {};

}  // namespace

InvertedStoreIndex::InvertedStoreIndex(
  const std::string& name, duckdb::TableIOManager& io,
  const duckdb::vector<duckdb::column_t>& column_ids,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
  duckdb::AttachedDatabase& db, ObjectId table_id, ObjectId index_id)
  : BoundIndex(name, kTypeName, duckdb::IndexConstraintType::NONE, column_ids,
               io, exprs, db),
    _table_id{table_id},
    _index_id{index_id} {}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return {};
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    return duckdb::ErrorData(duckdb::NotImplementedException(
      "store inverted index appends outside a commit (WAL replay) are not "
      "wired yet"));
  }
  auto writer =
    CreateInvertedIndexWriter<DuckDBWriteKind::Insert>(_table_id, _index_id,
                                                       *conn);
  if (!writer) {
    return {};
  }
  if (!writer->IndexedExpressions().empty()) {
    return duckdb::ErrorData(duckdb::NotImplementedException(
      "store inverted index expressions are not wired yet"));
  }
  auto snapshot = conn->EnsureCatalogSnapshot();
  auto table = snapshot->GetObject<catalog::Table>(_table_id);
  if (!table) {
    return {};
  }

  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  std::vector<std::string> keys(count);
  std::vector<std::string_view> key_views(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    keys[i] = key_utils::PrepareColumnKey(_table_id, catalog::Column::Id{0});
    primary_key::AppendSigned(keys[i], static_cast<int64_t>(row));
    key_views[i] = keys[i];
  }

  writer->Init(count, chunk);
  duckdb::idx_t pos = 0;
  for (const auto& col : table->Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    if (pos >= chunk.ColumnCount()) {
      break;
    }
    const ColumnDescriptor desc{col.GetId(), col.type};
    writer->SwitchColumn(desc, chunk.data[pos], key_views, count);
    ++pos;
  }
  writer->Finish();
  return {};
}

duckdb::ErrorData InvertedStoreIndex::Append(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

duckdb::ErrorData InvertedStoreIndex::Insert(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

void InvertedStoreIndex::Delete(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                                duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    return;
  }
  auto writer =
    CreateInvertedIndexWriter<DuckDBWriteKind::Delete>(_table_id, _index_id,
                                                       *conn);
  if (!writer) {
    return;
  }
  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  std::string key;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    key.clear();
    primary_key::AppendSigned(key, static_cast<int64_t>(row));
    writer->DeleteRow(key);
  }
  writer->Finish();
}

idx_t InvertedStoreIndex::TryDelete(
  duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
  duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
  duckdb::optional_ptr<duckdb::SelectionVector>) {
  Delete(l, chunk, row_ids);
  if (deleted_sel) {
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      deleted_sel->set_index(i, i);
    }
  }
  return chunk.size();
}

std::string InvertedStoreIndex::ToString(duckdb::IndexLock&, bool) {
  return "inverted store index";
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext,
  const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  // Postings live in the iresearch shard; one empty allocator entry keeps
  // the info IsValid() for WAL/checkpoint round-trips.
  duckdb::IndexStorageInfo info{name};
  info.allocator_infos.emplace_back();
  info.options[kTableIdOption] = duckdb::Value::UBIGINT(_table_id.id());
  info.options[kIndexIdOption] = duckdb::Value::UBIGINT(_index_id.id());
  return info;
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToWAL(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options) {
  return SerializeToDisk(duckdb::QueryContext{}, options);
}

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
}

void AttachInvertedStoreIndexCallbacks(duckdb::IndexType& type) {
  type.build_bind = [](duckdb::IndexBuildBindInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildBindData> { return nullptr; };
  type.build_global_init = [](duckdb::IndexBuildInitGlobalStateInput& input)
    -> duckdb::unique_ptr<duckdb::IndexBuildGlobalState> {
    auto state = duckdb::make_uniq<InvertedStoreBuildGlobalState>();
    state->index = duckdb::make_uniq<InvertedStoreIndex>(
      input.info.index_name,
      duckdb::TableIOManager::Get(input.table.GetStorage()),
      input.storage_ids, input.expressions, input.table.GetStorage().db,
      OptionId(input.info.options, InvertedStoreIndex::kTableIdOption),
      OptionId(input.info.options, InvertedStoreIndex::kIndexIdOption));
    return std::move(state);
  };
  type.build_local_init = [](duckdb::IndexBuildInitLocalStateInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildLocalState> {
    return duckdb::make_uniq<InvertedStoreBuildLocalState>();
  };
  type.build_sink = [](duckdb::IndexBuildSinkInput&,
                       duckdb::DataChunk& key_chunk, duckdb::DataChunk&) {
    // Pre-flip the store tables are empty at CREATE INDEX; the flip wires
    // the initial build over existing rows.
    if (key_chunk.size() != 0) {
      throw duckdb::NotImplementedException(
        "store inverted index initial build over existing rows is not "
        "wired yet");
    }
  };
  type.build_combine = [](duckdb::IndexBuildCombineInput&) {};
  type.build_finalize = [](duckdb::IndexBuildFinalizeInput& input)
    -> duckdb::unique_ptr<duckdb::BoundIndex> {
    auto& gstate = input.global_state.Cast<InvertedStoreBuildGlobalState>();
    return std::move(gstate.index);
  };
  type.create_instance = [](duckdb::CreateIndexInput& input)
    -> duckdb::unique_ptr<duckdb::BoundIndex> {
    return duckdb::make_uniq<InvertedStoreIndex>(
      input.name, input.table_io_manager, input.column_ids,
      input.unbound_expressions, input.db,
      OptionId(input.storage_info.options,
               InvertedStoreIndex::kTableIdOption),
      OptionId(input.storage_info.options,
               InvertedStoreIndex::kIndexIdOption));
  };
}

}  // namespace sdb::connector
