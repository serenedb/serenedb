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

#pragma once

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "catalog/table_options.h"
#include "connector/index_source.h"
#include "connector/index_source_rocksdb.h"
#include "connector/view_fast_path.h"

namespace rocksdb {

class Snapshot;
class Transaction;

}  // namespace rocksdb
namespace sdb::connector {

class ViewRocksDBIndexSource final : public IndexSource {
 public:
  ViewRocksDBIndexSource(duckdb::ClientContext& context, ViewFastPath fast_path,
                         std::span<const duckdb::idx_t> projected_columns,
                         std::span<const duckdb::LogicalType> projected_types,
                         std::span<const catalog::Column::Id> bind_column_ids,
                         const rocksdb::Snapshot* snapshot,
                         rocksdb::Transaction* txn);

  PrimaryKeyBatch CreatePkBatch() const final {
    return PrimaryKeyBatch{std::in_place_type<PrimaryKeysBytes>};
  }
  duckdb::idx_t Materialize(duckdb::ClientContext& context,
                            PrimaryKeyBatch& batch, duckdb::idx_t start,
                            duckdb::idx_t count,
                            duckdb::DataChunk& output) final;

 private:
  ViewFastPath _fast_path;

  std::vector<duckdb::idx_t> _real_proj_slots;
  duckdb::vector<duckdb::LogicalType> _scratch_types;
  duckdb::vector<duckdb::LogicalType> _projected_types;
  // nullptr when scratch and projected types match.
  std::vector<duckdb::unique_ptr<duckdb::ExpressionExecutor>> _cast_executors;
  // ExpressionExecutor holds the Expression by ref, must outlive it.
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _cast_expressions;

  // Per cast column: scratch Vector + its cache. ResetFromCache before each
  // call clears stale invalid bits (DeserializeValueIntoDuckDB only ever
  // SetInvalids; without reset, prior calls' invalids would persist).
  // unique_ptr because VectorCache is non-movable.
  std::vector<duckdb::unique_ptr<duckdb::VectorCache>> _per_col_scratch_cache;
  std::vector<duckdb::unique_ptr<duckdb::Vector>> _per_col_scratch;

  std::vector<duckdb::Vector*> _decode_targets;

  // optional<> because RocksDBIndexSource is built in the ctor body, after
  // the projection arrays it consumes are filled.
  std::optional<RocksDBIndexSource> _inner_lookup;
};

}  // namespace sdb::connector
