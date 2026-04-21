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
#include <memory>
#include <span>
#include <string_view>

#include "catalog/table_options.h"

namespace rocksdb {

class Snapshot;

}  // namespace rocksdb
namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct SereneDBScanBindData;

// Fills real-column slots of `output` for rows identified by a list of
// primary-key byte strings. Implementations provide per-storage
// resolution (RocksDB point Get, parquet row-number filter pushdown,
// CSV re-scan). Virtual-column slots (rowid / tableoid / score /
// offsets) are the CALLER's responsibility -- this interface only
// handles actual table columns.
//
// Projection metadata and the full pk list are given to the factory at
// construction time so implementations can do eager setup (bind
// parquet, build the OR-equality filter, init a persistent scan, ...).
// Each `Materialize(batch, output)` call then streams the next batch
// of rows with no lazy first-call gating.
//
// Indexing convention (fixed at construction):
// - `projected_columns[proj]` = bind column index for projection slot
//   `proj`, or `duckdb::DConstants::INVALID_INDEX` for virtual slots
//   (skipped by the materializer).
// - `bind_column_ids[bind_col]` = catalog column id of the bind column.
// - `projected_types[proj]` = logical type of output slot `proj`.
//
// Missing rows (not found in storage) set the validity bit of the
// corresponding output slot to INVALID.
class RowMaterializer {
 public:
  virtual ~RowMaterializer() = default;

  // `pk_bytes` is a slice of the `all_pks` given to the factory; its
  // size is the number of rows to fill in `output` (one-to-one by
  // position).
  virtual void Materialize(std::span<const std::string_view> pk_bytes,
                           duckdb::DataChunk& output) = 0;
};

// Factory: picks the right materializer based on the scan's bind_data.
// - RocksDB-backed tables -> RocksDBRowMaterializer (per-row Get with
//   snapshot isolation).
// - File-backed (parquet) tables -> ParquetRowMaterializer. Eagerly
//   binds parquet, decodes `all_pks` into the big `file_row_number IN
//   (...)` filter, and initializes ONE persistent parquet scan. Later
//   Materialize calls stream chunks from that scan, so the per-batch
//   cost is just O(batch_size) hash lookups instead of re-running the
//   parquet init + row-group walk.
// - File-backed (csv / json) tables -> CounterRowMaterializer (reads
//   the file once at construction, serves batches from the in-memory
//   buffer).
//
// `all_pks` must be the full pk list the caller will iterate in
// batches; implementations that benefit (parquet, counter) use it,
// RocksDB ignores it.
std::unique_ptr<RowMaterializer> MakeRowMaterializer(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  const rocksdb::Snapshot* snapshot,
  std::span<const std::string> all_pks,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids);

// Short label naming the materializer that MakeRowMaterializer would
// pick for `bind_data`. Used in EXPLAIN output so the chosen
// materialization strategy is visible at plan time without having to
// construct the materializer.
std::string_view RowMaterializerName(const SereneDBScanBindData& bind_data);

}  // namespace sdb::connector
