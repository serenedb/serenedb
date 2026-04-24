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
class Transaction;

}  // namespace rocksdb
namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct SereneDBScanBindData;

// Fills real-column slots of `output` for rows identified by a list of
// primary-key byte strings. Implementations provide per-storage
// resolution (RocksDB point Get, file_row_number IN-filter pushdown on
// parquet/csv/json). Virtual-column slots (rowid / tableoid / score /
// offsets) are the CALLER's responsibility -- this interface only
// handles actual table columns.
//
// Projection metadata is given to the factory at construction time so
// implementations can do reader-bind / catalog lookup work once. The
// pk list is NOT given up-front: each `Materialize(batch, output)` call
// decides how to resolve those specific pks (per-batch IN filter for
// file-backed readers; per-pk Get for RocksDB). Memory is bounded by
// batch_size, not by the full query result.
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

  // `pk_bytes.size()` is the number of rows to fill in `output`
  // (one-to-one by position).
  virtual void Materialize(std::span<const std::string_view> pk_bytes,
                           duckdb::DataChunk& output) = 0;
};

std::unique_ptr<RowMaterializer> MakeRowMaterializer(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  const rocksdb::Snapshot* snapshot,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  rocksdb::Transaction* txn);

}  // namespace sdb::connector
