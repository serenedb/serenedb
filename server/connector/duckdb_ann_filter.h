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

#include <faiss/impl/IDSelector.h>

#include <duckdb.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <limits>
#include <memory>

#include "connector/lookup.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

struct SereneDBScanBindData;

// faiss IDSelector that materializes the candidate row's filter columns and
// evaluates per-row filter expressions to gate HNSW result inclusion.
//
// Per is_member call: pulls the row's PK from iresearch via SegmentPkIterator,
// invokes LookupRows (dispatches File/RocksDB internally) to fetch just the
// filter columns into a reusable scratch chunk, and runs the cached
// ExpressionExecutor.
class ANNFilter final : public faiss::IDSelector {
 public:
  ANNFilter(duckdb::ClientContext& context, const irs::IndexReader& reader,
            const SereneDBScanBindData& bind_data,
            const rocksdb::Snapshot* snapshot, rocksdb::Transaction* txn,
            std::vector<duckdb::idx_t> filter_projected_columns,
            std::vector<duckdb::LogicalType> filter_types,
            std::vector<catalog::Column::Id> filter_bind_column_ids,
            std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs);

  bool is_member(faiss::idx_t id) const override;

 private:
  duckdb::ClientContext& _context;
  const irs::IndexReader& _reader;
  const SereneDBScanBindData& _bind_data;
  const rocksdb::Snapshot* _snapshot;
  rocksdb::Transaction* _txn;

  // Projection metadata for the FILTER columns (a subset of the table's
  // columns -- only what filter expressions reference). Different from the
  // outer scan's projection because the filter only needs its own inputs.
  std::vector<duckdb::idx_t> _filter_projected_columns;
  std::vector<duckdb::LogicalType> _filter_types;
  std::vector<catalog::Column::Id> _filter_bind_column_ids;

  std::vector<duckdb::unique_ptr<duckdb::Expression>> _exprs;
  mutable duckdb::ExpressionExecutor _executor;
  mutable duckdb::DataChunk _scratch;
  mutable duckdb::DataChunk _bool_out;

  // Cached File-backed lookup session (lazy, reused across is_member calls).
  // Empty for RocksDB-backed tables -- LookupRows dispatches to RocksDBLookup
  // which doesn't need a session. mutable so const is_member can lazily fill.
  mutable std::shared_ptr<FileLookupSession> _file_lookup_session;

  // TODO(codeworse): Will be erased, because filter should be per-segment
  // using parallel index execution.
  mutable SegmentPkIterator _it;
  mutable uint32_t _it_segment_id = std::numeric_limits<uint32_t>::max();
};

void InitAnnFilter(
  std::unique_ptr<ANNFilter>& filter, duckdb::ClientContext& context,
  const std::vector<duckdb::unique_ptr<duckdb::Expression>>& filter_expressions,
  const std::vector<catalog::Column::Id>& filter_column_ids, ObjectId index_id,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data);

}  // namespace sdb::connector
