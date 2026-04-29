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

struct ANNFilterContext {
  duckdb::ClientContext& context;
  duckdb::unique_ptr<duckdb::Expression> filter_expr;
  std::vector<duckdb::LogicalType> filter_types;

  const SereneDBScanBindData& bind_data;
  const rocksdb::Snapshot* rocks_snapshot;
  const rocksdb::Transaction* rocksdb_txn;
  std::vector<duckdb::idx_t> filter_projection;
  std::vector<catalog::Column::Id> filter_column_ids;
};

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
  // Cached File-backed lookup session (lazy, reused across is_member calls).
  // Empty for RocksDB-backed tables -- LookupRows dispatches to RocksDBLookup
  // which doesn't need a session. mutable so const is_member can lazily fill.
  mutable std::shared_ptr<FileLookupSession> _file_lookup_session;
  mutable SegmentPkIterator _it;
};

void InitAnnFilterContext(
  std::unique_ptr<ANNFilterContext>& filter, duckdb::ClientContext& context,
  const duckdb::Expression* filter_expression,
  const std::vector<catalog::Column::Id>& filter_column_ids, ObjectId index_id,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data);

}  // namespace sdb::connector
