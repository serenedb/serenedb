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

#include "connector/index_source.h"
#include "connector/search_pk_lookup.h"

namespace rocksdb {

class Snapshot;
class Transaction;

}  // namespace rocksdb
namespace sdb::connector {

struct SereneDBScanBindData;

struct ANNFilterContext {
  duckdb::ClientContext& context;
  duckdb::unique_ptr<duckdb::Expression> filter_expr;
  std::vector<duckdb::LogicalType> filter_types;

  const SereneDBScanBindData& bind_data;
  const rocksdb::Snapshot* rocksdb_snapshot;
  rocksdb::Transaction* rocksdb_txn;
  std::vector<duckdb::idx_t> filter_projection;
  std::vector<catalog::Column::Id> filter_column_ids;
};

// faiss IDSelector: per HNSW candidate, materializes the row's filter
// columns and evaluates the filter expressions to gate inclusion.
class ANNFilter final : public faiss::IDSelector {
 public:
  ANNFilter(const ANNFilterContext& ctx, const irs::SubReader& segment);

  bool is_member(faiss::idx_t id) const final;

 private:
  const ANNFilterContext& _ctx;
  const irs::SubReader& _segment;
  mutable duckdb::ExpressionExecutor _executor;
  mutable duckdb::DataChunk _scratch;
  mutable duckdb::DataChunk _bool_out;

  // Default-constructed to std::monostate; switched on first is_member call.
  mutable PrimaryKeyBatch _pk_batch;

  mutable std::shared_ptr<IndexSource> _index_source;

  mutable SegmentPkIterator _it;
};

void InitAnnFilterContext(
  std::unique_ptr<ANNFilterContext>& filter, duckdb::ClientContext& context,
  const duckdb::Expression* filter_expression,
  const std::vector<catalog::Column::Id>& filter_column_ids, ObjectId index_id,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data);

}  // namespace sdb::connector
