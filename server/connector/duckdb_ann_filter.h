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
#include <iresearch/search/filter.hpp>
#include <limits>
#include <memory>
#include <optional>

#include "catalog/table_options.h"
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

// Row-by-row filter: per HNSW candidate, materializes the row's filter
// columns and evaluates the filter expression. Expensive (RocksDB lookup
// per candidate).
class ANNFilter {
 public:
  ANNFilter(const ANNFilterContext& ctx);

  void Reset(const irs::IndexReader& reader, size_t seg_idx);
  bool Accept(faiss::idx_t id) const;

 private:
  const ANNFilterContext& _ctx;
  mutable duckdb::ExpressionExecutor _executor;
  mutable duckdb::DataChunk _scratch;
  mutable duckdb::DataChunk _bool_out;

  mutable PrimaryKeyBatch _pk_batch;
  mutable std::shared_ptr<IndexSource> _index_source;
  // Owns the per-(query, segment) ReadContext for the PK column. Reset()
  // per segment.
  mutable irs::ColumnReader::BlobPointReader _pk_cursor;
};

void InitAnnFilterContext(
  std::unique_ptr<ANNFilterContext>& filter, duckdb::ClientContext& context,
  const duckdb::Expression* filter_expression,
  const std::vector<catalog::Column::Id>& filter_column_ids,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data);

class TextScanFilter {
 public:
  TextScanFilter(const irs::Filter::Query& query);

  bool Accept(faiss::idx_t id) const;
  void Reset(const irs::SubReader& segment);

 private:
  const irs::Filter::Query& _query;
  mutable irs::DocIterator::ptr _it;
};

class CompositeScanFilter final : public faiss::IDSelector {
 public:
  CompositeScanFilter() = default;

  void EnableText(const irs::Filter::Query& query) { _text.emplace(query); }
  void EnableAnn(const ANNFilterContext& ctx) { _ann.emplace(ctx); }

  bool Empty() const { return !_text && !_ann; }

  bool is_member(faiss::idx_t id) const final;

  void Reset(const irs::IndexReader& reader, size_t seg_idx);

 private:
  std::optional<TextScanFilter> _text;
  std::optional<ANNFilter> _ann;
};

}  // namespace sdb::connector
