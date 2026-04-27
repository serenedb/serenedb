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

#include "connector/row_materializer.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

class ANNFilter final : public faiss::IDSelector {
 public:
  ANNFilter(duckdb::ClientContext& context, const irs::IndexReader& reader,
            std::unique_ptr<RowMaterializer> materializer,
            std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs,
            std::vector<duckdb::LogicalType> filter_types);

  bool is_member(faiss::idx_t id) const override;

 private:
  const irs::IndexReader& _reader;
  std::unique_ptr<RowMaterializer> _materializer;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _exprs;
  mutable duckdb::ExpressionExecutor _executor;
  mutable duckdb::DataChunk _scratch;
  mutable duckdb::DataChunk _bool_out;
  // TODO(codeworse): Will be erased, because filter should be per-segment
  // using parallel index execution
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
