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
#include <iresearch/index/index_reader.hpp>
#include <memory>
#include <vector>

#include "connector/row_materializer.h"

namespace sdb::connector {

class ANNFilter final : public faiss::IDSelector {
 public:
  ANNFilter(duckdb::ClientContext& context, const irs::IndexReader& reader,
            std::unique_ptr<RowMaterializer> materializer,
            std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs,
            std::vector<duckdb::LogicalType> filter_types);

  bool is_member(faiss::idx_t id) const override;

 private:
  const irs::IndexReader& reader_;
  std::unique_ptr<RowMaterializer> materializer_;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs_;
  mutable duckdb::ExpressionExecutor executor_;
  mutable duckdb::DataChunk scratch_;   // 1 row, filter columns
  mutable duckdb::DataChunk bool_out_;  // 1 row, BOOLEAN per expression
};

}  // namespace sdb::connector
