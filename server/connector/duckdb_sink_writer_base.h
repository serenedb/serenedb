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

#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <span>
#include <string_view>

#include "connector/index_expression.hpp"
#include "connector/sink_writer_base.hpp"

namespace sdb::connector {

// DuckDB-native version of SinkIndexWriter.
// Same interface pattern, but uses DuckDB types instead of Velox.
class DuckDBSinkIndexWriter {
 public:
  DuckDBSinkIndexWriter() = default;
  virtual ~DuckDBSinkIndexWriter() = default;

  virtual void Init(duckdb::idx_t batch_size, const duckdb::DataChunk& input) {}

  virtual void Finish() = 0;
  virtual void Abort() = 0;

  virtual bool SwitchColumn(const ColumnDescriptor& col,
                            const duckdb::Vector& vec,
                            std::span<const std::string_view> row_keys,
                            duckdb::idx_t count) {
    SDB_ASSERT(false, "SwitchColumn call not implemented");
    return false;
  }

  virtual bool SwitchExpression(const ExpressionDescriptor& expr_desc,
                                const duckdb::Vector& /*vec*/,
                                std::span<const std::string_view> /*row_keys*/,
                                duckdb::idx_t /*count*/) {
    return false;
  }

  virtual std::span<const IndexedExpression> IndexedExpressions() const {
    return {};
  }

  // deletes row denoted by row_key. It is up to concrete writer to perform all
  // necessary deletes.
  virtual void DeleteRow(std::string_view row_key) {
    SDB_ASSERT(false, "DeleteRow call not implemented");
  }
};

}  // namespace sdb::connector
