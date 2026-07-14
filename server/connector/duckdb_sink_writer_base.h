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
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>
#include <string_view>

#include "catalog/table_options.h"
#include "connector/index_expression.hpp"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

struct ColumnDescriptor {
  catalog::Column::Id id;
  duckdb::LogicalType type;
};

struct PkChunk {
  std::span<const std::string_view> keys;
  const duckdb::Vector* column = nullptr;
};

struct ExpressionDescriptor {
  duckdb::LogicalType type;
  irs::field_id field_id = irs::field_limits::invalid();
};

// Index sink used by CREATE INDEX and store-index maintenance sessions:
// one virtual dispatch per column batch / per deleted row.
class DuckDBSinkIndexWriter {
 public:
  DuckDBSinkIndexWriter() = default;
  virtual ~DuckDBSinkIndexWriter() = default;

  virtual void Init(duckdb::idx_t batch_size, const PkChunk& pk) {}

  virtual void Finish() = 0;
  virtual void Abort() = 0;

  virtual bool SwitchColumn(const ColumnDescriptor& col,
                            const duckdb::Vector& vec, duckdb::idx_t count) {
    THROW_SQL_ERROR(ERR_MSG("SwitchColumn call not implemented"));
  }

  virtual bool SwitchExpression(const ExpressionDescriptor& expr_desc,
                                const duckdb::Vector& /*vec*/,
                                duckdb::idx_t /*count*/) {
    return false;
  }

  virtual std::span<const IndexedExpression> IndexedExpressions() const {
    return {};
  }

  virtual void DeleteRow(std::string_view row_key) {
    THROW_SQL_ERROR(ERR_MSG("DeleteRow call not implemented"));
  }
};

}  // namespace sdb::connector
