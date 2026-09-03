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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <span>
#include <string>

#include "connector/column_id.h"

namespace sdb::connector {

std::vector<ColumnId> CollectDependentColumns(const duckdb::Expression& expr);

std::string SerializeBoundExpression(const duckdb::Expression& expr);

duckdb::unique_ptr<duckdb::Expression> DeserializeBoundExpression(
  std::string_view bytes, duckdb::ClientContext& context);

// Rewrites binder-state noise so bytes match across binding contexts:
// alias/query_location cleared, is_operator=false, column refs keyed by
// stable catalog (table_id, col_id) instead of binder-allocated indices.
duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, duckdb::idx_t table_id,
  std::span<const ColumnId> col_index_to_id, duckdb::ClientContext& context);

void RejectJsonObjectArrayLeaves(const duckdb::Vector& result,
                                 duckdb::idx_t num_rows);

}  // namespace sdb::connector
