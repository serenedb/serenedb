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
#include <span>
#include <string>

#include "catalog/table_options.h"

namespace sdb::connector {

struct IndexedExpression {
  duckdb::unique_ptr<duckdb::Expression> normalized_expr;
  std::string serialized;
  std::vector<catalog::Column::Id> dependent_columns;
  irs::field_id field_id = 0;
  bool is_geojson = false;
};

const duckdb::BoundColumnRefExpression* TryGetJsonLeafColumnRef(
  const duckdb::Expression& expr);

std::vector<catalog::Column::Id> CollectDependentColumns(
  const duckdb::Expression& expr);

std::string SerializeBoundExpression(const duckdb::Expression& expr);

duckdb::unique_ptr<duckdb::Expression> DeserializeBoundExpression(
  std::string_view bytes, duckdb::ClientContext& context);

// Rewrites binder-state noise so bytes match across binding contexts:
// alias/query_location cleared, is_operator=false, column refs keyed by
// stable catalog (table_id, col_id) instead of binder-allocated indices.
duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, ObjectId table_id,
  std::span<const catalog::Column::Id> col_index_to_id,
  duckdb::ClientContext& context);

duckdb::unique_ptr<duckdb::Expression> ResolveBoundColumnRefsForChunk(
  const duckdb::Expression& expr, const duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id);

void RejectUserDefinedFunctions(const duckdb::Expression& expr,
                                duckdb::ClientContext& context);

// Pre-bind variant: walks parsed tree to reject macros (which inline at bind).
void RejectUserDefinedFunctions(const duckdb::ParsedExpression& expr,
                                duckdb::ClientContext& context);

void RejectJsonObjectArrayLeaves(const duckdb::Vector& result,
                                 duckdb::idx_t num_rows);

duckdb::Vector EvaluateExprOverChunk(
  const duckdb::Expression& bound_expr, duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& context, bool is_geojson = false);

}  // namespace sdb::connector
