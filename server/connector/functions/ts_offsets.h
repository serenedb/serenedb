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

#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/search/filter.hpp>
#include <memory>

#include "catalog/inverted_index.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"

namespace sdb::connector {

std::shared_ptr<irs::Filter> BuildFilterFromTSQuery(
  duckdb::ClientContext& context, const duckdb::Expression& tsquery_expr,
  catalog::Column::Id column_id,
  const std::shared_ptr<catalog::Tokenizer>& dict_tokenizer);

struct OffsetsBindData final : duckdb::FunctionData {
  std::shared_ptr<const catalog::InvertedIndex> inverted_index;
  catalog::Column::Id column_id = 0;

  std::shared_ptr<catalog::Tokenizer> dict_tokenizer;

  size_t limit = 0;
  std::shared_ptr<irs::Filter> stored_filter;

  bool IsStandalone() const noexcept { return dict_tokenizer != nullptr; }

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final;
  bool Equals(const duckdb::FunctionData& other) const final;
};

void OffsetsScalarFn(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result);

duckdb::unique_ptr<duckdb::FunctionLocalState> InitOffsetsLocalState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData* bind_data);

duckdb::unique_ptr<duckdb::FunctionData> OffsetsStandaloneBind(
  duckdb::BindScalarFunctionInput& input);

}  // namespace sdb::connector
