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

// Build an iresearch filter from a TSQUERY-producing expression
// (ts_phrase, ts_term, ts_like, etc.) bound to a synthetic single-
// column field. Used by standalone ts_offsets() and ts_highlight() forms
// to translate a user-supplied tsquery into a runnable filter without
// going through a SearchScan.
//
// `column_id` may be any value -- the only invariant is that the
// caller's runtime mini-segment indexes with the SAME id so the
// OffsetsCollector's name match holds.
std::shared_ptr<irs::Filter> BuildFilterFromTSQuery(
  duckdb::ClientContext& context, const duckdb::Expression& tsquery_expr,
  catalog::Column::Id column_id,
  const std::shared_ptr<catalog::Tokenizer>& dict_tokenizer);

// Bind data driving OffsetsScalarFn. Two variants packed in one struct:
//
//   * Inline form (attached by RewriteOffsetsCall for ts_offsets(col) on
//     a non-Offs text column). `inverted_index` + `column_id` are set;
//     tokenizer is resolved per chunk via a catalog lookup. The rewrite
//     shifts the call's children to (dummy, col) so body always lands
//     at args.data[1].
//
//   * Standalone form (OffsetsStandaloneBind for user-supplied
//     ts_offsets(dict, body, filter [, limit])). `dict_tokenizer` is set;
//     tokenizer is resolved per chunk via Tokenizer::GetTokenizer.
//     Body comes from args.data[1] by definition.
//
// Exactly one of `inverted_index` / `dict_tokenizer` is set; the
// runtime body always reads args.data[1] for the doc bytes.
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

// Runtime body for both inline and standalone forms. Body arg index is
// derived from the bind data (0 for inline, 1 for standalone). Without
// bind data attached, throws.
void OffsetsScalarFn(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result);

// init_local_state for all OFFSETS overloads. Allocates a per-thread
// OffsetsLocalState carrying a reusable MemoryIndex + a lazily filled
// TokenizerWrapper cache so chunked execution doesn't rebuild iresearch
// infra per scalar call.
duckdb::unique_ptr<duckdb::FunctionLocalState> InitOffsetsLocalState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData* bind_data);

// Bind callback for the standalone overload(s):
//   ts_offsets(VARCHAR dict, VARCHAR body, TSQUERY filter [, INTEGER limit])
// Resolves the dict at bind time (must be foldable), runs the existing
// filter builder against a synthetic `synthetic_body @@ filter`
// expression to translate the TSQUERY into an irs::Filter, and stashes
// both on OffsetsBindData. Throws on unknown dict / malformed filter.
duckdb::unique_ptr<duckdb::FunctionData> OffsetsStandaloneBind(
  duckdb::BindScalarFunctionInput& input);

}  // namespace sdb::connector
