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

#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/text/term_view.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <utility>

#include "connector/scan/deferred_check.h"

namespace sdb::connector {
namespace {

using WildcardCheckBind = SharedCheckBind<re2::RE2>;

void CheckWildcard(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto& func = state.expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& matcher = *func.BindInfo()->Cast<WildcardCheckBind>().data;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(), [&](duckdb::string_t terms) {
      return irs::MatchStoredTerms(matcher, irs::AsBytesView(terms));
    });
}

}  // namespace

DeferredCheck MakeWildcardCheck(const irs::Filter& source, irs::field_id column,
                                const duckdb::LogicalType& type) {
  auto bind = duckdb::make_uniq<WildcardCheckBind>(
    irs::utils::downCast<irs::ByWildcardNGram>(source).options().matcher);
  return Split(source, column, type, "sdb_wildcard_check", CheckWildcard,
               std::move(bind));
}

}  // namespace sdb::connector
