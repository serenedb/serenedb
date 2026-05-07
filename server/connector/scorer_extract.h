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

#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <optional>
#include <string_view>

#include "basics/result_or.h"
#include "catalog/scorer.h"

namespace sdb::connector {

// Extract a `catalog::Scorer` from a bound scorer function call. The
// expression must be one of the connector's scorer pseudo-functions
// (bm25, tfidf, raw_tf, lm_jm, lm_dirichlet, indri_dirichlet, dfi). The
// caller has already resolved the function name from `func.function.name`.
//
// children[0] is the tableoid anchor (for `BM25(idx.tableoid, ...)` SQL
// expressions) or a placeholder constant (for `WITH (optimize_top_k =
// 'bm25(...)')` strings). Either way it's skipped here -- only children[1+]
// carry scorer parameters.
//
// Returns nullopt if any param child is non-constant (the optimizer rule
// uses this signal to refuse to claim the expression). Throws via the
// returned Result on out-of-range param values.
ResultOr<std::optional<catalog::Scorer>> ExtractScorerFromBound(
  const duckdb::BoundFunctionExpression& func, std::string_view name);

}  // namespace sdb::connector
