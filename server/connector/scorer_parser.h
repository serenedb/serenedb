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

#include <duckdb/main/client_context.hpp>

#include <string_view>

#include "basics/result_or.h"
#include "catalog/scorer.h"

namespace sdb::connector {

// Parse a `WITH (optimize_top_k = '<scorer-expr>')` value into a
// catalog::Scorer. Accepts the same scorer call shapes the optimizer
// recognises in `BM25(idx.tableoid, ...)` / `TFIDF(idx.tableoid, ...)` SQL
// expressions, minus the tableoid anchor:
//
//   bm25                       -- defaults
//   bm25(1.2, 0.75)            -- positional k1, b
//   tfidf, tfidf(true)
//   raw_tf
//   lm_jm, lm_jm(0.1)
//   lm_dirichlet, lm_dirichlet(2000)
//   indri_dirichlet, indri_dirichlet(2000)
//   dfi, dfi('saturated')
//
// Implementation: the user string is reparsed and rewritten into a
// `bm25(0::BIGINT, ...)`-shaped call (the placeholder satisfies the
// scorer functions' first-arg type), then bound through DuckDB's
// ConstantBinder, and the resulting BoundFunctionExpression is fed into
// the same param-extraction helper used by the
// `ORDER BY BM25(...)` optimizer rule (see scorer_extract.h). Function
// overload resolution and implicit numeric coercion therefore come from
// DuckDB and stay consistent across both call sites.
//
// `context` is needed to construct the binder; in practice the DDL paths
// already hold a ClientContext (CatalogTransaction::GetContext or the
// PhysicalCreateIndex sink's ClientContext).
ResultOr<catalog::Scorer> ParseScorerExpression(
  duckdb::ClientContext& context, std::string_view input);

}  // namespace sdb::connector
