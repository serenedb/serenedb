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

#include <absl/functional/function_ref.h>

#include "connector/search_filter_builder.hpp"

namespace sdb::connector {

bool ClaimCurveMatch(
  irs::BooleanFilter& root,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters, size_t index,
  const ExpressionGetter& expressions, duckdb::ClientContext& context);

struct CurveRange {
  duckdb::unique_ptr<duckdb::Expression> component;
  const duckdb::Expression* lower;
  const duckdb::Expression* upper;
  bool lower_inclusive;
  bool upper_inclusive;
};

std::vector<CurveRange> CollectCurveRanges(
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  absl::FunctionRef<
    duckdb::unique_ptr<duckdb::Expression>(const duckdb::Expression&)>
    normalize);

bool AddCurveRangeCandidates(irs::BooleanFilter& root,
                             std::span<const CurveRange> ranges,
                             const duckdb::Expression& key,
                             const SearchColumnInfo& info,
                             duckdb::ClientContext& context);

bool AddCartesianCandidates(irs::BooleanFilter& root,
                            const duckdb::Expression& expression,
                            const ColumnGetter& columns,
                            const ExpressionGetter& expressions,
                            duckdb::ClientContext& context);

}  // namespace sdb::connector
