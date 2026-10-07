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
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <utility>

#include "connector/scan/deferred_check.h"

namespace sdb::connector {
namespace {

struct WildcardCheckBind final : duckdb::FunctionData {
  explicit WildcardCheckBind(std::shared_ptr<const re2::RE2> matcher)
    : matcher{std::move(matcher)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<WildcardCheckBind>(matcher);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return matcher == other.Cast<WildcardCheckBind>().matcher;
  }

  std::shared_ptr<const re2::RE2> matcher;
};

void CheckWildcard(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto& func = state.expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& matcher = *func.BindInfo()->Cast<WildcardCheckBind>().matcher;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(), [&](duckdb::string_t terms) {
      return irs::MatchStoredTerms(
        matcher, {reinterpret_cast<const irs::byte_type*>(terms.GetData()),
                  terms.GetSize()});
    });
}

}  // namespace

std::optional<DeferredCheck> DeferWildcard(irs::Filter::ptr& filter,
                                           const DeferContext&) {
  const auto& wildcard = irs::utils::downCast<irs::ByWildcardNGram>(*filter);
  const auto& options = wildcard.options();
  if (!options.matcher) {
    return std::nullopt;
  }
  auto index = std::make_unique<irs::ByWildcardNGram>();
  *index->mutable_field_id() = wildcard.field_id();
  *index->mutable_options() = options;
  index->mutable_options()->matcher = nullptr;
  index->SetBoost(wildcard.GetBoost());
  index->SetScorer(wildcard.GetScorer());
  const auto column = options.store_field_id;
  auto check = MakeColumnCheck(
    "sdb_wildcard_check", duckdb::LogicalType::BLOB, CheckWildcard,
    duckdb::make_uniq<WildcardCheckBind>(options.matcher));
  DeferredCheck deferred{.source = std::move(filter),
                         .column = column,
                         .type = duckdb::LogicalType::BLOB,
                         .check = std::move(check)};
  filter = std::move(index);
  return deferred;
}

}  // namespace sdb::connector
