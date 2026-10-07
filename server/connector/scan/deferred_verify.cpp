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

#include "connector/scan/deferred_verify.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/detail/token_phrase.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/wildcard_ngram_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <utility>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

using irs::utils::downCast;

struct VerifyBindData final : duckdb::FunctionData {
  explicit VerifyBindData(std::shared_ptr<const re2::RE2> matcher)
    : matcher{std::move(matcher)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<VerifyBindData>(matcher);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return matcher == other.Cast<VerifyBindData>().matcher;
  }

  std::shared_ptr<const re2::RE2> matcher;
};

void VerifyStoredTerms(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  const auto& func = state.expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& matcher = *func.BindInfo()->Cast<VerifyBindData>().matcher;
  duckdb::UnaryExecutor::Execute<duckdb::string_t, bool>(
    args.data[0], result, args.size(), [&](duckdb::string_t terms) {
      return irs::MatchStoredTerms(
        matcher, {reinterpret_cast<const irs::byte_type*>(terms.GetData()),
                  terms.GetSize()});
    });
}

duckdb::unique_ptr<duckdb::TableFilter> MakeColumnCheck(
  const char* name, const duckdb::LogicalType& type,
  duckdb::scalar_function_t function,
  duckdb::unique_ptr<duckdb::FunctionData> bind,
  duckdb::init_local_state_t init = nullptr) {
  duckdb::ScalarFunction fn(duckdb::Identifier{name}, {type},
                            duckdb::LogicalType::BOOLEAN, std::move(function));
  fn.SetInitStateCallback(init);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0ULL));
  auto expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children), std::move(bind));
  return duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(expr));
}

struct PhraseCheckBind final : duckdb::FunctionData {
  PhraseCheckBind(std::shared_ptr<const irs::TokenPhraseMatcher> matcher,
                  std::shared_ptr<const irs::PhraseTokens> tokens)
    : matcher{std::move(matcher)}, tokens{std::move(tokens)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PhraseCheckBind>(matcher, tokens);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return matcher == other.Cast<PhraseCheckBind>().matcher;
  }

  std::shared_ptr<const irs::TokenPhraseMatcher> matcher;
  std::shared_ptr<const irs::PhraseTokens> tokens;
};

struct PhraseCheckState final : duckdb::FunctionLocalState {
  explicit PhraseCheckState(const PhraseCheckBind& bind)
    : check{*bind.matcher, *bind.tokens, false} {}

  irs::PhraseCheck check;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitPhraseCheck(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<PhraseCheckState>(
    bind_data->Cast<PhraseCheckBind>());
}

void CheckPhrase(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                 duckdb::Vector& result) {
  auto& check = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<PhraseCheckState>()
                  .check;
  check.Bind(args);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  irs::PhraseVerdict verdict;
  for (duckdb::idx_t row = 0, count = args.size(); row != count; ++row) {
    out[row] = check.Check(row, verdict);
  }
}

duckdb::unique_ptr<duckdb::TableFilter> MakePhraseCheck(
  const irs::ByPhrase& filter, const irs::IndexReader& reader) {
  const auto& options = filter.options();
  const auto& tokens = options.tokens();
  auto matcher = std::make_shared<const irs::TokenPhraseMatcher>(
    tokens->Check(options), options.word_separator(), reader,
    filter.field_id());
  return MakeColumnCheck(
    "sdb_phrase_check", tokens->text.types.front(), CheckPhrase,
    duckdb::make_uniq<PhraseCheckBind>(std::move(matcher), tokens),
    InitPhraseCheck);
}

template<typename F, typename Visitor>
void VisitConjuncts(F& root, Visitor&& visit) {
  if (root.type() != irs::Type<irs::BooleanFilter>::id()) {
    visit(root);
    return;
  }
  for (const auto& child :
       downCast<const irs::BooleanFilter>(root).Filters(irs::Occur::Must)) {
    visit(static_cast<F&>(*child));
  }
}

void DeferPhrase(irs::ByPhrase& filter) {
  auto& options = *filter.mutable_options();
  const auto& tokens = options.tokens();
  if (!tokens || tokens->text.columns.size() != 1 ||
      !irs::TokenPhraseMatcher::Standalone(tokens->Check(options))) {
    return;
  }
  auto deferred = std::make_shared<irs::PhraseTokens>(*tokens);
  deferred->deferred = true;
  options.set_tokens(std::move(deferred));
}

void Defer(irs::Filter& filter, bool scored) {
  if (filter.type() == irs::Type<irs::ByWildcardNGram>::id()) {
    auto& options = *downCast<irs::ByWildcardNGram>(filter).mutable_options();
    options.deferred_verify = options.matcher != nullptr;
  } else if (!scored && filter.type() == irs::Type<irs::ByPhrase>::id()) {
    DeferPhrase(downCast<irs::ByPhrase>(filter));
  }
}

void AddDeferred(ScanGlobalState& state, const irs::Filter& filter,
                 const irs::IndexReader& reader) {
  const auto add = [&](irs::field_id field, duckdb::LogicalType type,
                       duckdb::unique_ptr<duckdb::TableFilter> check) {
    const auto& owned = state.verify_filters.emplace_back(std::move(check));
    auto& cf = state.col_filters.emplace_back();
    cf.field = field;
    cf.filter = owned.get();
    cf.row_gather = true;
    cf.type = std::move(type);
  };
  if (filter.type() == irs::Type<irs::ByWildcardNGram>::id()) {
    const auto& options =
      downCast<const irs::ByWildcardNGram>(filter).options();
    if (options.deferred_verify) {
      add(options.store_field_id, duckdb::LogicalType::BLOB,
          MakeColumnCheck("sdb_wildcard_ngram_verify",
                          duckdb::LogicalType::BLOB, VerifyStoredTerms,
                          duckdb::make_uniq<VerifyBindData>(options.matcher)));
    }
  } else if (filter.type() == irs::Type<irs::ByPhrase>::id()) {
    const auto& phrase = downCast<const irs::ByPhrase>(filter);
    const auto& tokens = phrase.options().tokens();
    if (tokens && tokens->deferred) {
      add(tokens->text.columns.front(), tokens->text.types.front(),
          MakePhraseCheck(phrase, reader));
    }
  }
}

}  // namespace

void DeferVerify(irs::Filter& root, bool scored) {
  VisitConjuncts(root, [&](irs::Filter& filter) { Defer(filter, scored); });
}

void AddDeferredVerifyFilters(ScanGlobalState& state, const irs::Filter& root,
                              const irs::IndexReader& reader) {
  const auto pushed = state.col_filters.size();
  VisitConjuncts(root, [&](const irs::Filter& filter) {
    AddDeferred(state, filter, reader);
  });
  if (state.col_filters.size() == pushed) {
    return;
  }
  using ColFilter = ScanGlobalState::ColFilter;
  const bool can_throw =
    absl::c_any_of(state.col_filters, [](const ColFilter& cf) {
      return !cf.is_score &&
             cf.filter->Cast<duckdb::ExpressionFilter>().expr->CanThrow();
    });
  if (can_throw) {
    absl::c_stable_partition(state.col_filters,
                             [](const ColFilter& cf) { return cf.row_gather; });
  }
}

}  // namespace sdb::connector
