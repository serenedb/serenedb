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

#include <algorithm>
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
#include <limits>
#include <memory>
#include <utility>
#include <vector>

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

duckdb::unique_ptr<duckdb::TableFilter> MakeVerifyFilter(
  std::shared_ptr<const re2::RE2> matcher) {
  duckdb::ScalarFunction fn(duckdb::Identifier{"sdb_wildcard_ngram_verify"},
                            {duckdb::LogicalType::BLOB},
                            duckdb::LogicalType::BOOLEAN, VerifyStoredTerms);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
    duckdb::LogicalType::BLOB, 0ULL));
  auto expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children),
    duckdb::make_uniq<VerifyBindData>(std::move(matcher)));
  return duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(expr));
}

struct PhraseCheckBind final : duckdb::FunctionData {
  PhraseCheckBind(std::shared_ptr<const irs::TokenPhraseMatcher> matcher,
                  irs::PhraseTokens::Factory tokenizer)
    : matcher{std::move(matcher)}, tokenizer{std::move(tokenizer)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PhraseCheckBind>(matcher, tokenizer);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    return matcher == other.Cast<PhraseCheckBind>().matcher;
  }

  std::shared_ptr<const irs::TokenPhraseMatcher> matcher;
  irs::PhraseTokens::Factory tokenizer;
};

struct PhraseCheckState final : duckdb::FunctionLocalState {
  explicit PhraseCheckState(const PhraseCheckBind& bind)
    : tokenizer{bind.tokenizer()}, sink{*bind.matcher, tokenizer->Traits()} {}

  std::shared_ptr<irs::analysis::Tokenizer> tokenizer;
  irs::ValueAnalyzer analyzer;
  irs::TokenPhraseSink sink;
  irs::TextRows rows;
  std::vector<duckdb::string_t> values;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitPhraseCheck(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<PhraseCheckState>(
    bind_data->Cast<PhraseCheckBind>());
}

void CheckPhrase(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                 duckdb::Vector& result) {
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<PhraseCheckState>();
  const auto count = args.size();
  local.rows.Bind(args.data[0], count);
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  irs::PhraseVerdict verdict;
  for (duckdb::idx_t row = 0; row != count; ++row) {
    out[row] = local.rows.Values(row, local.values) &&
               irs::CheckValues(local.sink, local.analyzer, *local.tokenizer,
                                local.values, false, verdict);
  }
}

uint32_t Clamp(uint64_t value) noexcept {
  return static_cast<uint32_t>(
    std::min<uint64_t>(value, std::numeric_limits<uint32_t>::max()));
}

duckdb::unique_ptr<duckdb::TableFilter> MakePhraseCheck(
  const irs::ByPhrase& filter, const irs::IndexReader& reader) {
  const auto& options = filter.options();
  const auto& tokens = *options.tokens();
  const auto field = filter.field_id();
  auto matcher = std::make_shared<const irs::TokenPhraseMatcher>(
    tokens.Check(options), options.word_separator(),
    [&](irs::bytes_view term) {
      uint64_t docs = 0;
      uint64_t freq = 0;
      for (const auto& segment : reader) {
        if (const auto* terms = segment.field(field)) {
          const auto meta = terms->Lookup(term);
          docs += meta.docs_count;
          freq += meta.freq;
        }
      }
      return irs::PostingMeta{.docs_count = Clamp(docs), .freq = Clamp(freq)};
    },
    tokens.match);
  duckdb::ScalarFunction fn(duckdb::Identifier{"sdb_phrase_check"},
                            {tokens.type}, duckdb::LogicalType::BOOLEAN,
                            CheckPhrase);
  fn.SetInitStateCallback(InitPhraseCheck);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.push_back(
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(tokens.type, 0ULL));
  auto expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children),
    duckdb::make_uniq<PhraseCheckBind>(std::move(matcher), tokens.tokenizer));
  return duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(expr));
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
  if (!tokens || !irs::TokenPhraseMatcher::Standalone(tokens->Check(options))) {
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
          MakeVerifyFilter(options.matcher));
    }
  } else if (filter.type() == irs::Type<irs::ByPhrase>::id()) {
    const auto& phrase = downCast<const irs::ByPhrase>(filter);
    const auto& tokens = phrase.options().tokens();
    if (tokens && tokens->deferred) {
      add(tokens->column, tokens->type, MakePhraseCheck(phrase, reader));
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
