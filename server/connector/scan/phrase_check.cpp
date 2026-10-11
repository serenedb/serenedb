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

#include <absl/algorithm/container.h>

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/detail/token_phrase.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <utility>

#include "connector/scan/deferred_check.h"

namespace sdb::connector {
namespace {

struct PhraseCheckData {
  PhraseCheckData(std::shared_ptr<const irs::PhraseTokens> tokens,
                  const irs::ByPhraseOptions& checked,
                  irs::bytes_view word_separator,
                  std::span<const irs::PhraseWordStat> words)
    : tokens{std::move(tokens)}, compiled{checked, word_separator, words} {}

  std::shared_ptr<const irs::PhraseTokens> tokens;
  irs::CompiledPhrase compiled;
};

using PhraseCheckBind = SharedCheckBind<PhraseCheckData>;

struct PhraseCheckState final : duckdb::FunctionLocalState {
  explicit PhraseCheckState(const PhraseCheckData& data)
    : check{data.compiled, *data.tokens, false} {}

  irs::PhraseCheck check;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitPhraseCheck(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  return duckdb::make_uniq<PhraseCheckState>(
    *bind_data->Cast<PhraseCheckBind>().data);
}

void CheckPhrase(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                 duckdb::Vector& result) {
  auto& check = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<PhraseCheckState>()
                  .check;
  check.Bind(args.data[0], args.size());
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  irs::PhraseVerdict verdict;
  for (duckdb::idx_t row = 0, count = args.size(); row != count; ++row) {
    out[row] = check.Check(row, verdict);
  }
}

}  // namespace

std::optional<DeferredCheck> DeferPhrase(irs::Filter::ptr& filter,
                                         const DeferContext& ctx) {
  const auto& phrase = irs::utils::downCast<irs::ByPhrase>(*filter);
  const auto& options = phrase.options();
  const auto& tokens = options.tokens();
  if (!tokens) {
    return std::nullopt;
  }
  const auto type = StoredType(ctx.reader, tokens->text);
  if (!type) {
    return std::nullopt;
  }
  const auto& checked = tokens->Check(options);
  if (!irs::IsStandalone(checked)) {
    return std::nullopt;
  }
  const bool patterns = absl::c_any_of(checked, [](const auto& info) {
    return irs::ByPhraseOptions::KindOf(info.part) == irs::SlotKind::Expansion;
  });
  if (checked.slop() != 0 && patterns &&
      tokens->tokenizer()->Traits().explicit_pos) {
    return std::nullopt;
  }
  auto index = irs::PartsConjunction(phrase, ctx.scorer);
  if (!index) {
    return std::nullopt;
  }
  auto data = std::make_shared<const PhraseCheckData>(
    tokens, checked, options.word_separator(),
    irs::PhraseWordStats(checked, ctx.reader, phrase.field_id()));
  return Split(
    filter, std::move(index), tokens->text, *type, "sdb_phrase_check",
    CheckPhrase,
    [&](std::shared_ptr<const irs::Filter>) {
      return duckdb::make_uniq<PhraseCheckBind>(std::move(data));
    },
    InitPhraseCheck);
}

}  // namespace sdb::connector
