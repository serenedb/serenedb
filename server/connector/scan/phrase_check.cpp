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
  PhraseCheckData(irs::ByPhraseOptions::TokenizerFactory tokenizer,
                  const irs::ByPhraseOptions& checked,
                  irs::bytes_view word_separator,
                  std::span<const irs::PhraseWordStat> words)
    : tokenizer{std::move(tokenizer)},
      compiled{checked, word_separator, words} {}

  irs::ByPhraseOptions::TokenizerFactory tokenizer;
  irs::CompiledPhrase compiled;
};

using PhraseCheckBind = SharedCheckBind<PhraseCheckData>;

struct PhraseCheckState final : duckdb::FunctionLocalState {
  explicit PhraseCheckState(const PhraseCheckData& data)
    : check{data.compiled, data.tokenizer, false} {}

  irs::PhraseCheck check;
};

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

struct CheckedPhrase {
  irs::field_id field;
  const irs::ByPhraseOptions& index;
  const irs::ByPhraseOptions& checked;
};

CheckedPhrase CheckedOf(const irs::Filter& filter) {
  if (filter.type() == irs::Type<irs::ByShinglePhrase>::id()) {
    const auto& phrase = irs::utils::downCast<irs::ByShinglePhrase>(filter);
    return {phrase.field_id(), phrase.options().cover, phrase.options().words};
  }
  const auto& phrase = irs::utils::downCast<irs::ByPhrase>(filter);
  return {phrase.field_id(), phrase.options(), phrase.options()};
}

}  // namespace

DeferredCheck MakePhraseCheck(std::shared_ptr<const irs::Filter> source,
                              irs::field_id column,
                              const duckdb::LogicalType& type,
                              const irs::IndexReader& reader) {
  const auto phrase = CheckedOf(*source);
  auto bind =
    duckdb::make_uniq<PhraseCheckBind>(std::make_shared<const PhraseCheckData>(
      phrase.checked.tokenizer(), phrase.checked, phrase.index.word_separator(),
      irs::PhraseWordStats(phrase.checked, reader, phrase.field)));
  return Split(std::move(source), column, type, "sdb_phrase_check", CheckPhrase,
               std::move(bind), InitCheck<PhraseCheckState, PhraseCheckData>);
}

}  // namespace sdb::connector
