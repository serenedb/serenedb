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
#include <iresearch/search/detail/compiled_phrase.hpp>
#include <iresearch/search/detail/token_phrase.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <utility>

#include "connector/scan/deferred_check.h"

namespace sdb::connector {
namespace {

const irs::ByPhrase& PhraseOf(const irs::Filter& source) noexcept {
  return irs::utils::downCast<irs::ByPhrase>(source);
}

struct PhraseCheckData {
  explicit PhraseCheckData(irs::Filter::ptr phrase,
                           const irs::IndexReader& reader)
    : source{std::move(phrase)},
      compiled{PhraseOf(*source).options(),
               {},
               irs::PhraseWordStats(PhraseOf(*source).options(), reader,
                                    PhraseOf(*source).field_id())} {}

  irs::Filter::ptr source;
  irs::CompiledPhrase compiled;
};

using PhraseCheckBind = SharedCheckBind<PhraseCheckData>;

struct PhraseCheckState final : duckdb::FunctionLocalState {
  explicit PhraseCheckState(const PhraseCheckData& data)
    : check{data.compiled, PhraseOf(*data.source).options().tokenizer(),
            false} {}

  irs::PhraseCheck check;
};

void CheckPhrase(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                 duckdb::Vector& result) {
  auto& check = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<PhraseCheckState>()
                  .check;
  check.Bind(args.data[0], args.size());
  auto* out = duckdb::FlatVector::GetDataMutable<bool>(result);
  for (duckdb::idx_t row = 0, count = args.size(); row != count; ++row) {
    out[row] = check.Check(row).freq != 0;
  }
}

}  // namespace

DeferredCheck MakePhraseCheck(irs::Filter::ptr source, irs::field_id column,
                              const duckdb::LogicalType& type,
                              const irs::IndexReader& reader) {
  auto data =
    std::make_shared<const PhraseCheckData>(std::move(source), reader);
  const auto& phrase = PhraseOf(*data->source);
  return Split(phrase, column, type, "sdb_phrase_check", CheckPhrase,
               duckdb::make_uniq<PhraseCheckBind>(std::move(data)),
               InitCheck<PhraseCheckState, PhraseCheckData>);
}

}  // namespace sdb::connector
