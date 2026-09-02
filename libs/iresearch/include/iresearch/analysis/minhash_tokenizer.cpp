////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "minhash_tokenizer.hpp"

#include <absl/base/internal/endian.h>

#include "basics/wyhash.h"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr uint64_t kHashSeed = 0xdeadbeef;

}  // namespace

Tokenizer::ptr MinHashTokenizer::Make(Options opts,
                                      duckdb::SharedObjectCache& cache) {
  if (!opts.num_hashes) {
    THROW_SQL_ERROR(ERR_MSG("minhash: num_hashes must be positive"));
  }
  Tokenizer::ptr sub;
  if (opts.analyzer) {
    sub = CreateTokenizer(std::move(*opts.analyzer), cache);
  }
  return std::make_unique<MinHashTokenizer>(std::move(sub), opts.num_hashes);
}

MinHashTokenizer::MinHashTokenizer(analysis::Tokenizer::ptr analyzer,
                                   uint32_t num_hashes)
  : _analyzer{std::move(analyzer)},
    _num_hashes{num_hashes},
    _minhash{_num_hashes} {
  if (!_analyzer) {
    _analyzer = std::make_unique<KeywordTokenizer>();
  }
}

std::tuple<> MinHashTokenizer::PrepareBatch(BlockTraits) {
  if (!_sub_writer) {
    _sub_writer = std::make_unique<TokenSink>();
    _sub_writer->Bind(*this, nullptr);
  }
  return {};
}

void MinHashTokenizer::Consume(TokenBatch& batch, DocRuns) {
  for (const auto& term : batch.Terms()) {
    _minhash.Insert(
      sdb::basics::WyHash(term.GetData(), term.GetSize(), kHashSeed));
  }
}

template<TokenLayout Layout>
bool MinHashTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  _minhash.Clear();
  if (!_analyzer->Fill(raw, *_sub_writer, {TokenLayout::Terms})) {
    _sub_writer->Discard();
    return false;
  }
  _sub_writer->Finish();
  EmitSignature<Layout>(sink);
  return true;
}

template<TokenLayout Layout>
void MinHashTokenizer::EmitSignature(TokenSink& sink) {
  for (const auto hash : _minhash) {
    const auto value = absl::little_endian::FromHost(hash);
    sink.Emit<Layout>(reinterpret_cast<const char*>(&value),
                      static_cast<uint32_t>(sizeof value), 1);
  }
}

template class TypedTokenizer<MinHashTokenizer>;

}  // namespace irs::analysis
