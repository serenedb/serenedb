////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <tuple>
#include <vector>

#include "basics/serializer.h"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

struct TokenizerConfig;

class WildcardAnalyzer final : public TypedTokenizer<WildcardAnalyzer>,
                               private util::Noncopyable {
  using Ngram = NGramTokenizer;

 public:
  struct Options {
    using Owner = WildcardAnalyzer;
    std::unique_ptr<TokenizerConfig> base_analyzer;
    size_t ngram_size = 3;
  };
  static Tokenizer::ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static constexpr std::string_view type_name() noexcept { return "wildcard"; }

  explicit WildcardAnalyzer(Tokenizer::ptr base_analyzer, size_t ngram_size);
  ~WildcardAnalyzer() override;

  template<TokenLayout Layout, bool KnownAscii>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

  TokenTraits Traits() const noexcept final { return {.store = true}; }

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = true};
  }

  std::tuple<bool> PrepareBatch(BlockTraits traits);

  void Bind(duckdb::ClientContext& ctx) final { _analyzer->Bind(ctx); }

  void Unbind() noexcept final { _analyzer->Unbind(); }

  size_t MemoryUsage() const noexcept final {
    return _analyzer->MemoryUsage() + _terms.capacity() +
           _fill_bounds.capacity() * sizeof(uint32_t) + _ngram.MemoryUsage() +
           (_sub_sink ? sizeof(TokenSink) : 0);
  }

  auto& ngram() noexcept { return _ngram; }

 private:
  template<bool Identity, TokenLayout Layout>
  void EmitTerms(TokenSink& sink);
  template<bool Identity, TokenLayout Layout>
  void EmitTermGrams(TokenSink& sink, const byte_type* term, uint32_t size);

  struct SubSink;

  Tokenizer::ptr _analyzer;
  Ngram _ngram;
  bstring _terms;
  std::vector<uint32_t> _fill_bounds;
  std::unique_ptr<SubSink> _sub_sink;
  bool _base_stable = false;
};

extern template class TypedTokenizer<WildcardAnalyzer>;

template<typename Context>
void SerdeWrite(Context ctx, const WildcardAnalyzer::Options& o) {
  sdb::basics::WriteTupleOrObject(ctx, std::tie(o.base_analyzer, o.ngram_size));
}

template<typename Context>
void SerdeRead(Context ctx, WildcardAnalyzer::Options& o) {
  auto refs = std::tie(o.base_analyzer, o.ngram_size);
  sdb::basics::ReadTupleOrObject(ctx, refs);
}

}  // namespace irs::analysis
