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

#include <cstdint>
#include <duckdb/storage/arena_allocator.hpp>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "basics/serializer.h"
#include "iresearch/analysis/text/dict/string_table.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/string.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

struct TokenizerConfig;

class ShingleTokenizer final : public TypedTokenizer<ShingleTokenizer>,
                               private util::Noncopyable {
 public:
  struct Options {
    using Owner = ShingleTokenizer;
    std::unique_ptr<TokenizerConfig> base_analyzer;
    uint32_t min_shingle_size = 2;
    uint32_t max_shingle_size = 2;
    bool output_unigrams = true;
    bool output_unigrams_if_no_shingles = false;
    bstring token_separator;
    bstring filler_token;
    std::vector<bstring> frequent_words;
    bool store_tokens = true;
  };

  static constexpr std::string_view type_name() noexcept { return "shingle"; }
  static Tokenizer::ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static constexpr uint32_t kMaxTokenSize = (uint32_t{1} << 30) - 1;
  static constexpr byte_type kDefaultSeparator{0xFF};

  static void WriteToken(bytes_view token, bstring& out);
  static const byte_type* ReadToken(const byte_type* p,
                                    bytes_view& token) noexcept;
  static const byte_type* ReadTokenChecked(const byte_type* p,
                                           const byte_type* end,
                                           bytes_view& token) noexcept;

  ShingleTokenizer(Tokenizer::ptr base, Options&& options);

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = _output_unigrams || !_producer_dense || _min != _max,
      .store = _store_tokens,
    };
  }

  void Bind(duckdb::ClientContext& ctx) final { _analyzer->Bind(ctx); }
  void Unbind() noexcept final { _analyzer->Unbind(); }
  size_t MemoryUsage() const noexcept final {
    return _analyzer->MemoryUsage() + _freq.capacity() * sizeof(uint8_t) +
           _shingle_ends.capacity() * sizeof(uint32_t) +
           _tok_psum.capacity() * sizeof(uint32_t) + _blob.capacity() +
           _frequent.MemoryBytes() +
           (_sub ? sizeof(Sub) + _sub->tokens.MemoryUsage() : 0);
  }

  auto PrepareBatch(BlockTraits) {
    if (!_sub) {
      _sub = std::make_unique<Sub>(_analyzer->Traits());
    }
    return std::tuple{_output_unigrams, _has_frequent, _store_tokens};
  }

  template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent,
           bool StoreTokens>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  IRS_FORCE_INLINE bool DrainBase(duckdb::string_t raw);
  template<bool HasFrequent>
  IRS_FORCE_INLINE void BuildTables(uint32_t n);
  template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent>
  IRS_FORCE_INLINE void EmitRuns(duckdb::string_t raw, TokenSink& sink,
                                 uint32_t n, bool no_shingles);
  IRS_FORCE_INLINE void StoreBlob(TokenSink& sink, uint32_t n);

  Tokenizer::ptr _analyzer;
  uint32_t _min;
  uint32_t _max;
  bool _output_unigrams;
  bool _output_unigrams_if_no_shingles;
  bool _has_frequent;
  bool _producer_dense = true;
  bool _store_tokens;
  bstring _separator;
  bstring _filler;
  dict::StringSet<std::string> _frequent;

  struct Sub {
    explicit Sub(TokenTraits producer) : tokens{producer} {}

    ValueAnalyzer analyzer;
    ValueTokens<TokenLayout::TermsPos> tokens;
  };

  std::unique_ptr<Sub> _sub;
  std::vector<uint8_t> _freq;
  std::vector<uint32_t> _shingle_ends;
  std::vector<uint32_t> _tok_psum;
  bstring _blob;
};

extern template class TypedTokenizer<ShingleTokenizer>;

template<typename Context>
void SerdeWrite(Context ctx, const ShingleTokenizer::Options& o) {
  sdb::basics::WriteTupleOrObject(
    ctx, std::tie(o.base_analyzer, o.min_shingle_size, o.max_shingle_size,
                  o.output_unigrams, o.output_unigrams_if_no_shingles,
                  o.token_separator, o.filler_token, o.frequent_words,
                  o.store_tokens));
}

template<typename Context>
void SerdeRead(Context ctx, ShingleTokenizer::Options& o) {
  auto refs = std::tie(o.base_analyzer, o.min_shingle_size, o.max_shingle_size,
                       o.output_unigrams, o.output_unigrams_if_no_shingles,
                       o.token_separator, o.filler_token, o.frequent_words,
                       o.store_tokens);
  sdb::basics::ReadTupleOrObject(ctx, refs);
}

}  // namespace irs::analysis
