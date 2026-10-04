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

#include "iresearch/analysis/text/dict/string_table.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/serializer.hpp"
#include "iresearch/utils/string.hpp"

namespace duckdb {

class SharedObjectCache;

}  // namespace duckdb
namespace irs::analysis {

struct TokenizerConfig;

class ShingleTokenizer final : public TypedTokenizer<ShingleTokenizer>,
                               private util::Noncopyable {
 public:
  static constexpr byte_type kDefaultSeparator{' '};

  struct Options {
    using Owner = ShingleTokenizer;
    std::unique_ptr<TokenizerConfig> base_analyzer;
    uint32_t min_shingle_size = 2;
    uint32_t max_shingle_size = 2;
    bool output_unigrams = true;
    bool fallback_unigrams = false;
    bstring token_separator = bstring(1, kDefaultSeparator);
    std::vector<bstring> frequent_words;
  };

  static constexpr std::string_view type_name() noexcept {
    return "generate_shingles";
  }
  static Tokenizer::ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  ShingleTokenizer(Tokenizer::ptr base, Options&& options);

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos =
        _output_unigrams || _producer.explicit_pos || _min != _max,
      .offsets = _producer.offsets,
    };
  }

  auto& Base(this auto& self) noexcept { return *self._analyzer; }
  uint32_t MinShingle() const noexcept { return _min; }
  uint32_t MaxShingle() const noexcept { return _max; }
  bool OutputUnigrams() const noexcept { return _output_unigrams; }
  bytes_view Separator() const noexcept { return _separator; }
  bool HasFrequentWords() const noexcept { return !_frequent.Empty(); }
  bool IsFrequent(bytes_view word) const noexcept {
    return _frequent.Contains(MakeTermView(ViewCast<char>(word)));
  }

  void Bind(duckdb::ClientContext& ctx) final { _analyzer->Bind(ctx); }
  void Unbind() noexcept final { _analyzer->Unbind(); }
  size_t MemoryUsage() const noexcept final {
    return _analyzer->MemoryUsage() + _freq.capacity() * sizeof(uint8_t) +
           _shingle_sizes.capacity() * sizeof(uint32_t) +
           _tok_psum.capacity() * sizeof(uint32_t) + _frequent.MemoryBytes() +
           (_sub ? sizeof(Sub) + _sub->tokens.MemoryUsage() +
                     _sub->offs_tokens.MemoryUsage()
                 : 0);
  }

  auto PrepareBatch(BlockTraits) {
    if (!_sub) {
      _sub = std::make_unique<Sub>(_producer);
    }
    return std::tuple{_output_unigrams, HasFrequentWords()};
  }

  template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

  bool FillTokens(std::span<const duckdb::string_t> tokens, TokenSink& sink,
                  FillCtx ctx) final;

 private:
  template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent,
           typename Base>
  IRS_FORCE_INLINE void EmitBaseTokens(const duckdb::string_t* raw,
                                       TokenSink& sink, const Base& base);
  template<bool HasFrequent>
  IRS_FORCE_INLINE void BuildTables(std::span<const duckdb::string_t> tok);

  Tokenizer::ptr _analyzer;
  uint32_t _min;
  uint32_t _max;
  bool _output_unigrams;
  bool _fallback_unigrams;
  TokenTraits _producer;
  bstring _separator;
  dict::StringSet<std::string> _frequent;

  struct Sub {
    explicit Sub(TokenTraits producer)
      : tokens{producer}, offs_tokens{producer} {}

    ValueAnalyzer analyzer;
    ValueTokens<TokenLayout::TermsPos> tokens;
    ValueTokens<TokenLayout::TermsPosOffs> offs_tokens;
  };

  std::unique_ptr<Sub> _sub;
  std::vector<uint8_t> _freq;
  std::vector<uint32_t> _shingle_sizes;
  std::vector<uint32_t> _tok_psum;
};

}  // namespace irs::analysis
