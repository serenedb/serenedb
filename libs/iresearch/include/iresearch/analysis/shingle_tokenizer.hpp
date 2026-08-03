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

#include <absl/container/flat_hash_set.h>

#include <cstdint>
#include <duckdb/storage/arena_allocator.hpp>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "basics/serializer.h"
#include "iresearch/analysis/token_accumulator.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis {

struct TokenizerConfig;

// Word-level shingle tokenizer, modelled on the Lucene/Elasticsearch shingle
// token filter. Wraps a base tokenizer and, from its token stream, emits as
// index terms the word shingles of every size in [min_shingle_size,
// max_shingle_size] (consecutive tokens joined by `token_separator`) and, when
// `output_unigrams`, the individual tokens. For "quick brown fox" with the
// defaults (min=max=2, output_unigrams) the terms are:
//   quick, "quick brown", brown, "brown fox", fox
//
// In parallel it packs the ordered original tokens into a store blob (each
// token a length-prefixed record, see WriteToken/ReadToken) so a phrase filter
// can verify exact contiguity on candidate documents without indexing
// positions.
//
// A base position increment > 1 (e.g. a stopword filter removing a token)
// becomes a gap: shingles never bridge it, and the position carries across so a
// positional query stays gap-aware. A removed token is never emitted as a term
// (not standalone, not inside a shingle); it appears only in the stored blob
// (as a filler the verifier treats as a wildcard).
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
    bstring token_separator;  // empty -> single 0xFF byte (set in the ctor)
    bstring filler_token;     // empty -> single '_' (set in the ctor)
    std::vector<bstring> frequent_words;
    bool store_tokens = true;
  };

  static constexpr std::string_view type_name() noexcept { return "shingle"; }
  static Tokenizer::ptr Make(Options opts);

  // Largest single token the store-blob length prefix can address (30-bit).
  static constexpr uint32_t kMaxTokenSize = (uint32_t{1} << 30) - 1;
  // Default token separator: invalid in UTF-8, so it cannot occur inside a
  // VARCHAR base token (the DDL layer restricts shingle columns to VARCHAR).
  static constexpr byte_type kDefaultSeparator{0xFF};

  // Length-prefix codec for the packed-token store blob. The token byte length
  // is written ahead of the bytes as 1/2/4 bytes, selected by the high 2 bits
  // of the first byte (00 -> 6-bit, 01 -> 14-bit, 10 -> 30-bit); self-
  // describing, so records carry no separators. Shared with the phrase filter.
  static void WriteToken(bytes_view token, bstring& out);
  static void WriteToken(duckdb::string_t token, bstring& out);
  static const byte_type* ReadToken(const byte_type* p,
                                    bytes_view& token) noexcept;
  static const byte_type* ReadTokenChecked(const byte_type* p,
                                           const byte_type* end,
                                           bytes_view& token) noexcept;

  ShingleTokenizer(Tokenizer::ptr base, Options&& options);

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = true,
      .store = _store_tokens,
    };
  }

  void Bind(duckdb::ClientContext& ctx) final { _analyzer->Bind(ctx); }
  void Unbind() noexcept final { _analyzer->Unbind(); }

  auto PrepareBatch() {
    if (!_scratch_writer) {
      _scratch_writer = std::make_unique<TokenSink>();
    }
    return std::tuple{_output_unigrams, _has_frequent, _store_tokens};
  }

  template<TokenLayout Layout, bool OutputUnigrams, bool HasFrequent,
           bool StoreTokens>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
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
  absl::flat_hash_set<std::string> _frequent;

  // Reused per-value scratch: the base's tokens materialized (via the shared
  // TokenAccumulator over `_arena`) into batch-safe string_t handles (inline,
  // or a view into `value`, or a copy into `_arena`), their batch-convention
  // positions, a per-token "needs interning at emit" flag (set for arena
  // copies), and (only when HasFrequent) a per-token frequent bit. `_arena` is
  // Reset per value. `_blob` is the store blob.
  duckdb::ArenaAllocator _arena{duckdb::Allocator::DefaultAllocator()};
  TokenAccumulator _accumulator{_arena};
  std::vector<duckdb::string_t> _tok;
  std::vector<uint32_t> _pos;
  std::vector<uint8_t> _needs_intern;
  std::vector<uint8_t> _freq;
  std::vector<uint32_t> _shingle_ends;
  std::vector<uint32_t> _tok_psum;
  bstring _blob;
  std::unique_ptr<TokenSink> _scratch_writer;
};

extern template class TypedTokenizer<ShingleTokenizer>;

template<typename Context>
void SerdeWrite(Context ctx, const ShingleTokenizer::Options& o) {
  sdb::basics::WriteTuple(
    ctx.io(),
    std::tie(o.base_analyzer, o.min_shingle_size, o.max_shingle_size,
             o.output_unigrams, o.output_unigrams_if_no_shingles,
             o.token_separator, o.filler_token, o.frequent_words,
             o.store_tokens),
    ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, ShingleTokenizer::Options& o) {
  auto refs = std::tie(o.base_analyzer, o.min_shingle_size, o.max_shingle_size,
                       o.output_unigrams, o.output_unigrams_if_no_shingles,
                       o.token_separator, o.filler_token, o.frequent_words,
                       o.store_tokens);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

}  // namespace irs::analysis
