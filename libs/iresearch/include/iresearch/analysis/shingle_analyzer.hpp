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
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include "basics/serializer.h"
#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis {

struct TokenizerConfig;

// Word-level shingle analyzer modelled on the Lucene/Elasticsearch shingle
// filter: emits every shingle size in [min_shingle_size, max_shingle_size]
// over the base analyzer's tokens (joined by `token_separator`, 0xFF by
// default -- invalid inside a UTF-8 token) plus, when `output_unigrams`, the
// tokens themselves. Each base token occupies exactly one position; the
// shingles emitted at a window front share it (inc = 0). In parallel the
// ordered tokens are packed into a StoreAttr blob of WriteToken records,
// complete only once next() has been driven to exhaustion. Base position gaps
// (a stopword filter) become filler tokens: they occupy a position and appear
// in the blob, but never inside an emitted term; query-side fillers act as
// wildcards while blob-side fillers are compared literally. A frequent_words
// list bounds the vocabulary: min-size shingles stay dense, wider sizes are
// emitted only for windows containing a frequent word. Same-position
// (increment 0) input is not handled.
class ShingleAnalyzer final : public TypedAnalyzer<ShingleAnalyzer> {
 public:
  struct Options {
    using Owner = ShingleAnalyzer;
    std::unique_ptr<TokenizerConfig> base_analyzer;
    uint32_t min_shingle_size = 2;
    uint32_t max_shingle_size = 2;
    bool output_unigrams = true;
    bool output_unigrams_if_no_shingles = false;
    bstring token_separator;
    bstring filler_token;
    bool store_tokens = true;
    std::vector<bstring> frequent_words;
  };

  static constexpr std::string_view type_name() noexcept { return "shingle"; }
  static Analyzer::ptr Make(Options opts);

  // Build TokenBlob() for `data` without emitting shingle terms.
  void DrainTokens(std::string_view data);

  Analyzer& BaseAnalyzer() noexcept { return *_analyzer; }

  static void AppendShingle(std::span<const bytes_view> tokens,
                            bytes_view separator, bstring& out);

  static constexpr uint32_t kMaxTokenSize = (uint32_t{1} << 30) - 1;

  static constexpr byte_type kDefaultSeparator{0xFF};

  // Record codec for the packed-token blob: a 1-, 2-, or 4-byte length prefix
  // selected by the two high bits of its first byte, then the token bytes.
  static void WriteToken(bytes_view token, std::string& out);
  static const byte_type* ReadToken(const byte_type* p,
                                    bytes_view& token) noexcept;
  // Returns nullptr on a truncated or corrupt record.
  static const byte_type* ReadTokenChecked(const byte_type* p,
                                           const byte_type* end,
                                           bytes_view& token) noexcept;

  ShingleAnalyzer(Analyzer::ptr base, Options&& options);

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  bool reset(std::string_view data) final;

  bool next() final;

  uint32_t MinShingleSize() const noexcept { return _min; }
  uint32_t MaxShingleSize() const noexcept { return _max; }
  bool OutputUnigrams() const noexcept { return _output_unigrams; }
  bytes_view Separator() const noexcept { return _separator; }
  bytes_view Filler() const noexcept { return _filler; }
  bool StoresTokens() const noexcept { return _store_tokens; }

  // Complete after DrainTokens regardless of store_tokens; after a next()
  // cycle only when store_tokens is on (off keeps just the window's tail).
  bytes_view TokenBlob() const noexcept {
    return ViewCast<byte_type>(std::string_view{_terms});
  }

  bool HasFrequentWords() const noexcept { return !_frequent.empty(); }
  bool IsFrequent(bytes_view token) const noexcept {
    return _frequent.contains(ViewCast<char>(token));
  }

 private:
  struct Slot {
    uint32_t offset{};
    uint32_t size{};
    bool filler{};
    bool frequent{};
  };

  static constexpr size_t kTrimThreshold = 4096;

  static constexpr uint32_t PrefixSize(uint32_t n) noexcept {
    return n <= 0x3F ? 1 : n <= 0x3FFF ? 2 : 4;
  }

  bytes_view TokenAt(const Slot& slot) const noexcept {
    return {reinterpret_cast<const byte_type*>(_terms.data()) + slot.offset,
            slot.size};
  }
  const Slot& WindowAt(uint32_t j) const noexcept {
    return _ring[(_ring_head + j) % _max];
  }
  void AppendToken(bytes_view token, bool is_filler = false);
  void TrimBlob();
  void PopFront() noexcept;
  bytes_view BuildShingle(uint32_t size);
  bool WindowHasFrequent(uint32_t size) const noexcept;
  bool WindowHasFiller(uint32_t size) const noexcept;
  void EmitPosition() noexcept;

  Analyzer::ptr _analyzer;
  const TermAttr* _base_term{};
  const IncAttr* _base_inc{};
  uint32_t _min;
  uint32_t _max;
  bool _output_unigrams;
  bool _output_unigrams_if_no_shingles;
  bool _store_tokens;
  bool _trim_blob{false};
  bstring _separator;
  IncAttr _inc;
  TermAttr _shingle_term;
  StoreAttr _store;
  std::string _terms;
  std::vector<Slot> _ring;
  std::vector<bytes_view> _join;
  bstring _scratch;
  size_t _ring_head{0};
  size_t _ring_len{0};
  size_t _real_tokens{0};
  uint32_t _emit_size{0};
  bool _base_exhausted{false};
  bool _front_emitted{false};
  uint32_t _pending_inc{1};
  absl::flat_hash_set<std::string> _frequent;
  bstring _filler;
  bstring _pending_token;
  uint32_t _pending_fillers{0};
  bool _has_pending{false};
};

template<typename Context>
void SerdeWrite(Context ctx, const ShingleAnalyzer::Options& o) {
  sdb::basics::WriteTuple(
    ctx.io(),
    std::tie(o.base_analyzer, o.min_shingle_size, o.max_shingle_size,
             o.output_unigrams, o.output_unigrams_if_no_shingles,
             o.token_separator, o.filler_token, o.frequent_words,
             o.store_tokens),
    ctx.arg());
}

template<typename Context>
void SerdeRead(Context ctx, ShingleAnalyzer::Options& o) {
  auto refs =
    std::tie(o.base_analyzer, o.min_shingle_size, o.max_shingle_size,
             o.output_unigrams, o.output_unigrams_if_no_shingles,
             o.token_separator, o.filler_token, o.frequent_words,
             o.store_tokens);
  sdb::basics::ReadTuple(ctx.io(), refs, ctx.arg());
}

}  // namespace irs::analysis
