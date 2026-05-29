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
#include <span>
#include <string>
#include <vector>

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"

namespace irs::analysis {

// Word-level shingle analyzer, modelled on the Lucene/Elasticsearch shingle
// token filter. Wraps a base analyzer and, from its token stream, emits as
// index terms the word shingles of every size in [min_shingle_size,
// max_shingle_size] (consecutive tokens joined by `token_separator`) and,
// when `output_unigrams`, the individual tokens. For "quick brown fox" with
// the defaults (min=max=2, output_unigrams) the terms are:
//   quick, "quick brown", brown, "brown fox", fox
//
// In parallel it packs the ordered original tokens into a StoreAttr blob (each
// token a length-prefixed record, see WriteToken/ReadToken) so a phrase filter
// can verify exact contiguity on candidate documents without indexing
// positions. A phrase of
// up to max_shingle_size tokens is then a single shingle-term lookup that is
// exact by construction (the shingle already encodes order + adjacency);
// longer phrases AND their overlapping max-size shingles and verify.
//
// Tokenization is lazy: reset() only resets the base analyzer; next() pulls
// base tokens through a sliding window of at most max_shingle_size tokens and
// appends each to the StoreAttr blob. StoreAttr is therefore complete only
// once next() has been driven to exhaustion -- the contract the indexer relies
// on (it reads StoreAttr after fully inverting the term stream).
//
// v1 treats the base token stream as linear: base position increments are not
// turned into filler tokens, so a removed stopword does not break shingle
// adjacency (a phrase match ignores it). Lucene-style filler/gap and
// same-position (synonym) handling is a future refinement.
//
// `token_separator` defaults to a single 0xFF byte, which cannot occur inside
// a UTF-8 token; for BLOB tokens a collision only ever yields an extra
// candidate, never a missed one, since the verifier re-checks every candidate.
class ShingleAnalyzer final : public TypedAnalyzer<ShingleAnalyzer> {
 public:
  struct Options {
    Analyzer::ptr base_analyzer;
    uint32_t min_shingle_size = 2;
    uint32_t max_shingle_size = 2;
    bool output_unigrams = true;
    bool output_unigrams_if_no_shingles = false;
    bstring token_separator;  // empty -> single 0xFF byte (set in the ctor)
  };

  static constexpr std::string_view type_name() noexcept { return "shingle"; }
  static bool normalize(std::string_view args, std::string& definition);
  static Analyzer::ptr make(std::string_view args);
  static void init();

  // Join `tokens` into a single shingle term in `out` (cleared first) using
  // `separator` between adjacent tokens. Shared by the analyzer's term
  // emission and the phrase filter's query decomposition so the two never
  // drift.
  static void AppendShingle(std::span<const bytes_view> tokens,
                            bytes_view separator, bstring& out);

  // Largest single token the StoreAttr length prefix can address (30-bit).
  static constexpr uint32_t kMaxTokenSize = (uint32_t{1} << 30) - 1;

  // Length-prefix codec for the packed-token StoreAttr blob. The token byte
  // length is written ahead of the bytes as 1, 2, or 4 bytes, selected by the
  // high 2 bits of the first byte (00 -> 1 byte / 6-bit length, 01 -> 2 bytes /
  // 14-bit, 10 -> 4 bytes / 30-bit); the length is self-describing, so records
  // carry no separators. Shared by the analyzer (write) and the phrase filter
  // (read) so the encode and decode never drift.
  static void WriteToken(bytes_view token, std::string& out);
  static const byte_type* ReadToken(const byte_type* p,
                                     bytes_view& token) noexcept;

  explicit ShingleAnalyzer(Options&& options) noexcept;

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  bool reset(std::string_view data) final;

  bool next() final;

  uint32_t MinShingleSize() const noexcept { return _min; }
  uint32_t MaxShingleSize() const noexcept { return _max; }
  bool OutputUnigrams() const noexcept { return _output_unigrams; }
  bytes_view Separator() const noexcept { return _separator; }

 private:
  // Bytes of the token whose packed record starts at `offset` in `_terms`.
  bytes_view TokenAt(uint32_t offset) const noexcept;
  void AppendToken(bytes_view token);  // append to _terms + push onto the ring
  void PopFront() noexcept;            // drop the window's front token
  bytes_view BuildShingle(uint32_t size);  // join window[0..size) into _scratch

  Analyzer::ptr _analyzer;
  const TermAttr* _base_term{};  // base analyzer's emitted term
  uint32_t _min;
  uint32_t _max;
  bool _output_unigrams;
  bool _output_unigrams_if_no_shingles;
  bstring _separator;
  IncAttr _inc;
  TermAttr _shingle_term;
  StoreAttr _store;
  std::string _terms;             // packed ordered tokens (the StoreAttr blob)
  std::vector<uint32_t> _ring;    // capacity _max: window token record offsets
  std::vector<bytes_view> _join;  // scratch holding the window tokens to join
  bstring _scratch;               // scratch buffer for the current shingle term
  size_t _ring_head{0};           // physical index of the window front in _ring
  size_t _ring_len{0};            // number of tokens currently in the window
  size_t _emitted_total{0};       // base tokens consumed so far
  uint32_t _emit_size{0};         // 0 = unigram phase; else current shingle size
  bool _base_exhausted{false};
};

}  // namespace irs::analysis
