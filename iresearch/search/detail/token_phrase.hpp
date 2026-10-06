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

#include <absl/container/flat_hash_map.h>

#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct TermReader;

enum class PhraseMatch : uint8_t {
  Anchor,
  Automaton,
  Positions,
};

struct PhraseTokens {
  using Factory = std::function<std::shared_ptr<analysis::Tokenizer>()>;

  field_id column = field_limits::invalid();
  Factory tokenizer;
  std::optional<ByPhraseOptions> spec;
  std::optional<PhraseMatch> match;

  bool operator==(const PhraseTokens& rhs) const noexcept {
    return column == rhs.column && spec == rhs.spec && match == rhs.match;
  }
};

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

class TokenPhraseMatcher {
 public:
  using Mode = PhraseMatch;

  TokenPhraseMatcher(const ByPhraseOptions& phrase,
                     std::span<const std::vector<bstring>> expanded,
                     const TermReader& reader,
                     std::optional<PhraseMatch> match = std::nullopt);

  TokenPhraseMatcher(TokenPhraseMatcher&&) = delete;
  TokenPhraseMatcher& operator=(TokenPhraseMatcher&&) = delete;

  Mode Primary() const noexcept { return _primary; }
  Mode Fallback() const noexcept { return _fallback; }
  bool Sloppy() const noexcept { return _slop != 0; }
  size_t Slots() const noexcept { return _offs_min.size(); }

 private:
  friend class TokenPhraseSink;

  static constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kMaxBits = 64;

  struct Accept {
    uint32_t begin = 0;
    uint32_t size = 0;
    uint64_t mask = 0;
  };

  struct Extra {
    uint64_t range = 0;
    uint64_t bit = 0;
    uint32_t index = 0;
  };

  const Accept* Find(const duckdb::string_t& term) const noexcept {
    const auto it = _accept.find(bytes_view{
      reinterpret_cast<const byte_type*>(term.GetData()), term.GetSize()});
    return it == _accept.end() ? nullptr : &it->second;
  }

  bool Accepts(uint32_t slot, const duckdb::string_t& term) const noexcept;

  void Index(std::span<const uint32_t> slots);
  void Layout();
  void PickAnchor(const TermReader& reader);

  std::vector<bstring> _owned;
  absl::flat_hash_map<bytes_view, Accept> _accept;
  std::vector<uint32_t> _slot_ids;
  std::vector<duckdb::string_t> _words;
  std::vector<uint8_t> _is_word;
  std::vector<PosAttr::value_t> _offs_min;
  std::vector<PosAttr::value_t> _offs_max;
  std::vector<PosAttr::value_t> _steps;
  std::vector<uint32_t> _groups;
  PosAttr::value_t _slop = 0;
  uint32_t _anchor = kNoSlot;
  uint64_t _left = 0;
  uint64_t _right = 0;
  std::vector<uint64_t> _slot_bits;
  std::vector<Extra> _extras;
  uint64_t _wild = 0;
  uint64_t _last = 0;
  uint32_t _length = 0;
  Mode _primary = Mode::Positions;
  Mode _fallback = Mode::Positions;
};

class TokenPhraseSink final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;
  static constexpr uint64_t kStepsPerToken = 4;
  static constexpr uint64_t kStepSlack = 256;

  TokenPhraseSink(const TokenPhraseMatcher& matcher, TokenTraits producer);

  void Begin(bool count);
  bool Restart();
  bool End(PhraseVerdict& out);

  void Prepare(duckdb::string_t) noexcept { _value_base = _last_pos; }
  void Discard() noexcept {}
  void Consume(TokenBatch& batch, DocRuns runs) final;

 private:
  using Mode = TokenPhraseMatcher::Mode;

  void Start(Mode mode);

  void AnchorBatch(const TokenBatch& batch);
  bool Hit(size_t at);
  uint64_t Right(uint32_t slot, size_t at);
  uint64_t Left(uint32_t slot, size_t at);
  bool Over() noexcept;
  void Carry();
  const duckdb::string_t& TermAt(size_t at) const noexcept {
    return at < _batch_base ? _carry_terms[at - _carry_base]
                            : _batch_terms[at - _batch_base];
  }
  uint32_t PosAt(size_t at) const noexcept {
    return at < _batch_base ? _carry_pos[at - _carry_base]
                            : _batch_pos[at - _batch_base];
  }

  void AutomatonBatch(const TokenBatch& batch);
  void Step(uint64_t mask);
  void Flush();

  void PositionsBatch(const TokenBatch& batch);
  bool EndPositions(PhraseVerdict& out);

  const TokenPhraseMatcher* _matcher;
  Mode _mode = Mode::Positions;
  bool _dense;
  bool _count = false;
  bool _done = false;
  bool _restart = false;
  uint32_t _last_pos = 0;
  uint32_t _value_base = 0;
  uint64_t _freq = 0;

  const duckdb::string_t* _batch_terms = nullptr;
  const uint32_t* _batch_pos = nullptr;
  size_t _batch_base = 0;
  size_t _end = 0;
  size_t _carry_base = 0;
  std::vector<duckdb::string_t> _carry_terms;
  std::vector<uint32_t> _carry_pos;
  std::vector<duckdb::string_t> _next_terms;
  std::vector<uint32_t> _next_pos;
  std::deque<std::string> _interned;
  std::vector<size_t> _pending;
  uint32_t _last_anchor = 0;
  uint64_t _steps = 0;

  uint64_t _d = 0;
  uint64_t _mask = 0;
  uint32_t _at = 0;
  std::vector<uint64_t> _c;
  std::vector<uint64_t> _sums;

  std::vector<std::vector<PosAttr::value_t>> _slots;
  std::vector<PosAttr::value_t> _valid;
  std::vector<PosAttr::value_t> _next;
  std::vector<uint64_t> _ways;
  std::vector<uint64_t> _next_ways;
  detail::slop::MatchScratch _slop;
};

class TokenPhraseReader {
 public:
  TokenPhraseReader(const ColReader& col_reader, const ColumnReader& column,
                    std::shared_ptr<analysis::Tokenizer> tokenizer,
                    const TokenPhraseMatcher& matcher);

  TokenPhraseReader(TokenPhraseReader&&) = delete;
  TokenPhraseReader& operator=(TokenPhraseReader&&) = delete;

  bool Match(doc_id_t doc, bool count, PhraseVerdict& out);

 private:
  bool Fetch(doc_id_t doc);
  bool Analyze();

  ReadContext _ctx;
  const ColumnReader* _column;
  ColumnReader::ScanState _state;
  ColumnReader::VectorScratch _out;
  duckdb::SelectionVector _sel;
  std::shared_ptr<analysis::Tokenizer> _tokenizer;
  ValueAnalyzer _analyzer;
  TokenPhraseSink _sink;
  std::vector<duckdb::string_t> _values;
  doc_id_t _doc = doc_limits::invalid();
  bool _fetched = false;
  bool _checked = false;
  bool _counted = false;
  bool _matched = false;
  PhraseVerdict _verdict;
  uint32_t _loads = 0;
};

}  // namespace irs
