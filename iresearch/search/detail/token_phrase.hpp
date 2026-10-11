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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/detail/term_acceptor.hpp"
#include "iresearch/search/detail/term_predicate.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct IndexReader;
struct TermReader;

struct PhraseTokens {
  using Factory = std::function<std::shared_ptr<analysis::Tokenizer>()>;

  field_id text = field_limits::invalid();
  Factory tokenizer;
  std::optional<ByPhraseOptions> spec;

  const ByPhraseOptions& Check(const ByPhraseOptions& phrase) const noexcept {
    return spec ? *spec : phrase;
  }

  bool operator==(const PhraseTokens& rhs) const noexcept {
    return text == rhs.text && spec == rhs.spec;
  }
};

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

struct PhraseWordStat {
  uint64_t docs = 0;
  uint64_t freq = 0;
};

bool IsAnchored(const ByPhraseOptions& phrase) noexcept;
bool IsStandalone(const ByPhraseOptions& phrase) noexcept;
std::vector<PhraseWordStat> PhraseWordStats(
  const ByPhraseOptions& phrase, std::span<const TermReader* const> readers);
std::vector<PhraseWordStat> PhraseWordStats(const ByPhraseOptions& phrase,
                                            const IndexReader& index,
                                            field_id field);

struct CompiledPhrase {
  static constexpr uint32_t kMaxBits = 64;

  struct Slot {
    PosAttr::value_t offs_min = 0;
    PosAttr::value_t offs_max = 0;
    std::optional<duckdb::string_t> word;
    TermAcceptorSource::ptr source;
    TermPredicate::ptr pattern;
    uint64_t bit = 0;
  };

  struct Accept {
    uint32_t begin = 0;
    uint32_t size = 0;
    uint64_t mask = 0;
  };

  struct Slop {
    PosAttr::value_t max = 0;
    std::vector<int64_t> offsets;
    std::vector<detail::slop::GroupPair> pairs;
  };

  struct Anchor {
    uint32_t slot = 0;
    uint64_t left = 0;
    uint64_t right = 0;
  };

  struct Automaton {
    struct Extra {
      uint64_t range = 0;
      uint64_t bit = 0;
      uint32_t index = 0;
    };

    std::vector<Extra> extras;
    uint64_t wild = 0;
    uint64_t last = 0;
    uint32_t length = 0;
  };

  CompiledPhrase(const ByPhraseOptions& phrase,
                 std::span<const std::vector<bstring>> expanded,
                 std::span<const PhraseWordStat> words);

  CompiledPhrase(const ByPhraseOptions& phrase, bytes_view word_separator,
                 std::span<const PhraseWordStat> words);

  CompiledPhrase(CompiledPhrase&&) = delete;
  CompiledPhrase& operator=(CompiledPhrase&&) = delete;

  bool Accepts(uint32_t slot, const duckdb::string_t& term) const;
  uint64_t MaskOf(const duckdb::string_t& term) const;

  template<typename Visitor>
  void ForEachSlot(const duckdb::string_t& term, Visitor&& visit) const {
    const auto view = AsBytesView(term);
    if (const auto* found = Find(view)) {
      for (uint32_t i = 0; i != found->size; ++i) {
        visit(slot_ids[found->begin + i]);
      }
    }
    ForEachPattern(view, visit);
  }

  std::vector<Slot> slots;
  std::vector<bstring> terms;
  Slop slop;
  bstring separator;
  containers::FlatHashMap<bytes_view, Accept> accept;
  std::vector<uint32_t> slot_ids;
  std::vector<uint32_t> pattern_slots;
  std::optional<Anchor> anchor;
  std::optional<Automaton> automaton;

 private:
  const Accept* Find(bytes_view term) const noexcept {
    const auto it = accept.find(term);
    return it == accept.end() ? nullptr : &it->second;
  }

  bool Plain(bytes_view term) const noexcept {
    return separator.empty() || term.find(separator) == bytes_view::npos;
  }

  template<typename Visitor>
  void ForEachPattern(bytes_view term, Visitor&& visit) const {
    if (pattern_slots.empty() || !Plain(term)) {
      return;
    }
    for (const auto slot : pattern_slots) {
      if (slots[slot].pattern->Accepts(term)) {
        visit(slot);
      }
    }
  }

  void Init(const ByPhraseOptions& phrase,
            std::span<const std::vector<bstring>> expanded, bool predicates,
            std::span<const PhraseWordStat> words);
  void AddPattern(uint32_t slot, const ByPhraseOptions::PhrasePart& part);
  void Index(std::span<const uint32_t> term_slots);
  void LayoutSlop();
  void LayoutAutomaton();
  void PickAnchor(std::span<const PhraseWordStat> words);
};

class RowValues {
 public:
  void Bind(duckdb::Vector& values, duckdb::idx_t count);
  std::span<const duckdb::string_t> At(duckdb::idx_t row);

 private:
  duckdb::UnifiedVectorFormat _format;
  duckdb::UnifiedVectorFormat _children;
  duckdb::LogicalTypeId _type = duckdb::LogicalTypeId::VARCHAR;
  uint64_t _array_size = 0;
  std::vector<duckdb::string_t> _values;
};

class PhraseCheck final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PhraseCheck(const CompiledPhrase& phrase, const PhraseTokens& tokens,
              bool count);

  void Bind(duckdb::Vector& values, duckdb::idx_t count) {
    _rows.Bind(values, count);
  }
  bool Check(duckdb::idx_t row, PhraseVerdict& out, uint32_t anchors = 0);

  void Prepare(duckdb::string_t) noexcept { _value_base = _last_pos; }
  void Discard() noexcept { _fed = 0; }
  void Consume(TokenBatch& batch, DocRuns runs) final;
  bool OnProgress(TokenBatch& batch) final;

 private:
  struct State {
    bool Counted(PhraseVerdict& out) const noexcept;

    const CompiledPhrase* phrase;
    bool count;
    bool done = false;
    bool restart = false;
    uint32_t anchors = 0;
    uint64_t freq = 0;
  };

  class AnchorMatch {
   public:
    void Reset();
    void Feed(const TokenBatch& batch, uint32_t from, State& state);
    void Consumed(const TokenBatch& batch, const State& state);
    void Drain(State& state);
    bool Finish(State& state, PhraseVerdict& out);

   private:
    static constexpr uint64_t kStepsPerToken = 4;
    static constexpr uint64_t kStepSlack = 256;

    struct Token {
      duckdb::string_t term;
      uint32_t pos;
    };

    const duckdb::string_t& TermAt(size_t at) const noexcept {
      return at < _batch_base ? _carry[at - _carry_base].term
                              : _batch_terms[at - _batch_base];
    }

    uint32_t PosAt(size_t at) const noexcept {
      return at < _batch_base ? _carry[at - _carry_base].pos
                              : _batch_pos[at - _batch_base];
    }

    void Carry(uint64_t span);
    bool Hit(size_t at, State& state);
    template<bool Right>
    uint64_t Ways(uint32_t slot, size_t at, State& state);
    bool Over(State& state) noexcept;

    const duckdb::string_t* _batch_terms = nullptr;
    const uint32_t* _batch_pos = nullptr;
    size_t _batch_base = 0;
    size_t _end = 0;
    size_t _carry_base = 0;
    std::vector<Token> _carry;
    std::vector<Token> _next;
    duckdb::ArenaAllocator _arena{duckdb::Allocator::DefaultAllocator()};
    std::vector<size_t> _pending;
    uint32_t _last = 0;
    uint64_t _steps = 0;
  };

  class AutomatonMatch {
   public:
    void Reset(const CompiledPhrase::Automaton& layout, bool count);
    void Feed(const TokenBatch& batch, uint32_t from, State& state);
    bool Finish(State& state, PhraseVerdict& out);

   private:
    void Step(uint64_t mask, State& state);
    void Flush(State& state);

    uint64_t _bits = 0;
    uint64_t _mask = 0;
    uint32_t _at = 0;
    std::vector<uint64_t> _counts;
    std::vector<uint64_t> _sums;
  };

  class PositionsMatch {
   public:
    void Reset(size_t slots);
    void Feed(const TokenBatch& batch, uint32_t from, State& state);
    bool Finish(State& state, PhraseVerdict& out);

   private:
    struct Way {
      PosAttr::value_t pos;
      uint64_t ways;
    };

    std::vector<std::vector<PosAttr::value_t>> _slots;
    std::vector<Way> _valid;
    std::vector<Way> _next;
    detail::slop::MatchScratch _scratch;
  };

  template<typename Match>
  Match& Use() {
    if (auto* match = std::get_if<Match>(&_match)) {
      return *match;
    }
    return _match.template emplace<Match>();
  }

  void Analyze(std::span<const duckdb::string_t> values);
  void Take(TokenBatch& batch);
  void Start(bool anchored);
  bool Restart();

  std::shared_ptr<analysis::Tokenizer> _tokenizer;
  ScanAnalyzer _analyzer;
  RowValues _rows;
  std::variant<AnchorMatch, AutomatonMatch, PositionsMatch> _match;
  State _state;
  bool _dense;
  uint32_t _last_pos = 0;
  uint32_t _value_base = 0;
  uint32_t _fed = 0;
};

}  // namespace irs
