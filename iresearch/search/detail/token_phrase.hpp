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

  struct WordStat {
    uint64_t docs = 0;
    uint64_t freq = 0;
  };

  CompiledPhrase(const ByPhraseOptions& phrase,
                 std::span<const std::vector<bstring>> expanded,
                 std::span<const WordStat> words);

  CompiledPhrase(const ByPhraseOptions& phrase, bytes_view word_separator,
                 std::span<const WordStat> words);

  CompiledPhrase(CompiledPhrase&&) = delete;
  CompiledPhrase& operator=(CompiledPhrase&&) = delete;

  static bool Standalone(const ByPhraseOptions& phrase) noexcept;
  static bool Anchored(const ByPhraseOptions& phrase) noexcept;
  static std::vector<WordStat> WordStats(
    const ByPhraseOptions& phrase, std::span<const TermReader* const> readers);
  static std::vector<WordStat> WordStats(const ByPhraseOptions& phrase,
                                         const IndexReader& index,
                                         field_id field);

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
            std::span<const WordStat> words);
  void AddPattern(uint32_t slot, const ByPhraseOptions::PhrasePart& part);
  void Index(std::span<const uint32_t> term_slots);
  void LayoutSlop();
  void LayoutAutomaton();
  void PickAnchor(std::span<const WordStat> words);
};

class PhraseCheck final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PhraseCheck(const CompiledPhrase& phrase, const PhraseTokens& tokens,
              bool count);

  void Bind(duckdb::Vector& values, duckdb::idx_t count);
  bool Check(duckdb::idx_t row, PhraseVerdict& out, uint32_t anchors = 0);

  void Prepare(duckdb::string_t) noexcept { _value_base = _last_pos; }
  void Discard() noexcept { _peeked = 0; }
  void Consume(TokenBatch& batch, DocRuns runs) final;
  bool Peek(TokenBatch& batch);

 private:
  static constexpr uint64_t kStepsPerToken = 4;
  static constexpr uint64_t kStepSlack = 256;

  struct Rows {
    duckdb::UnifiedVectorFormat format;
    duckdb::UnifiedVectorFormat children;
    duckdb::LogicalTypeId type = duckdb::LogicalTypeId::VARCHAR;
    uint64_t array_size = 0;
    std::vector<duckdb::string_t> values;
  };

  struct Token {
    duckdb::string_t term;
    uint32_t pos;
  };

  struct Way {
    PosAttr::value_t pos;
    uint64_t ways;
  };

  struct Anchor {
    void Reset();
    void Carry(uint64_t span);

    const duckdb::string_t& TermAt(size_t at) const noexcept {
      return at < batch_base ? carry[at - carry_base].term
                             : batch_terms[at - batch_base];
    }

    uint32_t PosAt(size_t at) const noexcept {
      return at < batch_base ? carry[at - carry_base].pos
                             : batch_pos[at - batch_base];
    }

    const duckdb::string_t* batch_terms = nullptr;
    const uint32_t* batch_pos = nullptr;
    size_t batch_base = 0;
    size_t end = 0;
    size_t carry_base = 0;
    std::vector<Token> carry;
    std::vector<Token> next;
    duckdb::ArenaAllocator arena{duckdb::Allocator::DefaultAllocator()};
    std::vector<size_t> pending;
    uint32_t last = 0;
    uint64_t steps = 0;
  };

  struct Automaton {
    void Reset(const CompiledPhrase::Automaton& layout, bool count);

    uint64_t state = 0;
    uint64_t mask = 0;
    uint32_t at = 0;
    std::vector<uint64_t> counts;
    std::vector<uint64_t> sums;
  };

  struct Positions {
    void Reset(size_t slots);

    std::vector<std::vector<PosAttr::value_t>> slots;
    std::vector<Way> valid;
    std::vector<Way> next;
    detail::slop::MatchScratch scratch;
  };

  template<typename State>
  State& Use() {
    if (auto* state = std::get_if<State>(&_state)) {
      return *state;
    }
    return _state.template emplace<State>();
  }

  std::span<const duckdb::string_t> Values(duckdb::idx_t row);
  void Analyze(std::span<const duckdb::string_t> values);
  void Take(TokenBatch& batch);

  void Start(bool anchored);
  bool Restart();
  bool End(PhraseVerdict& out);
  bool Counted(PhraseVerdict& out) const noexcept;

  void Feed(Anchor& anchor, const TokenBatch& batch, uint32_t from);
  bool Finish(Anchor& anchor, PhraseVerdict& out);
  bool Hit(Anchor& anchor, size_t at);
  template<bool Right>
  uint64_t Ways(Anchor& anchor, uint32_t slot, size_t at);
  bool Over(Anchor& anchor) noexcept;

  void Feed(Automaton& automaton, const TokenBatch& batch, uint32_t from);
  bool Finish(Automaton& automaton, PhraseVerdict& out);
  void Step(Automaton& automaton, uint64_t mask);
  void Flush(Automaton& automaton);

  void Feed(Positions& positions, const TokenBatch& batch, uint32_t from);
  bool Finish(Positions& positions, PhraseVerdict& out);

  const CompiledPhrase* _phrase;
  std::shared_ptr<analysis::Tokenizer> _tokenizer;
  ValueAnalyzer _analyzer;
  Rows _rows;
  std::variant<Anchor, Automaton, Positions> _state;
  bool _dense;
  bool _count;
  bool _done = false;
  bool _restart = false;
  uint32_t _last_pos = 0;
  uint32_t _value_base = 0;
  uint32_t _anchors = 0;
  uint32_t _peeked = 0;
  uint64_t _freq = 0;
};

}  // namespace irs
