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
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <functional>
#include <limits>
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
#include "iresearch/search/detail/text_source.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct IndexReader;
struct TermReader;

enum class PhraseMatch : uint8_t {
  Anchor,
  Automaton,
  Positions,
};

struct PhraseTokens {
  using Factory = std::function<std::shared_ptr<analysis::Tokenizer>()>;

  TextSource text;
  Factory tokenizer;
  std::optional<ByPhraseOptions> spec;
  bool deferred = false;

  const ByPhraseOptions& Check(const ByPhraseOptions& phrase) const noexcept {
    return spec ? *spec : phrase;
  }

  bool operator==(const PhraseTokens& rhs) const noexcept {
    return text == rhs.text && spec == rhs.spec && deferred == rhs.deferred;
  }
};

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

struct CompiledPhrase {
  static constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kMaxBits = 64;

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
    uint32_t slot = kNoSlot;
    uint64_t left = 0;
    uint64_t right = 0;
  };

  struct Automaton {
    struct Extra {
      uint64_t range = 0;
      uint64_t bit = 0;
      uint32_t index = 0;
    };

    std::vector<uint64_t> slot_bits;
    std::vector<Extra> extras;
    uint64_t wild = 0;
    uint64_t last = 0;
    uint32_t length = 0;
  };

  CompiledPhrase(const ByPhraseOptions& phrase,
                 std::span<const std::vector<bstring>> expanded,
                 const TermReader& reader,
                 std::optional<PhraseMatch> match = std::nullopt);

  CompiledPhrase(const ByPhraseOptions& phrase, bytes_view word_separator,
                 const IndexReader& index, field_id field,
                 std::optional<PhraseMatch> match = std::nullopt);

  CompiledPhrase(CompiledPhrase&&) = delete;
  CompiledPhrase& operator=(CompiledPhrase&&) = delete;

  static bool Standalone(const ByPhraseOptions& phrase) noexcept;

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
    if (pattern_slots.empty() || !Plain(view)) {
      return;
    }
    for (const auto slot : pattern_slots) {
      if (patterns[slot]->Accepts(view)) {
        visit(slot);
      }
    }
  }

  std::vector<bstring> terms;
  std::vector<TermAcceptorSource::ptr> sources;
  std::vector<PosAttr::value_t> offs_min;
  std::vector<PosAttr::value_t> offs_max;
  Slop slop;
  bstring separator;
  std::vector<duckdb::string_t> words;
  std::vector<uint8_t> is_word;
  containers::FlatHashMap<bytes_view, Accept> accept;
  std::vector<uint32_t> slot_ids;
  std::vector<TermPredicate::ptr> patterns;
  std::vector<uint32_t> pattern_slots;
  Anchor anchor;
  Automaton automaton;
  PhraseMatch primary = PhraseMatch::Positions;
  PhraseMatch fallback = PhraseMatch::Positions;

 private:
  const Accept* Find(bytes_view term) const noexcept {
    const auto it = accept.find(term);
    return it == accept.end() ? nullptr : &it->second;
  }

  bool Plain(bytes_view term) const noexcept {
    return separator.empty() || term.find(separator) == bytes_view::npos;
  }

  void Init(const ByPhraseOptions& phrase,
            std::span<const std::vector<bstring>> expanded, bool predicates,
            std::span<const TermReader* const> readers,
            std::optional<PhraseMatch> match);
  void AddPattern(uint32_t slot, const ByPhraseOptions::PhrasePart& part);
  void Index(std::span<const uint32_t> slots);
  void LayoutSlop();
  void LayoutAutomaton();
  void PickAnchor(std::span<const TermReader* const> readers);
};

class PhraseCheck final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PhraseCheck(const CompiledPhrase& phrase, const PhraseTokens& tokens,
              bool count);

  void Bind(duckdb::DataChunk& columns);
  bool Check(duckdb::idx_t row, PhraseVerdict& out);
  bool Restarted() const noexcept { return _restarted; }

  void Prepare(duckdb::string_t) noexcept { _value_base = _last_pos; }
  void Discard() noexcept {}
  void Consume(TokenBatch& batch, DocRuns runs) final;

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

  struct Anchor {
    void Reset();
    void Carry(uint64_t span);

    const duckdb::string_t& TermAt(size_t at) const noexcept {
      return at < batch_base ? carry_terms[at - carry_base]
                             : batch_terms[at - batch_base];
    }

    uint32_t PosAt(size_t at) const noexcept {
      return at < batch_base ? carry_pos[at - carry_base]
                             : batch_pos[at - batch_base];
    }

    const duckdb::string_t* batch_terms = nullptr;
    const uint32_t* batch_pos = nullptr;
    size_t batch_base = 0;
    size_t end = 0;
    size_t carry_base = 0;
    std::vector<duckdb::string_t> carry_terms;
    std::vector<uint32_t> carry_pos;
    std::vector<duckdb::string_t> next_terms;
    std::vector<uint32_t> next_pos;
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
    std::vector<PosAttr::value_t> valid;
    std::vector<PosAttr::value_t> next;
    std::vector<uint64_t> ways;
    std::vector<uint64_t> next_ways;
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

  void Start(PhraseMatch mode);
  bool Restart();
  bool End(PhraseVerdict& out);
  bool Counted(PhraseVerdict& out) const noexcept;

  void Feed(Anchor& anchor, const TokenBatch& batch);
  bool Finish(Anchor& anchor, PhraseVerdict& out);
  bool Hit(Anchor& anchor, size_t at);
  uint64_t Right(Anchor& anchor, uint32_t slot, size_t at);
  uint64_t Left(Anchor& anchor, uint32_t slot, size_t at);
  bool Over(Anchor& anchor) noexcept;

  void Feed(Automaton& automaton, const TokenBatch& batch);
  bool Finish(Automaton& automaton, PhraseVerdict& out);
  void Step(Automaton& automaton, uint64_t mask);
  void Flush(Automaton& automaton);

  void Feed(Positions& positions, const TokenBatch& batch);
  bool Finish(Positions& positions, PhraseVerdict& out);

  const CompiledPhrase* _phrase;
  std::shared_ptr<analysis::Tokenizer> _tokenizer;
  std::unique_ptr<TextExpression> _expression;
  ValueAnalyzer _analyzer;
  Rows _rows;
  std::variant<Anchor, Automaton, Positions> _state;
  bool _dense;
  bool _count;
  bool _done = false;
  bool _restart = false;
  bool _restarted = false;
  uint32_t _last_pos = 0;
  uint32_t _value_base = 0;
  uint64_t _freq = 0;
};

}  // namespace irs
