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
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/analysis/token_sinks.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/detail/compiled_phrase.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/detail/row_values.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"

namespace irs {

struct PhraseVerdict {
  uint32_t freq = 0;
  score_t scale = kNoBoost;
};

class PhraseCheck final : public TokenConsumer {
 public:
  static constexpr TokenLayout kLayout = TokenLayout::TermsPos;

  PhraseCheck(const CompiledPhrase& phrase,
              const ByPhraseOptions::TokenizerFactory& tokenizer, bool count);

  void Bind(duckdb::Vector& values, duckdb::idx_t count) {
    _rows.Bind(values, count);
  }
  PhraseVerdict Check(duckdb::idx_t row, uint32_t anchors = 0);

  void Prepare(duckdb::string_t) noexcept { _value_base = _last_pos; }
  void Discard() noexcept { _fed = 0; }
  void Consume(TokenBatch& batch, DocRuns runs) final;
  bool OnProgress(TokenBatch& batch) final;

 private:
  enum class Route : uint8_t {
    Anchor,
    Automaton,
    Positions,
  };

  struct State {
    const CompiledPhrase* phrase = nullptr;
    bool count = false;
    bool done = false;
    uint64_t freq = 0;
  };

  static PhraseVerdict Verdict(const State& state) noexcept;

  class AnchorMatch {
   public:
    explicit AnchorMatch(const TokenBatch& batch) noexcept
      : _batch_terms{batch.terms}, _batch_pos{batch.pos} {}

    void Reset(uint32_t anchors);
    void Feed(const TokenBatch& batch, uint32_t from, State& state);
    void Consumed(const TokenBatch& batch, const State& state);
    void Drain(State& state);
    bool Overran() const noexcept { return _overran; }
    PhraseVerdict Finish(const State& state) const;

   private:
    static constexpr uint64_t kStepsPerToken = 4;
    static constexpr uint64_t kStepSlack = 256;

    struct Token {
      duckdb::string_t term;
      uint32_t pos = 0;
    };

    const duckdb::string_t& TermAt(size_t at) const noexcept {
      SDB_ASSERT(at >= _carry_base && at < _end);
      return at < _batch_base ? _carry[at - _carry_base].term
                              : _batch_terms[at - _batch_base];
    }

    uint32_t PosAt(size_t at) const noexcept {
      SDB_ASSERT(at >= _carry_base && at < _end);
      return at < _batch_base ? _carry[at - _carry_base].pos
                              : _batch_pos[at - _batch_base];
    }

    void Carry(uint64_t left);
    bool Hit(size_t at, State& state);
    template<bool Right>
    uint64_t Ways(uint32_t slot, size_t at, State& state);
    bool Over(State& state) noexcept;

    std::span<const duckdb::string_t, TokenBatch::kCapacity> _batch_terms;
    std::span<const uint32_t, TokenBatch::kCapacity> _batch_pos;
    size_t _batch_base = 0;
    size_t _end = 0;
    size_t _carry_base = 0;
    std::vector<Token> _carry;
    duckdb::ArenaAllocator _arena{duckdb::Allocator::DefaultAllocator()};
    std::vector<size_t> _pending;
    uint32_t _last = 0;
    uint32_t _anchors = 0;
    uint64_t _steps = 0;
    bool _overran = false;
  };

  class AutomatonMatch {
   public:
    void Reset();
    void Feed(const TokenBatch& batch, uint32_t from, State& state);
    PhraseVerdict Finish(State& state);

   private:
    void Step(uint64_t mask, State& state);
    void Flush(State& state);

    uint64_t _bits = 0;
    uint64_t _mask = 0;
    uint32_t _at = 0;
  };

  class PositionsMatch {
   public:
    void Reset(size_t slots);
    void Feed(const TokenBatch& batch, uint32_t from, State& state);
    PhraseVerdict Finish(State& state);

   private:
    struct Way {
      PosAttr::value_t pos = 0;
      uint64_t ways = 0;
    };

    std::vector<std::vector<PosAttr::value_t>> _slots;
    std::vector<Way> _valid;
    std::vector<Way> _next;
    detail::slop::MatchScratch _scratch;
  };

  decltype(auto) WithMatch(auto&& visit) {
    switch (_route) {
      case Route::Anchor:
        return visit(_anchor);
      case Route::Automaton:
        return visit(_automaton);
      case Route::Positions:
        return visit(_positions);
    }
    std::unreachable();
  }

  void Analyze(duckdb::idx_t row);
  void Take(TokenBatch& batch);
  void Start(bool anchored, uint32_t anchors = 0);

  std::shared_ptr<analysis::Tokenizer> _tokenizer;
  ScanAnalyzer _analyzer;
  RowValues _rows;
  AnchorMatch _anchor;
  AutomatonMatch _automaton;
  PositionsMatch _positions;
  State _state;
  Route _route = Route::Anchor;
  bool _dense;
  uint32_t _last_pos = 0;
  uint32_t _value_base = 0;
  uint32_t _fed = 0;
};

}  // namespace irs
