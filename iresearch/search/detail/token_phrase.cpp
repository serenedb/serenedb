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

#include "iresearch/search/detail/token_phrase.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <span>
#include <utility>

namespace irs {

void PhraseCheck::AnchorMatch::Reset(uint32_t anchors) {
  _batch_base = 0;
  _end = 0;
  _carry_base = 0;
  _carry.clear();
  _arena.Reset();
  _pending.clear();
  _last = 0;
  _anchors = anchors;
  _steps = 0;
  _overran = false;
}

void PhraseCheck::AnchorMatch::Carry(uint64_t left) {
  const uint64_t first = PosAt(_pending.empty() ? _end - 1 : _pending.front());
  const auto keep = first > left ? first - left : 0;
  auto from = _end;
  while (from > _carry_base && PosAt(from - 1) >= keep) {
    --from;
  }
  _carry.erase(_carry.begin(),
               _carry.begin() + (std::min(from, _batch_base) - _carry_base));
  for (auto at = std::max(from, _batch_base); at != _end; ++at) {
    auto term = _batch_terms[at - _batch_base];
    if (!term.IsInlined()) {
      const auto size = static_cast<uint32_t>(term.GetSize());
      auto* data = _arena.Allocate(size);
      std::memcpy(data, term.GetData(), size);
      term = {reinterpret_cast<const char*>(data), size};
    }
    _carry.emplace_back(term, _batch_pos[at - _batch_base]);
  }
  _carry_base = from;
}

void PhraseCheck::AnchorMatch::Consumed(const TokenBatch& batch,
                                        const State& state) {
  if (!state.done && batch.count != 0) {
    Carry(state.phrase->anchor->left);
  }
}

void PhraseCheck::AutomatonMatch::Reset() {
  _bits = 0;
  _mask = 0;
  _at = 0;
}

void PhraseCheck::PositionsMatch::Reset(size_t n) {
  _slots.resize(n);
  for (auto& slot : _slots) {
    slot.clear();
  }
}

PhraseCheck::PhraseCheck(const CompiledPhrase& phrase,
                         const ByPhraseOptions::TokenizerFactory& tokenizer,
                         bool count)
  : _tokenizer{tokenizer()},
    _anchor{_analyzer.Batch()},
    _state{.phrase = &phrase, .count = count},
    _dense{!_tokenizer->Traits().explicit_pos} {}

PhraseVerdict PhraseCheck::Check(duckdb::idx_t row, uint32_t anchors) {
  Start(static_cast<bool>(_state.phrase->anchor), anchors);
  Analyze(row);
  if (_route == Route::Anchor) {
    _anchor.Drain(_state);
    if (_anchor.Overran()) {
      Start(false);
      Analyze(row);
    }
  }
  return WithMatch([&](auto& match) { return match.Finish(_state); });
}

void PhraseCheck::Analyze(duckdb::idx_t row) {
  _rows.ForEach(row, [&](duckdb::string_t value) {
    _analyzer.Scan(*_tokenizer, value, *this);
    return !_state.done;
  });
}

void PhraseCheck::Start(bool anchored, uint32_t anchors) {
  _state.done = false;
  _state.freq = 0;
  _last_pos = 0;
  _value_base = 0;
  const auto& phrase = *_state.phrase;
  if (anchored) {
    _route = Route::Anchor;
    _anchor.Reset(anchors);
  } else if (phrase.automaton && !_state.count) {
    _route = Route::Automaton;
    _automaton.Reset();
  } else {
    _route = Route::Positions;
    _positions.Reset(phrase.slots.size());
  }
}

void PhraseCheck::Consume(TokenBatch& batch, DocRuns) {
  Take(batch);
  _fed = 0;
  if (_route == Route::Anchor) {
    _anchor.Consumed(batch, _state);
  }
}

bool PhraseCheck::OnProgress(TokenBatch& batch) {
  Take(batch);
  return !_state.done;
}

void PhraseCheck::Take(TokenBatch& batch) {
  const auto from = std::exchange(_fed, batch.count);
  const auto count = batch.count;
  if (_state.done || from == count) {
    return;
  }
  if (_dense) {
    std::span<uint32_t> fresh{batch.pos + from, count - from};
    absl::c_iota(fresh, _last_pos + 1);
    _last_pos += count - from;
  } else {
    for (auto i = from; i != count; ++i) {
      batch.pos[i] += _value_base;
    }
    _last_pos = std::max(_last_pos, batch.pos[count - 1]);
  }
  WithMatch([&](auto& match) { match.Feed(batch, from, _state); });
}

void PhraseCheck::AnchorMatch::Drain(State& state) {
  if (state.done) {
    return;
  }
  _batch_base = _carry_base + _carry.size();
  _end = _batch_base;
  for (const auto at : _pending) {
    if (Hit(at, state)) {
      break;
    }
  }
  _pending.clear();
  state.done = true;
}

PhraseVerdict PhraseCheck::Verdict(const State& state) noexcept {
  if (state.freq == 0) {
    return {};
  }
  return {.freq = state.count ? std::saturate_cast<uint32_t>(state.freq) : 1};
}

void PhraseCheck::AnchorMatch::Feed(const TokenBatch& batch, uint32_t from,
                                    State& state) {
  SDB_ASSERT(batch.terms == _batch_terms.data());
  const auto& layout = *state.phrase->anchor;
  _batch_base = _carry_base + _carry.size();
  _end = _batch_base + batch.count;
  const uint64_t last = batch.pos[batch.count - 1];
  size_t keep = 0;
  for (const auto at : _pending) {
    if (PosAt(at) + layout.right < last) {
      if (Hit(at, state)) {
        return;
      }
    } else {
      _pending[keep++] = at;
    }
  }
  _pending.resize(keep);
  const auto& word = *state.phrase->slots[layout.slot].word;
  for (auto i = from; i != batch.count; ++i) {
    if (batch.terms[i] != word) {
      continue;
    }
    const auto pos = batch.pos[i];
    if (pos == _last) {
      continue;
    }
    _last = pos;
    const auto at = _batch_base + i;
    if (pos + layout.right < last) {
      if (Hit(at, state)) {
        return;
      }
    } else {
      _pending.emplace_back(at);
    }
  }
}

PhraseVerdict PhraseCheck::AnchorMatch::Finish(const State& state) const {
  SDB_ASSERT(state.done && !_overran);
  return Verdict(state);
}

bool PhraseCheck::AnchorMatch::Hit(size_t at, State& state) {
  const auto slot = state.phrase->anchor->slot;
  if (const auto left = Ways<false>(slot, at, state)) {
    state.freq += left * Ways<true>(slot, at, state);
  }
  state.done |=
    (!state.count && state.freq != 0) || (_anchors != 0 && --_anchors == 0);
  return state.done;
}

template<bool Right>
uint64_t PhraseCheck::AnchorMatch::Ways(uint32_t slot, size_t at,
                                        State& state) {
  const auto& phrase = *state.phrase;
  if (slot == (Right ? phrase.slots.size() - 1 : 0)) {
    return 1;
  }
  const auto other = Right ? slot + 1 : slot - 1;
  const auto& next = phrase.slots[other];
  const auto& gap = Right ? next : phrase.slots[slot];
  const uint64_t pos = PosAt(at);
  uint64_t ways = 0;
  uint64_t accepted = std::numeric_limits<uint64_t>::max();
  for (auto j = at; Right ? ++j < _end : j-- > _carry_base;) {
    const uint64_t p = PosAt(j);
    const auto distance = Right ? p - pos : pos - p;
    if (distance > gap.offs_max) {
      break;
    }
    if (Over(state)) {
      return 0;
    }
    if (distance < gap.offs_min || p == accepted ||
        !Accepts(phrase, next, TermAt(j))) {
      continue;
    }
    accepted = p;
    ways += Ways<Right>(other, j, state);
    if (_overran) {
      return 0;
    }
    if (!state.count && ways != 0) {
      return 1;
    }
  }
  return ways;
}

bool PhraseCheck::AnchorMatch::Over(State& state) noexcept {
  if (++_steps <= kStepsPerToken * _end + kStepSlack) {
    return false;
  }
  _overran = true;
  state.done = true;
  return true;
}

void PhraseCheck::AutomatonMatch::Feed(const TokenBatch& batch, uint32_t from,
                                       State& state) {
  for (auto i = from; i != batch.count; ++i) {
    const auto pos = batch.pos[i];
    if (pos != _at) {
      Flush(state);
      if (state.done) {
        return;
      }
      if (_at != 0) {
        for (auto gap = _at + 1; gap < pos && _bits != 0; ++gap) {
          Step(0, state);
        }
      }
      _at = pos;
    }
    _mask |= MaskOf(*state.phrase, batch.terms[i]);
  }
}

PhraseVerdict PhraseCheck::AutomatonMatch::Finish(State& state) {
  if (!state.done) {
    Flush(state);
  }
  return Verdict(state);
}

void PhraseCheck::AutomatonMatch::Flush(State& state) {
  if (_at == 0 || (_bits == 0 && _mask == 0)) {
    _mask = 0;
    return;
  }
  Step(_mask, state);
  _mask = 0;
}

void PhraseCheck::AutomatonMatch::Step(uint64_t mask, State& state) {
  const auto& layout = *state.phrase->automaton;
  auto next = ((_bits << 1) | 1) & (mask | layout.wild);
  for (const auto& extra : layout.extras) {
    if (_bits & extra.range) {
      next |= extra.bit & mask;
    }
  }
  _bits = next;
  if (_bits & layout.last) {
    state.freq = 1;
    state.done = true;
  }
}

void PhraseCheck::PositionsMatch::Feed(const TokenBatch& batch, uint32_t from,
                                       State& state) {
  for (auto i = from; i != batch.count; ++i) {
    const auto pos = batch.pos[i];
    ForEachSlot(*state.phrase, batch.terms[i], [&](uint32_t slot) {
      auto& slot_positions = _slots[slot];
      if (slot_positions.empty() || slot_positions.back() != pos) {
        slot_positions.emplace_back(pos);
      }
    });
  }
}

PhraseVerdict PhraseCheck::PositionsMatch::Finish(State& state) {
  const auto& phrase = *state.phrase;
  const auto n = phrase.slots.size();
  if (absl::c_any_of(_slots, [](const auto& slot) { return slot.empty(); })) {
    return {};
  }
  if (n > 1 && phrase.slop.max != 0) {
    detail::slop::SpanCursors cursors{_slots, _scratch};
    const auto res =
      detail::slop::Sweep<0>(cursors, phrase.slop.offsets, phrase.slop.max,
                             phrase.slop.pairs, _scratch, !state.count, [] {});
    if (!res.any) {
      return {};
    }
    if (!state.count) {
      return {.freq = 1};
    }
    return {
      .freq = std::saturate_cast<uint32_t>(res.freq),
      .scale = static_cast<score_t>(res.weight / static_cast<double>(res.freq)),
    };
  }

  _valid.clear();
  for (const auto p : _slots.back()) {
    _valid.emplace_back(p, 1);
  }
  for (size_t i = n - 1; i != 0; --i) {
    const auto& slot = phrase.slots[i];
    _next.clear();
    size_t lo = 0;
    size_t hi = 0;
    uint64_t window = 0;
    for (const auto p : _slots[i - 1]) {
      const uint64_t min = uint64_t{p} + slot.offs_min;
      const uint64_t max = uint64_t{p} + slot.offs_max;
      for (; hi != _valid.size() && _valid[hi].pos <= max; ++hi) {
        window += _valid[hi].ways;
      }
      for (; lo != hi && _valid[lo].pos < min; ++lo) {
        window -= _valid[lo].ways;
      }
      if (window != 0) {
        _next.emplace_back(
          p, std::min<uint64_t>(window, std::numeric_limits<uint32_t>::max()));
      }
    }
    std::swap(_valid, _next);
    if (_valid.empty()) {
      return {};
    }
  }
  for (const auto& way : _valid) {
    state.freq += way.ways;
  }
  return Verdict(state);
}

}  // namespace irs
