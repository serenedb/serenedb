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
#include <bit>
#include <cstring>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <limits>
#include <numeric>
#include <utility>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/index_reader.hpp"

namespace irs {
namespace {

duckdb::string_t AsString(bytes_view term) noexcept {
  return {reinterpret_cast<const char*>(term.data()),
          static_cast<uint32_t>(term.size())};
}

uint32_t Clamp(uint64_t freq) noexcept {
  return static_cast<uint32_t>(
    std::min<uint64_t>(freq, std::numeric_limits<uint32_t>::max()));
}

}  // namespace

TokenPhraseMatcher::TokenPhraseMatcher(
  const ByPhraseOptions& phrase, std::span<const std::vector<bstring>> expanded,
  const TermReader& reader, std::optional<PhraseMatch> match)
  : _slop{phrase.slop()} {
  Init(
    phrase, expanded, false,
    [&](bytes_view term) { return reader.Lookup(term); }, match);
}

TokenPhraseMatcher::TokenPhraseMatcher(const ByPhraseOptions& phrase,
                                       bytes_view separator, Lookup lookup,
                                       std::optional<PhraseMatch> match)
  : _separator{separator}, _slop{phrase.slop()} {
  SDB_ASSERT(Standalone(phrase));
  Init(phrase, {}, true, lookup, match);
}

TokenPhraseMatcher::TokenPhraseMatcher(const ByPhraseOptions& phrase,
                                       bytes_view separator,
                                       const IndexReader& index, field_id field,
                                       std::optional<PhraseMatch> match)
  : TokenPhraseMatcher{phrase, separator,
                       [&](bytes_view term) {
                         uint64_t docs = 0;
                         uint64_t freq = 0;
                         for (const auto& segment : index) {
                           if (const auto* terms = segment.field(field)) {
                             const auto meta = terms->Lookup(term);
                             docs += meta.docs_count;
                             freq += meta.freq;
                           }
                         }
                         return PostingMeta{.docs_count = Clamp(docs),
                                            .freq = Clamp(freq)};
                       },
                       match} {}

bool TokenPhraseMatcher::Standalone(const ByPhraseOptions& phrase) noexcept {
  return absl::c_all_of(phrase, [&](const auto& info) {
    return std::visit(
      [&]<typename Part>(const Part& part) {
        if constexpr (std::is_same_v<Part, ByTermOptions> ||
                      std::is_same_v<Part, TermSetOptions>) {
          return true;
        } else if constexpr (std::is_same_v<Part, ByPrefixOptions> ||
                             std::is_same_v<Part, ByRangeOptions>) {
          return phrase.slop() == 0;
        } else if constexpr (std::is_same_v<Part, AutomatonOptions>) {
          return phrase.slop() == 0 && part.source;
        } else if constexpr (std::is_same_v<Part,
                                            LevenshteinAutomatonOptions>) {
          return phrase.slop() == 0 && part.max_terms == 0 && part.source;
        } else {
          return false;
        }
      },
      info.part);
  });
}

void TokenPhraseMatcher::Init(const ByPhraseOptions& phrase,
                              std::span<const std::vector<bstring>> expanded,
                              bool patterns, Lookup lookup,
                              std::optional<PhraseMatch> match) {
  const auto n = static_cast<uint32_t>(phrase.size());
  _offs_min.reserve(n);
  _offs_max.reserve(n);
  _is_word.assign(n, 0);
  if (patterns) {
    _patterns.resize(n);
  }
  std::vector<uint32_t> slots;
  std::vector<size_t> word_of(n);
  bool stacked = false;
  uint32_t slot = 0;
  const auto accept = [&](bytes_view term) {
    _owned.emplace_back(term);
    slots.push_back(slot);
  };
  for (const auto& info : phrase) {
    _offs_min.push_back(info.offs_min);
    _offs_max.push_back(info.offs_max);
    stacked |= slot != 0 && info.offs_min == 0;
    switch (ByPhraseOptions::KindOf(info.part)) {
      case SlotKind::Term:
        _is_word[slot] = 1;
        word_of[slot] = _owned.size();
        accept(std::get<ByTermOptions>(info.part).term);
        break;
      case SlotKind::Set:
        for (const auto& term : std::get<TermSetOptions>(info.part).terms) {
          accept(term);
        }
        break;
      case SlotKind::Expansion:
        if (patterns) {
          AddPattern(slot, info.part);
        } else if (slot < expanded.size()) {
          for (const auto& term : expanded[slot]) {
            accept(term);
          }
        }
        break;
    }
    ++slot;
  }
  _words.resize(n);
  for (uint32_t i = 0; i != n; ++i) {
    if (_is_word[i]) {
      _words[i] = AsString(_owned[word_of[i]]);
    }
  }
  Index(slots);

  if (_slop != 0 || stacked) {
    if (_slop != 0 && n > 1) {
      SlopLayout();
    }
    return;
  }
  Layout();
  PickAnchor(lookup);
  _fallback = _length != 0 ? PhraseMatch::Automaton : PhraseMatch::Positions;
  _primary = _anchor != kNoSlot ? PhraseMatch::Anchor : _fallback;
  if (!match) {
    return;
  }
  switch (*match) {
    case PhraseMatch::Anchor:
      break;
    case PhraseMatch::Automaton:
      if (_length != 0) {
        _primary = PhraseMatch::Automaton;
      }
      break;
    case PhraseMatch::Positions:
      _primary = PhraseMatch::Positions;
      break;
  }
}

void TokenPhraseMatcher::AddPattern(uint32_t slot,
                                    const ByPhraseOptions::PhrasePart& part) {
  std::visit(
    [&]<typename Part>(const Part& options) {
      if constexpr (std::is_same_v<Part, ByPrefixOptions>) {
        _patterns[slot] =
          MakeTermPredicate([prefix = bstring{options.term}](bytes_view term) {
            return term.starts_with(prefix);
          });
      } else if constexpr (std::is_same_v<Part, ByRangeOptions>) {
        _patterns[slot] =
          MakeTermPredicate([range = options.range](bytes_view term) {
            return RangeAcceptor{{&range}, {&range}}(term);
          });
      } else if constexpr (std::is_same_v<Part, AutomatonOptions> ||
                           std::is_same_v<Part, LevenshteinAutomatonOptions>) {
        if (options.source) {
          _sources.push_back(options.source);
          _patterns[slot] = options.source->Predicate();
        }
      }
    },
    part);
  if (_patterns[slot]) {
    _pattern_slots.push_back(slot);
  }
}

bool TokenPhraseMatcher::Accepts(uint32_t slot,
                                 const duckdb::string_t& term) const {
  if (_is_word[slot]) {
    return _words[slot] == term;
  }
  const auto view = AsBytesView(term);
  if (!_patterns.empty() && _patterns[slot]) {
    return Plain(view) && _patterns[slot]->Accepts(view);
  }
  const auto* accept = Find(view);
  return accept &&
         absl::c_linear_search(
           std::span{_slot_ids}.subspan(accept->begin, accept->size), slot);
}

uint64_t TokenPhraseMatcher::MaskOf(const duckdb::string_t& term) const {
  const auto view = AsBytesView(term);
  uint64_t mask = 0;
  if (const auto* accept = Find(view)) {
    mask = accept->mask;
  }
  if (_pattern_slots.empty() || !Plain(view)) {
    return mask;
  }
  for (const auto slot : _pattern_slots) {
    if (_patterns[slot]->Accepts(view)) {
      mask |= _slot_bits[slot];
    }
  }
  return mask;
}

void TokenPhraseMatcher::Index(std::span<const uint32_t> slots) {
  std::vector<std::pair<bytes_view, uint32_t>> pending;
  pending.reserve(_owned.size());
  for (size_t i = 0; i != _owned.size(); ++i) {
    pending.emplace_back(_owned[i], slots[i]);
  }
  absl::c_sort(pending);

  _accept.reserve(pending.size());
  for (size_t i = 0; i != pending.size();) {
    const auto term = pending[i].first;
    Accept accept{.begin = static_cast<uint32_t>(_slot_ids.size())};
    for (; i != pending.size() && pending[i].first == term; ++i) {
      const auto slot = pending[i].second;
      if (accept.size == 0 || _slot_ids.back() != slot) {
        _slot_ids.push_back(slot);
        ++accept.size;
      }
    }
    _accept.emplace(term, accept);
  }
}

void TokenPhraseMatcher::SlopLayout() {
  const auto n = static_cast<uint32_t>(Slots());
  _offsets.assign(n, 0);
  for (uint32_t i = 1; i != n; ++i) {
    _offsets[i] = _offsets[i - 1] + _offs_max[i];
  }

  std::vector<uint32_t> groups(n);
  std::iota(groups.begin(), groups.end(), 0);
  const auto find = [&](uint32_t x) {
    while (groups[x] != x) {
      groups[x] = groups[groups[x]];
      x = groups[x];
    }
    return x;
  };
  for (const auto& [term, accept] : _accept) {
    const auto root = find(_slot_ids[accept.begin]);
    for (uint32_t i = 1; i < accept.size; ++i) {
      groups[find(_slot_ids[accept.begin + i])] = root;
    }
  }
  for (uint32_t i = 0; i != n; ++i) {
    groups[i] = find(i);
  }
  detail::slop::BuildGroupPairs(groups, n, _pairs);
}

void TokenPhraseMatcher::Layout() {
  const auto n = _offs_max.size();
  _slot_bits.assign(n, 0);
  uint64_t bit = 0;
  for (size_t k = 0; k != n; ++k) {
    if (k != 0) {
      const auto prev = bit;
      bit = prev + _offs_max[k];
      if (bit >= kMaxBits) {
        _slot_bits.clear();
        _extras.clear();
        _wild = 0;
        return;
      }
      for (auto g = prev + 1; g < bit; ++g) {
        _wild |= uint64_t{1} << g;
      }
      if (_offs_min[k] < _offs_max[k]) {
        uint64_t range = 0;
        for (auto j = prev + _offs_min[k] - 1; j + 2 <= bit; ++j) {
          range |= uint64_t{1} << j;
        }
        _extras.push_back({.range = range,
                           .bit = uint64_t{1} << bit,
                           .index = static_cast<uint32_t>(bit)});
      }
    }
    _slot_bits[k] = uint64_t{1} << bit;
  }
  _length = static_cast<uint32_t>(bit + 1);
  _last = uint64_t{1} << bit;
  for (auto& [term, accept] : _accept) {
    for (uint32_t i = 0; i != accept.size; ++i) {
      accept.mask |= _slot_bits[_slot_ids[accept.begin + i]];
    }
  }
}

void TokenPhraseMatcher::PickAnchor(Lookup lookup) {
  constexpr auto kUnknown = std::numeric_limits<double>::max();
  double best = kUnknown;
  for (uint32_t k = 0; k != _words.size(); ++k) {
    if (!_is_word[k]) {
      continue;
    }
    const auto& word = _words[k];
    const auto meta = lookup(AsBytesView(word));
    double cost = kUnknown;
    if (meta.docs_count != 0) {
      cost = meta.freq != 0 ? static_cast<double>(meta.freq) /
                                static_cast<double>(meta.docs_count)
                            : static_cast<double>(meta.docs_count);
    }
    if (_anchor == kNoSlot || cost < best ||
        (cost == best && word.GetSize() > _words[_anchor].GetSize())) {
      _anchor = k;
      best = cost;
    }
  }
  if (_anchor == kNoSlot) {
    return;
  }
  const auto anchor = _offs_max.begin() + _anchor + 1;
  _left = std::accumulate(_offs_max.begin() + 1, anchor, uint64_t{0});
  _right = std::accumulate(anchor, _offs_max.end(), uint64_t{0});
}

TokenPhraseSink::TokenPhraseSink(const TokenPhraseMatcher& matcher,
                                 TokenTraits producer, bool count)
  : _matcher{&matcher}, _dense{!producer.explicit_pos}, _count{count} {}

void TokenPhraseSink::Begin() { Start(_matcher->_primary); }

void TokenPhraseSink::Start(PhraseMatch mode) {
  const auto& m = *_matcher;
  _mode = mode;
  _done = false;
  _restart = false;
  _last_pos = 0;
  _value_base = 0;
  _freq = 0;
  switch (mode) {
    case PhraseMatch::Anchor:
      _batch_terms = nullptr;
      _batch_pos = nullptr;
      _batch_base = 0;
      _end = 0;
      _carry_base = 0;
      _carry_terms.clear();
      _carry_pos.clear();
      _arena.Reset();
      _pending.clear();
      _last_anchor = 0;
      _steps = 0;
      break;
    case PhraseMatch::Automaton:
      _d = 0;
      _mask = 0;
      _at = 0;
      if (_count && !m._extras.empty()) {
        _c.assign(m._length, 0);
        _sums.assign(m._extras.size(), 0);
      }
      break;
    case PhraseMatch::Positions:
      _slots.resize(m.Slots());
      for (auto& slot : _slots) {
        slot.clear();
      }
      break;
  }
}

void TokenPhraseSink::Consume(TokenBatch& batch, DocRuns) {
  const auto count = batch.count;
  if (_done || count == 0) {
    return;
  }
  if (_dense) {
    std::iota(batch.pos, batch.pos + count, _last_pos + 1);
    _last_pos += count;
  } else {
    for (uint32_t i = 0; i != count; ++i) {
      batch.pos[i] += _value_base;
    }
    _last_pos = std::max(_last_pos, batch.pos[count - 1]);
  }
  switch (_mode) {
    case PhraseMatch::Anchor:
      AnchorBatch(batch);
      break;
    case PhraseMatch::Automaton:
      AutomatonBatch(batch);
      break;
    case PhraseMatch::Positions:
      PositionsBatch(batch);
      break;
  }
}

bool TokenPhraseSink::Restart() {
  if (_mode == PhraseMatch::Anchor && !_done) {
    _batch_terms = nullptr;
    _batch_pos = nullptr;
    _batch_base = _carry_base + _carry_terms.size();
    _end = _batch_base;
    for (const auto at : _pending) {
      if (Hit(at)) {
        break;
      }
    }
    _pending.clear();
    _done = true;
  }
  if (!_restart) {
    return false;
  }
  Start(_matcher->_fallback);
  return true;
}

bool TokenPhraseSink::End(PhraseVerdict& out) {
  out = {};
  switch (_mode) {
    case PhraseMatch::Anchor:
      SDB_ASSERT(_done && !_restart);
      break;
    case PhraseMatch::Automaton:
      if (!_done) {
        Flush();
      }
      break;
    case PhraseMatch::Positions:
      return EndPositions(out);
  }
  if (_freq == 0) {
    return false;
  }
  out.freq = _count ? Clamp(_freq) : 1;
  return true;
}

void TokenPhraseSink::AnchorBatch(const TokenBatch& batch) {
  const auto& m = *_matcher;
  _batch_terms = batch.terms;
  _batch_pos = batch.pos;
  _batch_base = _carry_base + _carry_terms.size();
  _end = _batch_base + batch.count;
  const uint64_t last = batch.pos[batch.count - 1];
  size_t keep = 0;
  for (const auto at : _pending) {
    if (PosAt(at) + m._right < last) {
      if (Hit(at)) {
        return;
      }
    } else {
      _pending[keep++] = at;
    }
  }
  _pending.resize(keep);
  const auto& word = m._words[m._anchor];
  for (uint32_t i = 0; i != batch.count; ++i) {
    if (batch.terms[i] != word) {
      continue;
    }
    const auto pos = batch.pos[i];
    if (pos == _last_anchor) {
      continue;
    }
    _last_anchor = pos;
    const auto at = _batch_base + i;
    if (pos + m._right < last) {
      if (Hit(at)) {
        return;
      }
    } else {
      _pending.push_back(at);
    }
  }
  Carry();
}

bool TokenPhraseSink::Hit(size_t at) {
  const auto anchor = _matcher->_anchor;
  if (const auto left = Left(anchor, at)) {
    _freq += left * Right(anchor, at);
  }
  _done |= !_count && _freq != 0;
  return _done;
}

uint64_t TokenPhraseSink::Right(uint32_t slot, size_t at) {
  const auto& m = *_matcher;
  const auto next = slot + 1;
  if (next == m.Slots()) {
    return 1;
  }
  const uint64_t pos = PosAt(at);
  const auto from = pos + m._offs_min[next];
  const auto to = pos + m._offs_max[next];
  uint64_t ways = 0;
  uint64_t accepted = 0;
  for (auto j = at + 1; j < _end; ++j) {
    const uint64_t p = PosAt(j);
    if (p > to) {
      break;
    }
    ++_steps;
    if (Over()) {
      return 0;
    }
    if (p < from || p == accepted || !m.Accepts(next, TermAt(j))) {
      continue;
    }
    accepted = p;
    ways += Right(next, j);
    if (_restart) {
      return 0;
    }
    if (!_count && ways != 0) {
      return 1;
    }
  }
  return ways;
}

uint64_t TokenPhraseSink::Left(uint32_t slot, size_t at) {
  const auto& m = *_matcher;
  if (slot == 0) {
    return 1;
  }
  const auto prev = slot - 1;
  const uint64_t pos = PosAt(at);
  uint64_t ways = 0;
  uint64_t accepted = std::numeric_limits<uint64_t>::max();
  for (auto j = at; j-- > _carry_base;) {
    const uint64_t p = PosAt(j);
    const auto distance = pos - p;
    if (distance > m._offs_max[slot]) {
      break;
    }
    ++_steps;
    if (Over()) {
      return 0;
    }
    if (distance < m._offs_min[slot] || p == accepted ||
        !m.Accepts(prev, TermAt(j))) {
      continue;
    }
    accepted = p;
    ways += Left(prev, j);
    if (_restart) {
      return 0;
    }
    if (!_count && ways != 0) {
      return 1;
    }
  }
  return ways;
}

bool TokenPhraseSink::Over() noexcept {
  if (_steps <= kStepsPerToken * _end + kStepSlack) {
    return false;
  }
  _restart = true;
  _done = true;
  return true;
}

void TokenPhraseSink::Carry() {
  const auto& m = *_matcher;
  const uint64_t last = PosAt(_end - 1);
  const auto span = m._left + m._right;
  const auto keep = last > span ? last - span : 0;
  auto from = _end;
  while (from > _carry_base && PosAt(from - 1) >= keep) {
    --from;
  }
  _next_terms.clear();
  _next_pos.clear();
  for (auto at = from; at != _end; ++at) {
    auto term = TermAt(at);
    if (at >= _batch_base && !term.IsInlined()) {
      const auto size = static_cast<uint32_t>(term.GetSize());
      auto* data = _arena.Allocate(size);
      std::memcpy(data, term.GetData(), size);
      term = {reinterpret_cast<const char*>(data), size};
    }
    _next_terms.push_back(term);
    _next_pos.push_back(PosAt(at));
  }
  std::swap(_carry_terms, _next_terms);
  std::swap(_carry_pos, _next_pos);
  _carry_base = from;
}

void TokenPhraseSink::AutomatonBatch(const TokenBatch& batch) {
  const auto& m = *_matcher;
  for (uint32_t i = 0; i != batch.count; ++i) {
    const auto pos = batch.pos[i];
    if (pos != _at) {
      Flush();
      if (_done) {
        return;
      }
      if (_at != 0) {
        for (auto gap = _at + 1; gap < pos && _d != 0; ++gap) {
          Step(0);
        }
      }
      _at = pos;
    }
    _mask |= m.MaskOf(batch.terms[i]);
  }
}

void TokenPhraseSink::Flush() {
  if (_at == 0 || (_d == 0 && _mask == 0)) {
    _mask = 0;
    return;
  }
  Step(_mask);
  _mask = 0;
}

void TokenPhraseSink::Step(uint64_t b) {
  const auto& m = *_matcher;
  auto next = ((_d << 1) | 1) & (b | m._wild);
  for (const auto& e : m._extras) {
    if (_d & e.range) {
      next |= e.bit & b;
    }
  }
  if (!_count || m._extras.empty()) {
    _d = next;
    if (_d & m._last) {
      ++_freq;
      _done = !_count;
    }
    return;
  }
  if ((_d | next) == 0) {
    return;
  }
  for (size_t x = 0; x != m._extras.size(); ++x) {
    uint64_t sum = 0;
    for (auto r = m._extras[x].range & _d; r != 0; r &= r - 1) {
      sum += _c[std::countr_zero(r)];
    }
    _sums[x] = sum;
  }
  for (auto j = m._length - 1; j != 0; --j) {
    _c[j] = (next >> j & 1) ? _c[j - 1] : 0;
  }
  _c[0] = next & 1;
  for (size_t x = 0; x != m._extras.size(); ++x) {
    const auto& e = m._extras[x];
    if (b & e.bit) {
      _c[e.index] += _sums[x];
    }
  }
  _d = next;
  _freq += _c[m._length - 1];
}

void TokenPhraseSink::PositionsBatch(const TokenBatch& batch) {
  const auto& m = *_matcher;
  for (uint32_t i = 0; i != batch.count; ++i) {
    const auto pos = batch.pos[i];
    m.ForEachSlot(batch.terms[i], [&](uint32_t slot) {
      auto& positions = _slots[slot];
      if (positions.empty() || positions.back() != pos) {
        positions.push_back(pos);
      }
    });
  }
}

bool TokenPhraseSink::EndPositions(PhraseVerdict& out) {
  const auto& m = *_matcher;
  const auto n = m.Slots();
  if (absl::c_any_of(_slots, [](const auto& slot) { return slot.empty(); })) {
    return false;
  }
  if (n == 1) {
    out.freq = _count ? Clamp(_slots.front().size()) : 1;
    return true;
  }

  if (m._slop != 0) {
    detail::slop::SpanCursors cursors{_slots, _slop};
    const auto res = detail::slop::Sweep<0>(cursors, m._offsets, m._slop,
                                            m._pairs, _slop, !_count, [] {});
    if (!res.any) {
      return false;
    }
    if (_count) {
      out.freq = Clamp(res.freq);
      out.scale =
        static_cast<score_t>(res.weight / static_cast<double>(res.freq));
    } else {
      out.freq = 1;
    }
    return true;
  }

  _valid.assign(_slots.back().begin(), _slots.back().end());
  _ways.assign(_valid.size(), 1);
  for (size_t i = n - 1; i != 0; --i) {
    const auto& prev = _slots[i - 1];
    _next.clear();
    _next_ways.clear();
    size_t lo = 0;
    size_t hi = 0;
    uint64_t window = 0;
    for (const auto p : prev) {
      const uint64_t min = uint64_t{p} + m._offs_min[i];
      const uint64_t max = uint64_t{p} + m._offs_max[i];
      for (; hi != _valid.size() && _valid[hi] <= max; ++hi) {
        window += _ways[hi];
      }
      for (; lo != hi && _valid[lo] < min; ++lo) {
        window -= _ways[lo];
      }
      if (window != 0) {
        _next.push_back(p);
        _next_ways.push_back(window);
      }
    }
    std::swap(_valid, _next);
    std::swap(_ways, _next_ways);
    if (_valid.empty()) {
      return false;
    }
  }
  out.freq = _count ? Clamp(absl::c_accumulate(_ways, uint64_t{0})) : 1;
  return true;
}

bool CheckValues(TokenPhraseSink& sink, ValueAnalyzer& analyzer,
                 analysis::Tokenizer& tokenizer,
                 std::span<const duckdb::string_t> values, PhraseVerdict& out) {
  const auto analyze = [&] {
    for (const auto& value : values) {
      if (sink.Done()) {
        return;
      }
      analyzer.Analyze(tokenizer, value, sink);
    }
  };
  sink.Begin();
  analyze();
  if (sink.Restart()) {
    analyze();
  }
  return sink.End(out);
}

void TextRows::Bind(duckdb::Vector& values, duckdb::idx_t count) {
  const auto& type = values.GetType();
  _type = type.id();
  values.ToUnifiedFormat(count, _format);
  if (_type == duckdb::LogicalTypeId::LIST) {
    duckdb::ListVector::GetEntry(values).ToUnifiedFormat(
      duckdb::ListVector::GetListSize(values), _children);
  } else if (_type == duckdb::LogicalTypeId::ARRAY) {
    _array_size = duckdb::ArrayType::GetSize(type);
    duckdb::ArrayVector::GetEntry(values).ToUnifiedFormat(
      duckdb::ArrayVector::GetTotalSize(values), _children);
  }
}

std::span<const duckdb::string_t> TextRows::Values(duckdb::idx_t row) {
  const auto idx = _format.sel->get_index(row);
  if (!_format.validity.RowIsValid(idx)) {
    return {};
  }
  uint64_t begin = 0;
  uint64_t end = 0;
  if (_type == duckdb::LogicalTypeId::LIST) {
    const auto entry =
      duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(_format)[idx];
    begin = entry.offset;
    end = entry.offset + entry.length;
  } else if (_type == duckdb::LogicalTypeId::ARRAY) {
    begin = idx * _array_size;
    end = begin + _array_size;
  } else {
    return {
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(_format) + idx, 1};
  }
  _values.clear();
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(_children);
  for (auto i = begin; i != end; ++i) {
    const auto child = _children.sel->get_index(i);
    if (_children.validity.RowIsValid(child)) {
      _values.push_back(data[child]);
    }
  }
  return _values;
}

PhraseCheck::PhraseCheck(const TokenPhraseMatcher& matcher,
                         const PhraseTokens& tokens, bool count)
  : _tokenizer{tokens.tokenizer()},
    _expression{tokens.text.expression ? tokens.text.expression() : nullptr},
    _sink{matcher, _tokenizer->Traits(), count} {}

void PhraseCheck::Bind(duckdb::DataChunk& columns) {
  auto& values = _expression ? _expression->Evaluate(columns) : columns.data[0];
  _rows.Bind(values, columns.size());
}

bool PhraseCheck::Check(duckdb::idx_t row, PhraseVerdict& out) {
  return CheckValues(_sink, _analyzer, *_tokenizer, _rows.Values(row), out);
}

TokenPhraseReader::Input::Input(const ColumnReader& column, ReadContext& ctx)
  : column{&column},
    state{column.InitScan(ctx)},
    out{std::make_unique<ColumnReader::VectorScratch>(column.Type())} {}

TokenPhraseReader::TokenPhraseReader(
  const ColReader& col_reader, std::span<const ColumnReader* const> columns,
  const TokenPhraseMatcher& matcher, const PhraseTokens& tokens, bool count)
  : _ctx{col_reader},
    _sel{STANDARD_VECTOR_SIZE},
    _check{matcher, tokens, count} {
  SDB_ASSERT(!columns.empty());
  SDB_ASSERT(tokens.text.expression || columns.size() == 1);
  _inputs.reserve(columns.size());
  std::vector<duckdb::LogicalType> types;
  types.reserve(columns.size());
  for (const auto* column : columns) {
    _inputs.emplace_back(*column, _ctx);
    _row_count = std::min(_row_count, column->RowCount());
    types.push_back(column->Type());
  }
  _chunk.InitializeEmpty(types);
}

bool TokenPhraseReader::Match(doc_id_t doc, PhraseVerdict& out) {
  Match({&doc, 1}, {&out, 1});
  return out.freq != 0;
}

void TokenPhraseReader::Match(std::span<const doc_id_t> docs,
                              std::span<PhraseVerdict> verdicts) {
  SDB_ASSERT(docs.size() == verdicts.size());
  SDB_ASSERT(docs.size() <= STANDARD_VECTOR_SIZE);
  auto n = docs.size();
  while (n != 0 && docs[n - 1] - doc_limits::min() >= _row_count) {
    --n;
  }
  std::ranges::fill(verdicts.subspan(n), PhraseVerdict{});
  if (n == 0) {
    return;
  }
  const uint64_t anchor = docs.front() - doc_limits::min();
  for (size_t i = 0; i != n; ++i) {
    _sel.set_index(i, docs[i] - docs.front());
  }
  for (size_t i = 0; i != _inputs.size(); ++i) {
    auto& input = _inputs[i];
    auto& out = input.out->Reset();
    input.column->GatherScatter(input.state, anchor, _sel, n, out, 0);
    _chunk.data[i].Reference(out);
  }
  _chunk.SetChildCardinality(n);
  _check.Bind(_chunk);
  for (size_t i = 0; i != n; ++i) {
    _check.Check(i, verdicts[i]);
  }
}

}  // namespace irs
