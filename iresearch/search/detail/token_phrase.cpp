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
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <limits>
#include <numeric>
#include <utility>

#include "iresearch/formats/term_reader.hpp"

namespace irs {
namespace {

bool Equals(const duckdb::string_t& lhs, const duckdb::string_t& rhs) noexcept {
  return duckdb::string_t::StringComparisonOperators::Equals(lhs, rhs);
}

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
      _steps.assign(_offs_max.begin() + 1, _offs_max.end());
    }
    return;
  }
  Layout();
  PickAnchor(lookup);
  _fallback = _length != 0 ? Mode::Automaton : Mode::Positions;
  _primary = _anchor != kNoSlot ? Mode::Anchor : _fallback;
  if (!match) {
    return;
  }
  switch (*match) {
    case Mode::Anchor:
      break;
    case Mode::Automaton:
      if (_length != 0) {
        _primary = Mode::Automaton;
      }
      break;
    case Mode::Positions:
      _primary = Mode::Positions;
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
            return RangeMinAcceptor{&range}(term) &&
                   RangeMaxAcceptor{&range}(term);
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

bool TokenPhraseMatcher::Pattern(uint32_t slot,
                                 const duckdb::string_t& term) const {
  const auto view = View(term);
  if (!_separator.empty() && view.find(_separator) != bytes_view::npos) {
    return false;
  }
  return _patterns[slot]->Accepts(view);
}

bool TokenPhraseMatcher::Accepts(uint32_t slot,
                                 const duckdb::string_t& term) const {
  if (_is_word[slot]) {
    return Equals(_words[slot], term);
  }
  if (!_patterns.empty() && _patterns[slot]) {
    return Pattern(slot, term);
  }
  const auto* accept = Find(term);
  if (!accept) {
    return false;
  }
  const auto* ids = _slot_ids.data() + accept->begin;
  return std::find(ids, ids + accept->size, slot) != ids + accept->size;
}

uint64_t TokenPhraseMatcher::MaskOf(const duckdb::string_t& term) const {
  uint64_t mask = 0;
  if (const auto* accept = Find(term)) {
    mask = accept->mask;
  }
  for (const auto slot : _pattern_slots) {
    if (Pattern(slot, term)) {
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

  const auto n = static_cast<uint32_t>(_offs_min.size());
  _groups.resize(n);
  std::iota(_groups.begin(), _groups.end(), 0);
  const auto find = [&](uint32_t x) {
    while (_groups[x] != x) {
      _groups[x] = _groups[_groups[x]];
      x = _groups[x];
    }
    return x;
  };

  _accept.reserve(pending.size());
  for (size_t i = 0; i != pending.size();) {
    const auto term = pending[i].first;
    Accept accept{.begin = static_cast<uint32_t>(_slot_ids.size())};
    for (; i != pending.size() && pending[i].first == term; ++i) {
      const auto slot = pending[i].second;
      if (accept.size != 0 && _slot_ids.back() == slot) {
        continue;
      }
      if (accept.size != 0) {
        const auto a = find(_slot_ids[accept.begin]);
        const auto b = find(slot);
        if (a != b) {
          _groups[b] = a;
        }
      }
      _slot_ids.push_back(slot);
      ++accept.size;
    }
    _accept.emplace(term, accept);
  }

  for (uint32_t i = 0; i != n; ++i) {
    _groups[i] = find(i);
  }
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
    const auto meta = lookup(View(word));
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
  for (uint32_t k = 1; k <= _anchor; ++k) {
    _left += _offs_max[k];
  }
  for (auto k = _anchor + 1; k < _offs_max.size(); ++k) {
    _right += _offs_max[k];
  }
}

TokenPhraseSink::TokenPhraseSink(const TokenPhraseMatcher& matcher,
                                 TokenTraits producer)
  : _matcher{&matcher}, _dense{!producer.explicit_pos} {}

void TokenPhraseSink::Begin(bool count) {
  _count = count;
  Start(_matcher->_primary);
}

void TokenPhraseSink::Start(Mode mode) {
  const auto& m = *_matcher;
  _mode = mode;
  _done = false;
  _restart = false;
  _last_pos = 0;
  _value_base = 0;
  _freq = 0;
  switch (mode) {
    case Mode::Anchor:
      _batch_terms = nullptr;
      _batch_pos = nullptr;
      _batch_base = 0;
      _end = 0;
      _carry_base = 0;
      _carry_terms.clear();
      _carry_pos.clear();
      _interned.clear();
      _pending.clear();
      _last_anchor = 0;
      _steps = 0;
      break;
    case Mode::Automaton:
      _d = 0;
      _mask = 0;
      _at = 0;
      if (_count && !m._extras.empty()) {
        _c.assign(m._length, 0);
        _sums.assign(m._extras.size(), 0);
      }
      break;
    case Mode::Positions:
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
    case Mode::Anchor:
      AnchorBatch(batch);
      break;
    case Mode::Automaton:
      AutomatonBatch(batch);
      break;
    case Mode::Positions:
      PositionsBatch(batch);
      break;
  }
}

bool TokenPhraseSink::Restart() {
  if (_mode == Mode::Anchor && !_done) {
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
    case Mode::Anchor:
      SDB_ASSERT(_done && !_restart);
      break;
    case Mode::Automaton:
      if (!_done) {
        Flush();
      }
      break;
    case Mode::Positions:
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
    if (!Equals(batch.terms[i], word)) {
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
  const auto left = Left(anchor, at);
  if (_restart) {
    _done = true;
    return true;
  }
  if (left == 0) {
    return false;
  }
  const auto right = Right(anchor, at);
  if (_restart) {
    _done = true;
    return true;
  }
  if (right == 0) {
    return false;
  }
  _freq += left * right;
  if (!_count) {
    _done = true;
    return true;
  }
  return false;
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
      const auto& owned =
        _interned.emplace_back(term.GetData(), term.GetSize());
      term =
        duckdb::string_t{owned.data(), static_cast<uint32_t>(owned.size())};
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
  for (const auto& slot : _slots) {
    if (slot.empty()) {
      return false;
    }
  }
  if (n == 1) {
    out.freq = _count ? Clamp(_slots.front().size()) : 1;
    return true;
  }

  if (m._slop != 0) {
    const auto res =
      detail::slop::Run(_slots, m._slop, m._steps, _slop, !_count, m._groups);
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
                 std::span<const duckdb::string_t> values, bool count,
                 PhraseVerdict& out) {
  out = {};
  if (values.empty()) {
    return false;
  }
  const auto analyze = [&] {
    return absl::c_all_of(values, [&](const duckdb::string_t& value) {
      return analyzer.Analyze(tokenizer, value, sink);
    });
  };
  sink.Begin(count);
  auto analyzed = analyze();
  if (analyzed && sink.Restart()) {
    analyzed = analyze();
  }
  return analyzed && sink.End(out);
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

bool TextRows::Values(duckdb::idx_t row,
                      std::vector<duckdb::string_t>& out) const {
  out.clear();
  const auto idx = _format.sel->get_index(row);
  if (!_format.validity.RowIsValid(idx)) {
    return false;
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
    out.push_back(
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(_format)[idx]);
    return true;
  }
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(_children);
  for (auto i = begin; i != end; ++i) {
    const auto child = _children.sel->get_index(i);
    if (_children.validity.RowIsValid(child)) {
      out.push_back(data[child]);
    }
  }
  return !out.empty();
}

TokenPhraseReader::TokenPhraseReader(
  const ColReader& col_reader, const ColumnReader& column,
  std::shared_ptr<analysis::Tokenizer> tokenizer,
  const TokenPhraseMatcher& matcher)
  : _ctx{col_reader},
    _column{&column},
    _state{column.InitScan(_ctx)},
    _out{column.Type()},
    _sel{STANDARD_VECTOR_SIZE},
    _tokenizer{std::move(tokenizer)},
    _sink{matcher, _tokenizer->Traits()} {
  SDB_ASSERT(_tokenizer);
}

bool TokenPhraseReader::Match(doc_id_t doc, bool count, PhraseVerdict& out) {
  uint8_t matched = 0;
  Match({&doc, 1}, count, {&out, 1}, {&matched, 1});
  return matched != 0;
}

void TokenPhraseReader::Match(std::span<const doc_id_t> docs, bool count,
                              std::span<PhraseVerdict> verdicts,
                              std::span<uint8_t> matched) {
  SDB_ASSERT(docs.size() == verdicts.size());
  SDB_ASSERT(docs.size() == matched.size());
  SDB_ASSERT(docs.size() <= STANDARD_VECTOR_SIZE);
  const auto rows = _column->RowCount();
  auto n = docs.size();
  while (n != 0 && docs[n - 1] - doc_limits::min() >= rows) {
    --n;
  }
  std::fill(verdicts.begin() + n, verdicts.end(), PhraseVerdict{});
  std::fill(matched.begin() + n, matched.end(), 0);
  if (n != 0) {
    Gather(docs.first(n), count, verdicts.first(n), matched.first(n));
  }
}

void TokenPhraseReader::Gather(std::span<const doc_id_t> docs, bool count,
                               std::span<PhraseVerdict> verdicts,
                               std::span<uint8_t> matched) {
  const uint64_t anchor = docs.front() - doc_limits::min();
  for (size_t i = 0; i != docs.size(); ++i) {
    _sel.set_index(i, docs[i] - doc_limits::min() - anchor);
  }
  auto& values = _out.Reset();
  _column->GatherScatter(_state, anchor, _sel, docs.size(), values, 0);
  _rows.Bind(values, docs.size());
  for (size_t i = 0; i != docs.size(); ++i) {
    verdicts[i] = {};
    matched[i] =
      _rows.Values(i, _values) &&
      CheckValues(_sink, _analyzer, *_tokenizer, _values, count, verdicts[i]);
  }
}

}  // namespace irs
