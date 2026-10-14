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
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <limits>
#include <numeric>
#include <utility>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/json_leaf.hpp"

namespace irs {

bool IsAnchored(const ByPhraseOptions& phrase) noexcept {
  return phrase.slop() == 0 &&
         (phrase.empty() ||
          std::none_of(std::next(phrase.begin()), phrase.end(),
                       [](const auto& info) { return info.offs_min == 0; }));
}

std::vector<PhraseWordStat> PhraseWordStats(
  const ByPhraseOptions& phrase, std::span<const TermReader* const> readers) {
  std::vector<PhraseWordStat> words;
  if (!IsAnchored(phrase)) {
    return words;
  }
  words.resize(phrase.size());
  for (const auto* reader : readers) {
    auto* stat = words.data();
    for (const auto& info : phrase) {
      if (const auto* word = std::get_if<ByTermOptions>(&info.part)) {
        const auto meta = reader->Lookup(word->term);
        stat->docs += meta.docs_count;
        stat->freq += meta.freq;
      }
      ++stat;
    }
  }
  return words;
}

std::vector<PhraseWordStat> PhraseWordStats(const ByPhraseOptions& phrase,
                                            const IndexReader& index,
                                            field_id field) {
  constexpr size_t kSampled = 8;
  std::vector<const TermReader*> readers;
  readers.reserve(index.size());
  for (const auto& segment : index) {
    if (const auto* reader = segment.field(field)) {
      readers.emplace_back(reader);
    }
  }
  if (readers.size() > kSampled) {
    std::ranges::nth_element(
      readers, readers.begin() + kSampled, std::ranges::greater{},
      [](const TermReader* reader) { return reader->docs_count(); });
    readers.resize(kSampled);
  }
  return PhraseWordStats(phrase, readers);
}

bool IsStandalone(const ByPhraseOptions& phrase) noexcept {
  return absl::c_all_of(phrase, [](const auto& info) {
    return std::visit(
      []<typename Part>(const Part& part) {
        if constexpr (std::is_same_v<Part, ByTermOptions> ||
                      std::is_same_v<Part, TermSetOptions> ||
                      std::is_same_v<Part, ByPrefixOptions> ||
                      std::is_same_v<Part, ByRangeOptions>) {
          return true;
        } else if constexpr (std::is_same_v<Part, AutomatonOptions>) {
          return static_cast<bool>(part.source);
        } else if constexpr (std::is_same_v<Part,
                                            LevenshteinAutomatonOptions>) {
          return part.max_terms == 0 && part.source;
        } else {
          return false;
        }
      },
      info.part);
  });
}

bool HasPatternParts(const ByPhraseOptions& phrase) noexcept {
  return absl::c_any_of(phrase, [](const auto& info) {
    return ByPhraseOptions::KindOf(info.part) == SlotKind::Expansion;
  });
}

CompiledPhrase::CompiledPhrase(const ByPhraseOptions& phrase,
                               std::span<const std::vector<bstring>> expanded,
                               std::span<const PhraseWordStat> words)
  : slop{.max = phrase.slop()} {
  Init(phrase, expanded, false, words);
}

CompiledPhrase::CompiledPhrase(const ByPhraseOptions& phrase,
                               bytes_view word_separator,
                               std::span<const PhraseWordStat> words)
  : slop{.max = phrase.slop()}, separator{word_separator} {
  SDB_ASSERT(IsStandalone(phrase));
  Init(phrase, {}, true, words);
}

void CompiledPhrase::Init(const ByPhraseOptions& phrase,
                          std::span<const std::vector<bstring>> expanded,
                          bool predicates,
                          std::span<const PhraseWordStat> words) {
  const auto n = static_cast<uint32_t>(phrase.size());
  slots.resize(n);
  std::vector<uint32_t> term_slots;
  std::vector<std::optional<size_t>> word_of(n);
  uint32_t slot = 0;
  const auto add = [&](bytes_view term) {
    terms.emplace_back(term);
    term_slots.emplace_back(slot);
  };
  for (const auto& info : phrase) {
    slots[slot].offs_min = info.offs_min;
    slots[slot].offs_max = info.offs_max;
    switch (ByPhraseOptions::KindOf(info.part)) {
      case SlotKind::Term:
        word_of[slot] = terms.size();
        add(std::get<ByTermOptions>(info.part).term);
        break;
      case SlotKind::Set:
        for (const auto& term : std::get<TermSetOptions>(info.part).terms) {
          add(term);
        }
        break;
      case SlotKind::Expansion:
        if (predicates) {
          AddPattern(slot, info.part);
        } else if (slot < expanded.size()) {
          for (const auto& term : expanded[slot]) {
            add(term);
          }
        }
        break;
    }
    ++slot;
  }
  for (uint32_t i = 0; i != n; ++i) {
    if (word_of[i]) {
      slots[i].word =
        MakeTermView(ViewCast<char>(bytes_view{terms[*word_of[i]]}));
    }
  }
  Index(term_slots);

  if (!IsAnchored(phrase)) {
    if (slop.max != 0 && n > 1) {
      LayoutSlop();
    }
    return;
  }
  LayoutAutomaton();
  PickAnchor(words);
}

void CompiledPhrase::AddPattern(uint32_t slot,
                                const ByPhraseOptions::PhrasePart& part) {
  auto& target = slots[slot];
  std::visit(
    [&]<typename Part>(const Part& options) {
      if constexpr (std::is_same_v<Part, ByPrefixOptions>) {
        target.pattern =
          MakeTermPredicate([prefix = bstring{options.term}](bytes_view term) {
            return term.starts_with(prefix);
          });
      } else if constexpr (std::is_same_v<Part, ByRangeOptions>) {
        target.pattern =
          MakeTermPredicate([range = options.range](bytes_view term) {
            return RangeAcceptor{{&range}, {&range}}(term);
          });
      } else if constexpr (std::is_same_v<Part, AutomatonOptions> ||
                           std::is_same_v<Part, LevenshteinAutomatonOptions>) {
        if (options.source) {
          target.source = options.source;
          target.pattern = options.source->Predicate();
        }
      }
    },
    part);
  if (target.pattern) {
    pattern_slots.emplace_back(slot);
  }
}

bool CompiledPhrase::Accepts(uint32_t slot,
                             const duckdb::string_t& term) const {
  const auto& target = slots[slot];
  if (target.word) {
    return *target.word == term;
  }
  const auto view = AsBytesView(term);
  if (target.pattern) {
    return Plain(view) && target.pattern->Accepts(view);
  }
  const auto* found = Find(view);
  return found &&
         absl::c_linear_search(
           std::span{slot_ids}.subspan(found->begin, found->size), slot);
}

uint64_t CompiledPhrase::MaskOf(const duckdb::string_t& term) const {
  const auto view = AsBytesView(term);
  uint64_t mask = 0;
  if (const auto* found = Find(view)) {
    mask = found->mask;
  }
  ForEachPattern(view, [&](uint32_t slot) { mask |= slots[slot].bit; });
  return mask;
}

void CompiledPhrase::Index(std::span<const uint32_t> term_slots) {
  std::vector<std::pair<bytes_view, uint32_t>> pending;
  pending.reserve(terms.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    pending.emplace_back(terms[i], term_slots[i]);
  }
  absl::c_sort(pending);

  accept.reserve(pending.size());
  for (size_t i = 0; i != pending.size();) {
    const auto term = pending[i].first;
    Accept entry{.begin = static_cast<uint32_t>(slot_ids.size())};
    for (; i != pending.size() && pending[i].first == term; ++i) {
      const auto slot = pending[i].second;
      if (entry.size == 0 || slot_ids.back() != slot) {
        slot_ids.emplace_back(slot);
        ++entry.size;
      }
    }
    accept.emplace(term, entry);
  }
}

void CompiledPhrase::LayoutSlop() {
  const auto n = static_cast<uint32_t>(slots.size());
  slop.offsets.assign(n, 0);
  for (uint32_t i = 1; i != n; ++i) {
    slop.offsets[i] = slop.offsets[i - 1] + slots[i].offs_max;
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
  for (const auto& [term, entry] : accept) {
    const auto root = find(slot_ids[entry.begin]);
    for (uint32_t i = 1; i < entry.size; ++i) {
      groups[find(slot_ids[entry.begin + i])] = root;
    }
  }
  for (const auto p : pattern_slots) {
    groups[find(p)] = find(pattern_slots.front());
    for (const auto& [term, entry] : accept) {
      if (Plain(term) && slots[p].pattern->Accepts(term)) {
        groups[find(slot_ids[entry.begin])] = find(p);
      }
    }
  }
  for (uint32_t i = 0; i != n; ++i) {
    groups[i] = find(i);
  }
  detail::slop::BuildGroupPairs(groups, n, slop.pairs);
}

void CompiledPhrase::LayoutAutomaton() {
  Automaton layout;
  uint64_t bit = 0;
  for (size_t k = 0; k != slots.size(); ++k) {
    auto& slot = slots[k];
    if (k != 0) {
      const auto prev = bit;
      bit = prev + slot.offs_max;
      if (bit >= kMaxBits) {
        return;
      }
      for (auto g = prev + 1; g < bit; ++g) {
        layout.wild |= uint64_t{1} << g;
      }
      if (slot.offs_min < slot.offs_max) {
        uint64_t range = 0;
        for (auto j = prev + slot.offs_min - 1; j + 2 <= bit; ++j) {
          range |= uint64_t{1} << j;
        }
        layout.extras.emplace_back(range, uint64_t{1} << bit);
      }
    }
    slot.bit = uint64_t{1} << bit;
  }
  layout.last = uint64_t{1} << bit;
  for (auto& [term, entry] : accept) {
    for (uint32_t i = 0; i != entry.size; ++i) {
      entry.mask |= slots[slot_ids[entry.begin + i]].bit;
    }
  }
  automaton = std::move(layout);
}

void CompiledPhrase::PickAnchor(std::span<const PhraseWordStat> words) {
  constexpr auto kUnknown = std::numeric_limits<double>::max();
  double best = kUnknown;
  for (uint32_t k = 0; k != slots.size(); ++k) {
    if (!slots[k].word) {
      continue;
    }
    const auto& word = *slots[k].word;
    const auto [docs, freq] = k < words.size() ? words[k] : PhraseWordStat{};
    double cost = kUnknown;
    if (docs != 0) {
      cost = freq != 0 ? static_cast<double>(freq) / static_cast<double>(docs)
                       : static_cast<double>(docs);
    }
    if (!anchor || cost < best ||
        (cost == best &&
         word.GetSize() > slots[anchor->slot].word->GetSize())) {
      anchor = Anchor{.slot = k};
      best = cost;
    }
  }
  if (!anchor) {
    return;
  }
  for (uint32_t k = 1; k != slots.size(); ++k) {
    (k <= anchor->slot ? anchor->left : anchor->right) += slots[k].offs_max;
  }
}

void PhraseCheck::AnchorMatch::Reset(uint32_t anchors) {
  _batch_terms = nullptr;
  _batch_pos = nullptr;
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

void PhraseCheck::AnchorMatch::Carry(uint64_t span) {
  const uint64_t tail = PosAt(_end - 1);
  const auto keep = tail > span ? tail - span : 0;
  auto from = _end;
  while (from > _carry_base && PosAt(from - 1) >= keep) {
    --from;
  }
  _next.clear();
  for (auto at = from; at != _end; ++at) {
    auto term = TermAt(at);
    if (at >= _batch_base && !term.IsInlined()) {
      const auto size = static_cast<uint32_t>(term.GetSize());
      auto* data = _arena.Allocate(size);
      std::memcpy(data, term.GetData(), size);
      term = {reinterpret_cast<const char*>(data), size};
    }
    _next.emplace_back(term, PosAt(at));
  }
  std::swap(_carry, _next);
  _carry_base = from;
}

void PhraseCheck::AnchorMatch::Consumed(const TokenBatch& batch,
                                        const State& state) {
  if (!state.done && batch.count != 0) {
    const auto& layout = *state.phrase->anchor;
    Carry(layout.left + layout.right);
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
    _state{.phrase = &phrase, .count = count},
    _dense{!_tokenizer->Traits().explicit_pos} {}

RowValues::~RowValues() = default;

void RowValues::Bind(duckdb::Vector& values, duckdb::idx_t count) {
  const auto& type = values.GetType();
  _type = type.id();
  if (!_json && type.IsJSONType()) {
    _json = std::make_unique<JsonLeafReader>();
  }
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

std::span<const duckdb::string_t> RowValues::At(duckdb::idx_t row) {
  const auto values = Values(row);
  if (!_json || values.empty()) {
    return values;
  }
  SDB_ASSERT(values.size() == 1);
  const auto leaf =
    _json->Read({values.front().GetData(), values.front().GetSize()});
  if (leaf.kind != JsonLeafKind::String) {
    return {};
  }
  _text = {leaf.string.data(), static_cast<uint32_t>(leaf.string.size())};
  return {&_text, 1};
}

std::span<const duckdb::string_t> RowValues::Values(duckdb::idx_t row) {
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
  analysis::ForEachValidRow(_children, begin,
                            static_cast<uint32_t>(end - begin),
                            [&](uint32_t, uint32_t child) {
                              _values.emplace_back(data[child]);
                              return true;
                            });
  return _values;
}

bool PhraseCheck::Check(duckdb::idx_t row, PhraseVerdict& out,
                        uint32_t anchors) {
  const auto values = _rows.At(row);
  Start(_state.phrase->anchor.has_value(), anchors);
  Analyze(values);
  if (auto* anchor = std::get_if<AnchorMatch>(&_match)) {
    anchor->Drain(_state);
    if (anchor->Overran()) {
      Start(false);
      Analyze(values);
    }
  }
  out = {};
  return std::visit([&](auto& match) { return match.Finish(_state, out); },
                    _match);
}

void PhraseCheck::Analyze(std::span<const duckdb::string_t> values) {
  for (const auto& value : values) {
    if (_state.done) {
      return;
    }
    _analyzer.Scan(*_tokenizer, value, *this);
  }
}

void PhraseCheck::Start(bool anchored, uint32_t anchors) {
  _state.done = false;
  _state.freq = 0;
  _last_pos = 0;
  _value_base = 0;
  const auto& phrase = *_state.phrase;
  if (anchored) {
    Use<AnchorMatch>().Reset(anchors);
  } else if (phrase.automaton && !_state.count) {
    Use<AutomatonMatch>().Reset();
  } else {
    Use<PositionsMatch>().Reset(phrase.slots.size());
  }
}

void PhraseCheck::Consume(TokenBatch& batch, DocRuns) {
  Take(batch);
  _fed = 0;
  if (auto* anchor = std::get_if<AnchorMatch>(&_match)) {
    anchor->Consumed(batch, _state);
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
    std::iota(batch.pos + from, batch.pos + count, _last_pos + 1);
    _last_pos += count - from;
  } else {
    for (auto i = from; i != count; ++i) {
      batch.pos[i] += _value_base;
    }
    _last_pos = std::max(_last_pos, batch.pos[count - 1]);
  }
  std::visit([&](auto& match) { match.Feed(batch, from, _state); }, _match);
}

void PhraseCheck::AnchorMatch::Drain(State& state) {
  if (state.done) {
    return;
  }
  _batch_terms = nullptr;
  _batch_pos = nullptr;
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

bool PhraseCheck::State::Counted(PhraseVerdict& out) const noexcept {
  if (freq == 0) {
    return false;
  }
  out.freq = count ? std::saturate_cast<uint32_t>(freq) : 1;
  return true;
}

void PhraseCheck::AnchorMatch::Feed(const TokenBatch& batch, uint32_t from,
                                    State& state) {
  const auto& layout = *state.phrase->anchor;
  _batch_terms = batch.terms;
  _batch_pos = batch.pos;
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

bool PhraseCheck::AnchorMatch::Finish(State& state, PhraseVerdict& out) {
  SDB_ASSERT(state.done && !_overran);
  return state.Counted(out);
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
  const auto& gap = phrase.slots[Right ? other : slot];
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
        !phrase.Accepts(other, TermAt(j))) {
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
    _mask |= state.phrase->MaskOf(batch.terms[i]);
  }
}

bool PhraseCheck::AutomatonMatch::Finish(State& state, PhraseVerdict& out) {
  if (!state.done) {
    Flush(state);
  }
  return state.Counted(out);
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
    state.phrase->ForEachSlot(batch.terms[i], [&](uint32_t slot) {
      auto& slot_positions = _slots[slot];
      if (slot_positions.empty() || slot_positions.back() != pos) {
        slot_positions.emplace_back(pos);
      }
    });
  }
}

bool PhraseCheck::PositionsMatch::Finish(State& state, PhraseVerdict& out) {
  const auto& phrase = *state.phrase;
  const auto n = phrase.slots.size();
  if (absl::c_any_of(_slots, [](const auto& slot) { return slot.empty(); })) {
    return false;
  }
  if (n > 1 && phrase.slop.max != 0) {
    detail::slop::SpanCursors cursors{_slots, _scratch};
    const auto res =
      detail::slop::Sweep<0>(cursors, phrase.slop.offsets, phrase.slop.max,
                             phrase.slop.pairs, _scratch, !state.count, [] {});
    if (!res.any) {
      return false;
    }
    if (state.count) {
      out.freq = std::saturate_cast<uint32_t>(res.freq);
      out.scale =
        static_cast<score_t>(res.weight / static_cast<double>(res.freq));
    } else {
      out.freq = 1;
    }
    return true;
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
        _next.emplace_back(p, window);
      }
    }
    std::swap(_valid, _next);
    if (_valid.empty()) {
      return false;
    }
  }
  for (const auto& way : _valid) {
    state.freq += way.ways;
  }
  return state.Counted(out);
}

}  // namespace irs
