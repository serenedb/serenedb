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

CompiledPhrase::CompiledPhrase(const ByPhraseOptions& phrase,
                               std::span<const std::vector<bstring>> expanded,
                               const TermReader& reader,
                               std::optional<PhraseMatch> match)
  : slop{.max = phrase.slop()} {
  const TermReader* readers[] = {&reader};
  Init(phrase, expanded, false, readers, match);
}

CompiledPhrase::CompiledPhrase(const ByPhraseOptions& phrase,
                               bytes_view word_separator,
                               const IndexReader& index, field_id field,
                               std::optional<PhraseMatch> match)
  : slop{.max = phrase.slop()}, separator{word_separator} {
  SDB_ASSERT(Standalone(phrase));
  std::vector<const TermReader*> readers;
  readers.reserve(index.size());
  for (const auto& segment : index) {
    if (const auto* reader = segment.field(field)) {
      readers.push_back(reader);
    }
  }
  Init(phrase, {}, true, readers, match);
}

bool CompiledPhrase::Standalone(const ByPhraseOptions& phrase) noexcept {
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

void CompiledPhrase::Init(const ByPhraseOptions& phrase,
                          std::span<const std::vector<bstring>> expanded,
                          bool predicates,
                          std::span<const TermReader* const> readers,
                          std::optional<PhraseMatch> match) {
  const auto n = static_cast<uint32_t>(phrase.size());
  offs_min.reserve(n);
  offs_max.reserve(n);
  is_word.assign(n, 0);
  if (predicates) {
    patterns.resize(n);
  }
  std::vector<uint32_t> slots;
  std::vector<size_t> word_of(n);
  bool stacked = false;
  uint32_t slot = 0;
  const auto add = [&](bytes_view term) {
    terms.emplace_back(term);
    slots.push_back(slot);
  };
  for (const auto& info : phrase) {
    offs_min.push_back(info.offs_min);
    offs_max.push_back(info.offs_max);
    stacked |= slot != 0 && info.offs_min == 0;
    switch (ByPhraseOptions::KindOf(info.part)) {
      case SlotKind::Term:
        is_word[slot] = 1;
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
  words.resize(n);
  for (uint32_t i = 0; i != n; ++i) {
    if (is_word[i]) {
      words[i] = MakeTermView(ViewCast<char>(bytes_view{terms[word_of[i]]}));
    }
  }
  Index(slots);

  if (slop.max != 0 || stacked) {
    if (slop.max != 0 && n > 1) {
      LayoutSlop();
    }
    return;
  }
  LayoutAutomaton();
  PickAnchor(readers);
  if (!match) {
    return;
  }
  switch (*match) {
    case PhraseMatch::Anchor:
      break;
    case PhraseMatch::Automaton:
      if (automaton) {
        anchor.reset();
      }
      break;
    case PhraseMatch::Positions:
      anchor.reset();
      automaton.reset();
      break;
  }
}

void CompiledPhrase::AddPattern(uint32_t slot,
                                const ByPhraseOptions::PhrasePart& part) {
  std::visit(
    [&]<typename Part>(const Part& options) {
      if constexpr (std::is_same_v<Part, ByPrefixOptions>) {
        patterns[slot] =
          MakeTermPredicate([prefix = bstring{options.term}](bytes_view term) {
            return term.starts_with(prefix);
          });
      } else if constexpr (std::is_same_v<Part, ByRangeOptions>) {
        patterns[slot] =
          MakeTermPredicate([range = options.range](bytes_view term) {
            return RangeAcceptor{{&range}, {&range}}(term);
          });
      } else if constexpr (std::is_same_v<Part, AutomatonOptions> ||
                           std::is_same_v<Part, LevenshteinAutomatonOptions>) {
        if (options.source) {
          sources.push_back(options.source);
          patterns[slot] = options.source->Predicate();
        }
      }
    },
    part);
  if (patterns[slot]) {
    pattern_slots.push_back(slot);
  }
}

bool CompiledPhrase::Accepts(uint32_t slot,
                             const duckdb::string_t& term) const {
  if (is_word[slot]) {
    return words[slot] == term;
  }
  const auto view = AsBytesView(term);
  if (!patterns.empty() && patterns[slot]) {
    return Plain(view) && patterns[slot]->Accepts(view);
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
  if (pattern_slots.empty() || !Plain(view)) {
    return mask;
  }
  for (const auto slot : pattern_slots) {
    if (patterns[slot]->Accepts(view)) {
      mask |= automaton->slot_bits[slot];
    }
  }
  return mask;
}

void CompiledPhrase::Index(std::span<const uint32_t> slots) {
  std::vector<std::pair<bytes_view, uint32_t>> pending;
  pending.reserve(terms.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    pending.emplace_back(terms[i], slots[i]);
  }
  absl::c_sort(pending);

  accept.reserve(pending.size());
  for (size_t i = 0; i != pending.size();) {
    const auto term = pending[i].first;
    Accept entry{.begin = static_cast<uint32_t>(slot_ids.size())};
    for (; i != pending.size() && pending[i].first == term; ++i) {
      const auto slot = pending[i].second;
      if (entry.size == 0 || slot_ids.back() != slot) {
        slot_ids.push_back(slot);
        ++entry.size;
      }
    }
    accept.emplace(term, entry);
  }
}

void CompiledPhrase::LayoutSlop() {
  const auto n = static_cast<uint32_t>(offs_max.size());
  slop.offsets.assign(n, 0);
  for (uint32_t i = 1; i != n; ++i) {
    slop.offsets[i] = slop.offsets[i - 1] + offs_max[i];
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
  for (uint32_t i = 0; i != n; ++i) {
    groups[i] = find(i);
  }
  detail::slop::BuildGroupPairs(groups, n, slop.pairs);
}

void CompiledPhrase::LayoutAutomaton() {
  const auto n = offs_max.size();
  Automaton layout;
  layout.slot_bits.assign(n, 0);
  uint64_t bit = 0;
  for (size_t k = 0; k != n; ++k) {
    if (k != 0) {
      const auto prev = bit;
      bit = prev + offs_max[k];
      if (bit >= kMaxBits) {
        return;
      }
      for (auto g = prev + 1; g < bit; ++g) {
        layout.wild |= uint64_t{1} << g;
      }
      if (offs_min[k] < offs_max[k]) {
        uint64_t range = 0;
        for (auto j = prev + offs_min[k] - 1; j + 2 <= bit; ++j) {
          range |= uint64_t{1} << j;
        }
        layout.extras.push_back({.range = range,
                                 .bit = uint64_t{1} << bit,
                                 .index = static_cast<uint32_t>(bit)});
      }
    }
    layout.slot_bits[k] = uint64_t{1} << bit;
  }
  layout.length = static_cast<uint32_t>(bit + 1);
  layout.last = uint64_t{1} << bit;
  for (auto& [term, entry] : accept) {
    for (uint32_t i = 0; i != entry.size; ++i) {
      entry.mask |= layout.slot_bits[slot_ids[entry.begin + i]];
    }
  }
  automaton = std::move(layout);
}

void CompiledPhrase::PickAnchor(std::span<const TermReader* const> readers) {
  constexpr auto kUnknown = std::numeric_limits<double>::max();
  double best = kUnknown;
  for (uint32_t k = 0; k != words.size(); ++k) {
    if (!is_word[k]) {
      continue;
    }
    const auto& word = words[k];
    uint64_t docs = 0;
    uint64_t freq = 0;
    for (const auto* reader : readers) {
      const auto meta = reader->Lookup(AsBytesView(word));
      docs += meta.docs_count;
      freq += meta.freq;
    }
    double cost = kUnknown;
    if (docs != 0) {
      cost = freq != 0 ? static_cast<double>(freq) / static_cast<double>(docs)
                       : static_cast<double>(docs);
    }
    if (!anchor || cost < best ||
        (cost == best && word.GetSize() > words[anchor->slot].GetSize())) {
      anchor = Anchor{.slot = k};
      best = cost;
    }
  }
  if (!anchor) {
    return;
  }
  const auto split = offs_max.begin() + anchor->slot + 1;
  anchor->left = std::accumulate(offs_max.begin() + 1, split, uint64_t{0});
  anchor->right = std::accumulate(split, offs_max.end(), uint64_t{0});
}

void PhraseCheck::Anchor::Reset() {
  batch_terms = nullptr;
  batch_pos = nullptr;
  batch_base = 0;
  end = 0;
  carry_base = 0;
  carry_terms.clear();
  carry_pos.clear();
  arena.Reset();
  pending.clear();
  last = 0;
  steps = 0;
}

void PhraseCheck::Anchor::Carry(uint64_t span) {
  const uint64_t tail = PosAt(end - 1);
  const auto keep = tail > span ? tail - span : 0;
  auto from = end;
  while (from > carry_base && PosAt(from - 1) >= keep) {
    --from;
  }
  next_terms.clear();
  next_pos.clear();
  for (auto at = from; at != end; ++at) {
    auto term = TermAt(at);
    if (at >= batch_base && !term.IsInlined()) {
      const auto size = static_cast<uint32_t>(term.GetSize());
      auto* data = arena.Allocate(size);
      std::memcpy(data, term.GetData(), size);
      term = {reinterpret_cast<const char*>(data), size};
    }
    next_terms.push_back(term);
    next_pos.push_back(PosAt(at));
  }
  std::swap(carry_terms, next_terms);
  std::swap(carry_pos, next_pos);
  carry_base = from;
}

void PhraseCheck::Automaton::Reset(const CompiledPhrase::Automaton& layout,
                                   bool count) {
  state = 0;
  mask = 0;
  at = 0;
  if (count && !layout.extras.empty()) {
    counts.assign(layout.length, 0);
    sums.assign(layout.extras.size(), 0);
  }
}

void PhraseCheck::Positions::Reset(size_t n) {
  slots.resize(n);
  for (auto& slot : slots) {
    slot.clear();
  }
}

PhraseCheck::PhraseCheck(const CompiledPhrase& phrase,
                         const PhraseTokens& tokens, bool count)
  : _phrase{&phrase},
    _tokenizer{tokens.tokenizer()},
    _expression{tokens.text.expression ? tokens.text.expression() : nullptr},
    _dense{!_tokenizer->Traits().explicit_pos},
    _count{count} {}

void PhraseCheck::Bind(duckdb::DataChunk& columns) {
  auto& values = _expression ? _expression->Evaluate(columns) : columns.data[0];
  const auto& type = values.GetType();
  _rows.type = type.id();
  values.ToUnifiedFormat(columns.size(), _rows.format);
  if (_rows.type == duckdb::LogicalTypeId::LIST) {
    duckdb::ListVector::GetEntry(values).ToUnifiedFormat(
      duckdb::ListVector::GetListSize(values), _rows.children);
  } else if (_rows.type == duckdb::LogicalTypeId::ARRAY) {
    _rows.array_size = duckdb::ArrayType::GetSize(type);
    duckdb::ArrayVector::GetEntry(values).ToUnifiedFormat(
      duckdb::ArrayVector::GetTotalSize(values), _rows.children);
  }
}

bool PhraseCheck::Check(duckdb::idx_t row, PhraseVerdict& out) {
  const auto values = Values(row);
  Start(_phrase->anchor.has_value());
  Analyze(values);
  _restarted = Restart();
  if (_restarted) {
    Analyze(values);
  }
  return End(out);
}

void PhraseCheck::Analyze(std::span<const duckdb::string_t> values) {
  for (const auto& value : values) {
    if (_done) {
      return;
    }
    _analyzer.Analyze(*_tokenizer, value, *this);
  }
}

std::span<const duckdb::string_t> PhraseCheck::Values(duckdb::idx_t row) {
  auto& rows = _rows;
  const auto idx = rows.format.sel->get_index(row);
  if (!rows.format.validity.RowIsValid(idx)) {
    return {};
  }
  uint64_t begin = 0;
  uint64_t end = 0;
  if (rows.type == duckdb::LogicalTypeId::LIST) {
    const auto entry =
      duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
        rows.format)[idx];
    begin = entry.offset;
    end = entry.offset + entry.length;
  } else if (rows.type == duckdb::LogicalTypeId::ARRAY) {
    begin = idx * rows.array_size;
    end = begin + rows.array_size;
  } else {
    return {
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(rows.format) + idx,
      1};
  }
  rows.values.clear();
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(rows.children);
  for (auto i = begin; i != end; ++i) {
    const auto child = rows.children.sel->get_index(i);
    if (rows.children.validity.RowIsValid(child)) {
      rows.values.push_back(data[child]);
    }
  }
  return rows.values;
}

void PhraseCheck::Start(bool anchored) {
  _done = false;
  _restart = false;
  _last_pos = 0;
  _value_base = 0;
  _freq = 0;
  if (anchored) {
    Use<Anchor>().Reset();
  } else if (_phrase->automaton) {
    Use<Automaton>().Reset(*_phrase->automaton, _count);
  } else {
    Use<Positions>().Reset(_phrase->offs_min.size());
  }
}

void PhraseCheck::Consume(TokenBatch& batch, DocRuns) {
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
  std::visit([&](auto& state) { Feed(state, batch); }, _state);
}

bool PhraseCheck::Restart() {
  if (auto* anchor = std::get_if<Anchor>(&_state); anchor && !_done) {
    anchor->batch_terms = nullptr;
    anchor->batch_pos = nullptr;
    anchor->batch_base = anchor->carry_base + anchor->carry_terms.size();
    anchor->end = anchor->batch_base;
    for (const auto at : anchor->pending) {
      if (Hit(*anchor, at)) {
        break;
      }
    }
    anchor->pending.clear();
    _done = true;
  }
  if (!_restart) {
    return false;
  }
  Start(false);
  return true;
}

bool PhraseCheck::End(PhraseVerdict& out) {
  out = {};
  return std::visit([&](auto& state) { return Finish(state, out); }, _state);
}

bool PhraseCheck::Counted(PhraseVerdict& out) const noexcept {
  if (_freq == 0) {
    return false;
  }
  out.freq = _count ? std::saturate_cast<uint32_t>(_freq) : 1;
  return true;
}

void PhraseCheck::Feed(Anchor& anchor, const TokenBatch& batch) {
  const auto& layout = *_phrase->anchor;
  anchor.batch_terms = batch.terms;
  anchor.batch_pos = batch.pos;
  anchor.batch_base = anchor.carry_base + anchor.carry_terms.size();
  anchor.end = anchor.batch_base + batch.count;
  const uint64_t last = batch.pos[batch.count - 1];
  size_t keep = 0;
  for (const auto at : anchor.pending) {
    if (anchor.PosAt(at) + layout.right < last) {
      if (Hit(anchor, at)) {
        return;
      }
    } else {
      anchor.pending[keep++] = at;
    }
  }
  anchor.pending.resize(keep);
  const auto& word = _phrase->words[layout.slot];
  for (uint32_t i = 0; i != batch.count; ++i) {
    if (batch.terms[i] != word) {
      continue;
    }
    const auto pos = batch.pos[i];
    if (pos == anchor.last) {
      continue;
    }
    anchor.last = pos;
    const auto at = anchor.batch_base + i;
    if (pos + layout.right < last) {
      if (Hit(anchor, at)) {
        return;
      }
    } else {
      anchor.pending.push_back(at);
    }
  }
  anchor.Carry(layout.left + layout.right);
}

bool PhraseCheck::Finish(Anchor&, PhraseVerdict& out) {
  SDB_ASSERT(_done && !_restart);
  return Counted(out);
}

bool PhraseCheck::Hit(Anchor& anchor, size_t at) {
  const auto slot = _phrase->anchor->slot;
  if (const auto left = Left(anchor, slot, at)) {
    _freq += left * Right(anchor, slot, at);
  }
  _done |= !_count && _freq != 0;
  return _done;
}

uint64_t PhraseCheck::Right(Anchor& anchor, uint32_t slot, size_t at) {
  const auto& phrase = *_phrase;
  const auto next = slot + 1;
  if (next == phrase.offs_min.size()) {
    return 1;
  }
  const uint64_t pos = anchor.PosAt(at);
  const auto from = pos + phrase.offs_min[next];
  const auto to = pos + phrase.offs_max[next];
  uint64_t ways = 0;
  uint64_t accepted = 0;
  for (auto j = at + 1; j < anchor.end; ++j) {
    const uint64_t p = anchor.PosAt(j);
    if (p > to) {
      break;
    }
    if (Over(anchor)) {
      return 0;
    }
    if (p < from || p == accepted || !phrase.Accepts(next, anchor.TermAt(j))) {
      continue;
    }
    accepted = p;
    ways += Right(anchor, next, j);
    if (_restart) {
      return 0;
    }
    if (!_count && ways != 0) {
      return 1;
    }
  }
  return ways;
}

uint64_t PhraseCheck::Left(Anchor& anchor, uint32_t slot, size_t at) {
  const auto& phrase = *_phrase;
  if (slot == 0) {
    return 1;
  }
  const auto prev = slot - 1;
  const uint64_t pos = anchor.PosAt(at);
  uint64_t ways = 0;
  uint64_t accepted = std::numeric_limits<uint64_t>::max();
  for (auto j = at; j-- > anchor.carry_base;) {
    const uint64_t p = anchor.PosAt(j);
    const auto distance = pos - p;
    if (distance > phrase.offs_max[slot]) {
      break;
    }
    if (Over(anchor)) {
      return 0;
    }
    if (distance < phrase.offs_min[slot] || p == accepted ||
        !phrase.Accepts(prev, anchor.TermAt(j))) {
      continue;
    }
    accepted = p;
    ways += Left(anchor, prev, j);
    if (_restart) {
      return 0;
    }
    if (!_count && ways != 0) {
      return 1;
    }
  }
  return ways;
}

bool PhraseCheck::Over(Anchor& anchor) noexcept {
  if (++anchor.steps <= kStepsPerToken * anchor.end + kStepSlack) {
    return false;
  }
  _restart = true;
  _done = true;
  return true;
}

void PhraseCheck::Feed(Automaton& automaton, const TokenBatch& batch) {
  for (uint32_t i = 0; i != batch.count; ++i) {
    const auto pos = batch.pos[i];
    if (pos != automaton.at) {
      Flush(automaton);
      if (_done) {
        return;
      }
      if (automaton.at != 0) {
        for (auto gap = automaton.at + 1; gap < pos && automaton.state != 0;
             ++gap) {
          Step(automaton, 0);
        }
      }
      automaton.at = pos;
    }
    automaton.mask |= _phrase->MaskOf(batch.terms[i]);
  }
}

bool PhraseCheck::Finish(Automaton& automaton, PhraseVerdict& out) {
  if (!_done) {
    Flush(automaton);
  }
  return Counted(out);
}

void PhraseCheck::Flush(Automaton& automaton) {
  if (automaton.at == 0 || (automaton.state == 0 && automaton.mask == 0)) {
    automaton.mask = 0;
    return;
  }
  Step(automaton, automaton.mask);
  automaton.mask = 0;
}

void PhraseCheck::Step(Automaton& automaton, uint64_t mask) {
  const auto& layout = *_phrase->automaton;
  auto next = ((automaton.state << 1) | 1) & (mask | layout.wild);
  for (const auto& extra : layout.extras) {
    if (automaton.state & extra.range) {
      next |= extra.bit & mask;
    }
  }
  if (!_count || layout.extras.empty()) {
    automaton.state = next;
    if (automaton.state & layout.last) {
      ++_freq;
      _done = !_count;
    }
    return;
  }
  if ((automaton.state | next) == 0) {
    return;
  }
  auto& counts = automaton.counts;
  for (size_t x = 0; x != layout.extras.size(); ++x) {
    uint64_t sum = 0;
    for (auto r = layout.extras[x].range & automaton.state; r != 0;
         r &= r - 1) {
      sum += counts[std::countr_zero(r)];
    }
    automaton.sums[x] = sum;
  }
  for (auto j = layout.length - 1; j != 0; --j) {
    counts[j] = (next >> j & 1) ? counts[j - 1] : 0;
  }
  counts[0] = next & 1;
  for (size_t x = 0; x != layout.extras.size(); ++x) {
    const auto& extra = layout.extras[x];
    if (mask & extra.bit) {
      counts[extra.index] += automaton.sums[x];
    }
  }
  automaton.state = next;
  _freq += counts[layout.length - 1];
}

void PhraseCheck::Feed(Positions& positions, const TokenBatch& batch) {
  for (uint32_t i = 0; i != batch.count; ++i) {
    const auto pos = batch.pos[i];
    _phrase->ForEachSlot(batch.terms[i], [&](uint32_t slot) {
      auto& slot_positions = positions.slots[slot];
      if (slot_positions.empty() || slot_positions.back() != pos) {
        slot_positions.push_back(pos);
      }
    });
  }
}

bool PhraseCheck::Finish(Positions& positions, PhraseVerdict& out) {
  const auto& phrase = *_phrase;
  const auto n = phrase.offs_min.size();
  if (absl::c_any_of(positions.slots,
                     [](const auto& slot) { return slot.empty(); })) {
    return false;
  }
  if (n == 1) {
    out.freq =
      _count ? std::saturate_cast<uint32_t>(positions.slots.front().size()) : 1;
    return true;
  }

  if (phrase.slop.max != 0) {
    detail::slop::SpanCursors cursors{positions.slots, positions.scratch};
    const auto res = detail::slop::Sweep<0>(cursors, phrase.slop.offsets,
                                            phrase.slop.max, phrase.slop.pairs,
                                            positions.scratch, !_count, [] {});
    if (!res.any) {
      return false;
    }
    if (_count) {
      out.freq = std::saturate_cast<uint32_t>(res.freq);
      out.scale =
        static_cast<score_t>(res.weight / static_cast<double>(res.freq));
    } else {
      out.freq = 1;
    }
    return true;
  }

  auto& valid = positions.valid;
  auto& ways = positions.ways;
  valid.assign(positions.slots.back().begin(), positions.slots.back().end());
  ways.assign(valid.size(), 1);
  for (size_t i = n - 1; i != 0; --i) {
    const auto& prev = positions.slots[i - 1];
    positions.next.clear();
    positions.next_ways.clear();
    size_t lo = 0;
    size_t hi = 0;
    uint64_t window = 0;
    for (const auto p : prev) {
      const uint64_t min = uint64_t{p} + phrase.offs_min[i];
      const uint64_t max = uint64_t{p} + phrase.offs_max[i];
      for (; hi != valid.size() && valid[hi] <= max; ++hi) {
        window += ways[hi];
      }
      for (; lo != hi && valid[lo] < min; ++lo) {
        window -= ways[lo];
      }
      if (window != 0) {
        positions.next.push_back(p);
        positions.next_ways.push_back(window);
      }
    }
    std::swap(valid, positions.next);
    std::swap(ways, positions.next_ways);
    if (valid.empty()) {
      return false;
    }
  }
  out.freq =
    _count ? std::saturate_cast<uint32_t>(absl::c_accumulate(ways, uint64_t{0}))
           : 1;
  return true;
}

}  // namespace irs
