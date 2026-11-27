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

#include "iresearch/utils/conjunction_acceptor.hpp"

#include <algorithm>
#include <new>
#include <type_traits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/dispatch.hpp"

namespace irs {
namespace {

constexpr uint32_t kLabels = RegexpAcceptor::kMaxLabel + 1;

template<bool kPrefix, bool kBytes>
using FuzzyConjunctionFor = FuzzyConjunctionOf<
  std::conditional_t<kPrefix, BasicLevenshteinPrefixAcceptor<kBytes>,
                     BasicLevenshteinAcceptor<kBytes>>>;

template<typename F>
IRS_FORCE_INLINE decltype(auto) Typed(const FuzzyConjunction& dfa,
                                      bool starts_with, bool byte_mode, F&& f) {
  return Resolve(starts_with, [&]<bool kPrefix> IRS_FORCE_INLINE {
    return Resolve(byte_mode, [&]<bool kBytes> IRS_FORCE_INLINE {
      return f(static_cast<const FuzzyConjunctionFor<kPrefix, kBytes>&>(dfa));
    });
  });
}

template<typename Row>
size_t RowBytes(uint32_t classes) noexcept {
  const size_t bytes =
    sizeof(Row) + size_t{classes} * sizeof(std::atomic<const Row*>);
  return (bytes + alignof(Row) - 1) & ~(alignof(Row) - 1);
}

template<typename Row>
Row* NewRow(RowArena& arena, uint32_t classes) {
  auto* row = new (arena.Allocate(RowBytes<Row>(classes))) Row{};
  auto* next = row->Next();
  for (uint32_t c = 0; c != classes; ++c) {
    new (next + c) std::atomic<const Row*>{nullptr};
  }
  return row;
}

template<typename Row>
void Loop(Row* row, uint32_t classes) noexcept {
  for (uint32_t c = 0; c != classes; ++c) {
    row->Next()[c].store(row, std::memory_order_relaxed);
  }
}

template<typename KeyOf>
uint32_t Refine(KeyOf&& key_of, std::array<uint8_t, kLabels>& bytemap,
                std::array<uint8_t, kLabels>& representative) {
  containers::FlatHashMap<uint64_t, uint8_t> ids;
  uint32_t classes = 0;
  for (uint32_t label = 0; label != kLabels; ++label) {
    const auto [it, inserted] =
      ids.try_emplace(key_of(label), static_cast<uint8_t>(classes));
    if (inserted) {
      representative[classes++] = static_cast<uint8_t>(label);
    }
    bytemap[label] = it->second;
  }
  return classes;
}

uint64_t PatternsKey(
  std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
  uint32_t label) noexcept {
  uint64_t key = 0;
  for (const auto& pattern : patterns) {
    key = (key << 8) | pattern->Bytemap()[label];
  }
  return key;
}

const LevenshteinAcceptor::State& AutomatonState(
  const LevenshteinAcceptor::State& state) noexcept {
  return state;
}

const LevenshteinAcceptor::State& AutomatonState(
  const LevenshteinPrefixAcceptor::State& state) noexcept {
  return state.inner;
}

const LevenshteinAcceptor::State& StateOf(
  const LevenshteinAcceptor&, const FuzzyConjunction::Row& row) noexcept {
  return row.fuzzy;
}

LevenshteinPrefixAcceptor::State StateOf(
  const LevenshteinPrefixAcceptor&, const FuzzyConjunction::Row& row) noexcept {
  return {row.fuzzy, row.distance};
}

void Collect(std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
             std::span<std::shared_ptr<const RegexpAcceptor>> owned,
             RegexpConjunction::Parts& start, bytes_view& lower,
             std::span<const bstring>& suffixes, bytes_view& infix,
             KeyBytes& bytes) {
  SDB_ASSERT(patterns.size() <= owned.size());
  const auto shortest = [](std::span<const bstring> set) {
    size_t size = set.empty() ? 0 : std::numeric_limits<size_t>::max();
    for (const auto& suffix : set) {
      size = std::min(size, suffix.size());
    }
    return size;
  };
  for (size_t i = 0; i != patterns.size(); ++i) {
    const auto& pattern = *patterns[i];
    SDB_ASSERT(pattern.ok());
    owned[i] = patterns[i];
    start[i] = pattern.Start();
    lower = std::max(lower, pattern.LowerBound());
    if (pattern.ExemptKeys().empty() &&
        shortest(pattern.RequiredSuffixes()) > shortest(suffixes)) {
      suffixes = pattern.RequiredSuffixes();
    }
    if (pattern.RequiredInfix().size() > infix.size()) {
      infix = pattern.RequiredInfix();
    }
    Join(bytes, pattern.Bytes());
  }
}

}  // namespace

std::byte* RowArena::Allocate(size_t bytes) {
  if (_used + bytes > _capacity) {
    _capacity = std::max(
      _chunks.empty() ? kFirstChunkBytes : std::min(_capacity * 2, kChunkBytes),
      bytes);
    _chunks.emplace_back(
      std::make_unique_for_overwrite<std::byte[]>(_capacity));
    _used = 0;
  }
  auto* memory = _chunks.back().get() + _used;
  _used += bytes;
  _size += bytes;
  return memory;
}

RegexpConjunction::RegexpConjunction(
  std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
  size_t max_mem)
  : _size{patterns.size()}, _bytes{.dead = {}}, _max_mem{max_mem} {
  Parts start{};
  Collect(patterns, _patterns, start, _lower, _suffixes, _infix, _bytes);
  _classes =
    Refine([&](uint32_t label) { return PatternsKey(patterns, label); },
           _bytemap, _representative);

  absl::MutexLock lock{&_mutex};
  _dead = NewRow<Row>(_arena, _classes);
  _dead->dead = true;
  Loop(_dead, _classes);
  _dead->built.store(true, std::memory_order_relaxed);
  _unknown = NewRow<Row>(_arena, _classes);
  _unknown->unknown = true;
  _unknown->lo = 0;
  _unknown->hi = RegexpAcceptor::kMaxLabel;
  _unknown->loop.words.fill(~uint64_t{0});
  Loop(_unknown, _classes);
  _unknown->built.store(true, std::memory_order_relaxed);
  _start = InternLocked(start);
}

bool RegexpConjunction::Matches(bytes_view term) const {
  for (size_t i = 0; i != _size; ++i) {
    if (!_patterns[i]->Matches(term)) {
      return false;
    }
  }
  return true;
}

RegexpConjunction::State RegexpConjunction::StepSlow(State from,
                                                     uint8_t c) const {
  Build(const_cast<Row*>(from));
  return from->Next()[c].load(std::memory_order_acquire);
}

void RegexpConjunction::Build(Row* row) const {
  absl::MutexLock lock{&_mutex};
  BuildLocked(row);
}

void RegexpConjunction::BuildLocked(Row* row) const {
  if (row->built.load(std::memory_order_relaxed)) {
    return;
  }
  auto* next = row->Next();
  for (uint32_t c = 0; c != _classes; ++c) {
    const auto label = _representative[c];
    Parts parts{};
    size_t i = 0;
    for (; i != _size; ++i) {
      parts[i] = _patterns[i]->Step(row->parts[i], label);
      if (parts[i]->dead) {
        break;
      }
    }
    next[c].store(i == _size ? InternLocked(parts) : _dead,
                  std::memory_order_release);
  }
  uint8_t lo = 1;
  uint8_t hi = 0;
  for (uint32_t label = 0; label != kLabels; ++label) {
    const auto* target = next[_bytemap[label]].load(std::memory_order_relaxed);
    if (target->dead) {
      continue;
    }
    if (lo > hi) {
      lo = static_cast<uint8_t>(label);
    }
    hi = static_cast<uint8_t>(label);
    if (target == row) {
      row->loop.Add(static_cast<byte_type>(label));
    }
  }
  row->lo = lo;
  row->hi = hi;
  row->built.store(true, std::memory_order_release);
}

RegexpConjunction::State RegexpConjunction::InternLocked(
  const Parts& parts) const {
  bool accept = true;
  bool unknown = false;
  for (size_t i = 0; i != _size; ++i) {
    if (parts[i]->dead) {
      return _dead;
    }
    accept = accept && parts[i]->accept;
    unknown = unknown || parts[i]->unknown;
  }
  if (const auto it = _rows.find(parts); it != _rows.end()) {
    return it->second;
  }
  if (_arena.Size() + RowBytes<Row>(_classes) > _max_mem) {
    return _unknown;
  }
  Row* row = NewRow<Row>(_arena, _classes);
  row->parts = parts;
  row->accept = accept;
  row->unknown = unknown;
  _rows.emplace(parts, row);
  return row;
}

FuzzyConjunction::FuzzyConjunction(
  std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
  bytes_view lower, const KeyBytes& bytes,
  const std::array<uint8_t, 256>& fuzzy_bytemap, size_t max_mem,
  bool starts_with, bool byte_mode)
  : _size{patterns.size()},
    _starts_with{starts_with},
    _byte_mode{byte_mode},
    _lower{lower},
    _bytes{bytes},
    _max_mem{max_mem} {
  Parts start{};
  Collect(patterns, _patterns, start, _lower, _suffixes, _infix, _bytes);
  if (patterns.empty()) {
    _bytemap = fuzzy_bytemap;
    for (uint32_t label = 0; label != kLabels; ++label) {
      if (_bytemap[label] == _classes) {
        _representative[_classes++] = static_cast<uint8_t>(label);
      }
    }
  } else {
    _classes = Refine(
      [&](uint32_t label) {
        return (PatternsKey(patterns, label) << 8) | fuzzy_bytemap[label];
      },
      _bytemap, _representative);
  }

  absl::MutexLock lock{&_mutex};
  _dead = NewRow<Row>(_arena, _classes);
  _dead->dead = true;
  Loop(_dead, _classes);
  _dead->range.store(PackRange(1, 0), std::memory_order_relaxed);
  _unknown = NewRow<Row>(_arena, _classes);
  _unknown->unknown = true;
  Loop(_unknown, _classes);
  _unknown->range.store(PackRange(0, RegexpAcceptor::kMaxLabel),
                        std::memory_order_relaxed);
}

FuzzyConjunction::Parts FuzzyConjunction::StartParts() const noexcept {
  Parts start{};
  for (size_t i = 0; i != _size; ++i) {
    start[i] = _patterns[i]->Start();
  }
  return start;
}

size_t FuzzyConjunction::RowSize() const noexcept {
  return RowBytes<Row>(_classes);
}

FuzzyConjunction::Row* FuzzyConjunction::NewRowLocked() const {
  if (_arena.Size() + RowBytes<Row>(_classes) > _max_mem) {
    return nullptr;
  }
  return NewRow<Row>(_arena, _classes);
}

IRS_FORCE_INLINE FuzzyConjunction::State FuzzyConjunction::Link(
  State from, uint8_t c, bool alive, auto&& intern) const {
  auto& slot = const_cast<Row*>(from)->Next()[c];
  if (!alive) {
    slot.store(_dead, std::memory_order_release);
    return _dead;
  }
  absl::MutexLock lock{&_mutex};
  if (const auto* next = slot.load(std::memory_order_relaxed)) {
    return next;
  }
  const State next = intern();
  slot.store(next, std::memory_order_release);
  return next;
}

template<typename A>
FuzzyConjunction::State FuzzyConjunctionOf<A>::StartLocked() const {
  return InternLocked(StartParts(), _fuzzy->Start());
}

template<typename A>
IRS_FORCE_INLINE FuzzyConjunction::State FuzzyConjunctionOf<A>::StepMissed(
  State from, uint8_t c) const {
  const auto label = _representative[c];
  const auto state = _fuzzy->Step(StateOf(*_fuzzy, *from), label);
  bool alive = _fuzzy->Alive(state);
  Parts parts{};
  for (size_t i = 0; alive && i != _size; ++i) {
    parts[i] = _patterns[i]->Step(from->parts[i], label);
    alive = !parts[i]->dead;
  }
  return Link(from, c, alive, [&] { return InternLocked(parts, state); });
}

template<typename A>
IRS_FORCE_INLINE uint32_t FuzzyConjunctionOf<A>::RangeOf(Row& row) const {
  uint32_t lo = 1;
  uint32_t hi = 0;
  _fuzzy->LiveRange(StateOf(*_fuzzy, row), lo, hi);
  for (size_t i = 0; i != _size && lo <= hi; ++i) {
    uint32_t part_lo = 1;
    uint32_t part_hi = 0;
    _patterns[i]->LiveRange(row.parts[i], part_lo, part_hi);
    lo = std::max(lo, part_lo);
    hi = std::min(hi, part_hi);
  }
  if (lo > hi) {
    lo = 1;
    hi = 0;
  }
  const auto range = PackRange(lo, hi);
  row.range.store(range, std::memory_order_release);
  return range;
}

template<typename A>
IRS_FORCE_INLINE bool FuzzyConjunctionOf<A>::MatchesOf(
  bytes_view term, PayloadType& payload) const {
  for (size_t i = 0; i != _size; ++i) {
    if (!_patterns[i]->Matches(term)) {
      return false;
    }
  }
  return _fuzzy->Matches(term, payload);
}

template<typename A>
FuzzyConjunction::State FuzzyConjunctionOf<A>::InternLocked(
  const Parts& parts, const AcceptorState& state) const {
  SDB_ASSERT(_fuzzy->Alive(state));
  for (size_t i = 0; i != _size; ++i) {
    if (parts[i]->dead) {
      return _dead;
    }
  }
  const auto [it, inserted] = _rows.try_emplace(Key{parts, state}, nullptr);
  if (!inserted) {
    return it->second;
  }
  Row* row = NewRowLocked();
  if (!row) {
    _rows.erase(it);
    return _unknown;
  }
  row->parts = parts;
  row->fuzzy = AutomatonState(state);
  row->accept = _fuzzy->Accept(state, row->distance);
  for (size_t i = 0; i != _size; ++i) {
    row->accept = row->accept && parts[i]->accept;
    row->unknown = row->unknown || parts[i]->unknown;
  }
  it->second = row;
  return row;
}

void FuzzyConjunction::InternStart() {
  absl::MutexLock lock{&_mutex};
  _start = Typed(*this, _starts_with, _byte_mode,
                 [](const auto& typed) { return typed.StartLocked(); });
}

bool FuzzyConjunction::Matches(bytes_view term) const {
  PayloadType payload{};
  return Matches(term, payload);
}

bool FuzzyConjunction::Matches(bytes_view term, PayloadType& payload) const {
  return Typed(*this, _starts_with, _byte_mode,
               [&](const auto& typed)
                 IRS_FORCE_INLINE { return typed.MatchesOf(term, payload); });
}

FuzzyConjunction::State FuzzyConjunction::StepSlow(State from,
                                                   uint8_t c) const {
  return Typed(*this, _starts_with, _byte_mode,
               [&](const auto& typed)
                 IRS_FORCE_INLINE { return typed.StepMissed(from, c); });
}

uint32_t FuzzyConjunction::Range(Row& row) const {
  return Typed(*this, _starts_with, _byte_mode,
               [&](const auto& typed)
                 IRS_FORCE_INLINE { return typed.RangeOf(row); });
}

}  // namespace irs
