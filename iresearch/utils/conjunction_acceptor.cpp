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

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/bitset.hpp"

namespace irs {
namespace {

constexpr uint32_t kLabels = RegexpAcceptor::kMaxLabel + 1;

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
  absl::flat_hash_map<uint64_t, uint8_t> ids;
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

void Collect(std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
             std::span<std::shared_ptr<const RegexpAcceptor>> owned,
             RegexpConjunction::Parts& start, bytes_view& lower,
             bytes_view& suffix, bytes_view& infix) {
  SDB_ASSERT(!patterns.empty() && patterns.size() <= owned.size());
  for (size_t i = 0; i != patterns.size(); ++i) {
    const auto& pattern = *patterns[i];
    SDB_ASSERT(pattern.ok());
    owned[i] = patterns[i];
    start[i] = pattern.Start();
    lower = std::max(lower, pattern.LowerBound());
    if (pattern.RequiredSuffix().size() > suffix.size()) {
      suffix = pattern.RequiredSuffix();
    }
    if (pattern.RequiredInfix().size() > infix.size()) {
      infix = pattern.RequiredInfix();
    }
  }
}

}  // namespace

std::byte* RowArena::Allocate(size_t bytes) {
  if (_used + bytes > _capacity) {
    _capacity = std::max(kChunkBytes, bytes);
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
  : _size{patterns.size()}, _max_mem{max_mem} {
  Parts start{};
  Collect(patterns, _patterns, start, _lower, _suffix, _infix);
  _classes =
    Refine([&](uint32_t label) { return PatternsKey(patterns, label); },
           _bytemap, _representative);

  std::lock_guard lock{_mutex};
  _dead = NewRow<Row>(_arena, _classes);
  _dead->dead = true;
  Loop(_dead, _classes);
  _dead->built.store(true, std::memory_order_relaxed);
  _unknown = NewRow<Row>(_arena, _classes);
  _unknown->unknown = true;
  _unknown->lo = 0;
  _unknown->hi = RegexpAcceptor::kMaxLabel;
  _unknown->loop.fill(~bitset::word_t{0});
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
  std::lock_guard lock{_mutex};
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
      row->loop[bitset::word(label)] |= bitset::word_t{1} << bitset::bit(label);
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
  std::shared_ptr<const LevenshteinAcceptor> fuzzy, size_t max_mem)
  : _size{patterns.size()}, _fuzzy{std::move(fuzzy)}, _max_mem{max_mem} {
  SDB_ASSERT(_fuzzy);
  Parts start{};
  _lower = _fuzzy->LowerBound();
  Collect(patterns, _patterns, start, _lower, _suffix, _infix);
  const auto fuzzy_bytemap = _fuzzy->Bytemap();
  _classes = Refine(
    [&](uint32_t label) {
      return (PatternsKey(patterns, label) << 8) | fuzzy_bytemap[label];
    },
    _bytemap, _representative);

  std::lock_guard lock{_mutex};
  _dead = NewRow<Row>(_arena, _classes);
  _dead->dead = true;
  Loop(_dead, _classes);
  _dead->ranged.store(true, std::memory_order_relaxed);
  _unknown = NewRow<Row>(_arena, _classes);
  _unknown->unknown = true;
  _unknown->lo = 0;
  _unknown->hi = RegexpAcceptor::kMaxLabel;
  Loop(_unknown, _classes);
  _unknown->ranged.store(true, std::memory_order_relaxed);
  _start = InternLocked(start, _fuzzy->Start());
}

bool FuzzyConjunction::Matches(bytes_view term) const {
  for (size_t i = 0; i != _size; ++i) {
    if (!_patterns[i]->Matches(term)) {
      return false;
    }
  }
  return _fuzzy->Matches(term);
}

FuzzyConjunction::State FuzzyConjunction::StepSlow(State from,
                                                   uint8_t c) const {
  std::lock_guard lock{_mutex};
  auto& slot = const_cast<Row*>(from)->Next()[c];
  if (const auto* next = slot.load(std::memory_order_relaxed);
      next != nullptr) {
    return next;
  }
  const auto label = _representative[c];
  State next = _dead;
  if (const auto fuzzy = _fuzzy->Step(from->fuzzy, label);
      LevenshteinAcceptor::Alive(fuzzy)) {
    Parts parts{};
    size_t i = 0;
    for (; i != _size; ++i) {
      parts[i] = _patterns[i]->Step(from->parts[i], label);
      if (parts[i]->dead) {
        break;
      }
    }
    if (i == _size) {
      next = InternLocked(parts, fuzzy);
    }
  }
  slot.store(next, std::memory_order_release);
  return next;
}

void FuzzyConjunction::Range(Row* row) const {
  std::lock_guard lock{_mutex};
  if (row->ranged.load(std::memory_order_relaxed)) {
    return;
  }
  uint32_t lo = 1;
  uint32_t hi = 0;
  _fuzzy->LiveRange(row->fuzzy, lo, hi);
  for (size_t i = 0; i != _size && lo <= hi; ++i) {
    uint32_t part_lo = 1;
    uint32_t part_hi = 0;
    _patterns[i]->LiveRange(row->parts[i], part_lo, part_hi);
    lo = std::max(lo, part_lo);
    hi = std::min(hi, part_hi);
  }
  if (lo > hi) {
    lo = 1;
    hi = 0;
  }
  row->lo = static_cast<uint8_t>(lo);
  row->hi = static_cast<uint8_t>(hi);
  row->ranged.store(true, std::memory_order_release);
}

FuzzyConjunction::State FuzzyConjunction::InternLocked(
  const Parts& parts, const LevenshteinAcceptor::State& fuzzy) const {
  if (!LevenshteinAcceptor::Alive(fuzzy)) {
    return _dead;
  }
  bool accept = true;
  bool unknown = false;
  for (size_t i = 0; i != _size; ++i) {
    if (parts[i]->dead) {
      return _dead;
    }
    accept = accept && parts[i]->accept;
    unknown = unknown || parts[i]->unknown;
  }
  LevenshteinAcceptor::PayloadType distance{};
  accept = accept && _fuzzy->Accept(fuzzy, distance);
  const Key key{parts, {fuzzy.pstate, fuzzy.offset, fuzzy.acc, fuzzy.phase}};
  if (const auto it = _rows.find(key); it != _rows.end()) {
    return it->second;
  }
  if (_arena.Size() + RowBytes<Row>(_classes) > _max_mem) {
    return _unknown;
  }
  Row* row = NewRow<Row>(_arena, _classes);
  row->parts = parts;
  row->fuzzy = fuzzy;
  row->accept = accept;
  row->unknown = unknown;
  _rows.emplace(key, row);
  return row;
}

}  // namespace irs
