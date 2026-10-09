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

#include <absl/container/flat_hash_map.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/utils/levenshtein_acceptor.hpp"
#include "iresearch/utils/regexp_acceptor.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class RowArena {
 public:
  std::byte* Allocate(size_t bytes);

  size_t Size() const noexcept { return _size; }

 private:
  static constexpr size_t kFirstChunkBytes = size_t{4} << 10;
  static constexpr size_t kChunkBytes = size_t{64} << 10;

  std::vector<std::unique_ptr<std::byte[]>> _chunks;
  size_t _used{0};
  size_t _capacity{0};
  size_t _size{0};
};

class RegexpConjunction {
 public:
  static constexpr size_t kMaxPatterns = 4;

  using PayloadType = RegexpAcceptor::PayloadType;
  static constexpr bool kHasPayload = false;
  static constexpr bool kCheapRuns = true;
  static constexpr bool kMayBeUnknown = true;

  using Parts = std::array<RegexpAcceptor::State, kMaxPatterns>;

  struct Row {
    RegexpAcceptor::Mask loop{};
    Parts parts{};
    std::atomic_bool built{false};
    bool accept{false};
    bool dead{false};
    bool unknown{false};
    uint8_t lo{1};
    uint8_t hi{0};

    std::atomic<const Row*>* Next() noexcept {
      return reinterpret_cast<std::atomic<const Row*>*>(this + 1);
    }
    const std::atomic<const Row*>* Next() const noexcept {
      return reinterpret_cast<const std::atomic<const Row*>*>(this + 1);
    }
  };

  using State = const Row*;

  explicit RegexpConjunction(
    std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
    size_t max_mem = RegexpAcceptor::kDefaultMaxDfaMem);
  RegexpConjunction(const RegexpConjunction&) = delete;
  RegexpConjunction& operator=(const RegexpConjunction&) = delete;

  State Start() const noexcept { return _start; }

  bytes_view LowerBound() const noexcept { return _lower; }

  std::span<const bstring> RequiredSuffixes() const noexcept {
    return _suffixes;
  }

  std::span<const RegexpAcceptor::ExemptKey> ExemptKeys() const noexcept {
    return {};
  }

  bytes_view RequiredInfix() const noexcept { return _infix; }

  static bool Alive(State state) noexcept { return !state->dead; }

  static bool Unknown(State state) noexcept { return state->unknown; }

  State Step(State from, byte_type label) const {
    const auto c = _bytemap[label];
    if (const auto* next = from->Next()[c].load(std::memory_order_acquire);
        next != nullptr) [[likely]] {
      return next;
    }
    return StepSlow(from, c);
  }

  size_t StepRun(State from, const byte_type* p, size_t n, State& out) const {
    const auto i = RegexpAcceptor::Stay(Built(from).loop, p, n);
    out = i == n ? from : Step(from, p[i]);
    return i;
  }

  bool Accept(State state, PayloadType& payload) const noexcept {
    payload = 0;
    return state->accept;
  }

  bool LiveRange(State state, uint32_t& lo, uint32_t& hi) const {
    const auto& row = Built(state);
    lo = row.lo;
    hi = row.hi;
    return row.lo <= row.hi;
  }

  bool Matches(bytes_view term) const;

 private:
  const Row& Built(State state) const {
    if (!state->built.load(std::memory_order_acquire)) [[unlikely]] {
      Build(const_cast<Row*>(state));
    }
    return *state;
  }

  State StepSlow(State from, uint8_t c) const;
  void Build(Row* row) const;
  void BuildLocked(Row* row) const;
  State InternLocked(const Parts& parts) const;

  std::array<std::shared_ptr<const RegexpAcceptor>, kMaxPatterns> _patterns;
  size_t _size;
  std::array<uint8_t, RegexpAcceptor::kMaxLabel + 1> _bytemap{};
  std::array<uint8_t, RegexpAcceptor::kMaxLabel + 1> _representative{};
  uint32_t _classes{0};
  bytes_view _lower;
  std::span<const bstring> _suffixes;
  bytes_view _infix;
  size_t _max_mem;
  mutable std::mutex _mutex;
  mutable absl::flat_hash_map<Parts, Row*> _rows;
  mutable RowArena _arena;
  Row* _dead{nullptr};
  Row* _unknown{nullptr};
  State _start{nullptr};
};

class FuzzyConjunction {
 public:
  using PayloadType = LevenshteinAcceptor::PayloadType;
  static constexpr bool kHasPayload = true;
  static constexpr bool kCheapRuns = false;
  static constexpr bool kMayBeUnknown = true;

  using Parts = RegexpConjunction::Parts;

  struct Row {
    Parts parts{};
    LevenshteinAcceptor::State fuzzy{};
    std::atomic<uint32_t> range{0};
    bool accept{false};
    bool dead{false};
    bool unknown{false};
    PayloadType distance{0};

    std::atomic<const Row*>* Next() noexcept {
      return reinterpret_cast<std::atomic<const Row*>*>(this + 1);
    }
    const std::atomic<const Row*>* Next() const noexcept {
      return reinterpret_cast<const std::atomic<const Row*>*>(this + 1);
    }
  };

  using State = const Row*;

  FuzzyConjunction(
    std::span<const std::shared_ptr<const RegexpAcceptor>> patterns,
    std::shared_ptr<const LevenshteinAcceptor> fuzzy,
    size_t max_mem = RegexpAcceptor::kDefaultMaxDfaMem);
  FuzzyConjunction(const FuzzyConjunction&) = delete;
  FuzzyConjunction& operator=(const FuzzyConjunction&) = delete;

  static std::shared_ptr<const FuzzyConjunction> Make(
    std::shared_ptr<const LevenshteinAcceptor> fuzzy,
    size_t max_mem = RegexpAcceptor::kDefaultMaxDfaMem);

  State Start() const noexcept { return _start; }

  bytes_view LowerBound() const noexcept { return _lower; }

  std::span<const bstring> RequiredSuffixes() const noexcept {
    return _suffixes;
  }

  std::span<const RegexpAcceptor::ExemptKey> ExemptKeys() const noexcept {
    return {};
  }

  bytes_view RequiredInfix() const noexcept { return _infix; }

  static bool Alive(State state) noexcept { return !state->dead; }

  static bool Unknown(State state) noexcept { return state->unknown; }

  State Step(State from, byte_type label) const {
    const auto c = _bytemap[label];
    if (const auto* next = from->Next()[c].load(std::memory_order_acquire);
        next != nullptr) [[likely]] {
      return next;
    }
    return StepSlow(from, c);
  }

  bool Accept(State state, PayloadType& payload) const noexcept {
    payload = state->distance;
    return state->accept;
  }

  bool LiveRange(State state, uint32_t& lo, uint32_t& hi) const {
    auto range = state->range.load(std::memory_order_acquire);
    if (range == 0) [[unlikely]] {
      range = Range(const_cast<Row&>(*state));
    }
    lo = (range >> 8) & 0xFFU;
    hi = range & 0xFFU;
    return lo <= hi;
  }

  bool Matches(bytes_view term) const;

  bool Matches(bytes_view term, PayloadType& payload) const;

  uint32_t MaxDistance() const noexcept { return _fuzzy->MaxDistance(); }

 private:
  struct Key {
    Parts parts;
    std::array<uint32_t, 4> fuzzy;

    bool operator==(const Key&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const Key& key) {
      return H::combine(std::move(h), key.parts, key.fuzzy);
    }
  };

  static constexpr uint32_t kRanged = uint32_t{1} << 16;

  static constexpr uint32_t PackRange(uint32_t lo, uint32_t hi) noexcept {
    return kRanged | (lo << 8) | hi;
  }

  State StepSlow(State from, uint8_t c) const;
  uint32_t Range(Row& row) const;
  State InternLocked(const Parts& parts,
                     const LevenshteinAcceptor::State& fuzzy) const;

  std::array<std::shared_ptr<const RegexpAcceptor>,
             RegexpConjunction::kMaxPatterns>
    _patterns;
  size_t _size;
  std::shared_ptr<const LevenshteinAcceptor> _fuzzy;
  std::array<uint8_t, RegexpAcceptor::kMaxLabel + 1> _bytemap{};
  std::array<uint8_t, RegexpAcceptor::kMaxLabel + 1> _representative{};
  uint32_t _classes{0};
  bytes_view _lower;
  std::span<const bstring> _suffixes;
  bytes_view _infix;
  size_t _max_mem;
  mutable std::mutex _mutex;
  mutable absl::flat_hash_map<Key, Row*> _rows;
  mutable RowArena _arena;
  Row* _dead{nullptr};
  Row* _unknown{nullptr};
  State _start{nullptr};
};

}  // namespace irs
