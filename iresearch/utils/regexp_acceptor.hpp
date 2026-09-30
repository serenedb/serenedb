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
#include <string_view>
#include <vector>

#include "iresearch/utils/containers/bitset.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "re2/prog.h"

namespace irs {

class RegexpAcceptor {
 public:
  using PayloadType = byte_type;
  static constexpr bool kHasPayload = false;
  static constexpr bool kCheapRuns = true;
  static constexpr bool kMayBeUnknown = true;

  static constexpr uint32_t kMaxLabel = 255;
  static constexpr size_t kMaskWords = bitset::bits_to_words(kMaxLabel + 1);

  struct Row {
    std::array<bitset::word_t, kMaskWords> loop{};
    const int* set{nullptr};
    uint32_t set_size{0};
    uint32_t context{0};
    std::atomic_bool built{false};
    bool accept{false};
    bool dead{false};
    bool unknown{false};
    bool assertions{false};
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

  struct WildcardTag {};

  static constexpr int64_t kDefaultMaxMem = 64 << 20;
  static constexpr size_t kDefaultMaxDfaMem = size_t{16} << 20;

  RegexpAcceptor(bytes_view pattern, RegexpSyntax syntax = RegexpSyntax::Perl,
                 int64_t max_mem = kDefaultMaxMem,
                 size_t max_dfa_mem = kDefaultMaxDfaMem);
  RegexpAcceptor(WildcardTag, bytes_view pattern,
                 int64_t max_mem = kDefaultMaxMem,
                 size_t max_dfa_mem = kDefaultMaxDfaMem);
  RegexpAcceptor(const RegexpAcceptor&) = delete;
  RegexpAcceptor& operator=(const RegexpAcceptor&) = delete;
  ~RegexpAcceptor();

  bool ok() const noexcept { return _start != nullptr && !_start->dead; }

  State Start() const noexcept { return _start; }

  bytes_view LowerBound() const noexcept { return _lower; }

  bytes_view RequiredSuffix() const noexcept { return _suffix; }

  bool Finite() const noexcept { return _finite; }

  std::span<const bstring> Literals() const noexcept { return _literals; }

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
    payload = 0;
    return state->accept;
  }

  size_t StepRun(State from, const byte_type* p, size_t n, State& out) const {
    const auto& loop = Built(from).loop;
    size_t i = 0;
    for (; i + kRunChunk <= n; i += kRunChunk) {
      bitset::word_t moved = 0;
      for (size_t j = 0; j != kRunChunk; ++j) {
        const size_t label = p[i + j];
        moved |= ~(loop[bitset::word(label)] >> bitset::bit(label));
      }
      if ((moved & 1) != 0) {
        break;
      }
    }
    for (; i != n; ++i) {
      const size_t label = p[i];
      if (((loop[bitset::word(label)] >> bitset::bit(label)) & 1) == 0) {
        out = Step(from, p[i]);
        return i;
      }
    }
    out = from;
    return n;
  }

  bool LiveRange(State state, uint32_t& lo, uint32_t& hi) const {
    const auto& row = Built(state);
    lo = row.lo;
    hi = row.hi;
    return row.lo <= row.hi;
  }

  bool Matches(bytes_view term) const;

  size_t MemoryUsage() const;

#ifdef SDB_DEV
  static size_t Builds() noexcept;
  size_t Rows() const;
#endif

 private:
  static constexpr size_t kRunChunk = 8;
  static constexpr size_t kMaxBoundLength = 32;
  static constexpr size_t kFloodRows = 256;
  static constexpr size_t kChunkBytes = size_t{64} << 10;

  void Compile(bytes_view pattern, RegexpSyntax syntax, bool wildcard,
               int64_t max_mem);

  const Row& Built(State state) const {
    if (!state->built.load(std::memory_order_acquire)) [[unlikely]] {
      Build(const_cast<Row*>(state));
    }
    return *state;
  }

  State StepSlow(State from, uint8_t c) const;
  void Build(Row* row) const;
  void BuildLocked(Row* row) const;
  State InternLocked(const std::vector<int>& queue, uint32_t context) const;
  size_t RowBytes(size_t set_size) const noexcept;
  Row* AllocateLocked(size_t set_size) const;
  void AddToQueue(std::vector<int>& queue, std::vector<uint32_t>& index,
                  std::vector<int>& stack, int id, uint32_t satisfied) const;
  void Resolve(const int* set, uint32_t size, uint32_t satisfied,
               std::vector<int>& queue, std::vector<uint32_t>& index,
               std::vector<int>& stack) const;
  bool Simulate(State from, bytes_view rest) const;

  std::unique_ptr<re2::Prog> _prog;
  std::array<uint8_t, kMaxLabel + 1> _bytemap{};
  std::array<uint8_t, kMaxLabel + 1> _representative{};
  uint32_t _classes{0};
  State _start{nullptr};
  bstring _lower;
  bstring _suffix;
  std::vector<bstring> _literals;
  bool _finite{false};
  size_t _max_dfa_mem{kDefaultMaxDfaMem};

  mutable std::mutex _mutex;
  mutable absl::flat_hash_map<std::string_view, Row*> _rows;
  mutable std::vector<std::unique_ptr<std::byte[]>> _chunks;
  mutable size_t _chunk_used{kChunkBytes};
  mutable size_t _chunk_size{kChunkBytes};
  mutable size_t _arena_bytes{0};
  mutable size_t _dfa_mem{0};
  mutable Row* _dead{nullptr};
  mutable Row* _unknown{nullptr};
  mutable std::vector<int> _queue;
  mutable std::vector<uint32_t> _index;
  mutable std::vector<int> _stack;
  mutable std::vector<int> _canon;
  mutable std::vector<int> _resolved;
  mutable std::vector<uint32_t> _resolved_index;
};

}  // namespace irs
