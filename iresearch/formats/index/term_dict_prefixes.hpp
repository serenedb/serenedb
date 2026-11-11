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

#include <absl/algorithm/container.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kTrieExactBytes = 4;
inline constexpr uint64_t kTrieLeafTerms = 1;
inline constexpr uint64_t kTrieBytesPerTerm = 2;
inline constexpr uint64_t kTrieMinBudget = 16384;

struct TrieCursor {
  uint32_t node;
  uint32_t labels;
  uint32_t matched;
};

inline constexpr uint32_t kTrieBelow = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kTrieMissing = kTrieBelow - 1;
inline constexpr uint32_t kTrieEnd = kTrieMissing - 1;

class PrefixTrie {
 public:
  static constexpr size_t NodeBytes(uint32_t fanout,
                                    uint32_t edge_size) noexcept {
    return fanout + (3 * fanout + 7) / 8 + 2 +
           (edge_size == 0 ? 0 : edge_size + sizeof(uint32_t));
  }

  PrefixTrie(bytes_view encoded, field_id id);

  size_t MemoryBytes() const noexcept {
    return sizeof(*this) + _labels.capacity() + _fanouts.capacity() +
           _edges.capacity() +
           (_inner.capacity() + _ends.capacity() + _terminal.capacity() +
            _edged.capacity()) *
             sizeof(uint64_t) +
           (_start_samples.capacity() + _inner_ranks.capacity() +
            _edged_ranks.capacity() + _edge_starts.capacity()) *
             sizeof(uint32_t);
  }

  static TrieCursor Root() noexcept {
    return {.node = 0, .labels = 0, .matched = 0};
  }

  bytes_view Edge(TrieCursor cursor) const noexcept {
    return EdgeOf(cursor.node).substr(cursor.matched);
  }

  bool Terminal(TrieCursor cursor) const noexcept {
    return cursor.node == kTrieEnd ||
           (Bit(_terminal, cursor.node) &&
            cursor.matched == EdgeOf(cursor.node).size());
  }

  bytes_view Labels(TrieCursor cursor) const noexcept {
    if (cursor.node == kTrieEnd) {
      return {};
    }
    const auto edge = EdgeOf(cursor.node);
    if (cursor.matched != edge.size()) {
      return edge.substr(cursor.matched, 1);
    }
    return {_labels.data() + cursor.labels, _fanouts[cursor.node] + size_t{1}};
  }

  uint32_t NextLabel(TrieCursor cursor, uint32_t from) const noexcept {
    const auto labels = Labels(cursor);
    const auto it = absl::c_lower_bound(labels, from);
    return it == labels.end() ? kNoLabel : *it;
  }

  TrieCursor ChildAt(TrieCursor cursor, uint32_t slot) const noexcept {
    if (cursor.matched != EdgeOf(cursor.node).size()) {
      return {.node = cursor.node,
              .labels = cursor.labels,
              .matched = cursor.matched + 1};
    }
    const auto at = cursor.labels + slot;
    if (!Bit(_inner, at)) {
      return {.node = Bit(_ends, at) ? kTrieEnd : kTrieBelow,
              .labels = 0,
              .matched = 0};
    }
    const auto child = Rank(_inner, _inner_ranks, at) + 1;
    return {.node = child, .labels = Start(child), .matched = 0};
  }

  TrieCursor Child(TrieCursor cursor, byte_type label) const noexcept {
    const auto labels = Labels(cursor);
    const auto it = absl::c_lower_bound(labels, label);
    if (it == labels.end() || *it != label) {
      return {.node = kTrieMissing, .labels = 0, .matched = 0};
    }
    return ChildAt(cursor, static_cast<uint32_t>(it - labels.begin()));
  }

 private:
  static bool Bit(std::span<const uint64_t> bits, uint32_t at) noexcept {
    return (bits[at / 64] >> (at % 64) & 1) != 0;
  }

  static uint32_t Rank(std::span<const uint64_t> bits,
                       std::span<const uint32_t> ranks, uint32_t at) noexcept {
    const uint64_t below = (uint64_t{1} << (at % 64)) - 1;
    return ranks[at / 64] +
           static_cast<uint32_t>(std::popcount(bits[at / 64] & below));
  }

  bytes_view EdgeOf(uint32_t node) const noexcept {
    if (node == kTrieEnd || !Bit(_edged, node)) {
      return {};
    }
    const auto i = Rank(_edged, _edged_ranks, node);
    return bytes_view{_edges}.substr(_edge_starts[i],
                                     _edge_starts[i + 1] - _edge_starts[i]);
  }

  static constexpr uint32_t kStartStride = 8;

  uint32_t Start(uint32_t node) const noexcept {
    static_assert(std::endian::native == std::endian::little);
    const uint32_t group = node / kStartStride;
    const uint32_t skip = node % kStartStride;
    uint64_t fanouts = 0;
    std::memcpy(&fanouts, _fanouts.data() + group * kStartStride,
                sizeof(fanouts));
    fanouts &= (uint64_t{1} << (8 * skip)) - 1;
    fanouts =
      (fanouts & 0x00FF00FF00FF00FF) + (fanouts >> 8 & 0x00FF00FF00FF00FF);
    return _start_samples[group] + skip +
           static_cast<uint32_t>(fanouts * 0x0001000100010001 >> 48);
  }

  bstring _labels;
  bstring _fanouts;
  std::vector<uint32_t> _start_samples;
  std::vector<uint64_t> _inner;
  std::vector<uint32_t> _inner_ranks;
  std::vector<uint64_t> _ends;
  std::vector<uint64_t> _terminal;
  std::vector<uint64_t> _edged;
  std::vector<uint32_t> _edged_ranks;
  std::vector<uint32_t> _edge_starts;
  bstring _edges;
};

class PrefixTrieBuilder {
 public:
  void Add(bytes_view term);
  std::string Finish();
  void Clear() noexcept;

 private:
  static constexpr uint32_t kLeaf = std::numeric_limits<uint32_t>::max();
  static constexpr uint32_t kEnd = kLeaf - 1;

  struct Child {
    uint32_t node;
    byte_type label;
  };

  struct Built {
    uint32_t count;
    uint32_t depth;
    uint32_t edge_start;
    uint16_t fanout;
    bool terminal;
  };

  struct Frame {
    uint64_t count;
    uint32_t begin;
    bool terminal;
  };

  static bool Adaptive(const Built& node) noexcept {
    return node.depth >= kTrieExactBytes;
  }

  static uint32_t EdgeSize(const Built& node) noexcept {
    return node.depth - node.edge_start;
  }

  static byte_type KindOf(uint32_t node) noexcept;

  void Deepen(size_t depth);
  void CloseTo(size_t depth);
  uint32_t MakeNode(size_t depth, const Frame& frame);
  void Materialize();
  void Shrink(uint64_t budget);
  void Prune();
  void Encode(uint32_t root, std::string& out) const;

  bstring _prev;
  std::vector<Frame> _frames;
  std::vector<Child> _pending;
  std::vector<Built> _built;
  bstring _labels;
  bstring _kinds;
  std::vector<uint32_t> _inner;
  bstring _edges;
  uint64_t _terms = 0;
  uint64_t _limit = kTrieLeafTerms;
  uint64_t _bytes = 0;
  uint32_t _carry = kLeaf;
};

}  // namespace irs::term_dict
