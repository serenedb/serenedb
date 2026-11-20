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
#include <absl/base/internal/endian.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kNoLabel = 256;
inline constexpr uint64_t kTrieTermsPerByte = 2;
inline constexpr uint64_t kTrieMinBudget = 16384;

struct TrieCursor {
  uint32_t node;
  uint32_t labels;
  uint32_t matched;
  uint32_t edge = 0;
  uint32_t edge_size = 0;
  uint32_t lo = 0;
  uint32_t hi = 0;
  uint16_t fanout = 0;
  uint8_t width = 0;
  bool terminal = false;
};

inline constexpr uint32_t kTrieBelow = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kTrieMissing = kTrieBelow - 1;
inline constexpr TrieCursor kTrieUngated{
  .node = kTrieBelow, .labels = 0, .matched = 0};

enum class TrieKind : uint8_t {
  Leaf = 0,
  Inner = 1,
};

inline constexpr uint32_t kTrieWidths[]{0, 1, 2, 4};
inline constexpr byte_type kTrieWidthMask = 3;
inline constexpr byte_type kTrieTerminal = 4;

struct TrieNode {
  const byte_type* edge;
  const byte_type* labels;
  const byte_type* kinds;
  const byte_type* deltas;
  const byte_type* children;
  uint32_t edge_size;
  uint32_t fanout;
  uint32_t width;
  bool terminal;

  TrieKind KindAt(uint32_t slot) const noexcept {
    return static_cast<TrieKind>(kinds[slot / 4] >> (2 * (slot % 4)) & 3);
  }
  uint32_t BlockAt(uint32_t slot, uint32_t lo) const noexcept {
    switch (width) {
      case 1:
        return lo + deltas[slot];
      case 2:
        return lo + absl::little_endian::Load16(deltas + 2 * slot);
      case 4:
        return lo + absl::little_endian::Load32(deltas + 4 * slot);
      default:
        return lo;
    }
  }
  uint32_t ChildAt(uint32_t slot) const noexcept {
    constexpr uint64_t kLow = 0x5555555555555555;
    const uint32_t bytes = slot / 4;
    uint32_t inner = 0;
    uint32_t i = 0;
    for (; i + sizeof(uint64_t) <= bytes; i += sizeof(uint64_t)) {
      uint64_t word;
      std::memcpy(&word, kinds + i, sizeof(word));
      inner += std::popcount(word & ~(word >> 1) & kLow);
    }
    uint64_t tail = 0;
    for (uint32_t shift = 0; i != bytes; ++i, shift += 8) {
      tail |= uint64_t{kinds[i]} << shift;
    }
    inner += std::popcount(tail & ~(tail >> 1) & kLow);
    if (const uint32_t rest = slot % 4; rest != 0) {
      const uint32_t last = kinds[bytes] & ((1U << (2 * rest)) - 1);
      inner += std::popcount(last & ~(last >> 1) & 0x55U);
    }
    return absl::little_endian::Load32(children + 4 * inner);
  }
};

class PrefixTrie {
 public:
  static constexpr size_t EdgeBytes(uint32_t edge_size) noexcept {
    size_t bytes = 1;
    for (uint32_t rest = edge_size >> 7; rest != 0; rest >>= 7) {
      ++bytes;
    }
    return bytes + edge_size;
  }

  static constexpr size_t NodeBytes(uint32_t fanout, uint32_t edge_size,
                                    uint32_t inner) noexcept {
    return 2 + EdgeBytes(edge_size) + fanout + (fanout + 3) / 4 +
           4 * size_t{inner};
  }

  static constexpr size_t BlockBytes(uint32_t fanout, uint32_t width) noexcept {
    return width * size_t{fanout};
  }

  static bool Gates(TrieCursor cursor) noexcept {
    return cursor.node < kTrieMissing;
  }

  PrefixTrie() = default;
  PrefixTrie(bytes_view encoded, field_id id);

  size_t MemoryBytes() const noexcept {
    return sizeof(*this) + _nodes.capacity();
  }

  uint32_t Blocks() const noexcept { return _blocks; }

  TrieCursor Root() const noexcept {
    return _nodes.empty() ? kTrieUngated : At(_root, 0, _blocks - 1);
  }

  bytes_view Edge(TrieCursor cursor) const noexcept {
    return {_nodes.data() + cursor.edge + cursor.matched,
            cursor.edge_size - cursor.matched};
  }

  static bool MayEnd(TrieCursor cursor) noexcept {
    return !Gates(cursor) ||
           (cursor.terminal && cursor.matched == cursor.edge_size);
  }

  bytes_view Labels(TrieCursor cursor) const noexcept {
    SDB_ASSERT(Gates(cursor));
    if (cursor.matched != cursor.edge_size) {
      return {_nodes.data() + cursor.edge + cursor.matched, 1};
    }
    return {_nodes.data() + cursor.labels, cursor.fanout};
  }

  TrieCursor ChildAt(TrieCursor cursor, uint32_t slot) const noexcept {
    SDB_ASSERT(Gates(cursor));
    if (cursor.matched != cursor.edge_size) {
      ++cursor.matched;
      return cursor;
    }
    const auto node = NodeOf(cursor);
    const uint32_t lo = node.BlockAt(slot, cursor.lo);
    const uint32_t hi = slot + 1 == node.fanout
                          ? cursor.hi
                          : node.BlockAt(slot + 1, cursor.lo);
    if (node.KindAt(slot) == TrieKind::Inner) {
      return At(node.ChildAt(slot), lo, hi);
    }
    return {.node = kTrieBelow, .labels = 0, .matched = 0, .lo = lo, .hi = hi};
  }

  uint32_t SlotReaching(TrieCursor cursor, uint32_t slot,
                        uint32_t block) const noexcept;

  TrieCursor Child(TrieCursor cursor, byte_type label) const noexcept {
    const auto labels = Labels(cursor);
    const auto it = absl::c_lower_bound(labels, label);
    if (it == labels.end() || *it != label) {
      return {.node = kTrieMissing, .labels = 0, .matched = 0};
    }
    return ChildAt(cursor, static_cast<uint32_t>(it - labels.begin()));
  }

 private:
  TrieNode NodeOf(TrieCursor cursor) const noexcept {
    const auto* labels = _nodes.data() + cursor.labels;
    const auto* kinds = labels + cursor.fanout;
    const auto* deltas = kinds + (cursor.fanout + 3) / 4;
    return {
      .edge = _nodes.data() + cursor.edge,
      .labels = labels,
      .kinds = kinds,
      .deltas = deltas,
      .children = deltas + size_t{cursor.width} * cursor.fanout,
      .edge_size = cursor.edge_size,
      .fanout = cursor.fanout,
      .width = cursor.width,
      .terminal = cursor.terminal,
    };
  }
  TrieNode Node(uint32_t offset) const noexcept;
  TrieCursor At(uint32_t offset, uint32_t lo, uint32_t hi) const noexcept;

  bstring _nodes;
  uint32_t _root = 0;
  uint32_t _blocks = 0;
};

class PrefixTrieBuilder {
 public:
  void Add(bytes_view key, uint32_t block);
  std::string Finish();
  void Reset() noexcept;

 private:
  static constexpr uint32_t kLeaf = std::numeric_limits<uint32_t>::max();

  struct Child {
    uint32_t node;
    uint32_t block;
    byte_type label;
  };

  struct Built {
    uint32_t count;
    uint32_t depth;
    uint32_t edge_start;
    uint16_t fanout;
    bool spans;
    bool terminal;
  };

  struct Frame {
    uint64_t count;
    uint32_t begin;
    uint32_t first_block;
    uint32_t last_block;
    uint32_t level;
    bool terminal;
  };

  struct Rank {
    uint32_t level;
    uint64_t count;
  };

  static constexpr Rank kKeepAll{.level = std::numeric_limits<uint32_t>::max(),
                                 .count = 0};

  static bool Prunable(const Built& node) noexcept {
    return node.edge_start != 0;
  }

  static size_t BytesOf(const Built& node, uint32_t edge_size,
                        uint32_t inner) noexcept {
    return PrefixTrie::NodeBytes(node.fanout, edge_size, inner) +
           (node.spans ? node.fanout : 0);
  }

  static bool PrunedBefore(Rank lhs, Rank rhs) noexcept {
    return lhs.level != rhs.level ? lhs.level > rhs.level
                                  : lhs.count < rhs.count;
  }

  static uint32_t EdgeSize(const Built& node) noexcept {
    return node.depth - node.edge_start;
  }

  bool Drops(Rank rank) const noexcept { return !PrunedBefore(_cut, rank); }

  uint32_t InnerOf(uint32_t from, uint32_t fanout) const noexcept;
  void Push(uint32_t block);
  bool Branches(size_t frame) const noexcept;
  void Deepen(size_t depth);
  void CloseTo(size_t depth);
  uint32_t MakeNode(size_t depth, const Frame& frame);
  void Materialize();
  void Prune(uint64_t target);
  void Encode(uint32_t root, std::string& out) const;

  bstring _prev;
  std::vector<Frame> _frames;
  std::vector<Child> _pending;
  std::vector<Built> _built;
  bstring _labels;
  std::vector<uint32_t> _nodes;
  std::vector<uint32_t> _blocks;
  bstring _edges;
  uint64_t _terms = 0;
  Rank _cut = kKeepAll;
  uint64_t _bytes = 0;
  uint32_t _carry = kLeaf;
  uint32_t _prev_block = 0;
};

}  // namespace irs::term_dict
