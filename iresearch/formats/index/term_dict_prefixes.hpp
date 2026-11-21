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

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kTrieFull = 4;
inline constexpr uint32_t kTrieDepth = 16;
inline constexpr uint32_t kTrieSpan = 48;
inline constexpr uint32_t kNoNode = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kNoLabel = 256;

inline constexpr byte_type kTrieTerminal = 1;
inline constexpr byte_type kTrieOpen = 2;
inline constexpr uint32_t kDenseFanout = 16;

class PrefixTrie {
 public:
  static constexpr uint32_t kRoot = 0;

  struct Child {
    uint32_t label;
    uint32_t node;
  };

  PrefixTrie() = default;
  PrefixTrie(bytes_view encoded, field_id id);

  bool Empty() const noexcept { return _nodes == 0; }
  uint32_t Nodes() const noexcept { return _nodes; }
  uint32_t DenseNodes() const noexcept { return _dense; }
  bool Terminal(uint32_t node) const noexcept { return Bit(_terminal, node); }
  bool Open(uint32_t node) const noexcept { return Bit(_open, node); }

  bool Leaf(uint32_t node) const noexcept {
    if (node < _dense) {
      const auto* words = DenseWords(node);
      return (words[0] | words[1] | words[2] | words[3]) == 0;
    }
    return !Bit(_inner, node - _dense);
  }

  Child NextChild(uint32_t node, uint32_t from) const noexcept {
    constexpr Child kNone{.label = kNoLabel, .node = kNoNode};
    if (from > std::numeric_limits<byte_type>::max()) {
      return kNone;
    }
    if (node < _dense) {
      const auto* words = DenseWords(node);
      for (uint32_t word = from / 64; word != 4; ++word) {
        uint64_t bits = words[word];
        if (word == from / 64) {
          bits &= ~uint64_t{0} << (from % 64);
        }
        if (bits != 0) {
          const uint32_t label =
            word * 64 + static_cast<uint32_t>(std::countr_zero(bits));
          return {.label = label, .node = DenseChild(node, words, label)};
        }
      }
      return kNone;
    }
    const uint32_t sparse = node - _dense;
    if (!Bit(_inner, sparse)) {
      return kNone;
    }
    const uint32_t inner = InnerRank(sparse);
    const auto* labels = _labels.data();
    const auto* begin = labels + (_first[inner] - _dense);
    const auto* end = labels + (_first[inner + 1] - _dense);
    const auto* at = std::lower_bound(begin, end, static_cast<byte_type>(from));
    if (at == end) {
      return kNone;
    }
    return {.label = *at, .node = static_cast<uint32_t>(at - labels) + _dense};
  }

  uint32_t ChildOf(uint32_t node, byte_type label) const noexcept {
    if (node < _dense) {
      const auto* words = DenseWords(node);
      if ((words[label / 64] >> (label % 64) & 1) == 0) {
        return kNoNode;
      }
      return DenseChild(node, words, label);
    }
    const auto next = NextChild(node, label);
    return next.label == label ? next.node : kNoNode;
  }

  size_t MemoryBytes() const noexcept {
    return (_dense_words.capacity() + _inner.capacity() + _terminal.capacity() +
            _open.capacity()) *
             sizeof(uint64_t) +
           (_dense_first.capacity() + _inner_rank.capacity() +
            _first.capacity()) *
             sizeof(uint32_t) +
           _labels.capacity();
  }

 private:
  static bool Bit(const std::vector<uint64_t>& bits, uint32_t at) noexcept {
    return (bits[at / 64] >> (at % 64) & 1) != 0;
  }
  const uint64_t* DenseWords(uint32_t node) const noexcept {
    return _dense_words.data() + 4 * size_t{node};
  }
  uint32_t DenseChild(uint32_t node, const uint64_t* words,
                      uint32_t label) const noexcept {
    uint32_t below = 0;
    for (uint32_t word = 0; word != label / 64; ++word) {
      below += static_cast<uint32_t>(std::popcount(words[word]));
    }
    const uint64_t mask = (uint64_t{1} << (label % 64)) - 1;
    below += static_cast<uint32_t>(std::popcount(words[label / 64] & mask));
    return _dense_first[node] + below;
  }
  uint32_t InnerRank(uint32_t sparse) const noexcept {
    const uint64_t mask = (uint64_t{1} << (sparse % 64)) - 1;
    return _inner_rank[sparse / 64] +
           static_cast<uint32_t>(std::popcount(_inner[sparse / 64] & mask));
  }

  std::vector<uint64_t> _dense_words;
  std::vector<uint32_t> _dense_first;
  bstring _labels;
  std::vector<uint64_t> _inner;
  std::vector<uint32_t> _inner_rank;
  std::vector<uint32_t> _first;
  std::vector<uint64_t> _terminal;
  std::vector<uint64_t> _open;
  uint32_t _nodes = 0;
  uint32_t _dense = 0;
};

class PrefixTrieBuilder {
 public:
  void Add(bytes_view term);
  std::string Finish();
  void Reset() noexcept;

 private:
  struct Node {
    uint32_t first = kNoNode;
    uint32_t last = kNoNode;
    uint32_t next = kNoNode;
    uint32_t count = 0;
    uint32_t depth = 0;
    byte_type label = 0;
    bool terminal = false;
    bool expanded = false;
  };

  uint32_t AppendChild(uint32_t parent, byte_type label);
  void Expand(uint32_t node, size_t from);
  void Descend(bytes_view term);
  bool Expands(const Node& node) const noexcept {
    return node.expanded || node.depth < kTrieFull;
  }

  std::vector<Node> _nodes;
  std::vector<uint32_t> _open;
  std::vector<bstring> _window;
  size_t _added = 0;
};

}  // namespace irs::term_dict
