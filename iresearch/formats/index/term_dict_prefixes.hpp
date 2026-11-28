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
#include <duckdb/common/serializer/read_stream.hpp>
#include <duckdb/common/serializer/write_stream.hpp>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kTrieFull = 4;
inline constexpr uint32_t kTrieDepth = 16;
inline constexpr uint32_t kTrieSpan = 48;
inline constexpr uint32_t kTrieMaxSpan = 1024;
inline constexpr uint32_t kNoNode = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kNoLabel = 256;

inline constexpr byte_type kTrieTerminal = 1;
inline constexpr byte_type kTrieOpen = 2;
inline constexpr uint32_t kDenseFanout = 16;

struct TrieShape {
  bytes_view chain;
  std::span<const byte_type> flags;
  std::span<const byte_type> labels;
  std::span<const uint32_t> depths;
  std::span<const uint32_t> first;
};

class PrefixTrie {
 public:
  class Children {
   public:
    Children() = default;
    explicit Children(const uint64_t* words, uint32_t word, uint64_t bits,
                      uint32_t node) noexcept
      : _words{words},
        _bits{bits},
        _word{word},
        _label{word * 64 + static_cast<uint32_t>(std::countr_zero(bits))},
        _node{node} {}
    explicit Children(const byte_type* at, const byte_type* end,
                      uint32_t node) noexcept
      : _at{at},
        _end{end},
        _label{at != end ? uint32_t{*at} : kNoLabel},
        _node{node} {}

    uint32_t Label() const noexcept { return _label; }
    uint32_t Node() const noexcept { return _node; }

    void Next() noexcept {
      ++_node;
      if (_words) {
        _bits &= _bits - 1;
        while (_bits == 0) {
          if (++_word == 4) {
            _label = kNoLabel;
            return;
          }
          _bits = _words[_word];
        }
        _label = _word * 64 + static_cast<uint32_t>(std::countr_zero(_bits));
        return;
      }
      _label = ++_at != _end ? uint32_t{*_at} : kNoLabel;
    }

   private:
    const uint64_t* _words = nullptr;
    const byte_type* _at = nullptr;
    const byte_type* _end = nullptr;
    uint64_t _bits = 0;
    uint32_t _word = 0;
    uint32_t _label = kNoLabel;
    uint32_t _node = kNoNode;
  };

  explicit PrefixTrie(TrackedAllocator<> alloc);
  explicit PrefixTrie(const TrieShape& shape, TrackedAllocator<> alloc);
  PrefixTrie(duckdb::ReadStream& in, TrackedAllocator<> alloc);

  void Write(duckdb::WriteStream& out) const;

  bool Empty() const noexcept { return _nodes == 0; }
  uint32_t Nodes() const noexcept { return _nodes; }
  bytes_view Chain() const noexcept { return _chain; }
  uint32_t Root() const noexcept { return _chain.empty() ? 0 : _nodes; }
  bool Terminal(uint32_t node) const noexcept { return Bit(_terminal, node); }
  bool Open(uint32_t node) const noexcept { return Bit(_open, node); }

  bool Leaf(uint32_t node) const noexcept {
    if (node < _dense) {
      const auto* words = DenseWords(node);
      return (words[0] | words[1] | words[2] | words[3]) == 0;
    }
    if (node >= _nodes) {
      return false;
    }
    return !Bit(_inner, node - _dense);
  }

  Children ChildrenOf(uint32_t node, uint32_t from) const noexcept {
    if (from > std::numeric_limits<byte_type>::max()) {
      return {};
    }
    if (node < _dense) {
      const auto* words = DenseWords(node);
      uint32_t word = from / 64;
      uint64_t bits = words[word] & (~uint64_t{0} << (from % 64));
      while (bits == 0) {
        if (++word == 4) {
          return {};
        }
        bits = words[word];
      }
      const uint32_t label =
        word * 64 + static_cast<uint32_t>(std::countr_zero(bits));
      return Children{words, word, bits, DenseChild(node, words, label)};
    }
    if (node >= _nodes) {
      const uint32_t at = node - _nodes;
      const auto* label = _chain.data() + at;
      if (*label < from) {
        return {};
      }
      return Children{label, label + 1, at + 1 == _chain.size() ? 0 : node + 1};
    }
    const uint32_t sparse = node - _dense;
    if (!Bit(_inner, sparse)) {
      return {};
    }
    const uint32_t inner = InnerRank(sparse);
    const auto* labels = _labels.data();
    const auto* end = labels + (_first[inner + 1] - _dense);
    const auto* at = std::lower_bound(labels + (_first[inner] - _dense), end,
                                      static_cast<byte_type>(from));
    return Children{at, end, static_cast<uint32_t>(at - labels) + _dense};
  }

  uint32_t ChildOf(uint32_t node, byte_type label) const noexcept {
    if (node < _dense) {
      const auto* words = DenseWords(node);
      if ((words[label / 64] >> (label % 64) & 1) == 0) {
        return kNoNode;
      }
      return DenseChild(node, words, label);
    }
    const auto children = ChildrenOf(node, label);
    return children.Label() == label ? children.Node() : kNoNode;
  }

  size_t MemoryBytes() const noexcept {
    return (_dense_words.capacity() + _inner.capacity() + _terminal.capacity() +
            _open.capacity()) *
             sizeof(uint64_t) +
           (_dense_first.capacity() + _inner_rank.capacity() +
            _first.capacity()) *
             sizeof(uint32_t) +
           _labels.capacity() + _chain.capacity();
  }

 private:
  static bool Bit(std::span<const uint64_t> bits, uint32_t at) noexcept {
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

  TrackedVector<uint64_t> _dense_words;
  TrackedVector<uint32_t> _dense_first;
  TrackedBstring _chain;
  TrackedBstring _labels;
  TrackedVector<uint64_t> _inner;
  TrackedVector<uint32_t> _inner_rank;
  TrackedVector<uint32_t> _first;
  TrackedVector<uint64_t> _terminal;
  TrackedVector<uint64_t> _open;
  uint32_t _nodes = 0;
  uint32_t _dense = 0;
};

class PrefixTrieBuilder {
 public:
  explicit PrefixTrieBuilder(TrackedAllocator<> alloc)
    : _nodes{alloc},
      _open{alloc},
      _window{alloc},
      _window_bytes{alloc},
      _first{alloc} {}

  void Add(bytes_view term);
  void SetSpan(uint32_t span) noexcept { _span = span; }
  PrefixTrie Finish();
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

  struct Recent {
    size_t origin = 0;
    size_t size = 0;
  };

  uint32_t AppendChild(uint32_t parent, byte_type label);
  void Expand(uint32_t node, size_t from, size_t to);
  void Descend(bytes_view term);
  bool Expands(const Node& node) const noexcept {
    return node.expanded || node.depth < _origin + kTrieFull;
  }
  bytes_view Stored(size_t slot) const noexcept;
  byte_type LabelAt(size_t slot, size_t depth) const noexcept;
  size_t SharedWithLast(bytes_view term, size_t shared) const noexcept;
  void Remember(bytes_view term);

  TrackedVector<Node> _nodes;
  TrackedVector<uint32_t> _open;
  TrackedVector<Recent> _window;
  TrackedVector<byte_type> _window_bytes;
  TrackedBstring _first;
  size_t _origin = 0;
  size_t _added = 0;
  uint32_t _span = kTrieSpan;
};

}  // namespace irs::term_dict
