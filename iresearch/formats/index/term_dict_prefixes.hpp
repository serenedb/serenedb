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

class PrefixTrie {
 public:
  static constexpr uint32_t kRoot = 0;

  struct Child {
    uint32_t label;
    uint32_t node;
  };

  PrefixTrie() = default;
  PrefixTrie(bytes_view encoded, field_id id);

  bool Empty() const noexcept { return _labels.empty(); }
  uint32_t Nodes() const noexcept {
    return static_cast<uint32_t>(_labels.size());
  }
  byte_type Label(uint32_t node) const noexcept { return _labels[node]; }
  bool Terminal(uint32_t node) const noexcept {
    return (_flags[node] & kTrieTerminal) != 0;
  }
  bool Open(uint32_t node) const noexcept {
    return (_flags[node] & kTrieOpen) != 0;
  }
  uint32_t FirstChild(uint32_t node) const noexcept { return _first[node]; }
  uint32_t EndChild(uint32_t node) const noexcept { return _first[node + 1]; }
  bool Leaf(uint32_t node) const noexcept {
    return _first[node] == _first[node + 1];
  }

  Child NextChild(uint32_t node, uint32_t from) const noexcept {
    const uint32_t begin = _first[node];
    const uint32_t end = _first[node + 1];
    if (from > std::numeric_limits<byte_type>::max()) {
      return {.label = kNoLabel, .node = kNoNode};
    }
    const auto* labels = _labels.data();
    const auto* at = std::lower_bound(labels + begin, labels + end,
                                      static_cast<byte_type>(from));
    if (at == labels + end) {
      return {.label = kNoLabel, .node = kNoNode};
    }
    return {.label = *at, .node = static_cast<uint32_t>(at - labels)};
  }

  uint32_t ChildOf(uint32_t node, byte_type label) const noexcept {
    const auto next = NextChild(node, label);
    return next.label == label ? next.node : kNoNode;
  }

  size_t MemoryBytes() const noexcept {
    return _labels.capacity() + _flags.capacity() +
           _first.capacity() * sizeof(uint32_t);
  }

 private:
  bstring _labels;
  bstring _flags;
  std::vector<uint32_t> _first;
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
