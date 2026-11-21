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

#include "iresearch/formats/index/term_dict_prefixes.hpp"

#include <bit>
#include <iterator>
#include <utility>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs::term_dict {
namespace {

constexpr size_t kWindow = kTrieSpan + 1;

}  // namespace

PrefixTrie::PrefixTrie(bytes_view encoded, field_id id) {
  const auto corrupted = [&] { Corrupted("prefix trie", id); };
  const auto* p = encoded.data();
  const auto* const end = p + encoded.size();
  const auto count = ReadVarint<uint32_t>(p, end);
  if (!count || *count == 0 || *count > encoded.size()) {
    corrupted();
  }
  const uint32_t nodes = *count;
  std::vector<byte_type> flags(nodes);
  std::vector<byte_type> labels(nodes);
  std::vector<uint32_t> depths(nodes);
  std::vector<uint32_t> first(size_t{nodes} + 1);
  uint32_t next = 1;
  for (uint32_t node = 0; node != nodes; ++node) {
    if (p == end || (node != 0 && node >= next)) {
      corrupted();
    }
    const byte_type flag = *p++;
    const auto children = ReadVarint<uint32_t>(p, end);
    if (!children || (flag & ~(kTrieTerminal | kTrieOpen)) != 0 ||
        *children > static_cast<size_t>(end - p) || *children > nodes - next ||
        ((flag & kTrieOpen) != 0 && *children != 0)) {
      corrupted();
    }
    flags[node] = flag;
    first[node] = next;
    for (uint32_t i = 0; i != *children; ++i) {
      if (i != 0 && p[i] <= p[i - 1]) {
        corrupted();
      }
      labels[next + i] = p[i];
      depths[next + i] = depths[node] + 1;
    }
    p += *children;
    next += *children;
  }
  first[nodes] = next;
  if (next != nodes || p != end) {
    corrupted();
  }

  uint32_t dense = 0;
  while (dense != nodes) {
    uint32_t level_end = dense;
    uint64_t children = 0;
    while (level_end != nodes && depths[level_end] == depths[dense]) {
      children += first[level_end + 1] - first[level_end];
      ++level_end;
    }
    if (children < uint64_t{kDenseFanout} * (level_end - dense)) {
      break;
    }
    dense = level_end;
  }

  _nodes = nodes;
  _dense = dense;
  _dense_words.assign(4 * size_t{dense}, 0);
  _dense_first.resize(dense);
  for (uint32_t node = 0; node != dense; ++node) {
    _dense_first[node] = first[node];
    auto* words = _dense_words.data() + 4 * size_t{node};
    for (auto child = first[node]; child != first[node + 1]; ++child) {
      words[labels[child] / 64] |= uint64_t{1} << (labels[child] % 64);
    }
  }
  const uint32_t sparse = nodes - dense;
  _labels.assign(labels.data() + dense, sparse);
  _inner.assign((size_t{sparse} + 63) / 64, 0);
  _inner_rank.resize(_inner.size());
  for (uint32_t at = 0; at != sparse; ++at) {
    const uint32_t node = dense + at;
    if (first[node + 1] != first[node]) {
      _inner[at / 64] |= uint64_t{1} << (at % 64);
      _first.push_back(first[node]);
    }
  }
  _first.push_back(nodes);
  uint32_t rank = 0;
  for (size_t word = 0; word != _inner.size(); ++word) {
    _inner_rank[word] = rank;
    rank += static_cast<uint32_t>(std::popcount(_inner[word]));
  }
  _terminal.assign((size_t{nodes} + 63) / 64, 0);
  _open.assign(_terminal.size(), 0);
  for (uint32_t node = 0; node != nodes; ++node) {
    if ((flags[node] & kTrieTerminal) != 0) {
      _terminal[node / 64] |= uint64_t{1} << (node % 64);
    }
    if ((flags[node] & kTrieOpen) != 0) {
      _open[node / 64] |= uint64_t{1} << (node % 64);
    }
  }
}

uint32_t PrefixTrieBuilder::AppendChild(uint32_t parent, byte_type label) {
  const auto child = static_cast<uint32_t>(_nodes.size());
  _nodes.push_back({
    .depth = _nodes[parent].depth + 1,
    .label = label,
  });
  auto& node = _nodes[parent];
  if (node.last == kNoNode) {
    node.first = child;
  } else {
    _nodes[node.last].next = child;
  }
  node.last = child;
  return child;
}

void PrefixTrieBuilder::Expand(uint32_t node, size_t from) {
  _nodes[node].expanded = true;
  const size_t depth = _nodes[node].depth;
  uint32_t child = kNoNode;
  size_t child_from = 0;
  const auto close = [&] {
    if (child != kNoNode && _nodes[child].count > kTrieSpan &&
        _nodes[child].depth < kTrieDepth) {
      Expand(child, child_from);
    }
  };
  for (size_t i = from; i != _added; ++i) {
    const bytes_view term = _window[i % kWindow];
    if (term.size() == depth) {
      _nodes[node].terminal = true;
      continue;
    }
    const byte_type label = term[depth];
    if (child == kNoNode || _nodes[child].label != label) {
      close();
      child = AppendChild(node, label);
      child_from = i;
    }
    ++_nodes[child].count;
    if (term.size() == depth + 1) {
      _nodes[child].terminal = true;
    }
  }
  close();
}

void PrefixTrieBuilder::Descend(bytes_view term) {
  for (;;) {
    const uint32_t node = _open.back();
    const size_t depth = _nodes[node].depth;
    if (depth == term.size()) {
      _nodes[node].terminal = true;
      return;
    }
    if (!Expands(_nodes[node])) {
      return;
    }
    const byte_type label = term[depth];
    uint32_t child = _nodes[node].last;
    if (child == kNoNode || _nodes[child].label != label) {
      child = AppendChild(node, label);
    }
    _open.push_back(child);
  }
}

void PrefixTrieBuilder::Add(bytes_view term) {
  if (_nodes.empty()) {
    _nodes.push_back({.expanded = true});
    _open.assign(1, 0);
    _window.resize(kWindow);
  }
  size_t lcp = 0;
  if (_added != 0) {
    lcp = CommonPrefixLength(bytes_view{_window[(_added - 1) % kWindow]}, term);
  }
  _window[_added % kWindow].assign(term);
  ++_added;
  _open.resize(std::min(_open.size(), lcp + 1));
  Descend(term);
  for (size_t at = 0; at != _open.size(); ++at) {
    auto& node = _nodes[_open[at]];
    ++node.count;
    if (!Expands(node) && node.count > kTrieSpan && node.depth < kTrieDepth) {
      Expand(_open[at], _added - node.count);
      _open.resize(at + 1);
      Descend(term);
      break;
    }
  }
}

std::string PrefixTrieBuilder::Finish() {
  std::string out;
  if (_nodes.empty()) {
    return out;
  }
  auto it = std::back_inserter(out);
  WriteVarint(static_cast<uint32_t>(_nodes.size()), it);
  std::vector<uint32_t> queue{0};
  queue.reserve(_nodes.size());
  for (size_t at = 0; at != queue.size(); ++at) {
    const auto& node = _nodes[queue[at]];
    uint32_t children = 0;
    for (auto child = node.first; child != kNoNode;
         child = _nodes[child].next) {
      queue.push_back(child);
      ++children;
    }
    byte_type flags = node.terminal ? kTrieTerminal : 0;
    if (children == 0 && node.count > (node.terminal ? 1U : 0U)) {
      flags |= kTrieOpen;
    }
    out.push_back(static_cast<char>(flags));
    WriteVarint(children, it);
    for (auto child = node.first; child != kNoNode;
         child = _nodes[child].next) {
      out.push_back(static_cast<char>(_nodes[child].label));
    }
  }
  Reset();
  return out;
}

void PrefixTrieBuilder::Reset() noexcept {
  _nodes.clear();
  _open.clear();
  _window.clear();
  _added = 0;
}

}  // namespace irs::term_dict
