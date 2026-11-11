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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <iterator>
#include <span>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs::term_dict {
namespace {

constexpr uint32_t kLeafKind = 0;
constexpr uint32_t kInnerKind = 1;
constexpr uint32_t kEndKind = 2;

uint64_t Budget(uint64_t terms) noexcept {
  return std::max(kTrieBytesPerTerm * terms, kTrieMinBudget);
}

void SetBit(std::vector<uint64_t>& bits, uint64_t at) {
  if (bits.size() <= at / 64) {
    bits.resize(at / 64 + 1);
  }
  bits[at / 64] |= uint64_t{1} << (at % 64);
}

std::vector<uint32_t> RanksOf(std::span<const uint64_t> bits) {
  std::vector<uint32_t> ranks;
  ranks.reserve(bits.size());
  uint32_t rank = 0;
  for (const uint64_t word : bits) {
    ranks.push_back(rank);
    rank += static_cast<uint32_t>(std::popcount(word));
  }
  return ranks;
}

}  // namespace

PrefixTrie::PrefixTrie(bytes_view encoded, field_id id) {
  const auto corrupted = [&] { Corrupted("prefix trie", id); };
  const auto* p = encoded.data();
  const auto* const end = p + encoded.size();
  uint64_t referenced = 1;
  uint32_t nodes = 0;
  _edge_starts.push_back(0);
  while (p != end) {
    if (nodes >= referenced) {
      corrupted();
    }
    const auto header = ReadVarint<uint32_t>(p, end);
    if (!header) {
      corrupted();
    }
    const uint32_t edge_size = *header >> 1;
    if (edge_size >= static_cast<size_t>(end - p)) {
      corrupted();
    }
    const std::span edge{p, edge_size};
    p += edge_size;
    const uint32_t fanout = *p++ + 1U;
    const uint32_t kind_bytes = (fanout + 3) / 4;
    if (static_cast<size_t>(end - p) < fanout + kind_bytes) {
      corrupted();
    }
    const std::span labels{p, fanout};
    const std::span kinds{p + fanout, kind_bytes};
    p += fanout + kind_bytes;
    if (absl::c_adjacent_find(
          labels, [](byte_type lhs, byte_type rhs) { return lhs >= rhs; }) !=
          labels.end() ||
        (fanout % 4 != 0 && kinds.back() >> (2 * (fanout % 4)) != 0) ||
        _labels.size() + _edges.size() + fanout + edge_size >
          std::numeric_limits<uint32_t>::max()) {
      corrupted();
    }
    if (edge_size != 0) {
      SetBit(_edged, nodes);
      _edges.append(edge.data(), edge.size());
      _edge_starts.push_back(static_cast<uint32_t>(_edges.size()));
    }
    if ((*header & 1) != 0) {
      SetBit(_terminal, nodes);
    }
    const auto first = _labels.size();
    _fanouts.push_back(static_cast<byte_type>(fanout - 1));
    if (nodes % kStartStride == 0) {
      _start_samples.push_back(static_cast<uint32_t>(first));
    }
    _labels.append(labels.data(), labels.size());
    for (uint32_t i = 0; i != fanout; ++i) {
      switch (kinds[i / 4] >> (2 * (i % 4)) & 3) {
        case kLeafKind:
          break;
        case kInnerKind:
          SetBit(_inner, first + i);
          ++referenced;
          break;
        case kEndKind:
          SetBit(_ends, first + i);
          break;
        default:
          corrupted();
      }
    }
    if (referenced >= kTrieMissing) {
      corrupted();
    }
    ++nodes;
  }
  if (nodes == 0 || nodes != referenced) {
    corrupted();
  }
  _fanouts.resize((nodes + kStartStride - 1) / kStartStride * kStartStride);
  _inner.resize((_labels.size() + 63) / 64);
  _ends.resize((_labels.size() + 63) / 64);
  _terminal.resize((nodes + 63) / 64);
  _edged.resize((nodes + 63) / 64);
  _inner_ranks = RanksOf(_inner);
  _edged_ranks = RanksOf(_edged);
  _labels.shrink_to_fit();
  _edges.shrink_to_fit();
  _fanouts.shrink_to_fit();
  _start_samples.shrink_to_fit();
  _edge_starts.shrink_to_fit();
}

void PrefixTrieBuilder::Add(bytes_view term) {
  size_t last = std::min<size_t>(term.size(), kTrieExactBytes);
  if (_frames.empty()) {
    _frames.push_back({.count = 0, .begin = 0, .terminal = false});
  } else {
    SDB_ASSERT(bytes_view{_prev} < term);
    const size_t lcp = CommonPrefixLength(bytes_view{_prev}, term);
    Deepen(lcp + 1);
    CloseTo(lcp);
    Materialize();
    last = std::max(last, lcp + 1);
  }
  const auto begin = static_cast<uint32_t>(_pending.size());
  for (size_t depth = _frames.size(); depth <= last; ++depth) {
    _frames.push_back({.count = 0, .begin = begin, .terminal = false});
  }
  _frames.back().terminal = term.size() == last;
  ++_frames.back().count;
  _prev.assign(term);
  ++_terms;
  Shrink(2 * Budget(_terms));
}

void PrefixTrieBuilder::Deepen(size_t depth) {
  const size_t last = std::min(_prev.size(), depth);
  if (_frames.size() > last) {
    return;
  }
  --_frames.back().count;
  const auto begin = static_cast<uint32_t>(_pending.size());
  for (size_t at = _frames.size(); at <= last; ++at) {
    _frames.push_back({.count = 0, .begin = begin, .terminal = false});
  }
  _frames.back().terminal = _prev.size() == last;
  ++_frames.back().count;
}

void PrefixTrieBuilder::CloseTo(size_t depth) {
  while (_frames.size() > depth + 1) {
    const size_t at = _frames.size() - 1;
    const Frame frame = _frames.back();
    _frames.pop_back();
    _frames.back().count += frame.count;
    const byte_type label = _prev[at - 1];
    if (at < kTrieExactBytes && _pending.size() == frame.begin) {
      SDB_ASSERT(frame.terminal && frame.count == 1);
      _pending.push_back({.node = kEnd, .label = label});
      continue;
    }
    if (at >= kTrieExactBytes && frame.count <= _limit) {
      SDB_ASSERT(_carry == kLeaf);
      SDB_ASSERT(
        absl::c_all_of(std::span{_pending}.subspan(frame.begin),
                       [](const Child& child) { return child.node == kLeaf; }));
      _pending.resize(frame.begin);
      _pending.push_back({.node = kLeaf, .label = label});
      continue;
    }
    if (!frame.terminal && _pending.size() == frame.begin + 1 &&
        _pending.back().node < kEnd) {
      SDB_ASSERT(_pending.back().node == _carry);
      _built[_carry].edge_start = static_cast<uint32_t>(at);
      _pending.back().label = label;
      continue;
    }
    Materialize();
    _carry = MakeNode(at, frame);
    _pending.push_back({.node = _carry, .label = label});
  }
}

byte_type PrefixTrieBuilder::KindOf(uint32_t node) noexcept {
  return static_cast<byte_type>(node == kLeaf  ? kLeafKind
                                : node == kEnd ? kEndKind
                                               : kInnerKind);
}

uint32_t PrefixTrieBuilder::MakeNode(size_t depth, const Frame& frame) {
  const auto children = std::span{_pending}.subspan(frame.begin);
  const auto id = static_cast<uint32_t>(_built.size());
  _built.push_back({
    .count = static_cast<uint32_t>(
      std::min<uint64_t>(frame.count, std::numeric_limits<uint32_t>::max())),
    .depth = static_cast<uint32_t>(depth),
    .edge_start = static_cast<uint32_t>(depth),
    .fanout = static_cast<uint16_t>(children.size()),
    .terminal = frame.terminal,
  });
  for (const auto& child : children) {
    _labels.push_back(child.label);
    _kinds.push_back(KindOf(child.node));
    if (child.node < kEnd) {
      _inner.push_back(child.node);
    }
  }
  _pending.resize(frame.begin);
  if (Adaptive(_built.back())) {
    _bytes += PrefixTrie::NodeBytes(static_cast<uint32_t>(children.size()), 0);
  }
  return id;
}

void PrefixTrieBuilder::Materialize() {
  if (_carry == kLeaf) {
    return;
  }
  const auto& node = _built[_carry];
  const auto size = EdgeSize(node);
  _edges.append(bytes_view{_prev}.substr(node.edge_start, size));
  if (Adaptive(node)) {
    _bytes += PrefixTrie::NodeBytes(node.fanout, size) -
              PrefixTrie::NodeBytes(node.fanout, 0);
  }
  _carry = kLeaf;
}

void PrefixTrieBuilder::Shrink(uint64_t budget) {
  while (_bytes > budget) {
    _limit *= 2;
    Prune();
  }
}

void PrefixTrieBuilder::Prune() {
  SDB_ASSERT(_carry == kLeaf);
  std::vector<uint32_t> ids(_built.size());
  std::vector<Built> built;
  bstring labels;
  bstring kinds;
  std::vector<uint32_t> inner;
  bstring edges;
  uint64_t bytes = 0;
  size_t label_at = 0;
  size_t inner_at = 0;
  size_t edge_at = 0;
  for (size_t i = 0; i != _built.size(); ++i) {
    const auto node = _built[i];
    const auto edge = bytes_view{_edges}.substr(edge_at, EdgeSize(node));
    const auto from = label_at;
    label_at += node.fanout;
    edge_at += edge.size();
    if (Adaptive(node) && node.count <= _limit) {
      inner_at += static_cast<size_t>(absl::c_count(
        bytes_view{_kinds}.substr(from, node.fanout), byte_type{kInnerKind}));
      if (node.edge_start >= kTrieExactBytes) {
        ids[i] = kLeaf;
        continue;
      }
      const auto keep = kTrieExactBytes - 1 - node.edge_start;
      ids[i] = static_cast<uint32_t>(built.size());
      built.push_back({
        .count = node.count,
        .depth = kTrieExactBytes - 1,
        .edge_start = node.edge_start,
        .fanout = 1,
        .terminal = false,
      });
      edges.append(edge.substr(0, keep));
      labels.push_back(edge[keep]);
      kinds.push_back(byte_type{kLeafKind});
      continue;
    }
    ids[i] = static_cast<uint32_t>(built.size());
    for (auto k = from; k != label_at; ++k) {
      labels.push_back(_labels[k]);
      if (_kinds[k] != kInnerKind) {
        kinds.push_back(_kinds[k]);
        continue;
      }
      const auto child = ids[_inner[inner_at++]];
      kinds.push_back(KindOf(child));
      if (child != kLeaf) {
        inner.push_back(child);
      }
    }
    edges.append(edge);
    if (Adaptive(node)) {
      bytes += PrefixTrie::NodeBytes(node.fanout, EdgeSize(node));
    }
    built.push_back(node);
  }
  for (auto& child : _pending) {
    if (child.node < kEnd) {
      child.node = ids[child.node];
    }
  }
  _built = std::move(built);
  _labels = std::move(labels);
  _kinds = std::move(kinds);
  _inner = std::move(inner);
  _edges = std::move(edges);
  _bytes = bytes;
}

std::string PrefixTrieBuilder::Finish() {
  std::string out;
  if (_frames.empty()) {
    return out;
  }
  CloseTo(0);
  const Frame root = _frames.front();
  if (!root.terminal && _pending.size() == 1 && _pending.front().node < kEnd) {
    SDB_ASSERT(_pending.front().node == _carry);
    _built[_carry].edge_start = 0;
    Materialize();
  } else {
    Materialize();
    const auto id = MakeNode(0, root);
    _pending.push_back({.node = id, .label = 0});
  }
  Shrink(Budget(_terms));
  SDB_ASSERT(_pending.size() == 1 && _pending.front().node < kEnd);
  Encode(_pending.front().node, out);
  return out;
}

void PrefixTrieBuilder::Encode(uint32_t root, std::string& out) const {
  struct Offsets {
    uint32_t labels;
    uint32_t inner;
    uint32_t edge;
  };
  std::vector<Offsets> offsets;
  offsets.reserve(_built.size());
  Offsets next{.labels = 0, .inner = 0, .edge = 0};
  for (const auto& node : _built) {
    offsets.push_back(next);
    next.inner += static_cast<uint32_t>(
      absl::c_count(bytes_view{_kinds}.substr(next.labels, node.fanout),
                    byte_type{kInnerKind}));
    next.labels += node.fanout;
    next.edge += EdgeSize(node);
  }
  std::vector<uint32_t> queue{root};
  queue.reserve(_built.size());
  auto it = std::back_inserter(out);
  for (size_t i = 0; i != queue.size(); ++i) {
    const auto& node = _built[queue[i]];
    const auto at = offsets[queue[i]];
    const auto edge_size = EdgeSize(node);
    SDB_ASSERT(node.fanout != 0 && edge_size < (uint32_t{1} << 31));
    WriteVarint(edge_size << 1 | uint32_t{node.terminal}, it);
    out.append(ViewCast<char>(bytes_view{_edges}.substr(at.edge, edge_size)));
    out.push_back(static_cast<char>(node.fanout - 1));
    out.append(
      ViewCast<char>(bytes_view{_labels}.substr(at.labels, node.fanout)));
    const auto kinds = bytes_view{_kinds}.substr(at.labels, node.fanout);
    auto inner = at.inner;
    for (size_t k = 0; k < kinds.size(); k += 4) {
      uint32_t bits = 0;
      for (size_t j = k; j != std::min(k + 4, kinds.size()); ++j) {
        bits |= uint32_t{kinds[j]} << (2 * (j - k));
        if (kinds[j] == kInnerKind) {
          queue.push_back(_inner[inner++]);
        }
      }
      out.push_back(static_cast<char>(bits));
    }
  }
}

void PrefixTrieBuilder::Clear() noexcept {
  _prev.clear();
  _frames.clear();
  _pending.clear();
  _built.clear();
  _labels.clear();
  _kinds.clear();
  _inner.clear();
  _edges.clear();
  _terms = 0;
  _limit = kTrieLeafTerms;
  _bytes = 0;
  _carry = kLeaf;
}

}  // namespace irs::term_dict
