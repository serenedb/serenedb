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

uint64_t Budget(uint64_t terms) noexcept {
  return std::max(terms / kTrieTermsPerByte, kTrieMinBudget);
}

struct Range {
  uint32_t lo;
  uint32_t hi;
  bool exact;
};

}  // namespace

PrefixTrie::PrefixTrie(bytes_view encoded, field_id id) {
  const auto corrupted = [&] { Corrupted("prefix trie", id); };
  const auto* p = encoded.data();
  const auto* const end = p + encoded.size();
  const auto blocks = ReadVarint<uint32_t>(p, end);
  if (!blocks || *blocks < 2 || p == end ||
      static_cast<size_t>(end - p) > std::numeric_limits<uint32_t>::max()) {
    corrupted();
  }
  _blocks = *blocks;
  _nodes.assign(p, end);

  std::vector<uint32_t> offsets;
  std::vector<Range> ranges{{.lo = 0, .hi = _blocks - 1, .exact = true}};
  size_t referenced = 1;
  const auto* const base = _nodes.data();
  const auto* const limit = base + _nodes.size();
  for (const auto* at = base; at != limit;) {
    if (offsets.size() >= referenced || limit - at < 2) {
      corrupted();
    }
    const auto offset = static_cast<uint32_t>(at - base);
    offsets.push_back(offset);
    const uint32_t fanout = uint32_t{at[0]} + 1;
    const byte_type flags = at[1];
    at += 2;
    const auto edge_size = ReadVarint<uint32_t>(at, limit);
    const uint32_t kind_bytes = (fanout + 3) / 4;
    if (!edge_size || flags > (kTrieWidthMask | kTrieTerminal) ||
        static_cast<size_t>(limit - at) <
          *edge_size + size_t{fanout} + kind_bytes) {
      corrupted();
    }
    const uint32_t width = kTrieWidths[flags & kTrieWidthMask];
    const bytes_view labels{at + *edge_size, fanout};
    const bytes_view kinds{at + *edge_size + fanout, kind_bytes};
    at += *edge_size + size_t{fanout} + kind_bytes;
    if (absl::c_adjacent_find(
          labels, [](byte_type lhs, byte_type rhs) { return lhs >= rhs; }) !=
          labels.end() ||
        (fanout % 4 != 0 && kinds.back() >> (2 * (fanout % 4)) != 0)) {
      corrupted();
    }
    size_t inner = 0;
    for (uint32_t slot = 0; slot != fanout; ++slot) {
      const uint32_t kind = kinds[slot / 4] >> (2 * (slot % 4)) & 3U;
      if (kind > static_cast<uint32_t>(TrieKind::Inner)) {
        corrupted();
      }
      inner += kind == static_cast<uint32_t>(TrieKind::Inner);
    }
    const size_t tail = PrefixTrie::BlockBytes(fanout, width) + 4 * inner;
    if (static_cast<size_t>(limit - at) < tail) {
      corrupted();
    }
    const auto node = Node(offset);
    const auto range = ranges[offsets.size() - 1];
    if (width == 0 && range.hi > range.lo + uint32_t{!range.exact}) {
      corrupted();
    }
    for (uint32_t slot = 0; slot != fanout; ++slot) {
      const bool last = slot + 1 == fanout;
      const bool exact_hi = last && range.exact;
      const uint64_t lo = node.BlockAt(slot, range.lo);
      const uint64_t hi = last ? uint64_t{range.hi}
                               : uint64_t{node.BlockAt(slot + 1, range.lo)};
      if (lo < range.lo || hi > range.hi || lo > hi ||
          (slot == 0 && lo > range.lo + 1)) {
        corrupted();
      }
      if (node.KindAt(slot) == TrieKind::Inner) {
        ranges.push_back({.lo = static_cast<uint32_t>(lo),
                          .hi = static_cast<uint32_t>(hi),
                          .exact = exact_hi});
        ++referenced;
      }
    }
    at += tail;
  }
  if (offsets.size() != referenced) {
    corrupted();
  }
  size_t child = 1;
  for (const uint32_t offset : offsets) {
    const auto node = Node(offset);
    for (uint32_t slot = 0, inner = 0; slot != node.fanout; ++slot) {
      if (node.KindAt(slot) != TrieKind::Inner) {
        continue;
      }
      if (absl::little_endian::Load32(node.children + 4 * inner++) !=
          offsets[child++]) {
        corrupted();
      }
    }
  }
}

TrieNode PrefixTrie::Node(uint32_t offset) const noexcept {
  const auto* p = _nodes.data() + offset;
  TrieNode node;
  node.fanout = uint32_t{p[0]} + 1;
  node.width = kTrieWidths[p[1] & kTrieWidthMask];
  node.terminal = (p[1] & kTrieTerminal) != 0;
  p += 2;
  node.edge_size = vread<uint32_t>(p);
  node.edge = p;
  p += node.edge_size;
  node.labels = p;
  p += node.fanout;
  node.kinds = p;
  p += (node.fanout + 3) / 4;
  node.deltas = p;
  p += node.width * size_t{node.fanout};
  node.children = p;
  return node;
}

TrieCursor PrefixTrie::At(uint32_t offset, uint32_t lo,
                          uint32_t hi) const noexcept {
  const auto node = Node(offset);
  return {.node = offset,
          .labels = static_cast<uint32_t>(node.labels - _nodes.data()),
          .matched = 0,
          .edge = static_cast<uint32_t>(node.edge - _nodes.data()),
          .edge_size = node.edge_size,
          .lo = lo,
          .hi = hi,
          .fanout = static_cast<uint16_t>(node.fanout),
          .width = static_cast<uint8_t>(node.width),
          .terminal = node.terminal};
}

uint32_t PrefixTrie::SlotReaching(TrieCursor cursor, uint32_t slot,
                                  uint32_t block) const noexcept {
  SDB_ASSERT(cursor.node < kTrieMissing && cursor.matched == cursor.edge_size);
  const auto node = NodeOf(cursor);
  if (slot >= node.fanout) {
    return node.fanout;
  }
  uint32_t last = node.fanout - 1;
  while (slot < last) {
    const uint32_t mid = slot + (last - slot) / 2;
    if (node.BlockAt(mid + 1, cursor.lo) >= block) {
      last = mid;
    } else {
      slot = mid + 1;
    }
  }
  return slot + 1 == node.fanout && cursor.hi < block ? node.fanout : slot;
}

void PrefixTrieBuilder::Add(bytes_view key, uint32_t block) {
  SDB_ASSERT(_frames.empty() || block >= _prev_block);
  size_t last = 0;
  if (_frames.empty()) {
    Push(block);
  } else {
    SDB_ASSERT(bytes_view{_prev} < key);
    const size_t lcp = CommonPrefixLength(bytes_view{_prev}, key);
    Deepen(lcp + 1);
    CloseTo(lcp);
    Materialize();
    last = lcp + 1;
  }
  for (size_t depth = _frames.size(); depth <= last; ++depth) {
    Push(block);
  }
  _frames.back().terminal = key.size() == last;
  ++_frames.back().count;
  _prev.assign(key);
  _prev_block = block;
  ++_terms;
  if (_bytes > 2 * Budget(_terms)) {
    Prune(Budget(_terms));
  }
}

void PrefixTrieBuilder::Push(uint32_t block) {
  const auto begin = static_cast<uint32_t>(_pending.size());
  uint32_t level = 0;
  if (!_frames.empty()) {
    const auto& parent = _frames.back();
    level = parent.level + uint32_t{parent.terminal || parent.begin != begin};
  }
  _frames.push_back({
    .count = 0,
    .begin = begin,
    .first_block = block,
    .last_block = block,
    .level = level,
    .terminal = false,
  });
}

bool PrefixTrieBuilder::Branches(size_t frame) const noexcept {
  const bool open = frame + 1 != _frames.size();
  const size_t end = open ? _frames[frame + 1].begin : _pending.size();
  return _frames[frame].terminal ||
         end - _frames[frame].begin + size_t{open} > 1;
}

void PrefixTrieBuilder::Deepen(size_t depth) {
  const size_t last = std::min(_prev.size(), depth);
  if (_frames.size() > last) {
    return;
  }
  --_frames.back().count;
  for (size_t at = _frames.size(); at <= last; ++at) {
    Push(_prev_block);
  }
  _frames.back().terminal = _prev.size() == last;
  ++_frames.back().count;
}

void PrefixTrieBuilder::CloseTo(size_t depth) {
  while (_frames.size() > depth + 1) {
    const size_t at = _frames.size() - 1;
    const Frame frame = _frames.back();
    _frames.pop_back();
    auto& parent = _frames.back();
    parent.count += frame.count;
    parent.last_block = frame.last_block;
    Child slot{
      .node = kLeaf, .block = frame.first_block, .label = _prev[at - 1]};
    if (_pending.size() == frame.begin ||
        Drops({.level = frame.level, .count = frame.count})) {
      SDB_ASSERT(_carry == kLeaf);
      SDB_ASSERT(absl::c_all_of(
        std::span{_pending}.subspan(frame.begin),
        [](const Child& child) { return child.node == kLeaf; }));
      _pending.resize(frame.begin);
      _pending.push_back(slot);
      continue;
    }
    if (!frame.terminal && _pending.size() == frame.begin + 1 &&
        _pending.back().node != kLeaf) {
      SDB_ASSERT(_pending.back().node == _carry);
      _built[_carry].edge_start = static_cast<uint32_t>(at);
      slot.node = _carry;
      _pending.back() = slot;
      continue;
    }
    Materialize();
    _carry = MakeNode(at, frame);
    slot.node = _carry;
    _pending.push_back(slot);
  }
}

uint32_t PrefixTrieBuilder::InnerOf(uint32_t from,
                                    uint32_t fanout) const noexcept {
  return static_cast<uint32_t>(
    absl::c_count_if(std::span{_nodes}.subspan(from, fanout),
                     [](uint32_t child) { return child != kLeaf; }));
}

uint32_t PrefixTrieBuilder::MakeNode(size_t depth, const Frame& frame) {
  const auto children = std::span{_pending}.subspan(frame.begin);
  const auto id = static_cast<uint32_t>(_built.size());
  const auto from = static_cast<uint32_t>(_labels.size());
  _built.push_back({
    .count = static_cast<uint32_t>(
      std::min<uint64_t>(frame.count, std::numeric_limits<uint32_t>::max())),
    .depth = static_cast<uint32_t>(depth),
    .edge_start = static_cast<uint32_t>(depth),
    .fanout = static_cast<uint16_t>(children.size()),
    .spans = frame.first_block != frame.last_block,
    .terminal = frame.terminal,
  });
  for (const auto& child : children) {
    _labels.push_back(child.label);
    _nodes.push_back(child.node);
    _blocks.push_back(child.block);
  }
  _pending.resize(frame.begin);
  const auto& node = _built.back();
  if (Prunable(node)) {
    _bytes += BytesOf(node, 0, InnerOf(from, node.fanout));
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
  if (Prunable(node)) {
    _bytes += PrefixTrie::EdgeBytes(size) - PrefixTrie::EdgeBytes(0);
  }
  _carry = kLeaf;
}

void PrefixTrieBuilder::Prune(uint64_t target) {
  SDB_ASSERT(_carry == kLeaf);
  std::vector<uint32_t> firsts(_built.size());
  for (uint32_t i = 0, at = 0; i != _built.size(); ++i) {
    firsts[i] = at;
    at += _built[i].fanout;
  }
  std::vector<uint32_t> levels(_built.size());
  std::vector<uint8_t> counted(_built.size());
  const auto attach = [&](size_t begin, size_t end, uint32_t level) {
    for (size_t k = begin; k != end; ++k) {
      if (const auto node = _pending[k].node; node != kLeaf) {
        levels[node] = level;
      }
    }
  };
  if (_frames.empty()) {
    attach(0, _pending.size(), 0);
  }
  for (size_t at = 0; at != _frames.size(); ++at) {
    const size_t end =
      at + 1 != _frames.size() ? _frames[at + 1].begin : _pending.size();
    attach(_frames[at].begin, end,
           _frames[at].level + uint32_t{Branches(at)});
  }
  for (size_t i = _built.size(); i-- != 0;) {
    const auto& node = _built[i];
    for (auto k = firsts[i]; k != firsts[i] + node.fanout; ++k) {
      if (const auto child = _nodes[k]; child != kLeaf) {
        levels[child] = levels[i] + 1;
        counted[child] = Prunable(node);
      }
    }
  }
  struct Candidate {
    Rank rank;
    uint64_t bytes;
  };
  std::vector<Candidate> candidates;
  for (size_t i = 0; i != _built.size(); ++i) {
    const auto& node = _built[i];
    if (Prunable(node)) {
      candidates.push_back({
        .rank = {.level = levels[i], .count = node.count},
        .bytes = BytesOf(node, EdgeSize(node), 0) +
                 4 * uint64_t{counted[i]},
      });
    }
  }
  absl::c_sort(candidates, [](const Candidate& lhs, const Candidate& rhs) {
    return PrunedBefore(lhs.rank, rhs.rank);
  });
  uint64_t left = _bytes;
  for (const auto& candidate : candidates) {
    if (left <= target && !Drops(candidate.rank)) {
      break;
    }
    left -= std::min(left, candidate.bytes);
    if (PrunedBefore(_cut, candidate.rank)) {
      _cut = candidate.rank;
    }
  }
  std::vector<uint32_t> ids(_built.size());
  std::vector<Built> built;
  bstring labels;
  std::vector<uint32_t> nodes;
  std::vector<uint32_t> blocks;
  bstring edges;
  uint64_t bytes = 0;
  size_t edge_at = 0;
  for (size_t i = 0; i != _built.size(); ++i) {
    const auto node = _built[i];
    const auto edge = bytes_view{_edges}.substr(edge_at, EdgeSize(node));
    edge_at += edge.size();
    if (Prunable(node) &&
        Drops({.level = levels[i], .count = node.count})) {
      ids[i] = kLeaf;
      continue;
    }
    ids[i] = static_cast<uint32_t>(built.size());
    uint32_t inner = 0;
    for (auto k = firsts[i]; k != firsts[i] + node.fanout; ++k) {
      const auto child = _nodes[k] != kLeaf ? ids[_nodes[k]] : kLeaf;
      inner += child != kLeaf;
      labels.push_back(_labels[k]);
      nodes.push_back(child);
      blocks.push_back(_blocks[k]);
    }
    edges.append(edge);
    if (Prunable(node)) {
      bytes += BytesOf(node, EdgeSize(node), inner);
    }
    built.push_back(node);
  }
  for (auto& child : _pending) {
    if (child.node != kLeaf) {
      child.node = ids[child.node];
    }
  }
  _built = std::move(built);
  _labels = std::move(labels);
  _nodes = std::move(nodes);
  _blocks = std::move(blocks);
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
  if (!root.terminal && _pending.size() == 1 &&
      _pending.front().node != kLeaf) {
    SDB_ASSERT(_pending.front().node == _carry);
    _built[_carry].edge_start = 0;
    Materialize();
  } else {
    Materialize();
    const auto id = MakeNode(0, root);
    _pending.push_back({.node = id, .block = root.first_block, .label = 0});
  }
  _frames.clear();
  Prune(Budget(_terms));
  SDB_ASSERT(_pending.size() == 1 && _pending.front().node != kLeaf);
  SDB_ASSERT(_built[_pending.front().node].spans);
  Encode(_pending.front().node, out);
  return out;
}

void PrefixTrieBuilder::Encode(uint32_t root, std::string& out) const {
  struct Offsets {
    uint32_t labels;
    uint32_t edge;
  };
  std::vector<Offsets> offsets;
  offsets.reserve(_built.size());
  Offsets next{.labels = 0, .edge = 0};
  for (const auto& node : _built) {
    offsets.push_back(next);
    next.labels += node.fanout;
    next.edge += EdgeSize(node);
  }
  const auto width_code = [&](uint32_t id, uint32_t lo) -> uint32_t {
    const auto& node = _built[id];
    if (!node.spans) {
      return 0;
    }
    const uint32_t delta = _blocks[offsets[id].labels + node.fanout - 1] - lo;
    return delta <= 0xFF ? 1 : delta <= 0xFFFF ? 2 : 3;
  };
  std::vector<uint32_t> order{root};
  order.reserve(_built.size());
  std::vector<uint32_t> at(_built.size());
  std::vector<uint32_t> los(_built.size());
  uint32_t size = 0;
  for (size_t i = 0; i != order.size(); ++i) {
    const auto id = order[i];
    const auto& node = _built[id];
    const auto from = offsets[id].labels;
    at[id] = size;
    size += static_cast<uint32_t>(
      PrefixTrie::NodeBytes(node.fanout, EdgeSize(node),
                            InnerOf(from, node.fanout)) +
      PrefixTrie::BlockBytes(node.fanout,
                             kTrieWidths[width_code(id, los[id])]));
    for (uint32_t k = from; k != from + node.fanout; ++k) {
      if (const auto child = _nodes[k]; child != kLeaf) {
        los[child] = _blocks[k];
        order.push_back(child);
      }
    }
  }
  auto it = std::back_inserter(out);
  WriteVarint(_prev_block + 1, it);
  const auto put = [&](uint32_t value, uint32_t width) {
    for (uint32_t b = 0; b != width; ++b) {
      out.push_back(static_cast<char>(value >> (8 * b)));
    }
  };
  const auto bits = [&](uint32_t from, uint32_t count, uint32_t per,
                        auto&& value) {
    for (uint32_t k = 0; k < count; k += 8 / per) {
      uint32_t byte = 0;
      for (uint32_t j = k; j != std::min(k + 8 / per, count); ++j) {
        byte |= value(from + j) << (per * (j - k));
      }
      out.push_back(static_cast<char>(byte));
    }
  };
  for (const auto id : order) {
    const auto& node = _built[id];
    const auto from = offsets[id].labels;
    const auto code = width_code(id, los[id]);
    const auto width = kTrieWidths[code];
    SDB_ASSERT(node.fanout != 0 && node.fanout <= 256);
    out.push_back(static_cast<char>(node.fanout - 1));
    out.push_back(
      static_cast<char>(code | (node.terminal ? kTrieTerminal : 0)));
    WriteVarint(EdgeSize(node), it);
    out.append(ViewCast<char>(
      bytes_view{_edges}.substr(offsets[id].edge, EdgeSize(node))));
    out.append(ViewCast<char>(bytes_view{_labels}.substr(from, node.fanout)));
    bits(from, node.fanout, 2, [&](uint32_t k) {
      return static_cast<uint32_t>(_nodes[k] == kLeaf ? TrieKind::Leaf
                                                      : TrieKind::Inner);
    });
    for (uint32_t k = from; k != from + node.fanout; ++k) {
      put(_blocks[k] - los[id], width);
    }
    for (uint32_t k = from; k != from + node.fanout; ++k) {
      if (_nodes[k] != kLeaf) {
        put(at[_nodes[k]], 4);
      }
    }
  }
}

void PrefixTrieBuilder::Reset() noexcept {
  _prev.clear();
  _frames.clear();
  _pending.clear();
  _built.clear();
  _labels.clear();
  _nodes.clear();
  _blocks.clear();
  _edges.clear();
  _terms = 0;
  _bytes = 0;
  _carry = kLeaf;
  _cut = kKeepAll;
  _prev_block = 0;
}

}  // namespace irs::term_dict
