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

#include "iresearch/formats/index/term_dict_index.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <string_view>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"

namespace irs::term_dict {

DictIndex::DictIndex(const FieldHead& head, bytes_view prefix, uint32_t blocks,
                     const Separators& separators)
  : _prefix_size{static_cast<uint32_t>(prefix.size())},
    _blocks{blocks},
    _gated{GatesTerms(head.member, head.width)} {
  _min_key.assign(prefix);
  _min_key.append(ViewCast<byte_type>(std::string_view{head.min}));
  _max_key.assign(prefix);
  _max_key.append(ViewCast<byte_type>(std::string_view{head.max}));
  if (_max_key <= _min_key) {
    Corrupted("record", head.id);
  }
  if (blocks == 1 || !_gated) {
    if (!head.block_prefixes.empty() || !head.block_masks.empty() ||
        !head.block_mask_bytes.empty()) {
      Corrupted("record", head.id);
    }
  }
  if (blocks == 1) {
    return;
  }
  if (size_t{head.first_block} + blocks > separators.offsets.size()) {
    Corrupted("separators", head.id);
  }
  _root =
    Root{Slice(separators, head.first_block, blocks - 1), head.width != 0};
  for (uint32_t block = 0; block != blocks; ++block) {
    const auto bounds = Bounds(block);
    if ((bounds.last ? bounds.upper < bounds.lower
                     : bounds.upper <= bounds.lower) ||
        CommonPrefixLength(bounds.lower, bounds.upper) < prefix.size()) {
      Corrupted("separators", head.id);
    }
  }
  if (!_gated) {
    return;
  }
  if (head.block_prefixes.empty()) {
    Corrupted("block prefixes", head.id);
  }
  _trie = PrefixTrie{ViewCast<byte_type>(std::string_view{head.block_prefixes}),
                     head.id};
  _link_nodes.reserve(blocks);
  _link_depths.reserve(blocks);
  const size_t chain = _trie.Chain().size();
  std::vector<uint32_t> path{_trie.Root()};
  bstring previous;
  for (uint32_t block = 0; block != blocks; ++block) {
    const auto term = BlockPrefix(block).substr(prefix.size());
    const size_t keep =
      std::min(CommonPrefixLength(bytes_view{previous}, term), path.size() - 1);
    path.resize(keep + 1);
    for (size_t at = keep; at != term.size(); ++at) {
      const uint32_t node = path.back();
      if (_trie.Open(node) || _trie.Leaf(node)) {
        break;
      }
      const uint32_t child = _trie.ChildOf(node, term[at]);
      if (child == kNoNode) {
        break;
      }
      path.push_back(child);
    }
    if (path.size() <= chain) {
      Corrupted("block prefixes", head.id);
    }
    previous.assign(term);
    _link_nodes.push_back(path.back());
    _link_depths.push_back(static_cast<uint8_t>(path.size() - 1 - chain));
  }
  const auto& masks = head.block_masks;
  const auto& bytes = head.block_mask_bytes;
  if (masks.empty() && bytes.empty()) {
    return;
  }
  if (bytes.size() < kMaskUnionBytes ||
      bytes.size() > kMaskUnionBytes + kMaskBits) {
    Corrupted("block masks", head.id);
  }
  ByteSet held;
  for (size_t i = 0; i != held.words.size(); ++i) {
    held.words[i] =
      absl::little_endian::Load64(bytes.data() + i * sizeof(uint64_t));
  }
  const auto selected =
    ViewCast<byte_type>(std::string_view{bytes}).substr(kMaskUnionBytes);
  if (masks.size() !=
      (selected.empty() ? 0 : size_t{blocks} * sizeof(uint64_t))) {
    Corrupted("block masks", head.id);
  }
  _held = held;
  if (selected.empty()) {
    return;
  }
  uint64_t valid = 0;
  for (size_t bit = 0; bit != selected.size(); ++bit) {
    const byte_type b = selected[bit];
    if ((bit != 0 && b <= selected[bit - 1]) || !held.Contains(b)) {
      Corrupted("block masks", head.id);
    }
    _bits[b] = static_cast<uint8_t>(bit);
    _masked.Add(b);
    valid |= uint64_t{1} << bit;
  }
  _masks.resize(blocks);
  for (uint32_t block = 0; block != blocks; ++block) {
    _masks[block] = absl::little_endian::Load64(
      masks.data() + size_t{block} * sizeof(uint64_t));
    if ((_masks[block] & ~valid) != 0) {
      Corrupted("block masks", head.id);
    }
  }
}

BlockNeed DictIndex::NeedOf(const ByteSet& bytes) const noexcept {
  uint64_t missing = 0;
  uint64_t masked = 0;
  for (size_t i = 0; i != bytes.words.size(); ++i) {
    missing |= bytes.words[i] & ~_held.words[i];
    masked |= bytes.words[i] & _masked.words[i];
  }
  if (missing != 0) {
    return {.bits = 0, .impossible = true};
  }
  BlockNeed need;
  if (masked == 0) {
    return need;
  }
  for (size_t i = 0; i != bytes.words.size(); ++i) {
    for (uint64_t word = bytes.words[i] & _masked.words[i]; word != 0;
         word &= word - 1) {
      const auto b = i * 64 + static_cast<size_t>(std::countr_zero(word));
      need.bits |= uint64_t{1} << _bits[b];
    }
  }
  return need;
}

size_t DictIndex::MemoryBytes() const noexcept {
  return (Small() ? 0 : _root.MemoryBytes() + _trie.MemoryBytes()) +
         _link_nodes.capacity() * sizeof(uint32_t) + _link_depths.capacity() +
         _masks.capacity() * sizeof(uint64_t);
}

uint32_t DictIndex::BlockEnd(bytes_view upper) const {
  if (upper.empty() || upper > MaxTerm()) {
    return _blocks;
  }
  PaddedKey key;
  return BlockOf(key.Assign(KeyPrefix(), upper)) + 1;
}

void DictIndexBuilder::FinishMasks(FieldHead& head) const {
  if (_seen.empty()) {
    return;
  }
  ByteSet held;
  std::array<uint64_t, 256> present{};
  for (const auto& seen : _seen) {
    AddAll(held, seen);
    for (size_t i = 0; i != seen.words.size(); ++i) {
      for (uint64_t word = seen.words[i]; word != 0; word &= word - 1) {
        ++present[i * 64 + static_cast<size_t>(std::countr_zero(word))];
      }
    }
  }
  std::vector<byte_type> selected;
  for (size_t b = 0; b != present.size(); ++b) {
    if (present[b] != 0 && present[b] != _seen.size()) {
      selected.push_back(static_cast<byte_type>(b));
    }
  }
  absl::c_stable_sort(selected, [&](byte_type lhs, byte_type rhs) {
    return present[lhs] < present[rhs];
  });
  selected.resize(std::min(selected.size(), kMaskBits));
  absl::c_sort(selected);
  head.block_mask_bytes.resize(kMaskUnionBytes);
  for (size_t i = 0; i != held.words.size(); ++i) {
    absl::little_endian::Store64(
      head.block_mask_bytes.data() + i * sizeof(uint64_t), held.words[i]);
  }
  head.block_mask_bytes.append(
    ViewCast<char>(bytes_view{selected.data(), selected.size()}));
  if (selected.empty()) {
    return;
  }
  head.block_masks.resize(_seen.size() * sizeof(uint64_t));
  for (size_t block = 0; block != _seen.size(); ++block) {
    uint64_t mask = 0;
    for (size_t bit = 0; bit != selected.size(); ++bit) {
      if (_seen[block].Contains(selected[bit])) {
        mask |= uint64_t{1} << bit;
      }
    }
    absl::little_endian::Store64(
      head.block_masks.data() + block * sizeof(uint64_t), mask);
  }
}

void DictIndexBuilder::Finish(FieldHead& head) {
  head.block_prefixes = _trie.Finish();
  FinishMasks(head);
  _seen.clear();
}

}  // namespace irs::term_dict
