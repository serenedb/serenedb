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

#include <absl/base/internal/endian.h>

#include <string_view>

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
    if (!head.block_prefixes.empty() || !head.block_masks.empty()) {
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
  if (!_gated) {
    return;
  }
  if (head.block_prefixes.empty()) {
    Corrupted("block prefixes", head.id);
  }
  _trie = PrefixTrie{ViewCast<byte_type>(std::string_view{head.block_prefixes}),
                     head.id};
  const auto& masks = head.block_masks;
  if (masks.empty()) {
    return;
  }
  if (masks.size() != size_t{blocks} * kBlockMaskBytes) {
    Corrupted("block masks", head.id);
  }
  _masks.resize(blocks);
  _held = {};
  const auto* at = masks.data();
  for (auto& mask : _masks) {
    for (size_t i = 0; i != kBlockMaskWords; ++i) {
      mask[i] = absl::little_endian::Load64(at);
      _held[i] |= mask[i];
      at += sizeof(uint64_t);
    }
  }
}

size_t DictIndex::MemoryBytes() const noexcept {
  return (Small() ? 0 : _root.MemoryBytes() + _trie.MemoryBytes()) +
         _masks.capacity() * sizeof(BlockMask);
}

uint32_t DictIndex::BlockEnd(bytes_view upper) const {
  if (upper.empty() || upper > MaxTerm()) {
    return _blocks;
  }
  PaddedKey key;
  return BlockOf(key.Assign(KeyPrefix(), upper)) + 1;
}

void DictIndexBuilder::AppendMask(const ByteSet& seen) {
  for (size_t i = 0; i != kBlockMaskWords; ++i) {
    char bytes[sizeof(uint64_t)];
    absl::little_endian::Store64(bytes, seen.words[i]);
    _masks.append(bytes, sizeof(bytes));
  }
}

void DictIndexBuilder::Finish(FieldHead& head) {
  head.block_prefixes = _trie.Finish();
  head.block_masks = std::move(_masks);
  _masks.clear();
}

}  // namespace irs::term_dict
