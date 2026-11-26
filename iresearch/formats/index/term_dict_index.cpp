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
                     LocalBudget& budget)
  : _min_key{TrackedAllocator<byte_type>{budget}},
    _max_key{TrackedAllocator<byte_type>{budget}},
    _link_nodes{TrackedAllocator<uint32_t>{budget}},
    _link_depths{TrackedAllocator<uint8_t>{budget}},
    _masks{TrackedAllocator<uint64_t>{budget}},
    _prefix_size{static_cast<uint32_t>(prefix.size())},
    _blocks{blocks},
    _gated{GatesTerms(head.member, head.width)} {
  _min_key.assign(prefix);
  _min_key.append(ViewCast<byte_type>(std::string_view{head.min}));
  _max_key.assign(prefix);
  _max_key.append(ViewCast<byte_type>(std::string_view{head.max}));
  if (_max_key <= _min_key) {
    Corrupted("record", head.id);
  }
}

DictIndex::DictIndex(const FieldHead& head, bytes_view prefix, uint32_t blocks,
                     const Separators& separators, duckdb::ReadStream& in)
  : DictIndex{head, prefix, blocks, kEmpty} {
  if (blocks == 1) {
    return;
  }
  _root = Root{Slice(separators, head.first_block, blocks - 1), in};
  if (!_gated) {
    return;
  }
  _trie = PrefixTrie{in};
  ReadArray(in, _link_nodes);
  ReadArray(in, _link_depths);
  ReadArray(in, _masks);
  in.ReadData(reinterpret_cast<duckdb::data_ptr_t>(_held.words.data()),
              sizeof(_held.words));
  in.ReadData(reinterpret_cast<duckdb::data_ptr_t>(_masked.words.data()),
              sizeof(_masked.words));
  in.ReadData(_bits.data(), _bits.size());
}

void DictIndex::Write(duckdb::WriteStream& out) const {
  SDB_ASSERT(!Small());
  _root.Write(out);
  if (!_gated) {
    return;
  }
  _trie.Write(out);
  WriteArray(out, _link_nodes);
  WriteArray(out, _link_depths);
  WriteArray(out, _masks);
  out.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(_held.words.data()),
                sizeof(_held.words));
  out.WriteData(
    reinterpret_cast<duckdb::const_data_ptr_t>(_masked.words.data()),
    sizeof(_masked.words));
  out.WriteData(_bits.data(), _bits.size());
}

DictIndex::DictIndex(const FieldHead& head, bytes_view prefix, uint32_t blocks,
                     const Separators& separators, DictSections sections,
                     LocalBudget& budget)
  : DictIndex{head, prefix, blocks, budget} {
  if (blocks == 1) {
    return;
  }
  SDB_ASSERT(size_t{head.first_block} + blocks <= separators.offsets.size());
  _root = Root{Slice(separators, head.first_block, blocks - 1), head.width != 0,
               budget};
  if (!_gated) {
    return;
  }
  _trie = std::move(sections.trie);
  SDB_ASSERT(!_trie.Empty());
  _link_nodes.reserve(blocks);
  _link_depths.reserve(blocks);
  const size_t chain = _trie.Chain().size();
  TrackedVector<uint32_t> path{TrackedAllocator<uint32_t>{budget}};
  path.push_back(_trie.Root());
  TrackedBstring previous{TrackedAllocator<byte_type>{budget}};
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
      SDB_ASSERT(child != kNoNode);
      path.push_back(child);
    }
    SDB_ASSERT(path.size() > chain);
    previous.assign(term);
    _link_nodes.push_back(path.back());
    _link_depths.push_back(static_cast<uint8_t>(path.size() - 1 - chain));
  }
  _held = sections.held;
  for (size_t bit = 0; bit != sections.selected.size(); ++bit) {
    const byte_type b = sections.selected[bit];
    _bits[b] = static_cast<uint8_t>(bit);
    _masked.Add(b);
  }
  SDB_ASSERT(sections.masks.size() ==
             (sections.selected.empty() ? 0 : size_t{blocks}));
  _masks = std::move(sections.masks);
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

void DictIndexBuilder::FinishMasks(DictSections& sections) const {
  SDB_ASSERT(!_seen.empty());
  std::array<uint64_t, 256> present{};
  for (const auto& seen : _seen) {
    AddAll(sections.held, seen);
    for (size_t i = 0; i != seen.words.size(); ++i) {
      for (uint64_t word = seen.words[i]; word != 0; word &= word - 1) {
        ++present[i * 64 + static_cast<size_t>(std::countr_zero(word))];
      }
    }
  }
  auto& selected = sections.selected;
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
  if (selected.empty()) {
    return;
  }
  sections.masks.resize(_seen.size());
  for (size_t block = 0; block != _seen.size(); ++block) {
    uint64_t mask = 0;
    for (size_t bit = 0; bit != selected.size(); ++bit) {
      if (_seen[block].Contains(selected[bit])) {
        mask |= uint64_t{1} << bit;
      }
    }
    sections.masks[block] = mask;
  }
}

DictSections DictIndexBuilder::Finish() {
  SDB_ASSERT(_gated);
  auto& budget = *_seen.get_allocator().budget;
  DictSections sections{
    .trie = _trie.Finish(),
    .selected = TrackedVector<byte_type>{TrackedAllocator<byte_type>{budget}},
    .masks = TrackedVector<uint64_t>{TrackedAllocator<uint64_t>{budget}},
  };
  FinishMasks(sections);
  _seen.clear();
  return sections;
}

}  // namespace irs::term_dict
