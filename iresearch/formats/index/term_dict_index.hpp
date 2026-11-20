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

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "iresearch/formats/index/term_dict_key.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_prefixes.hpp"
#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/byte_set.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr size_t kBlockMaskWords = 2;
inline constexpr size_t kBlockMaskBytes = kBlockMaskWords * sizeof(uint64_t);
using BlockMask = std::array<uint64_t, kBlockMaskWords>;

constexpr bool GatesTerms(DictMember member, uint32_t width) noexcept {
  return width == 0 && member == DictMember::Strings;
}

class DictIndex {
 public:
  DictIndex(const FieldHead& head, bytes_view prefix, uint32_t blocks,
            const Separators& separators);

  uint32_t Blocks() const noexcept { return _blocks; }
  bool Small() const noexcept { return _blocks == 1; }
  bool Gated() const noexcept { return _gated; }
  size_t MemoryBytes() const noexcept;

  bytes_view KeyPrefix() const noexcept {
    return bytes_view{_min_key}.substr(0, _prefix_size);
  }
  bytes_view MinKey() const noexcept { return _min_key; }
  bytes_view MaxKey() const noexcept { return _max_key; }
  bytes_view MinTerm() const noexcept {
    return bytes_view{_min_key}.substr(_prefix_size);
  }
  bytes_view MaxTerm() const noexcept {
    return bytes_view{_max_key}.substr(_prefix_size);
  }

  uint32_t BlockOf(Probe key) const noexcept {
    return Small() ? 0 : static_cast<uint32_t>(_root.Find(key));
  }
  uint32_t BlockFrom(uint32_t block, Probe key) const noexcept {
    return block + 1 < _blocks && _root.NotAfter(block + 1, key)
             ? static_cast<uint32_t>(_root.FindFrom(block + 1, key))
             : block;
  }
  uint32_t FindFrom(uint32_t block, Probe key) const noexcept {
    return static_cast<uint32_t>(_root.FindFrom(block, key));
  }
  bytes_view Separator(uint32_t block) const noexcept {
    return _root.Separator(block);
  }
  bytes_view BlockPrefix(uint32_t block) const noexcept {
    const bytes_view lower =
      block == 0 ? bytes_view{_min_key} : _root.Separator(block);
    const bytes_view upper =
      block + 1 == _blocks ? bytes_view{_max_key} : _root.Separator(block + 1);
    return lower.substr(0, CommonPrefixLength(lower, upper));
  }
  uint32_t BlockEnd(bytes_view upper) const;

  const PrefixTrie& Trie() const noexcept { return _trie; }

  bool Masked() const noexcept { return !_masks.empty(); }
  bool Holds(uint32_t block, const ByteSet& need) const noexcept {
    const auto& mask = _masks[block];
    return (mask[0] & need.words[0]) == need.words[0] &&
           (mask[1] & need.words[1]) == need.words[1];
  }
  uint32_t Holding(uint32_t block, uint32_t end,
                   const ByteSet& need) const noexcept {
    SDB_ASSERT(end <= _masks.size());
    for (; block < end && !Holds(block, need); ++block) {
    }
    return block;
  }
  bool MayHoldAny(const ByteSet& bytes) const noexcept {
    return ((bytes.words[0] & _held[0]) | (bytes.words[1] & _held[1]) |
            bytes.words[2] | bytes.words[3]) != 0;
  }

 private:
  bstring _min_key;
  bstring _max_key;
  Root _root;
  PrefixTrie _trie;
  std::vector<BlockMask> _masks;
  BlockMask _held{~uint64_t{0}, ~uint64_t{0}};
  uint32_t _prefix_size;
  uint32_t _blocks;
  bool _gated;
};

class DictIndexBuilder {
 public:
  void Reset(bool gated) noexcept {
    _trie.Reset();
    _masks.clear();
    _gated = gated;
  }

  template<typename TermAt>
  void Add(uint32_t count, TermAt&& term_at) {
    SDB_ASSERT(count != 0);
    if (!_gated) {
      return;
    }
    ByteSet seen;
    for (uint32_t i = 0; i != count; ++i) {
      const bytes_view term = term_at(i);
      _trie.Add(term);
      AddBytes(seen, term);
    }
    AppendMask(seen);
  }

  void Finish(FieldHead& head);

 private:
  void AppendMask(const ByteSet& seen);

  PrefixTrieBuilder _trie;
  std::string _masks;
  bool _gated = false;
};

}  // namespace irs::term_dict
