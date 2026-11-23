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

inline constexpr size_t kMaskBits = 64;
inline constexpr size_t kMaskUnionBytes = sizeof(ByteSet::words);

struct BlockNeed {
  uint64_t bits = 0;
  bool impossible = false;
};

constexpr bool GatesTerms(DictMember member, uint32_t width) noexcept {
  return width == 0 && member == DictMember::Strings;
}

class DictIndex {
 public:
  DictIndex(const FieldHead& head, bytes_view prefix, uint32_t blocks,
            const Separators& separators);

  uint32_t Blocks() const noexcept { return _blocks; }
  bool Small() const noexcept { return _blocks == 1; }
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
  struct BlockBounds {
    bytes_view lower;
    bytes_view upper;
    bool last;
  };
  BlockBounds Bounds(uint32_t block) const noexcept {
    const bool last = block + 1 == _blocks;
    return {
      .lower = block == 0 ? bytes_view{_min_key} : _root.Separator(block),
      .upper = last ? bytes_view{_max_key} : _root.Separator(block + 1),
      .last = last,
    };
  }
  bytes_view BlockPrefix(uint32_t block) const noexcept {
    const auto bounds = Bounds(block);
    return bounds.lower.substr(0,
                               CommonPrefixLength(bounds.lower, bounds.upper));
  }
  uint32_t BlockEnd(bytes_view upper) const;

  const PrefixTrie& Trie() const noexcept { return _trie; }

  struct BlockLink {
    uint32_t node;
    uint32_t depth;
  };
  BlockLink Link(uint32_t block) const noexcept {
    return {.node = _link_nodes[block],
            .depth = _link_depths[block] +
                     static_cast<uint32_t>(_trie.Chain().size())};
  }

  BlockNeed NeedOf(const ByteSet& bytes) const noexcept;
  bool Holds(uint32_t block, BlockNeed need) const noexcept {
    if (need.bits == 0) {
      return !need.impossible;
    }
    return (_masks[block] & need.bits) == need.bits;
  }
  bool MayHoldAny(const ByteSet& bytes) const noexcept {
    return ((bytes.words[0] & _held.words[0]) |
            (bytes.words[1] & _held.words[1]) |
            (bytes.words[2] & _held.words[2]) |
            (bytes.words[3] & _held.words[3])) != 0;
  }

 private:
  bstring _min_key;
  bstring _max_key;
  Root _root;
  PrefixTrie _trie;
  std::vector<uint32_t> _link_nodes;
  std::vector<uint8_t> _link_depths;
  std::vector<uint64_t> _masks;
  ByteSet _held{
    .words = {~uint64_t{0}, ~uint64_t{0}, ~uint64_t{0}, ~uint64_t{0}}};
  ByteSet _masked;
  std::array<uint8_t, 256> _bits{};
  uint32_t _prefix_size;
  uint32_t _blocks;
  bool _gated;
};

class DictIndexBuilder {
 public:
  void Reset(bool gated) noexcept {
    _trie.Reset();
    _seen.clear();
    _terms = 0;
    _gated = gated;
  }

  template<typename TermAt>
  void Add(uint32_t count, TermAt&& term_at) {
    SDB_ASSERT(count != 0);
    if (!_gated) {
      return;
    }
    auto& seen = _seen.emplace_back();
    _terms += count;
    _trie.SetSpan(static_cast<uint32_t>(std::clamp<uint64_t>(
      _terms / _seen.size() / 2, kTrieSpan, kTrieMaxSpan)));
    for (uint32_t i = 0; i != count; ++i) {
      const bytes_view term = term_at(i);
      _trie.Add(term);
      AddBytes(seen, term);
    }
  }

  void Finish(FieldHead& head);

 private:
  void FinishMasks(FieldHead& head) const;

  PrefixTrieBuilder _trie;
  std::vector<ByteSet> _seen;
  uint64_t _terms = 0;
  bool _gated = false;
};

}  // namespace irs::term_dict
