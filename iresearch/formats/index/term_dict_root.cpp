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

#include "iresearch/formats/index/term_dict_root.hpp"

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/lower_bound.hpp"

namespace irs::term_dict {
namespace {

constexpr size_t kInside = std::numeric_limits<size_t>::max();
constexpr uint64_t kMaxHead = std::numeric_limits<uint64_t>::max();

using U64x8 = uint64_t __attribute__((vector_size(64)));
using I64x4 = int64_t __attribute__((vector_size(32)));
using I64x2 = int64_t __attribute__((vector_size(16)));
using U8x16 = uint8_t __attribute__((vector_size(16)));

enum class ArtKind : uint8_t {
  Node16 = 0,
  Node48 = 1,
  Node256 = 2,
};

constexpr size_t kArtHeader = 3;
constexpr size_t kArtInline = sizeof(uint64_t);
constexpr uint64_t kArtLongPath = 0xFF;
constexpr uint32_t kArtLeaf = uint32_t{1} << 31;
constexpr uint32_t kArtLongRest = kArtInline + 1;
constexpr uint32_t kArtMissing = std::numeric_limits<uint32_t>::max();
constexpr byte_type kArtAbsent = 0x80;

IRS_FORCE_INLINE uint64_t LoadHead(const byte_type* p, size_t size) noexcept {
  if (size >= sizeof(uint64_t)) {
    return absl::big_endian::Load64(p);
  }
  if (size >= sizeof(uint32_t)) {
    return uint64_t{absl::big_endian::Load32(p)} << 32 |
           uint64_t{absl::big_endian::Load32(p + size - sizeof(uint32_t))}
             << (64 - 8 * size);
  }
  if (size >= sizeof(uint16_t)) {
    return uint64_t{absl::big_endian::Load16(p)} << 48 |
           uint64_t{absl::big_endian::Load16(p + size - sizeof(uint16_t))}
             << (64 - 8 * size);
  }
  return size == 0 ? 0 : uint64_t{*p} << 56;
}

IRS_FORCE_INLINE size_t CountNotAfter(const uint64_t* heads,
                                      uint64_t head) noexcept {
  U64x8 lanes;
  std::memcpy(&lanes, heads, sizeof(lanes));
  const auto mask = lanes <= head;
  const I64x4 four = __builtin_shufflevector(mask, mask, 0, 1, 2, 3) +
                     __builtin_shufflevector(mask, mask, 4, 5, 6, 7);
  const I64x2 two = __builtin_shufflevector(four, four, 0, 1) +
                    __builtin_shufflevector(four, four, 2, 3);
  return static_cast<size_t>(-(two[0] + two[1]));
}

bytes_view SharedPrefix(const Separators& separators) noexcept {
  const size_t count = SeparatorCount(separators);
  if (count == 0) {
    return {};
  }
  const auto first = SeparatorAt(separators, 0);
  return first.substr(
    0, CommonPrefixLength(first, SeparatorAt(separators, count - 1)));
}

}  // namespace

uint64_t HeadAt(bytes_view key, size_t skip) noexcept {
  return skip >= key.size() ? 0
                            : LoadHead(key.data() + skip, key.size() - skip);
}

Separators DecodeSeparators(bytes_view encoded, size_t count) {
  Separators separators;
  separators.offsets.reserve(count + 1);
  const auto* p = encoded.data();
  size_t last = 0;
  for (size_t i = 0; i != count; ++i) {
    const auto shared = vread<uint32_t>(p);
    const auto suffix = vread<uint32_t>(p);
    SDB_ASSERT(shared <= last);
    auto& arena = separators.arena;
    const size_t start = arena.size();
    arena.resize(start + shared + suffix);
    std::memmove(arena.data() + start, arena.data() + start - last, shared);
    std::memcpy(arena.data() + start + shared, p, suffix);
    p += suffix;
    last = shared + suffix;
    separators.offsets.push_back(static_cast<uint32_t>(arena.size()));
  }
  SDB_ASSERT(p == encoded.data() + encoded.size());
  return separators;
}

Separators MakeSeparators(std::span<const bytes_view> separators) {
  Separators out;
  out.offsets.reserve(separators.size() + 1);
  for (const auto separator : separators) {
    out.arena.append(separator);
    out.offsets.push_back(static_cast<uint32_t>(out.arena.size()));
  }
  return out;
}

size_t SeparatorCount(const Separators& separators) noexcept {
  return separators.offsets.size() - 1;
}

bytes_view SeparatorAt(const Separators& separators, size_t i) noexcept {
  return {separators.arena.data() + separators.offsets[i],
          separators.offsets[i + 1] - separators.offsets[i]};
}

size_t SeparatorBytes(const Separators& separators) noexcept {
  return separators.arena.capacity() +
         separators.offsets.capacity() * sizeof(uint32_t);
}

EytzingerRoot::EytzingerRoot(Separators separators)
  : _separators{std::move(separators)},
    _prefix{SharedPrefix(_separators)},
    _count{SeparatorCount(_separators)} {
  const size_t count = _count;
  constexpr size_t kLine = 64 / sizeof(Node);
  _storage.resize(count + 1 + kLine);
  const auto misaligned =
    reinterpret_cast<uintptr_t>(_storage.data()) % 64 / sizeof(Node);
  auto* nodes = _storage.data() + (kLine - misaligned) % kLine;
  _nodes = nodes;
  size_t next = 0;
  const auto fill = [&](this auto& self,
                        size_t k) -> std::pair<size_t, size_t> {
    if (k > count) {
      return {next, next};
    }
    const auto left = self(2 * k);
    const size_t rank = next++;
    const auto right = self(2 * k + 1);
    const size_t lo = left.first;
    const size_t hi = right.second;
    size_t skip = _prefix.size();
    if (lo != 0 && hi != count) {
      skip = CommonPrefixLength(SeparatorAt(_separators, lo - 1),
                                SeparatorAt(_separators, hi));
    }
    skip = std::min<size_t>(skip, std::numeric_limits<uint32_t>::max());
    nodes[k] = Node{
      .head = HeadAt(SeparatorAt(_separators, rank), skip),
      .skip = static_cast<uint32_t>(skip),
      .rank = static_cast<uint32_t>(rank),
    };
    return {lo, hi};
  };
  fill(1);
  SDB_ASSERT(next == count);
}

IRS_FORCE_INLINE bool EytzingerRoot::NotAfter(
  const Node& node, bytes_view key, const byte_type* padded) const noexcept {
  SDB_ASSERT(node.skip <= key.size());
  const auto head = absl::big_endian::Load64(padded + node.skip);
  if (node.head != head) {
    return node.head < head;
  }
  return SeparatorAt(_separators, node.rank) <= key;
}

IRS_FORCE_INLINE size_t EytzingerRoot::Outside(bytes_view key) const noexcept {
  if (key.starts_with(_prefix)) {
    return kInside;
  }
  return key < bytes_view{_prefix} ? 0 : _count;
}

IRS_FORCE_INLINE size_t EytzingerRoot::Resolve(size_t k) const noexcept {
  k >>= std::countr_one(k) + 1;
  return k == 0 ? SeparatorCount(_separators) : _nodes[k].rank;
}

size_t EytzingerRoot::Find(bytes_view key) const noexcept {
  if (const auto outside = Outside(key); outside != kInside) {
    return outside;
  }
  const PaddedKey padded{key};
  size_t k = 1;
  while (k <= _count) {
    __builtin_prefetch(_nodes + 4 * k);
    k = 2 * k + (NotAfter(_nodes[k], key, padded.Data()) ? 1 : 0);
  }
  return Resolve(k);
}

size_t EytzingerRoot::FindFrom(size_t from, bytes_view key) const noexcept {
  return GallopIndex(from, SeparatorCount(_separators), [&](size_t i) {
    return SeparatorAt(_separators, i) <= key;
  });
}

TreeRoot::TreeRoot(Separators separators)
  : _separators{std::move(separators)},
    _prefix{SharedPrefix(_separators)},
    _count{SeparatorCount(_separators)} {
  std::vector<size_t> sizes{
    std::max<size_t>(1, (_count + kFanout - 1) / kFanout)};
  while (sizes.back() != 1) {
    sizes.push_back((sizes.back() + kFanout - 1) / kFanout);
  }
  const size_t height = sizes.size();
  _levels.resize(height);
  size_t total = 0;
  for (size_t level = 0; level != height; ++level) {
    _levels[level] = total;
    total += sizes[height - 1 - level];
  }
  _leaves = _levels.back();
  _nodes.resize(total + 1);
  for (auto& node : _nodes) {
    std::fill_n(node.heads, kFanout, kMaxHead);
  }
  auto* heads = reinterpret_cast<uint64_t*>(_nodes.data() + _leaves);
  for (size_t i = 0; i != _count; ++i) {
    heads[i] = HeadAt(SeparatorAt(_separators, i), _prefix.size());
  }
  for (size_t level = height - 1; level != 0; --level) {
    const size_t children = sizes[height - 1 - level];
    for (size_t child = 0; child != children; ++child) {
      _nodes[_levels[level - 1] + child / kFanout].heads[child % kFanout] =
        _nodes[_levels[level] + child].heads[0];
    }
  }
}

TreeRoot::Located TreeRoot::Locate(bytes_view key) const noexcept {
  const size_t prefix = _prefix.size();
  if (prefix != 0) {
    const size_t shared = std::min(prefix, key.size());
    if (const int c =
          shared == 0 ? 0 : std::memcmp(key.data(), _prefix.data(), shared);
        c != 0) {
      return {.head = 0, .outside = c < 0 ? 0 : _count};
    }
    if (key.size() < prefix) {
      return {.head = 0, .outside = 0};
    }
  }
  return {
    .head = LoadHead(key.data() + prefix, key.size() - prefix),
    .outside = kInside,
  };
}

size_t TreeRoot::Descend(uint64_t head) const noexcept {
  SDB_ASSERT(head != kMaxHead);
  size_t node = 0;
  for (size_t level = 0; level + 1 != _levels.size(); ++level) {
    const size_t count =
      CountNotAfter(_nodes[_levels[level] + node].heads, head);
    if (count == 0) {
      return 0;
    }
    node = node * kFanout + count - 1;
  }
  return node * kFanout + CountNotAfter(_nodes[_leaves + node].heads, head);
}

size_t TreeRoot::Resolve(size_t from, size_t upper, uint64_t head,
                         bytes_view key) const noexcept {
  const auto* heads = Heads();
  if (upper == from || heads[upper - 1] != head) {
    return upper;
  }
  const size_t lower =
    PartitionIndex(from, upper, [&](size_t i) { return heads[i] < head; });
  return PartitionIndex(
    lower, upper, [&](size_t i) { return SeparatorAt(_separators, i) <= key; });
}

size_t TreeRoot::Find(bytes_view key) const noexcept {
  const auto located = Locate(key);
  if (located.outside != kInside) {
    return located.outside;
  }
  const size_t upper =
    located.head == kMaxHead ? _count : Descend(located.head);
  return Resolve(0, upper, located.head, key);
}

size_t TreeRoot::FindFrom(size_t from, bytes_view key) const noexcept {
  if (from >= _count) {
    return from;
  }
  const auto located = Locate(key);
  if (located.outside != kInside) {
    return std::max(from, located.outside);
  }
  size_t upper = _count;
  if (located.head != kMaxHead) {
    const auto* heads = Heads();
    upper = from + CountNotAfter(heads + from, located.head);
    if (upper == from + kFanout) {
      upper = GallopIndex(upper, _count,
                          [&](size_t i) { return heads[i] <= located.head; });
    }
  }
  return Resolve(from, upper, located.head, key);
}

bool TreeRoot::NotAfter(size_t block, bytes_view key) const noexcept {
  SDB_ASSERT(block != 0 && block <= _count);
  const auto located = Locate(key);
  if (located.outside != kInside) {
    return located.outside != 0;
  }
  const uint64_t head = Heads()[block - 1];
  if (head != located.head) {
    return head < located.head;
  }
  return SeparatorAt(_separators, block - 1) <= key;
}

ArtRoot::ArtRoot(Separators separators) : _separators{std::move(separators)} {
  const size_t count = SeparatorCount(_separators);
  if (count < 2) {
    return;
  }
  const auto build = [&](this auto& self, size_t lo, size_t hi, size_t from,
                         size_t level) -> std::array<uint64_t, 2> {
    if (hi - lo == 1) {
      _visits += level;
      const auto separator = SeparatorAt(_separators, lo);
      const size_t rest = separator.size() - from;
      return {
        lo | uint64_t{kArtLeaf | std::min<uint32_t>(rest, kArtLongRest)} << 32,
        LoadHead(separator.data() + from, std::min(rest, kArtInline)),
      };
    }
    const auto first = SeparatorAt(_separators, lo);
    const size_t depth =
      CommonPrefixLength(first, SeparatorAt(_separators, hi - 1));
    const bool terminal = first.size() == depth;
    _visits += terminal ? level + 1 : 0;
    std::array<uint32_t, 257> starts{};
    std::array<byte_type, 256> labels{};
    size_t fanout = 0;
    for (size_t i = lo + terminal; i != hi; ++fanout) {
      const byte_type label = SeparatorAt(_separators, i)[depth];
      labels[fanout] = label;
      starts[fanout] = static_cast<uint32_t>(i);
      while (i != hi && SeparatorAt(_separators, i)[depth] == label) {
        ++i;
      }
    }
    starts[fanout] = static_cast<uint32_t>(hi);
    const auto kind = fanout <= 16   ? ArtKind::Node16
                      : fanout <= 48 ? ArtKind::Node48
                                     : ArtKind::Node256;
    const size_t table = kind == ArtKind::Node16   ? 16 / sizeof(uint64_t)
                         : kind == ArtKind::Node48 ? 256 / sizeof(uint64_t)
                                                   : 0;
    const size_t slots = kind == ArtKind::Node256 ? 256 : fanout;
    const size_t at = _words.size();
    const size_t children = at + kArtHeader + table;
    _words.resize(children + 2 * slots);
    const size_t path = depth - from;
    _words[at] = lo | uint64_t{hi} << 32;
    _words[at + 1] = depth | uint64_t{fanout} << 32 |
                     uint64_t{static_cast<uint8_t>(kind)} << 48 |
                     uint64_t{terminal} << 50 |
                     (path <= kArtInline ? path : kArtLongPath) << 56;
    _words[at + 2] =
      path <= kArtInline ? LoadHead(first.data() + from, path) : 0;
    auto* map = reinterpret_cast<byte_type*>(_words.data() + at + kArtHeader);
    if (kind == ArtKind::Node16) {
      std::memcpy(map, labels.data(), fanout);
    } else if (kind == ArtKind::Node48) {
      for (size_t label = 0, rank = 0; label != 256; ++label) {
        const bool present = rank != fanout && labels[rank] == label;
        map[label] = static_cast<byte_type>(present ? rank : rank | kArtAbsent);
        rank += present;
      }
    }
    for (size_t k = 0; k != fanout; ++k) {
      const auto child = self(starts[k], starts[k + 1], depth + 1, level + 1);
      const size_t slot = kind == ArtKind::Node256 ? labels[k] : k;
      _words[children + 2 * slot] = child[0];
      _words[children + 2 * slot + 1] = child[1];
    }
    if (kind == ArtKind::Node256) {
      uint64_t next = hi;
      for (size_t label = 256, rank = fanout; label-- != 0;) {
        if (rank != 0 && labels[rank - 1] == label) {
          next = starts[--rank];
          continue;
        }
        _words[children + 2 * label] = next | uint64_t{kArtMissing} << 32;
      }
    }
    return {lo | uint64_t{at} << 32, 0};
  };
  build(0, count, 0, 0);
}

IRS_FORCE_INLINE size_t ArtRoot::Leaf(const uint64_t* child, size_t depth,
                                      bytes_view key) const noexcept {
  const auto first = static_cast<uint32_t>(child[0]);
  const auto rest = static_cast<uint32_t>(child[0] >> 32) & ~kArtLeaf;
  const size_t from = depth + 1;
  const size_t tail = key.size() - from;
  const uint64_t head = LoadHead(key.data() + from, std::min(tail, kArtInline));
  if (head != child[1]) {
    return head > child[1] ? first + 1 : first;
  }
  if (rest != kArtLongRest) {
    return rest <= tail ? first + 1 : first;
  }
  return SeparatorAt(_separators, first).substr(from) <= key.substr(from)
           ? first + 1
           : first;
}

size_t ArtRoot::Find(bytes_view key) const noexcept {
  if (_words.empty()) {
    const size_t count = SeparatorCount(_separators);
    return count != 0 && SeparatorAt(_separators, 0) <= key ? 1 : 0;
  }
  const uint64_t* words = _words.data();
  size_t at = 0;
  size_t from = 0;
  for (;;) {
    const uint64_t* node = words + at;
    const auto first = static_cast<uint32_t>(node[0]);
    const auto end = static_cast<uint32_t>(node[0] >> 32);
    const uint64_t meta = node[1];
    const auto depth = static_cast<uint32_t>(meta);
    if (from != depth) {
      const size_t shared = std::min<size_t>(depth, key.size());
      if (const uint64_t path = meta >> 56; path != kArtLongPath) {
        const uint64_t head =
          shared > from ? LoadHead(key.data() + from, shared - from) : 0;
        if (head != node[2]) {
          return head < node[2] ? first : end;
        }
      } else if (from < shared) {
        const auto separator = SeparatorAt(_separators, first);
        if (const int c = std::memcmp(key.data() + from,
                                      separator.data() + from, shared - from);
            c != 0) {
          return c < 0 ? first : end;
        }
      }
    }
    if (key.size() <= depth) {
      return key.size() < depth ? first : first + ((meta >> 50) & 1);
    }
    const byte_type label = key[depth];
    const auto fanout = static_cast<uint32_t>(meta >> 32) & 0xFFFF;
    const auto kind = static_cast<ArtKind>((meta >> 48) & 3);
    const uint64_t* children = node + kArtHeader;
    uint32_t slot;
    if (kind == ArtKind::Node16) {
      U8x16 labels;
      std::memcpy(&labels, children, sizeof(labels));
      const auto less = std::bit_cast<std::array<uint64_t, 2>>(labels < label);
      const auto equal =
        std::bit_cast<std::array<uint64_t, 2>>(labels == label);
      const uint64_t low =
        fanout >= 8 ? ~uint64_t{0} : (uint64_t{1} << (8 * fanout)) - 1;
      const uint64_t high = fanout <= 8 ? 0
                            : fanout == 16
                              ? ~uint64_t{0}
                              : (uint64_t{1} << (8 * (fanout - 8))) - 1;
      slot = static_cast<uint32_t>(
        (std::popcount(less[0] & low) + std::popcount(less[1] & high)) / 8);
      children += 16 / sizeof(uint64_t);
      if (((equal[0] & low) | (equal[1] & high)) == 0) {
        return slot == fanout ? end : static_cast<uint32_t>(children[2 * slot]);
      }
    } else if (kind == ArtKind::Node48) {
      const byte_type mapped =
        reinterpret_cast<const byte_type*>(children)[label];
      slot = mapped & ~kArtAbsent;
      children += 256 / sizeof(uint64_t);
      if ((mapped & kArtAbsent) != 0) {
        return slot == fanout ? end : static_cast<uint32_t>(children[2 * slot]);
      }
    } else {
      slot = label;
    }
    const uint64_t* child = children + 2 * slot;
    const auto ref = static_cast<uint32_t>(child[0] >> 32);
    if (ref >= kArtLeaf) {
      if (ref == kArtMissing) {
        return static_cast<uint32_t>(child[0]);
      }
      return Leaf(child, depth, key);
    }
    at = ref;
    from = depth + 1;
  }
}

Root::Root(Separators separators, bool fixed) {
  const size_t count = SeparatorCount(separators);
  if (fixed) {
    ArtRoot art{separators};
    if (2 * art.Visits() <= TreeRoot::Height(count) * count) {
      _root.emplace<ArtRoot>(std::move(art));
      return;
    }
  }
  size_t ties = 0;
  if (count > 1) {
    const size_t skip = SharedPrefix(separators).size();
    uint64_t last = HeadAt(SeparatorAt(separators, 0), skip);
    for (size_t i = 1; i != count; ++i) {
      const uint64_t head = HeadAt(SeparatorAt(separators, i), skip);
      ties += head == last ? 1 : 0;
      last = head;
    }
  }
  if (2 * ties > count) {
    _root.emplace<EytzingerRoot>(std::move(separators));
  } else {
    _root.emplace<TreeRoot>(std::move(separators));
  }
}

}  // namespace irs::term_dict
