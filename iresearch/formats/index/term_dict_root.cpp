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
#include <bit>
#include <cstring>
#include <limits>
#include <optional>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs::term_dict {
namespace {

template<typename NotAfter>
IRS_FORCE_INLINE size_t UpperBound(size_t lo, size_t hi,
                                   NotAfter&& not_after) noexcept {
  if (lo == hi) {
    return lo;
  }
  size_t base = lo;
  size_t len = hi - lo;
  while (len > 1) {
    const size_t half = len / 2;
    base += not_after(base + half - 1) ? half : 0;
    len -= half;
  }
  return base + (not_after(base) ? 1 : 0);
}

template<typename NotAfter>
IRS_FORCE_INLINE size_t Gallop(size_t from, size_t count,
                               NotAfter&& not_after) noexcept {
  if (from >= count || !not_after(from)) {
    return from;
  }
  size_t lo = from + 1;
  size_t step = 1;
  while (lo + step - 1 < count && not_after(lo + step - 1)) {
    lo += step;
    step *= 2;
  }
  return UpperBound(lo, std::min(lo + step - 1, count), not_after);
}

constexpr size_t kInside = std::numeric_limits<size_t>::max();

class PaddedKey {
 public:
  explicit PaddedKey(bytes_view key) {
    if (key.size() + sizeof(uint64_t) <= sizeof(_inline)) {
      std::memcpy(_inline, key.data(), key.size());
      std::memset(_inline + key.size(), 0, sizeof(uint64_t));
      _data = _inline;
    } else {
      _heap.assign(key);
      _heap.append(sizeof(uint64_t), byte_type{0});
      _data = _heap.data();
    }
  }

  const byte_type* Data() const noexcept { return _data; }

 private:
  byte_type _inline[256];
  bstring _heap;
  const byte_type* _data;
};

IRS_FORCE_INLINE bool HeadNotAfter(uint64_t head, uint64_t key_head,
                                   bytes_view separator,
                                   bytes_view key) noexcept {
  if (head != key_head) {
    return head < key_head;
  }
  return separator <= key;
}

}  // namespace

uint64_t Head(bytes_view key) noexcept { return HeadAt(key, 0); }

uint64_t HeadAt(bytes_view key, size_t skip) noexcept {
  if (key.size() >= skip + sizeof(uint64_t)) {
    return absl::big_endian::Load64(key.data() + skip);
  }
  uint64_t head = 0;
  for (size_t i = skip; i < key.size(); ++i) {
    head |= uint64_t{key[i]} << (8 * (sizeof(uint64_t) - 1 - (i - skip)));
  }
  return head;
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

size_t SortedRoot::Find(bytes_view key) const noexcept {
  return UpperBound(0, SeparatorCount(_separators), [&](size_t i) {
    return SeparatorAt(_separators, i) <= key;
  });
}

size_t SortedRoot::FindFrom(size_t from, bytes_view key) const noexcept {
  return Gallop(from, SeparatorCount(_separators), [&](size_t i) {
    return SeparatorAt(_separators, i) <= key;
  });
}

void SortedRoot::FindBatch(std::span<const bytes_view> keys,
                           std::span<uint32_t> blocks) const noexcept {
  for (size_t i = 0; i != keys.size(); ++i) {
    blocks[i] = static_cast<uint32_t>(Find(keys[i]));
  }
}

SortedHeadsRoot::SortedHeadsRoot(Separators separators)
  : _separators{std::move(separators)} {
  const size_t count = SeparatorCount(_separators);
  _heads.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    _heads.push_back(Head(SeparatorAt(_separators, i)));
  }
}

size_t SortedHeadsRoot::Find(bytes_view key) const noexcept {
  const auto head = Head(key);
  return UpperBound(0, _heads.size(), [&](size_t i) {
    return HeadNotAfter(_heads[i], head, SeparatorAt(_separators, i), key);
  });
}

size_t SortedHeadsRoot::FindFrom(size_t from, bytes_view key) const noexcept {
  const auto head = Head(key);
  return Gallop(from, _heads.size(), [&](size_t i) {
    return HeadNotAfter(_heads[i], head, SeparatorAt(_separators, i), key);
  });
}

void SortedHeadsRoot::FindBatch(std::span<const bytes_view> keys,
                                std::span<uint32_t> blocks) const noexcept {
  for (size_t i = 0; i != keys.size(); ++i) {
    blocks[i] = static_cast<uint32_t>(Find(keys[i]));
  }
}

EytzingerRoot::EytzingerRoot(Separators separators)
  : _separators{std::move(separators)} {
  const size_t count = SeparatorCount(_separators);
  constexpr size_t kLine = 64 / sizeof(Node);
  _count = count;
  _height = std::bit_width(count);
  _storage.resize(count + 1 + kLine);
  const auto misaligned =
    reinterpret_cast<uintptr_t>(_storage.data()) % 64 / sizeof(Node);
  auto* nodes = _storage.data() + (kLine - misaligned) % kLine;
  _nodes = nodes;
  if (count != 0) {
    const auto first = SeparatorAt(_separators, 0);
    _prefix.assign(first.substr(
      0, CommonPrefixLength(first, SeparatorAt(_separators, count - 1))));
  }
  size_t next = 0;
  const auto fill = [&](this auto& self, size_t k) -> std::pair<size_t, size_t> {
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
  return Gallop(from, SeparatorCount(_separators), [&](size_t i) {
    return SeparatorAt(_separators, i) <= key;
  });
}

void EytzingerRoot::FindBatch(std::span<const bytes_view> keys,
                              std::span<uint32_t> blocks) const noexcept {
  constexpr size_t kLanes = 16;
  for (size_t base = 0; base < keys.size(); base += kLanes) {
    const size_t lanes = std::min(kLanes, keys.size() - base);
    size_t k[kLanes];
    std::optional<PaddedKey> padded[kLanes];
    for (size_t i = 0; i != lanes; ++i) {
      const auto outside = Outside(keys[base + i]);
      k[i] = outside == kInside ? 1 : 0;
      if (outside == kInside) {
        padded[i].emplace(keys[base + i]);
      } else {
        blocks[base + i] = static_cast<uint32_t>(outside);
      }
    }
    for (size_t level = 0; level != _height; ++level) {
      for (size_t i = 0; i != lanes; ++i) {
        if (k[i] != 0 && k[i] <= _count) {
          __builtin_prefetch(_nodes + 4 * k[i]);
          k[i] = 2 * k[i] + (NotAfter(_nodes[k[i]], keys[base + i],
                                      padded[i]->Data())
                               ? 1
                               : 0);
        }
      }
    }
    for (size_t i = 0; i != lanes; ++i) {
      if (k[i] != 0) {
        blocks[base + i] = static_cast<uint32_t>(Resolve(k[i]));
      }
    }
  }
}

BTreeRoot::BTreeRoot(Separators separators)
  : _separators{std::move(separators)} {
  const size_t count = SeparatorCount(_separators);
  _heads.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    _heads.push_back(Head(SeparatorAt(_separators, i)));
  }
  _nodes = (count + kKeys - 1) / kKeys;
  _tree.assign(_nodes * kKeys, std::numeric_limits<uint64_t>::max());
  _ranks.assign(_nodes * kKeys, static_cast<uint32_t>(count));
  size_t next = 0;
  const auto build = [&](this auto& self, size_t k) -> void {
    if (k >= _nodes) {
      return;
    }
    for (size_t i = 0; i != kKeys; ++i) {
      self(k * (kKeys + 1) + i + 1);
      if (next < count) {
        _tree[k * kKeys + i] = _heads[next];
        _ranks[k * kKeys + i] = static_cast<uint32_t>(next);
        ++next;
      }
    }
    self(k * (kKeys + 1) + kKeys + 1);
  };
  build(0);
  SDB_ASSERT(next == count);
}

size_t BTreeRoot::Find(bytes_view key) const noexcept {
  const auto head = Head(key);
  size_t hi = _heads.size();
  for (size_t k = 0; k < _nodes;) {
    const uint64_t* node = _tree.data() + k * kKeys;
    uint32_t i = 0;
    for (size_t j = 0; j != kKeys; ++j) {
      i += node[j] <= head ? 1 : 0;
    }
    if (i < kKeys) {
      hi = _ranks[k * kKeys + i];
    }
    k = k * (kKeys + 1) + i + 1;
  }
  if (hi == 0 || _heads[hi - 1] != head) {
    return hi;
  }
  const size_t lo =
    std::lower_bound(_heads.begin(), _heads.begin() + hi, head) -
    _heads.begin();
  return UpperBound(lo, hi, [&](size_t i) {
    return SeparatorAt(_separators, i) <= key;
  });
}

size_t BTreeRoot::FindFrom(size_t from, bytes_view key) const noexcept {
  const auto head = Head(key);
  return Gallop(from, _heads.size(), [&](size_t i) {
    return HeadNotAfter(_heads[i], head, SeparatorAt(_separators, i), key);
  });
}

void BTreeRoot::FindBatch(std::span<const bytes_view> keys,
                          std::span<uint32_t> blocks) const noexcept {
  for (size_t i = 0; i != keys.size(); ++i) {
    blocks[i] = static_cast<uint32_t>(Find(keys[i]));
  }
}

RadixRoot::RadixRoot(Separators separators, uint32_t radix_bytes)
  : _separators{std::move(separators)}, _radix_bytes{radix_bytes} {
  SDB_ASSERT(radix_bytes == 1 || radix_bytes == 2);
  const size_t count = SeparatorCount(_separators);
  _heads.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    _heads.push_back(Head(SeparatorAt(_separators, i)));
  }
  if (count != 0) {
    const auto first = SeparatorAt(_separators, 0);
    const auto last = SeparatorAt(_separators, count - 1);
    _prefix.assign(first.substr(0, CommonPrefixLength(first, last)));
  }
  const size_t buckets = size_t{1} << (8 * radix_bytes);
  _table.assign(buckets + 1, static_cast<uint32_t>(count));
  for (size_t i = count; i != 0; --i) {
    _table[Bucket(SeparatorAt(_separators, i - 1))] =
      static_cast<uint32_t>(i - 1);
  }
  for (size_t b = buckets; b != 0; --b) {
    _table[b - 1] = std::min(_table[b - 1], _table[b]);
  }
}

IRS_FORCE_INLINE size_t RadixRoot::Bucket(bytes_view key) const noexcept {
  size_t bucket = 0;
  for (size_t i = 0; i != _radix_bytes; ++i) {
    const size_t at = _prefix.size() + i;
    bucket = bucket << 8 | (at < key.size() ? key[at] : 0);
  }
  return bucket;
}

size_t RadixRoot::Find(bytes_view key) const noexcept {
  const size_t prefix = _prefix.size();
  const size_t shared = std::min(prefix, key.size());
  if (const int c = std::memcmp(key.data(), _prefix.data(), shared); c != 0) {
    return c < 0 ? 0 : _heads.size();
  }
  if (key.size() < prefix) {
    return 0;
  }
  const size_t bucket = Bucket(key);
  const auto head = Head(key);
  return UpperBound(_table[bucket], _table[bucket + 1], [&](size_t i) {
    return HeadNotAfter(_heads[i], head, SeparatorAt(_separators, i), key);
  });
}

size_t RadixRoot::FindFrom(size_t from, bytes_view key) const noexcept {
  const auto head = Head(key);
  return Gallop(from, _heads.size(), [&](size_t i) {
    return HeadNotAfter(_heads[i], head, SeparatorAt(_separators, i), key);
  });
}

void RadixRoot::FindBatch(std::span<const bytes_view> keys,
                          std::span<uint32_t> blocks) const noexcept {
  for (size_t i = 0; i != keys.size(); ++i) {
    blocks[i] = static_cast<uint32_t>(Find(keys[i]));
  }
}

}  // namespace irs::term_dict
