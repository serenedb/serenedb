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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <variant>
#include <vector>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::analysis::delim {

inline constexpr size_t kLongNeedleThreshold = 8;

struct NoDelimFinder {
  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view, OnDelim&&) const noexcept {}
};

struct OneCharFinder {
  byte_type delim;

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    classify::DrainClassified(
      data.data(), data.size(), true,
      [&](const byte_type* block)
        IRS_FORCE_INLINE { return classify::ClassifyEqBlock(block, delim); },
      [&](byte_type c) IRS_FORCE_INLINE { return c == delim; },
      [&](size_t pos) IRS_FORCE_INLINE { on_delim(pos, size_t{1}); });
  }
};

struct ManyCharsFinder {
  static constexpr size_t kMaxBlockDelims = 8;

  IRS_FORCE_INLINE void Add(byte_type b) noexcept {
    if (bytes.Contains(b)) {
      return;
    }
    bytes.Add(b);
    nibbles.Add(b);
    if (ndelims < kMaxBlockDelims) {
      delims[ndelims] = b;
    }
    ++ndelims;
  }

  bool Compared() const noexcept { return ndelims <= kMaxBlockDelims; }

  bool Blockable() const noexcept { return Compared() || nibbles.Blockable(); }

  IRS_FORCE_INLINE uint32_t Classify(const byte_type* block) const noexcept {
    if (Compared()) {
      return classify::ClassifyAnyEqBlock(block, {delims.data(), ndelims});
    }
    return classify::ClassifyNibbleBlock(block, nibbles);
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    classify::DrainClassified(
      data.data(), data.size(), Blockable(),
      [&](const byte_type* block) IRS_FORCE_INLINE { return Classify(block); },
      [&](byte_type c) IRS_FORCE_INLINE { return bytes.Contains(c); },
      [&](size_t pos) IRS_FORCE_INLINE { on_delim(pos, size_t{1}); });
  }

  classify::ByteSet bytes;
  classify::NibbleSet nibbles;
  std::array<byte_type, kMaxBlockDelims> delims{};
  size_t ndelims = 0;
};

struct ByteRangesFinder {
  static constexpr size_t kMaxBlockRanges = 8;

  explicit ByteRangesFinder(const classify::ByteSet& set) : bytes{set} {
    int prev = -2;
    for (int b = 0; b < 256; ++b) {
      if (!set.Contains(static_cast<byte_type>(b))) {
        continue;
      }
      nibbles.Add(static_cast<byte_type>(b));
      if (b == prev + 1) {
        if (nranges <= kMaxBlockRanges) {
          ++ranges[nranges - 1].span;
        }
      } else {
        if (nranges < kMaxBlockRanges) {
          ranges[nranges] = {static_cast<byte_type>(b), 0};
        }
        ++nranges;
      }
      prev = b;
    }
  }

  bool Ranged() const noexcept { return nranges <= kMaxBlockRanges; }

  bool Blockable() const noexcept { return Ranged() || nibbles.Blockable(); }

  IRS_FORCE_INLINE uint32_t Classify(const byte_type* block) const noexcept {
    if (Ranged()) {
      return classify::ClassifyAnyInRangeBlock(block, {ranges.data(), nranges});
    }
    return classify::ClassifyNibbleBlock(block, nibbles);
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    classify::DrainClassified(
      data.data(), data.size(), Blockable(),
      [&](const byte_type* block) IRS_FORCE_INLINE { return Classify(block); },
      [&](byte_type c) IRS_FORCE_INLINE { return bytes.Contains(c); },
      [&](size_t pos) IRS_FORCE_INLINE { on_delim(pos, size_t{1}); });
  }

  classify::ByteSet bytes;
  classify::NibbleSet nibbles;
  std::array<classify::ByteRange, kMaxBlockRanges> ranges{};
  size_t nranges = 0;
};

IRS_FORCE_INLINE inline bool BytesEqual(const byte_type* a, const byte_type* b,
                                        size_t n) {
  for (size_t i = 0; i < n; ++i) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

template<typename OnDelim>
IRS_NO_INLINE size_t FindSparse(bytes_view data, bytes_view needle, size_t from,
                                size_t since, OnDelim& on_delim) {
  for (;;) {
    const size_t hit = data.find(needle, from);
    if (hit == bytes_view::npos) {
      return hit;
    }
    on_delim(hit, needle.size());
    const size_t end = hit + needle.size();
    if (hit - since < classify::kClassifyBlock) {
      return end;
    }
    from = since = end;
  }
}

template<bool SparseFind, typename Verify, typename OnDelim>
IRS_FORCE_INLINE void ForEachFirstLastMatch(bytes_view data, bytes_view needle,
                                            Verify&& verify,
                                            OnDelim&& on_delim) {
  const auto* p = data.data();
  const size_t size = data.size();
  const size_t n = needle.size();
  if (size < n) {
    return;
  }
  const auto first = needle.front();
  const auto last = needle.back();
  const size_t last_start = size - n;
  size_t pos = 0;
  if (last_start + 1 >= classify::kClassifyBlock) {
    for (;;) {
      const size_t base =
        std::min(pos, last_start + 1 - classify::kClassifyBlock);
      const uint32_t firsts = classify::ClassifyEqBlock(p + base, first) &
                              (~uint32_t{0} << (pos - base));
      if constexpr (SparseFind) {
        if (firsts == 0) {
          pos = FindSparse(data, needle, base + classify::kClassifyBlock, pos,
                           on_delim);
          if (pos > last_start) {
            return;
          }
          continue;
        }
      }
      size_t next = base + classify::kClassifyBlock;
      auto mask = firsts & classify::ClassifyEqBlock(p + base + n - 1, last);
      while (mask != 0) {
        const size_t at = base + std::countr_zero(mask);
        if (!verify(at)) {
          mask &= mask - 1;
          continue;
        }
        on_delim(at, n);
        const size_t end = at + n;
        if (end >= next) {
          next = end;
          break;
        }
        mask &= ~uint32_t{0} << (end - base);
      }
      if (next > last_start) {
        return;
      }
      pos = next;
    }
  }
  while (pos <= last_start) {
    if (p[pos] == first && p[pos + n - 1] == last && verify(pos)) {
      on_delim(pos, n);
      pos += n;
      continue;
    }
    ++pos;
  }
}

struct OneStringFinder {
  bstring delim;
  uint64_t prefix = 0;
  uint64_t mask = 0;

  explicit OneStringFinder(bstring&& delimiter) : delim{std::move(delimiter)} {
    SDB_ASSERT(delim.size() >= 2 && delim.size() <= kLongNeedleThreshold);
    std::memcpy(&prefix, delim.data(), delim.size());
    mask = ~uint64_t{0} >> (8 * (sizeof(uint64_t) - delim.size()));
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    const auto* p = data.data();
    const size_t size = data.size();
    ForEachFirstLastMatch<true>(
      data, delim,
      [&](size_t at) IRS_FORCE_INLINE {
        if (at + sizeof(uint64_t) <= size) {
          uint64_t window;
          std::memcpy(&window, p + at, sizeof window);
          return ((window ^ prefix) & mask) == 0;
        }
        return BytesEqual(p + at, delim.data(), delim.size());
      },
      on_delim);
  }
};

struct OneLongStringFinder {
  bstring delim;

  explicit OneLongStringFinder(bstring&& delimiter)
    : delim{std::move(delimiter)} {
    SDB_ASSERT(delim.size() > kLongNeedleThreshold);
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    const auto* p = data.data();
    const size_t n = delim.size();
    ForEachFirstLastMatch<false>(
      data, delim,
      [&](size_t at) IRS_FORCE_INLINE {
        return std::memcmp(p + at + 1, delim.data() + 1, n - 2) == 0;
      },
      on_delim);
  }
};

struct MultiStringFinder {
  static constexpr size_t kPrefix = sizeof(uint64_t);

  explicit MultiStringFinder(std::vector<bstring>&& delimiters) {
    for (auto& d : delimiters) {
      SDB_ASSERT(!d.empty());
      first.Add(d.front());
      const size_t head = std::min(d.size(), kPrefix);
      std::array<byte_type, kPrefix> ones{};
      std::fill_n(ones.begin(), head, byte_type{0xFF});
      uint64_t prefix = 0;
      uint64_t mask = 0;
      std::memcpy(&prefix, d.data(), head);
      std::memcpy(&mask, ones.data(), kPrefix);
      prefixes.push_back(prefix);
      masks.push_back(mask);
      sizes.push_back(static_cast<uint32_t>(d.size()));
      delims.push_back(std::move(d));
    }
  }

  IRS_FORCE_INLINE size_t MatchAt(const byte_type* tail, size_t n) const {
    if (n < kPrefix) [[unlikely]] {
      for (size_t j = 0; j < delims.size(); ++j) {
        if (sizes[j] <= n && BytesEqual(tail, delims[j].data(), sizes[j])) {
          return sizes[j];
        }
      }
      return 0;
    }
    uint64_t t8;
    std::memcpy(&t8, tail, kPrefix);
    for (size_t j = 0; j < delims.size(); ++j) {
      if (((t8 ^ prefixes[j]) & masks[j]) != 0) {
        continue;
      }
      const size_t size = sizes[j];
      if (size <= kPrefix) {
        return size;
      }
      if (size <= n && BytesEqual(tail + kPrefix, delims[j].data() + kPrefix,
                                  size - kPrefix)) {
        return size;
      }
    }
    return 0;
  }

  template<typename OnDelim>
  IRS_FORCE_INLINE void ForEachDelim(bytes_view data,
                                     OnDelim&& on_delim) const {
    const auto* p = data.data();
    const size_t size = data.size();
    size_t pos = 0;
    if (first.Blockable() && size >= classify::kClassifyBlock) {
      for (;;) {
        const size_t base = std::min(pos, size - classify::kClassifyBlock);
        auto mask = first.Classify(p + base) & (~uint32_t{0} << (pos - base));
        size_t next = base + classify::kClassifyBlock;
        while (mask != 0) {
          const size_t at = base + std::countr_zero(mask);
          const size_t skip = MatchAt(p + at, size - at);
          if (skip == 0) {
            mask &= mask - 1;
            continue;
          }
          on_delim(at, skip);
          const size_t end = at + skip;
          if (end >= next) {
            next = end;
            break;
          }
          mask &= ~uint32_t{0} << (end - base);
        }
        if (next >= size) {
          return;
        }
        pos = next;
      }
    }
    while (pos < size) {
      const size_t skip =
        first.bytes.Contains(p[pos]) ? MatchAt(p + pos, size - pos) : 0;
      if (skip == 0) {
        ++pos;
        continue;
      }
      on_delim(pos, skip);
      pos += skip;
    }
  }

  std::vector<uint64_t> prefixes;
  std::vector<uint64_t> masks;
  std::vector<uint32_t> sizes;
  std::vector<bstring> delims;
  ManyCharsFinder first;
};

using Finder = std::variant<std::monostate, NoDelimFinder, OneCharFinder,
                            ManyCharsFinder, ByteRangesFinder, OneStringFinder,
                            OneLongStringFinder, MultiStringFinder>;

inline Finder FinderFor(bstring&& delimiter) {
  if (delimiter.size() == 1) {
    return OneCharFinder{delimiter.front()};
  }
  if (delimiter.size() > kLongNeedleThreshold) {
    return OneLongStringFinder{std::move(delimiter)};
  }
  return OneStringFinder{std::move(delimiter)};
}

inline Finder FinderFor(const classify::ByteSet& set) {
  ManyCharsFinder chars;
  for (int b = 0; b < 256; ++b) {
    if (set.Contains(static_cast<byte_type>(b))) {
      chars.Add(static_cast<byte_type>(b));
    }
  }
  if (chars.ndelims == 0) {
    return {};
  }
  if (chars.ndelims == 1) {
    return OneCharFinder{chars.delims.front()};
  }
  if (!chars.Compared()) {
    return ByteRangesFinder{set};
  }
  return chars;
}

inline Finder FinderFor(std::vector<bstring>&& delimiters) {
  if (absl::c_all_of(delimiters,
                     [](const auto& delim) { return delim.size() == 1; })) {
    if (delimiters.empty()) {
      return NoDelimFinder{};
    }
    if (delimiters.size() == 1) {
      return OneCharFinder{delimiters[0][0]};
    }
    ManyCharsFinder chars;
    for (const auto& delim : delimiters) {
      chars.Add(delim[0]);
    }
    return chars;
  }
  if (delimiters.size() == 1) {
    return FinderFor(std::move(delimiters[0]));
  }
  return MultiStringFinder{std::move(delimiters)};
}

}  // namespace irs::analysis::delim
