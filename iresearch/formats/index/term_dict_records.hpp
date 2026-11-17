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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kRecordAnchor = 32;
inline constexpr uint32_t kMaxRecordBits = 57;

inline constexpr byte_type kRunSingles = 1;
inline constexpr byte_type kRunMultis = 2;
inline constexpr byte_type kRunInline = 4;
inline constexpr byte_type kRunStored = 8;
inline constexpr byte_type kRunKinds =
  kRunSingles | kRunMultis | kRunInline | kRunStored;

constexpr uint32_t PositionValues(const PostingFeatures& features) noexcept {
  return features.offs ? 2 : 1;
}

template<typename Value>
void WritePacked(DataOutput& out, uint64_t count, uint32_t bits,
                 Value&& value) {
  SDB_ASSERT(bits <= 64);
  if (bits == 0) {
    return;
  }
  std::array<byte_type, sizeof(uint64_t)> word;
  uint64_t pending = 0;
  uint32_t filled = 0;
  for (uint64_t i = 0; i != count; ++i) {
    const uint64_t v = value(i);
    SDB_ASSERT(static_cast<uint32_t>(std::bit_width(v)) <= bits);
    pending |= v << filled;
    filled += bits;
    if (filled >= 64) {
      absl::little_endian::Store64(word.data(), pending);
      out.WriteData(word.data(), word.size());
      filled -= 64;
      pending = filled == 0 ? 0 : v >> (bits - filled);
    }
  }
  absl::little_endian::Store64(word.data(), pending);
  out.WriteData(word.data(), (filled + 7) / 8);
}

struct Packed {
  IRS_FORCE_INLINE uint64_t At(uint64_t i) const noexcept {
    const uint64_t bit = i * bits;
    return absl::little_endian::Load64(data + bit / 8) >> (bit % 8) &
           ((uint64_t{1} << bits) - 1);
  }

  uint64_t Sum(uint64_t from, uint64_t to) const noexcept {
    uint64_t sum = 0;
    if (bits != 0) {
      for (; from != to; ++from) {
        sum += At(from);
      }
    }
    return sum;
  }

  const byte_type* data = nullptr;
  uint32_t bits = 0;
};

IRS_FORCE_INLINE inline bool BitAt(const byte_type* bits, uint32_t i) noexcept {
  return (bits[i / 8] >> (i % 8) & 1) != 0;
}

IRS_FORCE_INLINE inline uint32_t RankAt(const byte_type* bits,
                                        uint32_t i) noexcept {
  uint32_t rank = 0;
  for (; i >= 64; i -= 64, bits += sizeof(uint64_t)) {
    rank +=
      static_cast<uint32_t>(std::popcount(absl::little_endian::Load64(bits)));
  }
  return rank +
         static_cast<uint32_t>(std::popcount(absl::little_endian::Load64(bits) &
                                             ((uint64_t{1} << i) - 1)));
}

struct Sparse {
  IRS_FORCE_INLINE uint64_t KeyAt(uint32_t j) const noexcept {
    return keys.bits == 0 ? j : keys.At(j);
  }

  uint32_t UpTo(uint64_t key) const noexcept {
    if (keys.bits == 0) {
      return static_cast<uint32_t>(std::min<uint64_t>(key + 1, count));
    }
    uint32_t lo = 0;
    uint32_t hi = count;
    while (lo < hi) {
      const uint32_t mid = lo + (hi - lo) / 2;
      if (keys.At(mid) <= key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  uint32_t Find(uint64_t key) const noexcept {
    const uint32_t up_to = UpTo(key);
    return up_to != 0 && KeyAt(up_to - 1) == key ? up_to - 1 : count;
  }

  Packed keys;
  std::array<Packed, 2> values;
  uint32_t count = 0;
};

struct RunView {
  uint32_t MultiRank(uint32_t i) const noexcept {
    return multi_bits ? RankAt(multi_bits, i) : multis == count ? i : 0;
  }
  bool Multi(uint32_t i) const noexcept {
    return multi_bits ? BitAt(multi_bits, i) : multis == count;
  }
  uint32_t InlineRank(uint32_t multi) const noexcept {
    return inline_bits         ? RankAt(inline_bits, multi)
           : inlines == multis ? multi
                               : 0;
  }
  bool Inline(uint32_t multi) const noexcept {
    return inline_bits ? BitAt(inline_bits, multi) : inlines == multis;
  }

  const byte_type* multi_bits = nullptr;
  const byte_type* inline_bits = nullptr;
  Packed singles;
  Packed docs;
  Packed excess;
  Packed sizes;
  Packed inline_anchors;
  Packed starts;
  Packed freq_anchors;
  Packed vec_starts;
  Packed lanes;
  Sparse big;
  Sparse crossings;
  Sparse extents;
  uint64_t single_base = 0;
  uint64_t inline_base = 0;
  uint64_t start_base = 0;
  uint64_t pos_start = 0;
  uint64_t pay_start = 0;
  uint64_t vec_base = 0;
  uint32_t count = 0;
  uint32_t multis = 0;
  uint32_t inlines = 0;
  uint32_t pos_offset = 0;
};

RunView ParseRun(const byte_type* p, PostingFeatures features);

class RecordDecoder {
 public:
  explicit RecordDecoder(IndexFeatures features) noexcept
    : _features{features} {}

  void Reset(const byte_type* run) { _view = ParseRun(run, _features); }

  uint32_t Count() const noexcept { return _view.count; }
  uint64_t InlineEnd() const noexcept {
    return _view.inline_base + _bytes + _size;
  }

  void Seek(uint32_t i, PostingMeta& meta) {
    if (i >= _view.count) [[unlikely]] {
      Corrupted("posting record");
    }
    _i = i;
    _multi = _view.MultiRank(i);
    _inline = _view.InlineRank(_multi);
    if (_features.pos) {
      const uint32_t anchor = i / kRecordAnchor;
      const uint32_t from = anchor * kRecordAnchor;
      const uint32_t multi = _view.MultiRank(from);
      _freq = (anchor == 0 ? 0 : _view.freq_anchors.At(anchor - 1)) +
              (i - from) + (_multi - multi) + _view.docs.Sum(multi, _multi) +
              _view.excess.Sum(from, i);
    }
    if (_view.inlines != 0) {
      const uint32_t anchor = _inline / kRecordAnchor;
      const uint32_t from = anchor * kRecordAnchor;
      _bytes = (anchor == 0 ? 0 : _view.inline_anchors.At(anchor - 1)) +
               (_inline - from) + _view.sizes.Sum(from, _inline);
    }
    Decode(meta);
  }

  void Next(PostingMeta& meta) {
    if (_i + 1 >= _view.count) [[unlikely]] {
      Corrupted("posting record");
    }
    _freq += _last_freq;
    _bytes += _size;
    _multi += uint32_t{_was_multi};
    _inline += uint32_t{_size != 0};
    ++_i;
    Decode(meta);
  }

 private:
  void Decode(PostingMeta& meta) {
    const auto& view = _view;
    meta.clear();
    _was_multi = view.Multi(_i);
    _size = 0;
    if (_was_multi) {
      meta.docs_count = static_cast<uint32_t>(view.docs.At(_multi)) + 2;
      if (view.Inline(_multi)) {
        _size = static_cast<uint32_t>(view.sizes.At(_inline)) + 1;
        meta.inline_size = static_cast<uint8_t>(_size);
      } else {
        const uint32_t stored = _multi - _inline;
        meta.doc_start = view.start_base + view.starts.At(stored);
        if (meta.docs_count > doc_limits::kBlockSize) {
          const uint32_t j = view.big.Find(stored);
          if (j == view.big.count) [[unlikely]] {
            Corrupted("posting record");
          }
          meta.doc_delta = static_cast<uint32_t>(view.big.values[0].At(j));
        }
      }
    } else {
      meta.docs_count = 1;
      meta.doc_delta =
        static_cast<uint32_t>(view.single_base + view.singles.At(_i - _multi));
    }
    if (_features.freq) {
      meta.freq = meta.docs_count + static_cast<uint32_t>(view.excess.At(_i));
    }
    _last_freq = meta.freq;
    if (_features.pos) {
      const uint64_t at = view.pos_offset + _freq;
      meta.pos_offset = static_cast<uint16_t>(at % PosGroup::kPositions);
      meta.pos_start = view.pos_start;
      meta.pay_start = view.pay_start;
      if (const uint32_t up_to = view.crossings.UpTo(_i); up_to != 0) {
        meta.pos_start += view.crossings.values[0].At(up_to - 1);
        if (_features.offs) {
          meta.pay_start += view.crossings.values[1].At(up_to - 1);
        }
      }
      if (meta.freq > pos_limits::kBlockSize) {
        const uint32_t j = view.extents.Find(_i);
        if (j == view.extents.count) [[unlikely]] {
          Corrupted("posting record");
        }
        meta.pos_extent = static_cast<uint32_t>(view.extents.values[0].At(j));
        if (_features.offs) {
          meta.pay_extent = static_cast<uint32_t>(view.extents.values[1].At(j));
        }
      }
    } else if (_features.vec) {
      meta.pay_start = view.vec_base + view.vec_starts.At(_i);
      meta.pos_offset = static_cast<uint16_t>(view.lanes.At(_i));
    }
  }

  PostingFeatures _features;
  RunView _view;
  uint64_t _freq = 0;
  uint64_t _bytes = 0;
  uint32_t _i = 0;
  uint32_t _multi = 0;
  uint32_t _inline = 0;
  uint32_t _size = 0;
  uint32_t _last_freq = 0;
  bool _was_multi = false;
};

class RecordRun {
 public:
  struct Range {
    void Add(uint64_t value) noexcept {
      min = std::min(min, value);
      max = std::max(max, value);
    }
    uint32_t Bits() const noexcept {
      return min > max ? 0 : static_cast<uint32_t>(std::bit_width(max - min));
    }

    uint64_t min = std::numeric_limits<uint64_t>::max();
    uint64_t max = 0;
  };

  struct SparseStats {
    void Add(uint64_t key, uint64_t v0, uint64_t v1) noexcept {
      identity = identity && key == count;
      ++count;
      last_key = key;
      max[0] = std::max(max[0], v0);
      max[1] = std::max(max[1], v1);
    }
    uint32_t KeyBits() const noexcept {
      return identity ? 0 : static_cast<uint32_t>(std::bit_width(last_key));
    }

    uint64_t last_key = 0;
    std::array<uint64_t, 2> max{};
    uint32_t count = 0;
    bool identity = true;
  };

  struct Stats {
    Range singles;
    Range starts;
    Range vec_starts;
    SparseStats big;
    SparseStats crossings;
    SparseStats extents;
    uint64_t docs = 0;
    uint64_t excess = 0;
    uint64_t sizes = 0;
    uint64_t inline_base = 0;
    uint64_t inline_bytes = 0;
    uint64_t inline_anchor = 0;
    uint64_t pos_start = 0;
    uint64_t pay_start = 0;
    uint64_t freq = 0;
    uint64_t freq_anchor = 0;
    uint64_t last_group = 0;
    uint64_t lanes = 0;
    uint32_t count = 0;
    uint32_t multis = 0;
    uint32_t inlines = 0;
    uint32_t pos_offset = 0;
  };

  void Start(IndexFeatures features) noexcept {
    _features = PostingFeatures{features};
    Clear();
  }

  void Clear() noexcept;
  bool Empty() const noexcept { return _stats.count == 0; }
  uint64_t Bytes() const noexcept { return SizeOf(_stats); }
  Stats Stage(const PostingMeta& meta, uint64_t inline_at) const;
  uint64_t SizeOf(const Stats& stats) const noexcept;
  void Commit(const PostingMeta& meta, const Stats& staged);
  void Write(DataOutput& out) const;

 private:
  struct SparseColumn {
    void Clear() noexcept {
      keys.clear();
      values[0].clear();
      values[1].clear();
    }
    void Add(uint64_t key, uint64_t v0, uint64_t v1) {
      keys.push_back(key);
      values[0].push_back(v0);
      values[1].push_back(v1);
    }

    std::vector<uint64_t> keys;
    std::array<std::vector<uint64_t>, 2> values;
  };

  uint64_t SparseBytes(const SparseStats& stats,
                       uint32_t values) const noexcept;
  void WriteSparse(DataOutput& out, const SparseStats& stats,
                   const SparseColumn& column, uint32_t values) const;

  PostingFeatures _features;
  Stats _stats;
  std::vector<uint8_t> _multi_bits;
  std::vector<uint8_t> _inline_bits;
  std::vector<uint64_t> _singles;
  std::vector<uint64_t> _docs;
  std::vector<uint64_t> _excess;
  std::vector<uint64_t> _sizes;
  std::vector<uint64_t> _inline_anchors;
  std::vector<uint64_t> _starts;
  std::vector<uint64_t> _freq_anchors;
  std::vector<uint64_t> _vec_starts;
  std::vector<uint64_t> _lanes;
  SparseColumn _big;
  SparseColumn _crossings;
  SparseColumn _extents;
};

}  // namespace irs::term_dict
