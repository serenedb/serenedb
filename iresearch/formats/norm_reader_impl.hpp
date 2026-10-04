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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

IRS_FORCE_INLINE inline void ReadNormSlots(const NormRegion& region,
                                           const doc_id_t* IRS_RESTRICT docs,
                                           uint32_t* IRS_RESTRICT values,
                                           size_t n) noexcept {
  SDB_ASSERT(n != 0);
  const auto* base = region.base;
  switch (region.bits) {
    case 0:
      std::fill_n(values, n, region.value);
      return;
    case 8:
      for (size_t i = 0; i != n; ++i) {
        values[i] = base[docs[i]];
      }
      return;
    case 16:
      for (size_t i = 0; i != n; ++i) {
        values[i] = absl::little_endian::Load16(base + size_t{docs[i]} * 2);
      }
      return;
    default:
      SDB_ASSERT(region.bits == 32);
      for (size_t i = 0; i != n; ++i) {
        values[i] = absl::little_endian::Load32(base + size_t{docs[i]} * 4);
      }
  }
}

IRS_NO_INLINE inline void PatchNormEscapes(const NormRegion& region,
                                           const doc_id_t* IRS_RESTRICT docs,
                                           uint32_t* IRS_RESTRICT values,
                                           size_t n) noexcept {
  size_t i = 0;
#ifdef __AVX2__
  const __m256i limit =
    _mm256_set1_epi32(static_cast<int>(region.first_code - 1));
  for (; i + 8 <= n; i += 8) {
    auto m = static_cast<uint32_t>(
      _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpgt_epi32(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i)),
        limit))));
    for (; m != 0; m &= m - 1) {
      const auto j = i + static_cast<size_t>(std::countr_zero(m));
      values[j] = region.Exception(docs[j], values[j]);
    }
  }
#endif
  for (; i != n; ++i) {
    if (values[i] >= region.first_code) {
      values[i] = region.Exception(docs[i], values[i]);
    }
  }
}

IRS_FORCE_INLINE inline void PatchNormSlots(const NormRegion& region,
                                            const doc_id_t* IRS_RESTRICT docs,
                                            uint32_t* IRS_RESTRICT values,
                                            size_t n) noexcept {
  bool escaped = false;
  for (size_t i = 0; i != n; ++i) {
    escaped |= values[i] >= region.first_code;
  }
  if (escaped) [[unlikely]] {
    PatchNormEscapes(region, docs, values, n);
  }
}

class NormReaderBase : public NormReader {
 public:
  score_t GetAvg() const noexcept final { return _avg; }

 protected:
  explicit NormReaderBase(const NormColumnReader& column) noexcept
    : _column{&column},
      _avg{
        column.NonZeroCount() == 0
          ? score_t{}
          : static_cast<score_t>(static_cast<double>(column.Sum()) /
                                 static_cast<double>(column.NonZeroCount()))} {}

  static constexpr size_t kCalls = 16;
  static constexpr size_t kMaxProbe = 64;
  static constexpr uint64_t kMaxSpanPerPage = 2;
  static constexpr uint64_t kMaxGapPages = 2;
  static constexpr size_t kBits = BitsRequired<uint64_t>();
  static constexpr uint32_t kPageShift =
    static_cast<uint32_t>(std::countr_zero(file_utils::kPage));

  IRS_FORCE_INLINE const byte_type* At(doc_id_t doc) const noexcept {
    return _region->base + uint64_t{doc} * _bytes;
  }

  IRS_FORCE_INLINE const byte_type* End(doc_id_t doc) const noexcept {
    return At(doc) + _bytes;
  }

  IRS_FORCE_INLINE uint64_t PageOf(doc_id_t doc) const noexcept {
    return _region->page +
           ((reinterpret_cast<uintptr_t>(At(doc)) >> kPageShift) -
            _region->first_page);
  }

  IRS_FORCE_INLINE void Touched(const doc_id_t* docs, size_t n) noexcept {
    if (_bytes == 0) {
      return;
    }
    if (docs[0] < _window_first || docs[0] >= _window_end) [[unlikely]] {
      Enter(docs[0]);
    }
    if (_window_done) [[likely]] {
      return;
    }
    Cold(docs, n);
  }

  IRS_NO_INLINE void Cold(const doc_id_t* docs, size_t n) noexcept {
    if (_seen.empty()) {
      Init();
    }
    if (!Test(_probed_at + _window)) {
      Set(_probed_at + _window);
      const auto* at = At(docs[0]);
      const auto resident = [&](size_t last) {
        const auto span = _column->Window(last);
        return file_utils::IsResident(
          at, static_cast<size_t>(span.data() + span.size() - at));
      };
      _probe = _window == _probe_end ? std::min(_probe * 2, kMaxProbe) : 1;
      auto last = _window + _probe - 1;
      if (n > 1 && (uint64_t{docs[n - 1] - docs[0]} + 1) * _bytes <=
                     uint64_t{n} * file_utils::kPage) {
        last = std::max<size_t>(
          last, _region->window +
                  ((docs[n - 1] - _region->first_doc) >> kNormWindowShift));
      }
      last = std::min(last, _region->window + _region->windows - 1);
      if (resident(last)) {
        for (auto window = _window; window <= last; ++window) {
          Set(_probed_at + window);
          Done(window);
        }
        _probe_end = last + 1;
        return;
      }
      _probe = 1;
      if (last != _window && resident(_window)) {
        Done(_window);
        _probe_end = _window + 1;
        return;
      }
    }
    const bool spread = PageOf(docs[0]) != PageOf(docs[n - 1]);
    auto first = std::numeric_limits<uint64_t>::max();
    uint64_t last = 0;
    for (size_t i = 0; i != n;) {
      auto end = PageOf(docs[i]);
      if (!Mark(end)) {
        ++i;
        continue;
      }
      first = std::min(first, end);
      size_t j = i + 1;
      for (; j != n; ++j) {
        const auto page = PageOf(docs[j]);
        if (page > end + kMaxGapPages + 1) {
          break;
        }
        Mark(page);
        end = page;
      }
      last = std::max(last, end);
      if (spread) {
        const auto* from = At(docs[i]);
        file_utils::Prefetch(from,
                             static_cast<size_t>(End(docs[j - 1]) - from));
      }
      i = j;
    }
    if (last < first) {
      return;
    }
    _recent[_fresh++ % kCalls] = {first, last};
    if (_fresh < kCalls) {
      return;
    }
    auto lo = first;
    auto hi = last;
    for (const auto& [a, b] : _recent) {
      lo = std::min(lo, a);
      hi = std::max(hi, b);
    }
    if (Count(lo, hi) * kMaxSpanPerPage < hi - lo + 1) {
      return;
    }
    _dense = true;
    const auto span = _column->Window(_window);
    const auto* at = At(docs[0]);
    file_utils::Prefetch(at,
                         static_cast<size_t>(span.data() + span.size() - at));
    Done(_window);
    Ahead(_window + 1);
  }

  void Ahead(size_t window) noexcept {
    SDB_ASSERT(_dense);
    uint64_t budget = file_utils::kMaxReadahead;
    for (const auto count = _column->WindowCount();
         window < count && budget != 0; ++window) {
      const auto span = _column->Window(window);
      budget -= std::min<uint64_t>(budget, span.size());
      if (Test(window)) {
        continue;
      }
      if (!file_utils::IsResident(span.data(), span.size())) {
        file_utils::Prefetch(span.data(), span.size());
      }
      Done(window);
    }
  }

  void Enter(doc_id_t doc) noexcept {
    const auto& r = *_region;
    const auto local = (doc - r.first_doc) >> kNormWindowShift;
    _window = r.window + local;
    _window_first = r.first_doc + (local << kNormWindowShift);
    _window_end = static_cast<doc_id_t>(std::min<uint64_t>(
      uint64_t{_window_first} + (uint64_t{1} << kNormWindowShift), r.end_doc));
    _window_done = !_seen.empty() && Test(_window);
    if (_dense) {
      Ahead(_window);
    }
  }

  void Init() {
    const auto windows = _column->WindowCount();
    _probed_at = (windows + kBits - 1) / kBits * kBits;
    _seen_at = 2 * _probed_at;
    _seen.assign((_seen_at + _column->PageCount() + kBits - 1) / kBits, 0);
  }

  IRS_FORCE_INLINE bool Test(size_t bit) const noexcept {
    return (_seen[bit / kBits] >> (bit % kBits)) & 1;
  }

  IRS_FORCE_INLINE void Set(size_t bit) noexcept {
    _seen[bit / kBits] |= uint64_t{1} << (bit % kBits);
  }

  IRS_FORCE_INLINE bool Mark(uint64_t page) noexcept {
    const auto bit = _seen_at + page;
    if (Test(bit)) {
      return false;
    }
    Set(bit);
    return true;
  }

  void Done(size_t window) noexcept {
    Set(window);
    _window_done |= window == _window;
  }

  uint64_t Count(uint64_t lo, uint64_t hi) const noexcept {
    uint64_t count = 0;
    for (auto bit = _seen_at + lo, end = _seen_at + hi + 1; bit < end;) {
      const auto offset = bit % kBits;
      const auto take = std::min<uint64_t>(kBits - offset, end - bit);
      const auto mask =
        take == kBits ? ~uint64_t{0} : ((uint64_t{1} << take) - 1) << offset;
      count += std::popcount(_seen[bit / kBits] & mask);
      bit += take;
    }
    return count;
  }

  IRS_FORCE_INLINE void Read(const doc_id_t* docs, uint32_t* values,
                             size_t n) const noexcept {
    ReadNormSlots(*_region, docs, values, n);
    if (_region->exceptions) {
      PatchNormSlots(*_region, docs, values, n);
    }
  }

  IRS_FORCE_INLINE uint32_t ReadOne(doc_id_t doc) const noexcept {
    const auto value = _region->Slot(doc);
    return _region->exceptions && value >= _region->first_code
             ? _region->Exception(doc, value)
             : value;
  }

  void Position(const NormRegion& region) noexcept {
    _region = &region;
    _bytes = region.bits / 8;
    _window_first = 0;
    _window_end = 0;
    _window_done = false;
  }

  const NormColumnReader* _column;
  const NormRegion* _region = nullptr;
  uint32_t _bytes = 0;
  bool _window_done = false;
  bool _dense = false;
  doc_id_t _window_first = 0;
  doc_id_t _window_end = 0;
  size_t _window = 0;
  size_t _probe = 1;
  size_t _probe_end = std::numeric_limits<size_t>::max();
  size_t _probed_at = 0;
  size_t _seen_at = 0;
  uint64_t _fresh = 0;
  std::array<std::pair<uint64_t, uint64_t>, kCalls> _recent{};
  std::vector<uint64_t> _seen;
  score_t _avg;
};

class SingleRegionNormReader : public NormReaderBase {
 public:
  explicit SingleRegionNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    SDB_ASSERT(column.RegionCount() == 1);
    Position(column.Region(0));
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    if (docs.empty()) {
      return;
    }
    Fetch(docs.data(), values.data(), docs.size());
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    Touched(&doc, 1);
    return ReadOne(doc);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) noexcept final {
    Fetch(docs.data(), values.data(), docs.size());
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    Fetch(docs.data(), values.data(), docs.size());
  }

 private:
  IRS_FORCE_INLINE void Fetch(const doc_id_t* docs, uint32_t* values,
                              size_t n) noexcept {
    SDB_ASSERT(std::is_sorted(docs, docs + n));
    Touched(docs, n);
    Read(docs, values, n);
  }
};

class MultiRegionNormReader : public NormReaderBase {
 public:
  explicit MultiRegionNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    Position(column.Region(0));
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    if (docs.empty()) {
      return;
    }
    Fetch(docs.data(), values.data(), docs.size());
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    if (doc < _region->first_doc || doc >= _region->end_doc) [[unlikely]] {
      Position(_column->Locate(doc));
    }
    Touched(&doc, 1);
    return ReadOne(doc);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) noexcept final {
    Fetch(docs.data(), values.data(), docs.size());
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    Fetch(docs.data(), values.data(), docs.size());
  }

 private:
  IRS_FORCE_INLINE void Fetch(const doc_id_t* docs, uint32_t* values,
                              size_t n) noexcept {
    SDB_ASSERT(std::is_sorted(docs, docs + n));
    if (docs[0] >= _region->first_doc && docs[n - 1] < _region->end_doc)
      [[likely]] {
      Touched(docs, n);
      Read(docs, values, n);
      return;
    }
    Split(docs, values, n);
  }

  void Split(const doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values,
             size_t n) noexcept {
    for (size_t i = 0; i != n;) {
      if (docs[i] < _region->first_doc || docs[i] >= _region->end_doc) {
        Position(_column->Locate(docs[i]));
      }
      size_t j = i + 1;
      while (j != n && docs[j] < _region->end_doc) {
        ++j;
      }
      Touched(docs + i, j - i);
      Read(docs + i, values + i, j - i);
      i = j;
    }
  }
};

inline memory::managed_ptr<NormReader> MakePersistedNormReader(
  const NormColumnReader& column) {
  SDB_ASSERT(column.RegionCount() > 0);
  if (column.RegionCount() == 1) {
    return memory::make_managed<SingleRegionNormReader>(column);
  }
  return memory::make_managed<MultiRegionNormReader>(column);
}

}  // namespace irs
