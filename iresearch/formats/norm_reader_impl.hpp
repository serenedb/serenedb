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
                                           size_t n) {
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
                                            size_t n) {
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
  static constexpr size_t kProbePages =
    ((kMaxProbe << kNormWindowShift) * sizeof(uint32_t)) / file_utils::kPage +
    1;
  static constexpr uint64_t kMaxSpanPerPage = 2;
  static constexpr uint64_t kAheadBytes = 16 * 1024;
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

  IRS_FORCE_INLINE uint64_t PageAt(const byte_type* at) const noexcept {
    return _region->page + ((reinterpret_cast<uintptr_t>(at) >> kPageShift) -
                            _region->first_page);
  }

  IRS_FORCE_INLINE uint64_t PageOf(doc_id_t doc) const noexcept {
    return PageAt(At(doc));
  }

  IRS_FORCE_INLINE void Touched(const doc_id_t* docs, size_t n) noexcept {
    if (_bytes == 0) {
      return;
    }
    if (docs[0] < _window_first || docs[0] >= _window_end) [[unlikely]] {
      Enter(docs[0]);
    }
    if (_window_done && docs[n - 1] < _window_end) [[likely]] {
      return;
    }
    Cold(docs, n);
  }

  IRS_NO_INLINE void Cold(const doc_id_t* docs, size_t n) noexcept {
    if (_seen.empty()) {
      Init();
    }
    const auto epoch = file_utils::ResidencyEpoch();
    const bool valid = _column->Residency().Valid(epoch);
    if (_verified && valid) {
      const auto& residency = _column->Residency();
      if (docs[n - 1] < _window_end) {
        const auto span = _column->Window(_window);
        if (residency.Test(PageAt(span.data()),
                           PageAt(span.data() + span.size() - 1))) {
          Set(_probed_at + _window);
          Done(_window);
          return;
        }
      } else if (residency.Test(PageOf(docs[0]), PageOf(docs[n - 1]))) {
        return;
      }
      if (Trusted(docs, n)) {
        return;
      }
    }
    if (Test(_probed_at + _window) || !Probe(docs, n, epoch, valid)) {
      Bring(docs, n, valid);
    }
    if (valid || _column->Residency().Adopt(epoch)) {
      for (size_t i = 0; i != n; ++i) {
        _column->Residency().Set(PageOf(docs[i]));
      }
    }
  }

  void Bring(const doc_id_t* docs, size_t n, bool valid) noexcept {
    const bool spread = PageOf(docs[0]) != PageOf(docs[n - 1]);
    auto first = std::numeric_limits<uint64_t>::max();
    uint64_t last = 0;
    for (size_t i = 0; i != n;) {
      auto end = PageOf(docs[i]);
      if (!Mark(end)) {
        ++i;
        continue;
      }
      const auto begin = end;
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
      if (spread && !(valid && _column->Residency().Test(begin, end))) {
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

  bool Trusted(const doc_id_t* docs, size_t n) const noexcept {
    for (size_t i = 0; i != n; ++i) {
      if (!_column->Residency().Test(PageOf(docs[i]))) {
        return false;
      }
    }
    return true;
  }

  bool Probe(const doc_id_t* docs, size_t n, uint32_t epoch,
             bool valid) noexcept {
    Set(_probed_at + _window);
    _verified = true;
    _probe = _window == _probe_end ? std::min(_probe * 2, kMaxProbe) : 1;
    auto last = _window + _probe - 1;
    if (n > 1 && (uint64_t{docs[n - 1] - docs[0]} + 1) * _bytes <=
                   uint64_t{n} * file_utils::kPage) {
      last = std::max<size_t>(
        last, _region->window +
                ((docs[n - 1] - _region->first_doc) >> kNormWindowShift));
    }
    last = std::min(
      {last, _window + kMaxProbe - 1, _region->window + _region->windows - 1});
    const auto* begin = _column->Window(_window).data();
    const auto tail = _column->Window(last);
    const auto* end = tail.data() + tail.size();
    std::array<unsigned char, kProbePages> pages;
    const auto page =
      file_utils::Residency(begin, static_cast<size_t>(end - begin), pages);
    if (page == 0) {
      return false;
    }
    const auto base = reinterpret_cast<uintptr_t>(begin) / page;
    const auto resident = [&](const byte_type* from, const byte_type* to) {
      unsigned char all = 1;
      for (auto i = reinterpret_cast<uintptr_t>(from) / page - base,
                e = reinterpret_cast<uintptr_t>(to - 1) / page - base;
           i <= e; ++i) {
        all &= pages[i];
      }
      return (all & 1) != 0;
    };
    bool stale = false;
    const bool marks = valid || _column->Residency().Adopt(epoch);
    for (auto p = reinterpret_cast<uintptr_t>(begin) >> kPageShift,
              e = reinterpret_cast<uintptr_t>(end - 1) >> kPageShift;
         p <= e; ++p) {
      const auto column_page =
        PageAt(reinterpret_cast<const byte_type*>(p << kPageShift));
      if ((pages[(p << kPageShift) / page - base] & 1) == 0) {
        stale |= valid && _column->Residency().Test(column_page);
      } else if (marks) {
        _column->Residency().Set(column_page);
      }
    }
    if (stale) {
      file_utils::InvalidateResidency();
    }
    const auto* at = At(docs[0]);
    if (resident(at, end)) {
      for (auto window = _window; window <= last; ++window) {
        Set(_probed_at + window);
        Done(window);
      }
      _probe_end = last + 1;
      return true;
    }
    _probe = 1;
    const auto head = _column->Window(_window);
    if (last != _window && resident(at, head.data() + head.size())) {
      Done(_window);
      _probe_end = _window + 1;
      return true;
    }
    return false;
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
    file_utils::SyncResidency();
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
                             size_t n) const {
    ReadNormSlots(*_region, docs, values, n);
    if (_region->exceptions) {
      PatchNormSlots(*_region, docs, values, n);
    }
  }

  IRS_FORCE_INLINE void PrefetchNext(const doc_id_t* docs,
                                     size_t n) const noexcept {
    const auto* const last = At(docs[n - 1]);
    const auto span = static_cast<uint64_t>(last - At(docs[0]));
    if (span > kAheadBytes) {
      return;
    }
    for (uint64_t offset = ABSL_CACHELINE_SIZE; offset <= span;
         offset += ABSL_CACHELINE_SIZE) {
      __builtin_prefetch(last + offset);
    }
  }

  IRS_FORCE_INLINE uint32_t ReadOne(doc_id_t doc) const {
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
  bool _verified = false;
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

template<bool Multi>
class PersistedNormReader : public NormReaderBase {
 public:
  explicit PersistedNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    SDB_ASSERT(Multi || column.RegionCount() == 1);
    Position(column.Region(0));
  }

  void Get(std::span<const doc_id_t> docs, std::span<uint32_t> values) final {
    SDB_ASSERT(docs.size() <= values.size());
    if (docs.empty()) {
      return;
    }
    Fetch(docs.data(), values.data(), docs.size());
  }

  uint32_t Get(doc_id_t doc) final {
    SDB_ASSERT(doc >= doc_limits::min());
    if constexpr (Multi) {
      if (doc < _region->first_doc || doc >= _region->end_doc) [[unlikely]] {
        Position(_column->Locate(doc));
      }
    }
    Touched(&doc, 1);
    return ReadOne(doc);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) final {
    Fetch(docs.data(), values.data(), docs.size());
  }

  void GetPostingBlock(std::span<const doc_id_t, kPostingBlock> docs,
                       std::span<uint32_t, kPostingBlock> values) final {
    Fetch(docs.data(), values.data(), docs.size());
    PrefetchNext(docs.data(), docs.size());
  }

 private:
  IRS_FORCE_INLINE void Fetch(const doc_id_t* docs, uint32_t* values,
                              size_t n) {
    SDB_ASSERT(std::is_sorted(docs, docs + n));
    if constexpr (Multi) {
      if (docs[0] < _region->first_doc || docs[n - 1] >= _region->end_doc)
        [[unlikely]] {
        Split(docs, values, n);
        return;
      }
    }
    Touched(docs, n);
    Read(docs, values, n);
  }

  void Split(const doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values,
             size_t n) {
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
    return memory::make_managed<PersistedNormReader<false>>(column);
  }
  return memory::make_managed<PersistedNormReader<true>>(column);
}

}  // namespace irs
