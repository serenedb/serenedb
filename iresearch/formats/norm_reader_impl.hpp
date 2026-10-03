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

IRS_FORCE_INLINE inline void ReadNormSlots(const byte_type* IRS_RESTRICT base,
                                           uint32_t bits,
                                           const doc_id_t* IRS_RESTRICT docs,
                                           uint32_t* IRS_RESTRICT values,
                                           size_t n) noexcept {
  SDB_ASSERT(n != 0);
  switch (bits) {
    case 0:
      std::memset(values, 0, n * sizeof(uint32_t));
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
    case 24:
      for (size_t i = 0; i != n; ++i) {
        values[i] =
          absl::little_endian::Load32(base + size_t{docs[i]} * 3) & 0xFFFFFF;
      }
      return;
    default:
      SDB_ASSERT(bits == 32);
      for (size_t i = 0; i != n; ++i) {
        values[i] = absl::little_endian::Load32(base + size_t{docs[i]} * 4);
      }
  }
}

IRS_NO_INLINE inline void PatchNormEscapes(const NormColumnReader& column,
                                           uint32_t escape,
                                           const doc_id_t* IRS_RESTRICT docs,
                                           uint32_t* IRS_RESTRICT values,
                                           size_t n) noexcept {
  size_t i = 0;
#ifdef __AVX2__
  const __m256i needle = _mm256_set1_epi32(static_cast<int>(escape));
  for (; i + 8 <= n; i += 8) {
    auto m = static_cast<uint32_t>(
      _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i)),
        needle))));
    for (; m != 0; m &= m - 1) {
      const auto j = i + static_cast<size_t>(std::countr_zero(m));
      values[j] = column.Exception(docs[j]);
    }
  }
#endif
  for (; i != n; ++i) {
    if (values[i] == escape) {
      values[i] = column.Exception(docs[i]);
    }
  }
}

IRS_FORCE_INLINE inline void PatchNormSlots(const NormColumnReader& column,
                                            uint32_t escape,
                                            const doc_id_t* IRS_RESTRICT docs,
                                            uint32_t* IRS_RESTRICT values,
                                            size_t n) noexcept {
  bool escaped = false;
  for (size_t i = 0; i != n; ++i) {
    escaped |= values[i] == escape;
  }
  if (escaped) [[unlikely]] {
    PatchNormEscapes(column, escape, docs, values, n);
  }
}

class NormReaderBase : public NormReader {
 public:
  score_t GetAvg() const noexcept final { return _avg; }

 protected:
  explicit NormReaderBase(const NormColumnReader& column) noexcept
    : _column{&column},
      _page_bits{std::max<uint32_t>(column.MaxBits(), 1)},
      _avg{
        column.NonZeroCount() == 0
          ? score_t{}
          : static_cast<score_t>(static_cast<double>(column.Sum()) /
                                 static_cast<double>(column.NonZeroCount()))} {}

  static constexpr size_t kCalls = 16;
  static constexpr uint64_t kMaxSpanPerPage = 2;
  static constexpr uint64_t kMaxGapPages = 2;
  static constexpr size_t kBits = BitsRequired<uint64_t>();
  static constexpr uint32_t kPageShift =
    static_cast<uint32_t>(std::countr_zero(file_utils::kPage)) + 3;

  IRS_FORCE_INLINE const byte_type* At(doc_id_t doc) const noexcept {
    return _base + ((uint64_t{doc} * _bits) >> 3);
  }

  IRS_FORCE_INLINE const byte_type* End(doc_id_t doc) const noexcept {
    return _base + ((uint64_t{doc} * _bits + _bits + 7) >> 3);
  }

  IRS_FORCE_INLINE uint64_t PageOf(doc_id_t doc) const noexcept {
    return (uint64_t{doc - doc_limits::min()} * _page_bits) >> kPageShift;
  }

  IRS_FORCE_INLINE void Touched(const doc_id_t* docs, size_t n) noexcept {
    if (_rg_done) [[likely]] {
      return;
    }
    Cold(docs, n);
  }

  IRS_NO_INLINE void Cold(const doc_id_t* docs, size_t n) noexcept {
    if (_seen.empty()) {
      Init();
    }
    if (!Test(_probed_at + _rg)) {
      Set(_probed_at + _rg);
      const auto span = _column->RowGroupExtent(_rg);
      const auto* at = At(docs[0]);
      if (file_utils::IsResident(
            at, static_cast<size_t>(span.data() + span.size() - at))) {
        Done(_rg);
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
    const auto span = _column->RowGroupExtent(_rg);
    const auto* at = At(docs[0]);
    file_utils::Prefetch(at,
                         static_cast<size_t>(span.data() + span.size() - at));
    Done(_rg);
    Ahead(_rg + 1);
  }

  void Ahead(size_t rg) noexcept {
    SDB_ASSERT(_dense);
    uint64_t budget = file_utils::kMaxReadahead;
    for (const auto count = _column->RowGroupCount(); rg < count && budget != 0;
         ++rg) {
      const auto span = _column->RowGroupExtent(rg);
      budget -= std::min<uint64_t>(budget, span.size());
      if (Test(rg)) {
        continue;
      }
      if (!file_utils::IsResident(span.data(), span.size())) {
        file_utils::Prefetch(span.data(), span.size());
      }
      Done(rg);
    }
  }

  void Entered() noexcept {
    _rg_done = !_seen.empty() && Test(_rg);
    if (_dense) {
      Ahead(_rg);
    }
  }

  void Init() {
    const auto rgs = _column->RowGroupCount();
    _probed_at = (rgs + kBits - 1) / kBits * kBits;
    _seen_at = 2 * _probed_at;
    const auto pages =
      (((_column->RowCount() - 1) * _page_bits) >> kPageShift) + 1;
    _seen.assign((_seen_at + pages + kBits - 1) / kBits, 0);
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

  void Done(size_t rg) noexcept {
    Set(rg);
    _rg_done |= rg == _rg;
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
    ReadNormSlots(_base, _bits, docs, values, n);
    if (_escapes) {
      PatchNormSlots(*_column, _escape, docs, values, n);
    }
  }

  IRS_FORCE_INLINE uint32_t ReadOne(doc_id_t doc) const noexcept {
    const auto value = NormSlot(_base, _bits, doc);
    return _escapes && value == _escape ? _column->Exception(doc) : value;
  }

  void Track(const NormColumnReader::Window& window) noexcept {
    _rg = window.rg;
    _rg_first_doc = window.first_doc;
    _rg_end_doc = window.end_doc;
    Entered();
  }

  void Position(const NormColumnReader::Window& window) noexcept {
    _base = window.base;
    _bits = window.bits;
    _escape = static_cast<uint32_t>((uint64_t{1} << window.bits) - 1);
    _escapes = _column->HasExceptions() && window.bits != 0;
    Track(window);
  }

  const NormColumnReader* _column;
  const byte_type* _base = nullptr;
  uint32_t _bits = 0;
  uint32_t _escape = 0;
  bool _escapes = false;
  bool _rg_done = false;
  bool _dense = false;
  doc_id_t _rg_first_doc = 0;
  doc_id_t _rg_end_doc = 0;
  size_t _rg = 0;
  uint32_t _page_bits;
  size_t _probed_at = 0;
  size_t _seen_at = 0;
  uint64_t _fresh = 0;
  std::array<std::pair<uint64_t, uint64_t>, kCalls> _recent{};
  std::vector<uint64_t> _seen;
  score_t _avg;
};

class StreamNormReader : public NormReaderBase {
 public:
  explicit StreamNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    SDB_ASSERT(column.Uniform());
    Position(column.Rg(0));
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
    Follow(doc);
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
  IRS_FORCE_INLINE void Follow(doc_id_t doc) noexcept {
    if (doc < _rg_first_doc || doc >= _rg_end_doc) [[unlikely]] {
      Track(_column->Locate(doc));
    }
  }

  IRS_FORCE_INLINE void Fetch(const doc_id_t* docs, uint32_t* values,
                              size_t n) noexcept {
    SDB_ASSERT(std::is_sorted(docs, docs + n));
    Follow(docs[0]);
    Touched(docs, n);
    Read(docs, values, n);
  }
};

class WindowedNormReader : public NormReaderBase {
 public:
  explicit WindowedNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    Position(column.Rg(0));
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
    if (doc < _rg_first_doc || doc >= _rg_end_doc) [[unlikely]] {
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
    if (docs[0] >= _rg_first_doc && docs[n - 1] < _rg_end_doc) [[likely]] {
      Touched(docs, n);
      Read(docs, values, n);
      return;
    }
    Split(docs, values, n);
  }

  void Split(const doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values,
             size_t n) noexcept {
    for (size_t i = 0; i != n;) {
      if (docs[i] < _rg_first_doc || docs[i] >= _rg_end_doc) {
        Position(_column->Locate(docs[i]));
      }
      size_t j = i + 1;
      while (j != n && docs[j] < _rg_end_doc) {
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
  SDB_ASSERT(column.RowGroupCount() > 0);
  if (column.Uniform()) {
    return memory::make_managed<StreamNormReader>(column);
  }
  return memory::make_managed<WindowedNormReader>(column);
}

}  // namespace irs
