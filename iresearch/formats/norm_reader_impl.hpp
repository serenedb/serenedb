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

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<uint8_t Width>
IRS_FORCE_INLINE uint32_t ReadNormAt(const byte_type* IRS_RESTRICT base,
                                     uint64_t doc) noexcept {
  static_assert(Width == 1 || Width == 2 || Width == 4);
  if constexpr (Width == 1) {
    return base[doc];
  } else if constexpr (Width == 2) {
    return absl::little_endian::Load16(base + doc * 2);
  } else {
    return absl::little_endian::Load32(base + doc * 4);
  }
}

template<uint8_t Width, size_t N>
IRS_FORCE_INLINE void ReadNorms(const byte_type* IRS_RESTRICT base,
                                std::span<const doc_id_t, N> docs,
                                uint32_t* IRS_RESTRICT values) noexcept {
  if constexpr (N == std::dynamic_extent) {
    for (size_t i = 0, n = docs.size(); i != n; ++i) {
      values[i] = ReadNormAt<Width>(base, docs[i]);
    }
  } else {
    [&]<size_t... I>(std::index_sequence<I...>) IRS_FORCE_INLINE {
      ((values[I] = ReadNormAt<Width>(base, docs[I])), ...);
    }(std::make_index_sequence<N>{});
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
  static constexpr uint64_t kMaxSpanPerPage = 2;
  static constexpr uint64_t kMaxGapPages = 2;
  static constexpr size_t kBits = BitsRequired<uint64_t>();

  IRS_FORCE_INLINE const byte_type* At(doc_id_t doc,
                                       uint8_t width) const noexcept {
    return _bytes + size_t{doc} * width;
  }

  IRS_FORCE_INLINE uint64_t PageOf(doc_id_t doc) const noexcept {
    return uint64_t{doc - doc_limits::min()} >> _page_shift;
  }

  IRS_FORCE_INLINE void Touched(const doc_id_t* docs, size_t n,
                                uint8_t width) noexcept {
    if (_rg_done) [[likely]] {
      return;
    }
    Cold(docs, n, width);
  }

  IRS_NO_INLINE void Cold(const doc_id_t* docs, size_t n,
                          uint8_t width) noexcept {
    if (_bits.empty()) {
      Init();
    }
    if (!Test(_probed_at + _rg)) {
      Set(_probed_at + _rg);
      const auto span = _column->RowGroupExtent(_rg);
      const auto* at = At(docs[0], width);
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
        const auto* from = At(docs[i], width);
        const auto* to = At(docs[j - 1], width) + width;
        file_utils::Prefetch(from, static_cast<size_t>(to - from));
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
    const auto* at = At(docs[0], width);
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
    _rg_done = !_bits.empty() && Test(_rg);
    if (_dense) {
      Ahead(_rg);
    }
  }

  void Init() {
    const auto rgs = _column->RowGroupCount();
    _page_shift = static_cast<uint8_t>(
      std::countr_zero(file_utils::kPage / _column->ByteSize(0)));
    _probed_at = (rgs + kBits - 1) / kBits * kBits;
    _seen_at = 2 * _probed_at;
    const auto pages = ((_column->RowCount() - 1) >> _page_shift) + 1;
    _bits.assign((_seen_at + pages + kBits - 1) / kBits, 0);
  }

  IRS_FORCE_INLINE bool Test(size_t bit) const noexcept {
    return (_bits[bit / kBits] >> (bit % kBits)) & 1;
  }

  IRS_FORCE_INLINE void Set(size_t bit) noexcept {
    _bits[bit / kBits] |= uint64_t{1} << (bit % kBits);
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
      count += std::popcount(_bits[bit / kBits] & mask);
      bit += take;
    }
    return count;
  }

  template<size_t N>
  IRS_FORCE_INLINE void Patch(std::span<const doc_id_t, N> docs,
                              uint32_t* IRS_RESTRICT values) const noexcept {
    if (_exceptions.empty()) {
      return;
    }
    bool escaped = false;
    for (size_t i = 0; i != docs.size(); ++i) {
      escaped |= values[i] == kNormEscape;
    }
    if (escaped) [[unlikely]] {
      PatchEscapes(docs.data(), values, docs.size());
    }
  }

  IRS_NO_INLINE void PatchEscapes(const doc_id_t* docs, uint32_t* values,
                                  size_t n) const noexcept {
    for (size_t i = 0; i != n; ++i) {
      if (values[i] == kNormEscape) {
        values[i] =
          NormColumnReader::Exception(_exceptions, docs[i] - _exceptions_base);
      }
    }
  }

  IRS_FORCE_INLINE uint32_t Patch(doc_id_t doc, uint32_t value) const noexcept {
    if (value == kNormEscape && !_exceptions.empty()) [[unlikely]] {
      return NormColumnReader::Exception(_exceptions, doc - _exceptions_base);
    }
    return value;
  }

  const NormColumnReader* _column;
  const byte_type* _bytes = nullptr;
  std::span<const byte_type> _exceptions;
  doc_id_t _exceptions_base = doc_limits::min();
  size_t _rg = 0;
  bool _rg_done = false;
  bool _dense = false;
  uint8_t _page_shift = 0;
  size_t _probed_at = 0;
  size_t _seen_at = 0;
  uint64_t _fresh = 0;
  std::array<std::pair<uint64_t, uint64_t>, kCalls> _recent{};
  std::vector<uint64_t> _bits;
  score_t _avg;
};

template<uint8_t W>
class StaticNormWidth {
 public:
  IRS_FORCE_INLINE void Set(uint8_t width) noexcept { SDB_ASSERT(width == W); }
  IRS_FORCE_INLINE constexpr uint8_t Get() const noexcept { return W; }

  IRS_FORCE_INLINE static uint32_t At(const byte_type* IRS_RESTRICT base,
                                      doc_id_t doc) noexcept {
    return ReadNormAt<W>(base, doc);
  }
  template<size_t N>
  IRS_FORCE_INLINE static void Read(const byte_type* IRS_RESTRICT base,
                                    std::span<const doc_id_t, N> docs,
                                    uint32_t* IRS_RESTRICT values) noexcept {
    ReadNorms<W>(base, docs, values);
  }
};

class DynamicNormWidth {
 public:
  IRS_FORCE_INLINE void Set(uint8_t width) noexcept { _width = width; }
  IRS_FORCE_INLINE uint8_t Get() const noexcept { return _width; }

  IRS_FORCE_INLINE uint32_t At(const byte_type* IRS_RESTRICT base,
                               doc_id_t doc) const noexcept {
    return ReadNormValue(base + static_cast<uint64_t>(doc) * _width, _width);
  }
  template<size_t N>
  IRS_FORCE_INLINE void Read(const byte_type* IRS_RESTRICT base,
                             std::span<const doc_id_t, N> docs,
                             uint32_t* IRS_RESTRICT values) const noexcept {
    switch (_width) {
      case 1:
        return ReadNorms<1>(base, docs, values);
      case 2:
        return ReadNorms<2>(base, docs, values);
      default:
        SDB_ASSERT(_width == 4);
        return ReadNorms<4>(base, docs, values);
    }
  }

 private:
  uint8_t _width = 0;
};

template<typename Width, bool Escapes>
class SingleRgNormReader : public NormReaderBase {
 public:
  explicit SingleRgNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    SDB_ASSERT(column.RowGroupCount() == 1);
    SDB_ASSERT(column.RowCount() != 0);
    _width.Set(column.ByteSize(0));
    _bytes =
      column.RowGroupBytes(0).data() - size_t{_width.Get()} * doc_limits::min();
    if constexpr (Escapes) {
      _exceptions = column.Rg(0).exceptions;
    }
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(!docs.empty());
    SDB_ASSERT(docs.size() <= values.size());
    SDB_ASSERT(absl::c_is_sorted(docs));
    Touched(docs.data(), docs.size(), _width.Get());
    _width.Read(_bytes, docs, values.data());
    if constexpr (Escapes) {
      Patch(docs, values.data());
    }
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    Touched(&doc, 1, _width.Get());
    if constexpr (Escapes) {
      return Patch(doc, _width.At(_bytes, doc));
    } else {
      return _width.At(_bytes, doc);
    }
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    Touched(docs.data(), docs.size(), _width.Get());
    _width.Read(_bytes, docs, values.data());
    if constexpr (Escapes) {
      Patch(docs, values.data());
    }
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    Touched(docs.data(), docs.size(), _width.Get());
    _width.Read(_bytes, docs, values.data());
    if constexpr (Escapes) {
      Patch(docs, values.data());
    }
  }

 private:
  [[no_unique_address]] Width _width;
};

template<typename Width, bool Escapes>
class WindowedNormReader : public NormReaderBase {
 public:
  explicit WindowedNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    SDB_ASSERT(column.RowGroupCount() > 1);
    SDB_ASSERT(column.RowCount() != 0);
    Position(column.Rg(0));
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    if (docs.empty()) {
      return;
    }
    SDB_ASSERT(absl::c_is_sorted(docs));
    if (InWindow(docs)) [[likely]] {
      Touched(docs.data(), docs.size(), _width.Get());
      _width.Read(_bytes, docs, values.data());
      if constexpr (Escapes) {
        Patch(docs, values.data());
      }
      return;
    }
    Split(docs.data(), values.data(), docs.size());
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    if (!InWindow(doc)) [[unlikely]] {
      Position(Locate(doc));
    }
    Touched(&doc, 1, _width.Get());
    if constexpr (Escapes) {
      return Patch(doc, _width.At(_bytes, doc));
    } else {
      return _width.At(_bytes, doc);
    }
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    if (InWindow(docs)) [[likely]] {
      Touched(docs.data(), docs.size(), _width.Get());
      _width.Read(_bytes, docs, values.data());
      if constexpr (Escapes) {
        Patch(docs, values.data());
      }
      return;
    }
    Split(docs.data(), values.data(), kScoreBlock);
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    if (InWindow(docs)) [[likely]] {
      Touched(docs.data(), docs.size(), _width.Get());
      _width.Read(_bytes, docs, values.data());
      if constexpr (Escapes) {
        Patch(docs, values.data());
      }
      return;
    }
    Split(docs.data(), values.data(), kPostingBlock);
  }

 private:
  bool InWindow(doc_id_t doc) const noexcept {
    return doc >= _rg_first_doc && doc < _rg_end_doc;
  }

  bool InWindow(auto docs) const noexcept {
    SDB_ASSERT(!docs.empty());
    return docs.front() >= _rg_first_doc && docs.back() < _rg_end_doc;
  }

  NormColumnReader::RgInfo Locate(doc_id_t doc) const noexcept {
    return _column->Locate(static_cast<uint64_t>(doc) - doc_limits::min());
  }

  void Position(const NormColumnReader::RgInfo& info) noexcept {
    _width.Set(info.byte_size);
    _rg_first_doc = static_cast<doc_id_t>(info.first_row + doc_limits::min());
    _rg_end_doc = static_cast<doc_id_t>(_rg_first_doc + info.row_count);
    _bytes =
      info.bytes.data() - static_cast<size_t>(_width.Get()) * _rg_first_doc;
    if constexpr (Escapes) {
      _exceptions = info.exceptions;
      _exceptions_base = _rg_first_doc;
    }
    _rg = info.rg;
    Entered();
  }

  void Split(const doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values,
             size_t n) noexcept {
    for (size_t i = 0; i != n;) {
      if (!InWindow(docs[i])) {
        Position(Locate(docs[i]));
      }
      size_t j = i + 1;
      while (j != n && docs[j] < _rg_end_doc) {
        ++j;
      }
      Touched(docs + i, j - i, _width.Get());
      const std::span<const doc_id_t> run{docs + i, j - i};
      _width.Read(_bytes, run, values + i);
      if constexpr (Escapes) {
        Patch(run, values + i);
      }
      i = j;
    }
  }

  doc_id_t _rg_first_doc = 0;
  doc_id_t _rg_end_doc = 0;
  [[no_unique_address]] Width _width;
};

template<uint8_t ByteSize, bool Escapes>
using MultiRgNormReader =
  WindowedNormReader<StaticNormWidth<ByteSize>, Escapes>;

template<bool Escapes>
using MixedRgNormReader = WindowedNormReader<DynamicNormWidth, Escapes>;

template<bool Single, uint8_t ByteSize, bool Escapes>
using FixedRgNormReader =
  std::conditional_t<Single,
                     SingleRgNormReader<StaticNormWidth<ByteSize>, Escapes>,
                     MultiRgNormReader<ByteSize, Escapes>>;

inline memory::managed_ptr<NormReader> MakePersistedNormReader(
  const NormColumnReader& column) {
  const auto row_groups = column.RowGroupCount();
  SDB_ASSERT(row_groups > 0);

  if (!column.UniformByteSize()) {
    if (column.HasExceptions()) {
      return memory::make_managed<MixedRgNormReader<true>>(column);
    }
    return memory::make_managed<MixedRgNormReader<false>>(column);
  }

  return ResolveBool(
    row_groups == 1, [&]<bool Single>() -> memory::managed_ptr<NormReader> {
      switch (const auto byte_size = column.ByteSize(0)) {
        case 1:
          if (column.HasExceptions()) {
            return memory::make_managed<FixedRgNormReader<Single, 1, true>>(
              column);
          }
          return memory::make_managed<FixedRgNormReader<Single, 1, false>>(
            column);
        case 2:
          return memory::make_managed<FixedRgNormReader<Single, 2, false>>(
            column);
        default:
          SDB_ASSERT(byte_size == 4);
          return memory::make_managed<FixedRgNormReader<Single, 4, false>>(
            column);
      }
    });
}

}  // namespace irs
