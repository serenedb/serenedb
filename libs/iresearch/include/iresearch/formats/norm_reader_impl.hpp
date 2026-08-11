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
#include <type_traits>

#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
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
    : _avg{
        column.NonZeroCount() == 0
          ? score_t{}
          : static_cast<score_t>(static_cast<double>(column.Sum()) /
                                 static_cast<double>(column.NonZeroCount()))} {}

  const byte_type* _bytes = nullptr;
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

template<typename Width>
class SingleRgNormReader : public NormReaderBase {
 public:
  explicit SingleRgNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column} {
    SDB_ASSERT(column.RowGroupCount() == 1);
    SDB_ASSERT(column.RowCount() != 0);
    _width.Set(column.ByteSize(0));
    _bytes =
      column.RowGroupBytes(0).data() - size_t{_width.Get()} * doc_limits::min();
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    SDB_ASSERT(absl::c_is_sorted(docs));
    _width.Read(_bytes, docs, values.data());
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    return _width.At(_bytes, doc);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    _width.Read(_bytes, docs, values.data());
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    _width.Read(_bytes, docs, values.data());
  }

 private:
  [[no_unique_address]] Width _width;
};

template<typename Width>
class WindowedNormReader : public NormReaderBase {
 public:
  explicit WindowedNormReader(const NormColumnReader& column) noexcept
    : NormReaderBase{column}, _column{&column} {
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
      _width.Read(_bytes, docs, values.data());
      return;
    }
    Split(docs.data(), values.data(), docs.size());
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    if (!InWindow(doc)) [[unlikely]] {
      Position(Locate(doc));
    }
    return _width.At(_bytes, doc);
  }

  void GetScoreBlock(std::span<const doc_id_t, kScoreBlock> docs,
                     std::span<uint32_t, kScoreBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    if (InWindow(docs)) [[likely]] {
      _width.Read(_bytes, docs, values.data());
      return;
    }
    Split(docs.data(), values.data(), kScoreBlock);
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    SDB_ASSERT(absl::c_is_sorted(docs));
    if (InWindow(docs)) [[likely]] {
      _width.Read(_bytes, docs, values.data());
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
      _width.Read(_bytes, std::span<const doc_id_t>{docs + i, j - i},
                  values + i);
      i = j;
    }
  }

  const NormColumnReader* _column;
  doc_id_t _rg_first_doc = 0;
  doc_id_t _rg_end_doc = 0;
  [[no_unique_address]] Width _width;
};

template<uint8_t ByteSize>
using MultiRgNormReader = WindowedNormReader<StaticNormWidth<ByteSize>>;

using MixedRgNormReader = WindowedNormReader<DynamicNormWidth>;

template<bool Single, uint8_t ByteSize>
using FixedRgNormReader =
  std::conditional_t<Single, SingleRgNormReader<StaticNormWidth<ByteSize>>,
                     MultiRgNormReader<ByteSize>>;

inline memory::managed_ptr<NormReader> MakePersistedNormReader(
  const NormColumnReader& column) {
  const auto row_groups = column.RowGroupCount();
  SDB_ASSERT(row_groups > 0);

  if (!column.UniformByteSize()) {
    return memory::make_managed<MixedRgNormReader>(column);
  }

  return ResolveBool(
    row_groups == 1, [&]<bool Single>() -> memory::managed_ptr<NormReader> {
      switch (const auto byte_size = column.ByteSize(0)) {
        case 1:
          return memory::make_managed<FixedRgNormReader<Single, 1>>(column);
        case 2:
          return memory::make_managed<FixedRgNormReader<Single, 2>>(column);
        default:
          SDB_ASSERT(byte_size == 4);
          return memory::make_managed<FixedRgNormReader<Single, 4>>(column);
      }
    });
}

}  // namespace irs
