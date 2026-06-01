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

#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<uint8_t ByteSize>
class SingleRgNormReader : public NormReader {
 public:
  // `_bytes` is the RG payload pre-shifted by `ByteSize * doc_limits::min()`
  // so callers index by raw `doc` directly -- the per-element
  // `doc - doc_limits::min()` subtraction is folded into the base pointer
  // once at construction. Reads still land in the original buffer because
  // `doc >= doc_limits::min()` is a precondition.
  explicit SingleRgNormReader(const NormColumnReader& column) noexcept
    : _bytes{column.RowGroupBytes(0).data() - ByteSize * doc_limits::min()},
      _sum{column.Sum()},
      _non_zero{column.NonZeroCount()} {
    SDB_ASSERT(column.RowGroupCount() == 1);
    SDB_ASSERT(column.ByteSize(0) == ByteSize);
    SDB_ASSERT(column.RowCount() != 0);
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    const auto* IRS_RESTRICT const bytes = _bytes;
    auto* IRS_RESTRICT const values_data = values.data();
    const auto* IRS_RESTRICT const docs_data = docs.data();
    for (size_t i = 0, n = docs.size(); i != n; ++i) {
      values_data[i] = ReadAt(bytes, docs_data[i]);
    }
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    return ReadAt(_bytes, doc);
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    const auto* IRS_RESTRICT const bytes = _bytes;
    auto* IRS_RESTRICT const values_data = values.data();
    const auto* IRS_RESTRICT const docs_data = docs.data();
    if (docs_data[kPostingBlock - 1] - docs_data[0] == kPostingBlock - 1) {
      const auto first = docs_data[0];
      for (scores_size_t i = 0; i != kPostingBlock; ++i) {
        values_data[i] = ReadAt(bytes, first + i);
      }
    } else {
#pragma clang loop unroll(full)
      for (scores_size_t i = 0; i != kPostingBlock; ++i) {
        values_data[i] = ReadAt(bytes, docs_data[i]);
      }
    }
  }

  score_t GetAvg() const noexcept final {
    if (_non_zero == 0) {
      return {};
    }
    return static_cast<double>(_sum) / static_cast<double>(_non_zero);
  }

 private:
  IRS_FORCE_INLINE static uint32_t ReadAt(const byte_type* IRS_RESTRICT base,
                                          uint64_t doc) noexcept {
    if constexpr (ByteSize == 1) {
      return base[doc];
    } else if constexpr (ByteSize == 2) {
      return absl::little_endian::Load16(base + doc * 2);
    } else {
      return absl::little_endian::Load32(base + doc * 4);
    }
  }

  const byte_type* _bytes;
  uint64_t _sum;
  uint64_t _non_zero;
};

class MultiRgNormReader : public NormReader {
 public:
  explicit MultiRgNormReader(const NormColumnReader& column) noexcept
    : _column{&column} {
    SDB_ASSERT(column.RowGroupCount() > 1);
  }

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    GetBatch(docs, values);
  }

  uint32_t Get(doc_id_t doc) noexcept final {
    SDB_ASSERT(doc >= doc_limits::min());
    SDB_ASSERT(_column->RowCount() != 0);
    if (doc < _rg_first_doc || _rg_end_doc <= doc) {
      RefreshRowGroup(static_cast<uint64_t>(doc) - doc_limits::min());
    }
    const auto in_rg = static_cast<uint64_t>(doc) - _rg_first_doc;
    if (_byte_size == 1) {
      return _bytes[in_rg];
    }
    if (_byte_size == 2) {
      return absl::little_endian::Load16(_bytes + in_rg * 2);
    }
    return absl::little_endian::Load32(_bytes + in_rg * 4);
  }

  void GetPostingBlock(
    std::span<const doc_id_t, kPostingBlock> docs,
    std::span<uint32_t, kPostingBlock> values) noexcept final {
    GetBatch(docs, values);
  }

  score_t GetAvg() const noexcept final {
    const auto nz = _column->NonZeroCount();
    if (nz == 0) {
      return {};
    }
    return static_cast<double>(_column->Sum()) / static_cast<double>(nz);
  }

 private:
  template<uint8_t ByteSize>
  IRS_FORCE_INLINE static void DecodeRun(const byte_type* IRS_RESTRICT base,
                                         uint64_t rg_first_doc,
                                         std::span<const doc_id_t> docs,
                                         std::span<uint32_t> values) noexcept {
    static_assert(ByteSize == 1 || ByteSize == 2 || ByteSize == 4);
    for (size_t i = 0; i < docs.size(); ++i) {
      const auto in_rg = static_cast<uint64_t>(docs[i]) - rg_first_doc;
      if constexpr (ByteSize == 1) {
        values[i] = base[in_rg];
      } else if constexpr (ByteSize == 2) {
        values[i] = absl::little_endian::Load16(base + in_rg * 2);
      } else {
        values[i] = absl::little_endian::Load32(base + in_rg * 4);
      }
    }
  }

  void DispatchRun(std::span<const doc_id_t> docs,
                   std::span<uint32_t> values) const noexcept {
    SDB_ASSERT(!docs.empty());
    if (_byte_size == 1) {
      DecodeRun<1>(_bytes, _rg_first_doc, docs, values);
    } else if (_byte_size == 2) {
      DecodeRun<2>(_bytes, _rg_first_doc, docs, values);
    } else {
      DecodeRun<4>(_bytes, _rg_first_doc, docs, values);
    }
  }

  void GetBatch(std::span<const doc_id_t> docs,
                std::span<uint32_t> values) noexcept {
    SDB_ASSERT(_column->RowCount() != 0);
    if (docs.empty()) {
      return;
    }
    size_t i = 0;
    while (i < docs.size()) {
      if (docs[i] < _rg_first_doc || _rg_end_doc <= docs[i]) {
        RefreshRowGroup(static_cast<uint64_t>(docs[i]) - doc_limits::min());
      }
      size_t j = i + 1;
      while (j < docs.size() && docs[j] >= _rg_first_doc &&
             docs[j] < _rg_end_doc) {
        ++j;
      }
      DispatchRun(docs.subspan(i, j - i), values.subspan(i, j - i));
      i = j;
    }
  }

  void RefreshRowGroup(uint64_t row_pos) noexcept {
    const auto [rg, _] = _column->Locate(row_pos);
    const auto info = _column->Rg(rg);
    _bytes = info.bytes.data();
    _byte_size = info.byte_size;
    _rg_first_doc = info.first_row + doc_limits::min();
    _rg_end_doc = _rg_first_doc + info.row_count;
  }

  const NormColumnReader* _column;
  const byte_type* _bytes = nullptr;
  uint64_t _rg_first_doc = 0;
  uint64_t _rg_end_doc = 0;
  uint8_t _byte_size = 0;
};

inline memory::managed_ptr<NormReader> MakePersistedNormReader(
  const NormColumnReader& column) {
  if (column.RowGroupCount() == 1) {
    switch (column.ByteSize(0)) {
      case 1:
        return memory::make_managed<SingleRgNormReader<1>>(column);
      case 2:
        return memory::make_managed<SingleRgNormReader<2>>(column);
      default:
        SDB_ASSERT(column.ByteSize(0) == 4);
        return memory::make_managed<SingleRgNormReader<4>>(column);
    }
  }
  return memory::make_managed<MultiRgNormReader>(column);
}

}  // namespace irs
