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
  // Norms are partitioned on the same grid as the postings, so one row group
  // of the column serves one row group of the field and the reader is bound to
  // it. `_bytes` is that row group's payload pre-shifted by
  // `ByteSize * doc_limits::min()` so callers index by their raw local `doc`
  // directly -- the per-element `doc - doc_limits::min()` subtraction is folded
  // into the base pointer once at construction. Reads still land in the
  // original buffer because `doc >= doc_limits::min()` is a precondition.
  SingleRgNormReader(const NormColumnReader& column, uint32_t rg) noexcept
    : _bytes{column.RowGroupBytes(rg).data() -
             ByteSize * static_cast<ptrdiff_t>(doc_limits::min())},
      _sum{column.Sum()},
      _non_zero{column.NonZeroCount()} {
    SDB_ASSERT(column.ByteSize(rg) == ByteSize);
    SDB_ASSERT(column.RowGroupRowCount(rg) != 0);
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

// `rg` is the row group the reader's ids are local to. The norm column is
// partitioned on the same grid as the field's postings -- both take the index's
// one `row_group_size` -- so the row group selects the payload and the ids need
// no rebasing.
inline memory::managed_ptr<NormReader> MakePersistedNormReader(
  const NormColumnReader& column, uint32_t rg = 0) {
  SDB_ASSERT(rg < column.RowGroupCount());
  switch (column.ByteSize(rg)) {
    case 1:
      return memory::make_managed<SingleRgNormReader<1>>(column, rg);
    case 2:
      return memory::make_managed<SingleRgNormReader<2>>(column, rg);
    default:
      SDB_ASSERT(column.ByteSize(rg) == 4);
      return memory::make_managed<SingleRgNormReader<4>>(column, rg);
  }
}

}  // namespace irs
