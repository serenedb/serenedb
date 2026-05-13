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
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

// Bridges `columnstore::NormColumnReader` to the `NormReader` interface
// scorers (BM25 / TFIDF / DFI / LM-* / Indri) consume via
// SubReader::norms(field_id). Per-row-group raw 1/2/4-byte payload spans
// are cached on the reader, so the per-doc Get() on the BM25 hot path is
// one stride-indexed load.
//
// doc_id space: iresearch doc_ids are 1-indexed (doc_limits::min() == 1);
// row positions in the columnstore are 0-indexed. The adapter applies the
// translation once per call.
class PersistedNormReader : public NormReader {
 public:
  explicit PersistedNormReader(
    const columnstore::NormColumnReader& column) noexcept
    : _column{&column} {}

  void Get(std::span<const doc_id_t> docs,
           std::span<uint32_t> values) noexcept final {
    SDB_ASSERT(docs.size() <= values.size());
    GetBatch(docs, values);
  }

  uint32_t Get(doc_id_t doc) noexcept final { return GetOne(doc); }

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
  uint32_t GetOne(doc_id_t doc) const noexcept {
    SDB_ASSERT(doc >= doc_limits::min());
    return _column->Get(doc - doc_limits::min());
  }

  // Batch read with row-group reuse: locate the RG once, stride-index
  // every subsequent doc that falls in the same RG. Crossing only pays
  // an extra log(rg_count) upper_bound. BM25's posting block is the hot
  // caller -- usually all kPostingBlock docs land in one RG, so this is
  // O(1 + kPostingBlock) instead of O(kPostingBlock * log(rg_count)).
  void GetBatch(std::span<const doc_id_t> docs,
                std::span<uint32_t> values) const noexcept {
    uint64_t rg_first_row = 0;
    uint64_t rg_end_row = 0;
    std::span<const byte_type> rg_bytes;
    uint8_t byte_size = 0;
    for (size_t i = 0; i < docs.size(); ++i) {
      SDB_ASSERT(docs[i] >= doc_limits::min());
      const auto row_pos = static_cast<uint64_t>(docs[i]) - doc_limits::min();
      if (row_pos < rg_first_row || row_pos >= rg_end_row) {
        const auto [rg, _] = _column->Locate(row_pos);
        rg_bytes = _column->RowGroupBytes(rg);
        byte_size = _column->ByteSize(rg);
        rg_first_row = _column->RowGroupFirstRow(rg);
        rg_end_row = rg_first_row + _column->RowGroupRowCount(rg);
      }
      const auto in_rg = row_pos - rg_first_row;
      values[i] = columnstore::ReadNormValue(
        rg_bytes.data() + in_rg * byte_size, byte_size);
    }
  }

  const columnstore::NormColumnReader* _column;
};

}  // namespace irs
