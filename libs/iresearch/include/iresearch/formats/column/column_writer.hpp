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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <span>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColWriter;

struct WriteChunk {
  duckdb::Vector data;
  duckdb::idx_t count;
};

class ColumnWriter final {
 public:
  ColumnWriter(ColWriter& owner, field_id id, duckdb::LogicalType type,
               bool skip_validity, uint32_t row_group_size,
               duckdb::CompressionType forced, bool hyperloglog);

  ColumnWriter(const ColumnWriter&) = delete;
  ColumnWriter& operator=(const ColumnWriter&) = delete;

  void Append(const duckdb::Vector& vec, duckdb::idx_t count);

  void Append(uint64_t start_row, const duckdb::Vector& vec,
              duckdb::idx_t count);

  template<typename Fill>
  void PushInStaging(uint64_t row, Fill&& fill) {
    PadNullsTo(row);
    auto& back = OpenChunk();
    fill(back.data, back.count);
    ++back.count;
    duckdb::FlatVector::SetSize(back.data, back.count);
    ++_staged_rows;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }

  void PadNullsTo(uint64_t target_row);

  void SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll);

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  const ColumnMeta& Meta() const noexcept { return _meta; }

 private:
  friend class ColWriter;

  void AppendDense(const duckdb::Vector& vec, duckdb::idx_t count);
  void PadNestedNulls(uint64_t count);
  WriteChunk& OpenChunk();

  void SealRowGroup();

  duckdb::optional_ptr<const duckdb::CompressionFunction> PickCodec(
    const duckdb::LogicalType& codec_type, std::span<WriteChunk> chunks,
    duckdb::CompressionType forced,
    duckdb::unique_ptr<duckdb::AnalyzeState>& out_state);

  void Compress(const duckdb::CompressionFunction& picked,
                duckdb::unique_ptr<duckdb::AnalyzeState> state,
                const duckdb::LogicalType& codec_type,
                std::span<WriteChunk> chunks,
                std::vector<ColumnBlockMeta>& sink);

  void SealValidity(std::span<WriteChunk> chunks, uint64_t row_count,
                    std::vector<ColumnBlockMeta>& sink);

  void SealNestedValidity(std::span<WriteChunk> chunks, uint64_t row_count,
                          bool skip_validity, size_t child_count,
                          ColumnMeta& meta);

  void SealStruct(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                  uint64_t row_count, bool skip_validity,
                  duckdb::CompressionType forced, ColumnMeta& meta);

  void SealArray(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                 uint64_t row_count, bool skip_validity,
                 duckdb::CompressionType forced, ColumnMeta& meta);

  void SealList(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                uint64_t row_count, bool skip_validity,
                duckdb::CompressionType forced, ColumnMeta& meta);

  void SealVariant(const duckdb::LogicalType& type,
                   std::span<WriteChunk> chunks, uint64_t row_count,
                   bool skip_validity, duckdb::CompressionType forced,
                   ColumnMeta& meta);

  void SealColumn(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                  uint64_t row_count, bool skip_validity,
                  duckdb::CompressionType forced, ColumnMeta& meta);

  WriteContext& WriteCtx() const noexcept;
  IndexOutput& Out() const noexcept;

  ColWriter* _owner;
  field_id _id;
  duckdb::LogicalType _type;
  bool _skip_validity = false;
  uint32_t _row_group_size = 0;
  duckdb::CompressionType _forced = duckdb::CompressionType::COMPRESSION_AUTO;
  std::vector<WriteChunk> _staged;
  std::vector<duckdb::VectorCache> _staged_caches;
  bool _is_nested = false;
  std::unique_ptr<duckdb::Vector> _null_pad;
  duckdb::idx_t _staged_chunks = 0;
  uint64_t _staged_rows = 0;
  uint64_t _row_start = 0;
  bool _hll_auto = false;
  duckdb::Vector _hll_hashes{duckdb::LogicalType::HASH, nullptr};
  int64_t _variant_min_shred_size = -1;
  duckdb::LogicalType _force_variant_shredding;
  ColumnMeta _meta;
};

}  // namespace irs
