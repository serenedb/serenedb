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
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/list_ingest.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"

namespace irs {

void EmitEmptyValidity(uint64_t rows, std::vector<ColumnBlockMeta>& sink);

struct ColumnWriterOptions {
  bool skip_validity = false;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  codecs::CodecType compression = codecs::CodecType::Auto;
  bool hyperloglog = false;
  ColCodecParams codec_params;
};

class ColumnWriter final {
 public:
  ColumnWriter(WriteContext& ctx, field_id id, duckdb::LogicalType type,
               const ColumnWriterOptions& options);

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

  void Finish() { SealRowGroup(); }

  void SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll);

  const duckdb::LogicalType& Type() const noexcept { return _type; }
  const ColumnMeta& Meta() const noexcept { return _meta; }

 private:
  friend class ColWriter;

  template<typename Fill>
  void Stage(uint64_t count, Fill&& fill);
  void AppendList(const duckdb::Vector& vec, duckdb::idx_t count);
  void AppendStruct(const duckdb::Vector& vec, duckdb::idx_t count);
  WriteChunk& OpenChunk();
  bool IngestsList() const noexcept {
    return !_ingest.empty() &&
           _type.InternalType() == duckdb::PhysicalType::LIST;
  }

  void SealRowGroup();

  template<typename Encode>
  uint64_t WriteBlocks(const codecs::CompressionFunction& function,
                       const duckdb::LogicalType& type,
                       std::vector<ColumnBlockMeta>& sink, Encode&& encode);

  uint64_t Compress(const codecs::CompressionFunction& function,
                    const duckdb::LogicalType& type,
                    std::span<const WriteChunk> chunks,
                    std::vector<ColumnBlockMeta>& sink);

  void CompressData(const duckdb::LogicalType& type,
                    std::span<const WriteChunk> chunks,
                    codecs::CodecType method,
                    std::vector<ColumnBlockMeta>& sink);

  void CompressStrings(const duckdb::LogicalType& type,
                       std::span<const WriteChunk> chunks,
                       std::vector<ColumnBlockMeta>& sink);

  void CompressPlain(const duckdb::LogicalType& type, codecs::CodecType codec,
                     std::span<WriteChunk> chunks,
                     std::vector<ColumnBlockMeta>& sink);

  bool SealString(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                  uint64_t row_count, ColumnMeta& meta);

  void SealNumeric(const duckdb::LogicalType& type,
                   std::span<WriteChunk> chunks, uint64_t row_count,
                   ColumnMeta& meta);

  void SealValidity(std::span<WriteChunk> chunks, uint64_t row_count,
                    std::vector<ColumnBlockMeta>& sink);

  void Claim(ColumnMeta& meta, const duckdb::LogicalType& type) const;

  void SealNestedValidity(std::span<WriteChunk> chunks, uint64_t row_count,
                          bool skip_validity, size_t child_count,
                          ColumnMeta& meta);

  void SealStruct(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                  uint64_t row_count, bool skip_validity, ColumnMeta& meta,
                  std::span<const std::unique_ptr<ListIngest>> ingest);

  void SealArray(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                 uint64_t row_count, bool skip_validity, ColumnMeta& meta);

  void SealList(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                uint64_t row_count, bool skip_validity, ColumnMeta& meta);

  void SealListParts(const duckdb::LogicalType& type,
                     std::span<WriteChunk> chunks, uint64_t row_count,
                     bool skip_validity, ListIngest& ingest, ColumnMeta& meta);

  void SealVariant(const duckdb::LogicalType& type,
                   std::span<WriteChunk> chunks, uint64_t row_count,
                   bool skip_validity, ColumnMeta& meta);

  void SealColumn(const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
                  uint64_t row_count, bool skip_validity, ColumnMeta& meta,
                  std::span<const std::unique_ptr<ListIngest>> ingest = {});

  WriteContext& WriteCtx() const noexcept { return *_ctx; }

  WriteContext* _ctx;
  field_id _id;
  duckdb::LogicalType _type;
  bool _skip_validity = false;
  bool _ingest_lists = false;
  uint32_t _row_group_size = 0;
  codecs::CodecType _named = codecs::CodecType::Auto;
  codecs::CodecType _method = codecs::CodecType::Auto;
  codecs::CodecType _global_method = codecs::CodecType::Auto;
  codecs::CodecSet _disabled;

  ColCodecParams _codec_params;
  std::vector<WriteChunk> _staged;
  std::vector<duckdb::VectorCache> _staged_caches;
  std::unique_ptr<duckdb::Vector> _null_pad;
  duckdb::idx_t _staged_chunks = 0;
  uint64_t _staged_rows = 0;
  uint64_t _row_start = 0;
  bool _hll_auto = false;
  duckdb::Vector _hll_hashes{duckdb::LogicalType::HASH, nullptr};
  int64_t _variant_min_shred_size = -1;
  duckdb::LogicalType _force_variant_shredding;
  std::vector<std::unique_ptr<ListIngest>> _ingest;
  std::vector<duckdb::UnifiedVectorFormat> _field_rows;
  ColumnMeta _meta;
};

}  // namespace irs
