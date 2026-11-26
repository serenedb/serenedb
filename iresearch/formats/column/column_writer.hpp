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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/serializer/write_stream.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/storage/statistics/distinct_statistics.hpp>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/formats/column/list_ingest.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColWriter;

void DropAllValidMask(WriteChunk& chunk);

void UpdateDistinct(duckdb::DistinctStatistics& distinct, DistinctStats mode,
                    const duckdb::Vector& values, duckdb::idx_t count,
                    duckdb::Vector& hashes);

struct StagedColumn {
  field_id id;
  duckdb::LogicalType type;
  std::vector<WriteChunk> chunks;
  DistinctStats distinct = DistinctStats::None;
};

struct ColumnWriterOptions {
  bool skip_validity = false;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  codecs::CodecType compression = codecs::CodecType::Auto;
  DistinctStats distinct = DistinctStats::None;
  ColCodecParams codec_params;

  bool operator==(const ColumnWriterOptions&) const = default;
};

struct ColumnWriteState {
  codecs::ColumnTuning tuning;
  uint64_t list_running = 0;
  uint64_t list_distinct = 0;
  std::vector<ColumnWriteState> children;
};

class ColumnWriter final {
 public:
  ColumnWriter(ColWriter& owner, field_id id, duckdb::LogicalType type,
               const ColumnWriterOptions& options, bool deferred);

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
    if (back.count == duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
      DropAllValidMask(back);
    }
    duckdb::FlatVector::SetSize(back.data, back.count);
    ++_staged_rows;
    if (!_deferred && _staged_rows == _rg_rows) {
      SealRowGroup();
    }
  }

  void PadNullsTo(uint64_t target_row);

  bool CopyableFrom(const ColumnReader& column) const;
  [[nodiscard]] bool CopyBlocks(const ColumnReader& column, ReadContext& source,
                                absl::FunctionRef<bool()> progress);
  void CopyRows(uint64_t start_row, const ColumnReader& column,
                ReadContext& source, uint64_t first, uint64_t count);

  std::vector<WriteChunk> Snapshot(uint64_t rows) const;
  size_t StagedBytes() const noexcept;

  void Finish() { SealRowGroup(); }

  void SetDistinct(duckdb::shared_ptr<duckdb::DistinctStatistics> distinct);

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  const ColumnWriterOptions& Options() const noexcept { return _options; }
  const ColumnMeta& Meta() const noexcept { return _meta; }
  ColumnMeta TakeMeta() noexcept { return std::move(_meta); }

 private:
  template<typename Fill>
  void Stage(uint64_t count, Fill&& fill);
  void AppendList(const duckdb::Vector& vec, duckdb::idx_t count);
  void AppendStruct(const duckdb::Vector& vec, duckdb::idx_t count);
  WriteChunk& OpenChunk();
  bool IngestsList() const noexcept {
    return !_ingest.empty() &&
           _type.InternalType() == duckdb::PhysicalType::LIST;
  }

  bool LayoutMatches(const ColumnReader& column) const noexcept;
  codecs::CodecType ForcedCodec() const;
  bool ValidityCopyable(const ColumnReader& column, uint64_t first,
                        uint64_t end) const noexcept;
  void CopyBlock(const ColumnBlockMeta& block, ReadContext& source,
                 std::vector<ColumnBlockMeta>& sink);
  void CopyValidity(const ColumnReader& column, ReadContext& source,
                    uint64_t first, uint64_t end);
  void DecodeRows(const ColumnReader& column, ReadContext& source,
                  uint64_t first, uint64_t count);

  void SealRowGroup();
  void SealStaged(const WriteRows& rows);
  void OpenRowGroup() noexcept;

  bool Compress(const duckdb::LogicalType& type, const WriteRows& rows,
                codecs::CodecType method, codecs::ColumnTuning& tuning,
                std::vector<ColumnBlockMeta>& sink);

  bool Enabled(codecs::CodecType method,
               const duckdb::LogicalType& type) const noexcept;

  void CompressPlain(const duckdb::LogicalType& type, codecs::CodecType codec,
                     const WriteRows& rows, std::vector<ColumnBlockMeta>& sink);

  void SealValidity(const WriteRows& rows, std::vector<ColumnBlockMeta>& sink);

  void Claim(ColumnMeta& meta, const duckdb::LogicalType& type) const;

  void SealNestedValidity(const WriteRows& rows, bool skip_validity,
                          size_t child_count, ColumnMeta& meta,
                          ColumnWriteState& state);

  void SealStruct(const duckdb::LogicalType& type, const WriteRows& rows,
                  bool skip_validity, ColumnMeta& meta, ColumnWriteState& state,
                  std::span<const std::unique_ptr<ListIngest>> ingest);

  void SealArray(const duckdb::LogicalType& type, const WriteRows& rows,
                 bool skip_validity, ColumnMeta& meta, ColumnWriteState& state);

  void SealList(const duckdb::LogicalType& type, const WriteRows& rows,
                bool skip_validity, ColumnMeta& meta, ColumnWriteState& state);

  void SealListParts(const duckdb::LogicalType& type, const WriteRows& rows,
                     bool skip_validity, ListIngest& ingest, ColumnMeta& meta,
                     ColumnWriteState& state);

  void SealVariant(const duckdb::LogicalType& type, const WriteRows& rows,
                   bool skip_validity, ColumnMeta& meta,
                   ColumnWriteState& state);

  void SealColumn(const duckdb::LogicalType& type, const WriteRows& rows,
                  bool skip_validity, ColumnMeta& meta, ColumnWriteState& state,
                  std::span<const std::unique_ptr<ListIngest>> ingest = {});

  WriteContext& WriteCtx() const noexcept;

  ColWriter* _owner;
  field_id _id;
  duckdb::LogicalType _type;
  ColumnWriterOptions _options;
  codecs::CodecType _method = codecs::CodecType::Auto;
  codecs::CodecType _global_method = codecs::CodecType::Auto;
  codecs::CodecSet _disabled;
  std::vector<WriteChunk> _staged;
  std::vector<duckdb::VectorCache> _staged_caches;
  std::unique_ptr<duckdb::Vector> _null_pad;
  duckdb::idx_t _staged_chunks = 0;
  uint64_t _staged_rows = 0;
  uint64_t _row_start = 0;
  uint64_t _rg_rows = 0;
  const ReadContext* _shared_source = nullptr;
  uint64_t _shared_from = 0;
  uint64_t _shared_to = 0;
  bool _deferred = false;
  duckdb::Vector _distinct_hashes{duckdb::LogicalType::HASH, nullptr};
  int64_t _variant_min_shred_size = -1;
  duckdb::LogicalType _force_variant_shredding;
  std::vector<std::unique_ptr<ListIngest>> _ingest;
  std::vector<duckdb::UnifiedVectorFormat> _field_rows;
  ColumnMeta _meta;
  ColumnWriteState _state;
};

}  // namespace irs
