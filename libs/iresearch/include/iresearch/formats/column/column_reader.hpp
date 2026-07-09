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
#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace duckdb {

class Serializer;
class Deserializer;
class CompressionFunction;

}  // namespace duckdb
namespace irs {

struct BlockWindow {
  size_t block = 0;
  duckdb::idx_t begin = 0;
  duckdb::idx_t end = 0;
};

struct IotaRange {
  using contiguous_range_tag = void;
  uint64_t start;
  uint64_t count;
  constexpr size_t size() const noexcept { return static_cast<size_t>(count); }
  constexpr uint64_t operator[](size_t i) const noexcept { return start + i; }
};

template<typename Rows>
inline size_t ConsecutiveRunLength(const Rows& rows, size_t i) noexcept {
  SDB_ASSERT(i < rows.size());
  if constexpr (requires { typename Rows::contiguous_range_tag; }) {
    return rows.size() - i;
  }
  size_t run = 1;
  while (i + run < rows.size() &&
         static_cast<uint64_t>(rows[i + run]) ==
           static_cast<uint64_t>(rows[i + run - 1]) + 1) {
    ++run;
  }
  return run;
}

struct ColumnBlockMeta {
  duckdb::BaseStatistics statistics;
  uint64_t tuple_count = 0;
  uint64_t file_offset = 0;
  uint64_t byte_size = 0;
  const duckdb::CompressionFunction* codec = nullptr;
};

struct ColumnMeta;

struct VariantRgMeta {
  uint64_t row_count = 0;
  std::unique_ptr<ColumnMeta> unshredded;
  std::unique_ptr<ColumnMeta> shredded;
};

struct ColumnMeta {
  field_id id = 0;
  duckdb::LogicalType type;
  std::vector<ColumnBlockMeta> data;
  std::vector<ColumnBlockMeta> validity;
  std::vector<ColumnMeta> children;
  std::vector<VariantRgMeta> variant_rgs;
  duckdb::shared_ptr<duckdb::HyperLogLog> hyperloglog;
  uint64_t write_list_running = 0;
};

void SerializeColumnMeta(duckdb::Serializer& s, const ColumnMeta& meta);
ColumnMeta DeserializeColumnMeta(duckdb::Deserializer& d);

struct VariantScanState;

class ColumnReader {
 public:
  struct VectorScratch {
    explicit VectorScratch(const duckdb::LogicalType& type)
      : cache{duckdb::Allocator::DefaultAllocator(), type}, vector{cache} {}
    VectorScratch(const VectorScratch&) = delete;
    VectorScratch(VectorScratch&&) = delete;

    duckdb::Vector& Reset() {
      cache.ResetFromCache(vector);
      return vector;
    }

    duckdb::VectorCache cache;
    duckdb::Vector vector;
  };

  struct ScanState {
    ScanState();
    ScanState(ScanState&&);
    ScanState& operator=(ScanState&&);
    ~ScanState();

    duckdb::ColumnScanState st{nullptr};
    BlockWindow window{};
    std::vector<std::unique_ptr<duckdb::ColumnSegment>> segments;
    std::vector<ScanState> child_states;
    std::unique_ptr<VariantScanState> variant;
    ReadContext* ctx = nullptr;
    bool initialized = false;
    duckdb::SelectionVector sel;
    std::unique_ptr<VectorScratch> list_offsets;
  };

  virtual ~ColumnReader() = default;

  static std::unique_ptr<ColumnReader> Make(ColumnMeta&& meta);

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  uint64_t RowCount() const noexcept { return _row_count; }
  size_t DataRgCount() const noexcept { return _segments.size(); }
  bool HasValidity() const noexcept { return _validity != nullptr; }
  const ColumnReader* Validity() const noexcept { return _validity.get(); }
  std::span<const ColumnBlockMeta> DataBlocks() const noexcept {
    return _segments;
  }
  uint64_t DataBlockFirstRow(size_t block) const noexcept {
    SDB_ASSERT(block < _offsets.size());
    return _offsets[block];
  }
  bool NullsInData() const noexcept;
  const duckdb::HyperLogLog* HyperLogLog() const noexcept {
    return _hyperloglog.get();
  }

  const duckdb::BaseStatistics& MergedStatistics() const noexcept {
    return *_stats;
  }

  const ColumnReader* Child() const noexcept {
    return (_type.id() == duckdb::LogicalTypeId::ARRAY ||
            _type.id() == duckdb::LogicalTypeId::LIST ||
            _type.id() == duckdb::LogicalTypeId::MAP) &&
               !_children.empty()
             ? _children.front().get()
             : nullptr;
  }
  uint64_t ArraySize() const noexcept { return _array_size; }

  size_t StructFieldCount() const noexcept {
    return _type.id() == duckdb::LogicalTypeId::STRUCT ? _children.size() : 0;
  }
  const ColumnReader& StructField(size_t i) const noexcept {
    SDB_ASSERT(i < _children.size());
    return *_children[i];
  }

  // Zonemap for row group `rg`, used by pushed table-filter pruning.
  const duckdb::BaseStatistics& RowGroupStatistics(size_t rg) const noexcept {
    SDB_ASSERT(rg < _segments.size());
    return _segments[rg].statistics;
  }
  bool IsValidityRgEmpty(size_t vrg) const noexcept {
    SDB_ASSERT(_validity && vrg < _validity->_segments.size());
    return _validity->_segments[vrg].codec->type ==
           duckdb::CompressionType::COMPRESSION_EMPTY;
  }

  BlockWindow Locate(uint64_t row, BlockWindow hint = {}) const noexcept;

  virtual uint64_t RowGroupEnd(uint64_t row) const noexcept;

  std::unique_ptr<duckdb::ColumnSegment> OpenSegment(size_t rg,
                                                     ReadContext& ctx) const {
    return Open(BlockWindow{rg, _offsets[rg], _offsets[rg + 1]}, ctx);
  }

  ScanState InitScan(ReadContext& ctx) const;

  virtual uint64_t GatherCursor(const ScanState& s) const noexcept {
    return s.window.begin + s.st.offset_in_column;
  }

  virtual duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                             duckdb::idx_t count) const;

  virtual duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                                  duckdb::idx_t count,
                                  duckdb::idx_t result_offset) const;

  virtual void Skip(ScanState& s, duckdb::idx_t count) const;

  virtual void GatherScatter(ScanState& s, uint64_t anchor,
                             const duckdb::SelectionVector& sel,
                             duckdb::idx_t hits, duckdb::Vector& out,
                             duckdb::idx_t at) const;

  virtual void GatherDense(ScanState& s, uint64_t anchor,
                           const duckdb::SelectionVector& sel,
                           duckdb::idx_t hits, duckdb::idx_t span,
                           duckdb::Vector& out) const;

  class PointReader {
   public:
    PointReader(const ColReader& col_reader, const ColumnReader& col);

    PointReader(const PointReader&) = delete;
    PointReader& operator=(const PointReader&) = delete;

    bool FetchRow(uint64_t row, duckdb::Vector& out, duckdb::idx_t out_offset);

   protected:
    ReadContext _ctx;
    const ColumnReader* _reader;
    std::unique_ptr<duckdb::ColumnSegment> _block;
    std::unique_ptr<duckdb::ColumnSegment> _validity_block;
    duckdb::ColumnFetchState _fetch_state;
    duckdb::ColumnFetchState _validity_fetch_state;
    BlockWindow _window{};
    BlockWindow _validity_window{};
    size_t _cached_block = static_cast<size_t>(-1);
    size_t _cached_validity_block = static_cast<size_t>(-1);
  };

  class BlobPointReader final : public PointReader {
   public:
    using PointReader::PointReader;

    bytes_view FetchRow(uint64_t row) {
      duckdb::FlatVector::ValidityMutable(_buf).Reset();
      if (!PointReader::FetchRow(row, _buf, 0)) {
        return {};
      }
      const auto& s = duckdb::FlatVector::GetData<duckdb::string_t>(_buf)[0];
      return bytes_view{reinterpret_cast<const byte_type*>(s.GetData()),
                        s.GetSize()};
    }
    bytes_view FetchDoc(doc_id_t doc) {
      return FetchRow(static_cast<uint64_t>(doc) - doc_limits::min());
    }
    bool IsNullRow(uint64_t row) {
      duckdb::FlatVector::ValidityMutable(_buf).Reset();
      return !PointReader::FetchRow(row, _buf, 0);
    }
    bool IsNullDoc(doc_id_t doc) {
      return IsNullRow(static_cast<uint64_t>(doc) - doc_limits::min());
    }

   private:
    duckdb::Vector _buf{duckdb::LogicalType::BLOB, 1};
  };

 protected:
  ColumnReader(field_id id, duckdb::LogicalType type,
               std::vector<ColumnBlockMeta> segments,
               std::unique_ptr<ColumnReader> validity,
               std::vector<std::unique_ptr<ColumnReader>> children);

  void FinishStats(duckdb::BaseStatistics stats);

  void SkipRows(ScanState& s, duckdb::idx_t count) const;

  duckdb::idx_t ScanVector(ScanState& s, duckdb::Vector& result,
                           duckdb::idx_t count,
                           duckdb::ScanVectorType scan_type,
                           duckdb::idx_t base_result_offset = 0) const;

  virtual void NewOutputVector(ScanState& s) const;
  static void ResetOutput(const ColumnReader& reader, ScanState& s) {
    reader.NewOutputVector(s);
  }

  field_id _id;
  duckdb::LogicalType _type;
  std::vector<ColumnBlockMeta> _segments;
  std::vector<uint64_t> _offsets;
  uint64_t _row_count = 0;
  std::unique_ptr<ColumnReader> _validity;
  std::vector<std::unique_ptr<ColumnReader>> _children;
  uint64_t _array_size = 0;
  duckdb::shared_ptr<duckdb::HyperLogLog> _hyperloglog;
  duckdb::unique_ptr<duckdb::BaseStatistics> _stats;

 private:
  std::unique_ptr<duckdb::ColumnSegment> Open(const BlockWindow& w,
                                              ReadContext& ctx) const;
  bool NextSegment(BlockWindow& w) const noexcept;
  void BeginScanVector(ScanState& s) const;
  duckdb::ScanVectorType GetVectorScanType(ScanState& s, duckdb::idx_t count,
                                           duckdb::Vector& result) const;
};

}  // namespace irs
