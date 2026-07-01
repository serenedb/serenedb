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
#include <atomic>
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
#include <string>
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
  duckdb::CompressionType compression_type =
    duckdb::CompressionType::COMPRESSION_AUTO;
};

enum class VariantShredState : uint8_t {
  Unshredded = 0,
  Partial = 1,
  Full = 2,
};

struct ColumnMeta;

struct VariantRgMeta {
  uint64_t row_count = 0;
  VariantShredState shred_state = VariantShredState::Unshredded;
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

class ColumnReader final {
 public:
  struct ScanState;

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

  struct VariantRgScan {
    std::unique_ptr<ScanState> unshredded;
    std::unique_ptr<ScanState> shredded;
    std::unique_ptr<VectorScratch> intermediate;
    uint64_t local_pos = 0;
    std::unique_ptr<ScanState> leaf;
    const ColumnReader* leaf_reader = nullptr;
    uint64_t leaf_pos = 0;
  };

  struct ScanState {
    duckdb::ColumnScanState st{nullptr};
    BlockWindow window{};
    std::vector<std::unique_ptr<duckdb::ColumnSegment>> segments;
    std::vector<ScanState> child_states;
    std::vector<VariantRgScan> variant_rgs;
    std::unique_ptr<VectorScratch> scratch;
    uint64_t variant_cursor = 0;
    ReadContext* ctx = nullptr;
    bool initialized = false;
    int8_t variant_extract_ok = -1;
    std::vector<std::string> variant_extract_path;
    duckdb::LogicalType variant_extract_type;
    int8_t variant_leaf_resolved = -1;
    duckdb::LogicalType variant_leaf_type;
  };

  static std::unique_ptr<ColumnReader> Make(ColumnMeta&& meta);

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  uint64_t RowCount() const noexcept { return _row_count; }
  size_t DataRgCount() const noexcept { return _segments.size(); }
  bool HasValidity() const noexcept { return _validity != nullptr; }
  const ColumnReader* Validity() const noexcept { return _validity.get(); }
  bool NullsInData() const noexcept { return _nulls_in_data; }
  const duckdb::HyperLogLog* HyperLogLog() const noexcept {
    return _hyperloglog.get();
  }

  uint64_t CursorRow(const ScanState& s) const noexcept {
    return GatherCursor(s);
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

  size_t ValidityRgCount() const noexcept {
    return _validity ? _validity->_segments.size() : 0;
  }
  uint64_t ValidityRgFirstRow(size_t vrg) const noexcept {
    SDB_ASSERT(_validity && vrg < _validity->_offsets.size());
    return _validity->_offsets[vrg];
  }
  uint64_t ValidityRgRowCount(size_t vrg) const noexcept {
    SDB_ASSERT(_validity && vrg + 1 < _validity->_offsets.size());
    return _validity->_offsets[vrg + 1] - _validity->_offsets[vrg];
  }
  bool IsValidityRgEmpty(size_t vrg) const noexcept {
    SDB_ASSERT(_validity && vrg < _validity->_segments.size());
    return _validity->_segments[vrg].compression_type ==
           duckdb::CompressionType::COMPRESSION_EMPTY;
  }

  BlockWindow Locate(uint64_t row, BlockWindow hint = {}) const noexcept;

  std::unique_ptr<duckdb::ColumnSegment> OpenSegment(size_t rg,
                                                     ReadContext& ctx) const {
    return Open(BlockWindow{rg, _offsets[rg], _offsets[rg + 1]}, ctx);
  }

  ScanState InitScan(ReadContext& ctx) const;

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const;

  void Skip(ScanState& s, duckdb::idx_t count) const;

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const;

  template<typename DocIds>
  void Gather(ScanState& s, const DocIds& doc_ids, duckdb::Vector& result,
              duckdb::idx_t out_offset) const {
    const size_t n = doc_ids.size();
    size_t i = 0;
    uint64_t cur = GatherCursor(s);
    while (i < n) {
      const size_t run = ConsecutiveRunLength(doc_ids, i);
      const auto target = static_cast<uint64_t>(doc_ids[i]);
      SDB_ASSERT(target >= cur, "Gather requires ascending doc ids");
      if (target > cur) {
        Skip(s, target - cur);
      }
      cur = target + run;
      ScanCount(s, result, static_cast<duckdb::idx_t>(run),
                out_offset + static_cast<duckdb::idx_t>(i));
      i += run;
    }
  }

  template<typename DocIds>
  bool TryGatherVariantExtract(ScanState& s, const DocIds& doc_ids,
                               std::span<const std::string_view> path,
                               duckdb::Vector& out,
                               duckdb::idx_t out_offset) const {
    if (_type.id() != duckdb::LogicalTypeId::VARIANT || path.empty() ||
        _variant_rgs.empty()) {
      return false;
    }
    const auto& out_type = out.GetType();
    const bool same_key =
      s.variant_extract_ok >= 0 && s.variant_extract_type == out_type &&
      s.variant_extract_path.size() == path.size() &&
      std::equal(path.begin(), path.end(), s.variant_extract_path.begin());
    if (!same_key) {
      s.variant_extract_ok = -1;
      s.variant_extract_type = out_type;
      s.variant_extract_path.assign(path.begin(), path.end());
      for (auto& vstate : s.variant_rgs) {
        vstate.leaf.reset();
        vstate.leaf_reader = nullptr;
        vstate.leaf_pos = 0;
      }
    }
    if (s.variant_extract_ok == 0) {
      return false;
    }
    if (s.variant_extract_ok < 0) {
      for (const auto& rg : _variant_rgs) {
        const ColumnReader* leaf =
          rg.shred_state != VariantShredState::Unshredded && rg.shredded
            ? FindShreddedLeaf(*rg.shredded, path)
            : nullptr;
        if (!leaf || leaf->Type() != out_type) {
          s.variant_extract_ok = 0;
          return false;
        }
      }
      s.variant_extract_ok = 1;
    }
    const size_t n = doc_ids.size();
    size_t i = 0;
    while (i < n) {
      const size_t run = ConsecutiveRunLength(doc_ids, i);
      const auto target = static_cast<uint64_t>(doc_ids[i]);
      ForEachVariantRun(target, static_cast<duckdb::idx_t>(run),
                        [&](size_t rg, duckdb::idx_t local_start,
                            duckdb::idx_t rlen, duckdb::idx_t done) {
                          const bool entire =
                            out_offset == 0 && i == 0 && done == 0 && rlen == n;
                          ExtractShreddedLeafRun(
                            s, rg, path, local_start, rlen, out,
                            out_offset + static_cast<duckdb::idx_t>(i) + done,
                            entire);
                        });
      i += run;
    }
    return true;
  }

  bool ResolveUniformShreddedLeaf(std::span<const std::string_view> path,
                                  duckdb::LogicalType& leaf_type) const;

  void FetchRow(ReadContext& ctx, duckdb::ColumnFetchState& fetch_state,
                uint64_t row, duckdb::Vector& result,
                duckdb::idx_t result_idx) const;

  class PointReader {
   public:
    PointReader(const ColReader& col_reader, const ColumnReader& col);

    PointReader(const PointReader&) = delete;
    PointReader& operator=(const PointReader&) = delete;

    // Fetch `row` into result[out_offset]; returns false if the row is NULL.
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

 private:
  ColumnReader(field_id id, duckdb::LogicalType type, uint64_t row_count,
               std::vector<ColumnBlockMeta> segments,
               std::unique_ptr<ColumnReader> validity,
               std::vector<std::unique_ptr<ColumnReader>> children);

  duckdb::idx_t ScanStruct(ScanState& s, duckdb::Vector& result,
                           duckdb::idx_t count) const;

  duckdb::idx_t ScanVariant(ScanState& s, duckdb::Vector& result,
                            duckdb::idx_t count,
                            duckdb::idx_t result_offset) const;
  void ReconstructVariantRun(ScanState& s, size_t rg, duckdb::idx_t local_start,
                             duckdb::idx_t run, duckdb::Vector& result,
                             duckdb::idx_t out_off) const;
  static duckdb::Vector& ScratchVector(ScanState& s,
                                       const duckdb::LogicalType& type);
  void SkipVariant(ScanState& s, duckdb::idx_t count) const;
  void SeekVariantRg(ScanState& s, size_t rg, duckdb::idx_t target_local) const;
  const ColumnReader* FindShreddedLeaf(
    const ColumnReader& node, std::span<const std::string_view> path) const;
  void ExtractShreddedLeafRun(ScanState& s, size_t rg,
                              std::span<const std::string_view> path,
                              duckdb::idx_t local_start, duckdb::idx_t run,
                              duckdb::Vector& out, duckdb::idx_t out_off,
                              bool allow_entire) const;
  template<typename Fn>
  void ForEachVariantRun(uint64_t start_row, duckdb::idx_t count,
                         Fn&& fn) const {
    duckdb::idx_t done = 0;
    while (done < count) {
      const uint64_t row = start_row + done;
      const auto it =
        std::upper_bound(_variant_offsets.begin(), _variant_offsets.end(), row);
      const auto rg = static_cast<size_t>(it - _variant_offsets.begin()) - 1;
      const uint64_t rg_begin = _variant_offsets[rg];
      const uint64_t rg_end = _variant_offsets[rg + 1];
      const auto run = std::min<duckdb::idx_t>(
        count - done, static_cast<duckdb::idx_t>(rg_end - row));
      fn(rg, static_cast<duckdb::idx_t>(row - rg_begin), run, done);
      done += run;
    }
  }
  uint64_t GatherCursor(const ScanState& s) const noexcept {
    switch (_kind) {
      case Kind::Variant:
        return s.variant_cursor;
      case Kind::Struct:
      case Kind::Array: {
        if (_validity) {
          return _validity->GatherCursor(s.child_states[0]);
        }
        SDB_ASSERT(!_children.empty() && s.child_states.size() > 1);
        const uint64_t child =
          _children.front()->GatherCursor(s.child_states[1]);
        return _kind == Kind::Array && _array_size != 0 ? child / _array_size
                                                        : child;
      }
      case Kind::List:
      case Kind::Primitive:
        return s.window.begin + s.st.offset_in_column;
    }
    return s.window.begin + s.st.offset_in_column;
  }

  duckdb::BaseStatistics BuildMergedStatistics() const;

  const duckdb::CompressionFunction& ResolveCodec(size_t block,
                                                  ReadContext& ctx) const;
  std::unique_ptr<duckdb::ColumnSegment> Open(const BlockWindow& w,
                                              ReadContext& ctx) const;
  bool NextSegment(BlockWindow& w) const noexcept;
  void BeginScanVector(ScanState& s) const;
  void AttachScanPin(ScanState& s, duckdb::Vector& result) const;
  duckdb::idx_t ScanVector(ScanState& s, duckdb::Vector& result,
                           duckdb::idx_t count,
                           duckdb::ScanVectorType scan_type,
                           duckdb::idx_t base_result_offset = 0) const;
  duckdb::ScanVectorType GetVectorScanType(ScanState& s, duckdb::idx_t count,
                                           duckdb::Vector& result) const;

  struct VariantRg {
    VariantShredState shred_state = VariantShredState::Unshredded;
    std::unique_ptr<ColumnReader> unshredded;
    std::unique_ptr<ColumnReader> shredded;
    duckdb::LogicalType intermediate_type;
  };

  enum class Kind : uint8_t { Primitive, Struct, Array, List, Variant };

  static Kind ClassifyKind(duckdb::LogicalTypeId id) noexcept {
    switch (id) {
      case duckdb::LogicalTypeId::VARIANT:
        return Kind::Variant;
      case duckdb::LogicalTypeId::STRUCT:
        return Kind::Struct;
      case duckdb::LogicalTypeId::ARRAY:
        return Kind::Array;
      case duckdb::LogicalTypeId::LIST:
      case duckdb::LogicalTypeId::MAP:
        return Kind::List;
      default:
        return Kind::Primitive;
    }
  }

  field_id _id;
  duckdb::LogicalType _type;
  Kind _kind;
  std::vector<ColumnBlockMeta> _segments;
  mutable std::vector<std::atomic<const duckdb::CompressionFunction*>> _codecs;
  std::vector<uint64_t> _offsets;
  uint64_t _row_count = 0;
  bool _nulls_in_data = false;
  std::unique_ptr<ColumnReader> _validity;
  std::vector<std::unique_ptr<ColumnReader>> _children;
  uint64_t _array_size = 0;
  std::vector<VariantRg> _variant_rgs;
  std::vector<uint64_t> _variant_offsets;
  duckdb::shared_ptr<duckdb::HyperLogLog> _hyperloglog;
  duckdb::unique_ptr<duckdb::BaseStatistics> _stats;
};

}  // namespace irs
