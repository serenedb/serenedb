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

#include "iresearch/formats/column/column_reader.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <duckdb/storage/compression/dict_fsst/decompression.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/statistics/array_stats.hpp>
#include <duckdb/storage/statistics/list_stats.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <memory>
#include <optional>
#include <utility>

#include "basics/assert.h"
#include "basics/error.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/internal/overflow_string_io.hpp"
#include "iresearch/store/data_input.hpp"

namespace irs {
namespace {

class ScanBufferHolder final : public duckdb::AuxiliaryDataHolder {
 public:
  ScanBufferHolder(duckdb::BufferHandle pin,
                   duckdb::buffer_ptr<duckdb::DictionaryEntry> dict) noexcept
    : _dict{std::move(dict)}, _pin{std::move(pin)} {}

 private:
  duckdb::buffer_ptr<duckdb::DictionaryEntry> _dict;
  duckdb::BufferHandle _pin;
};

std::vector<uint64_t> BuildOffsets(
  const std::vector<ColumnBlockMeta>& segments) {
  std::vector<uint64_t> offsets;
  offsets.reserve(segments.size() + 1);
  uint64_t running = 0;
  offsets.push_back(0);
  for (const auto& m : segments) {
    running += m.tuple_count;
    offsets.push_back(running);
  }
  return offsets;
}

void SerializeColumnSegmentMeta(duckdb::Serializer& s,
                                const ColumnBlockMeta& m) {
  s.WriteProperty<uint8_t>(0, "compression_type",
                           static_cast<uint8_t>(m.compression_type));
  s.WriteProperty<uint64_t>(2, "tuple_count", m.tuple_count);
  s.WriteProperty<uint64_t>(3, "file_offset", m.file_offset);
  s.WriteProperty<uint64_t>(4, "byte_size", m.byte_size);
  s.WriteProperty<duckdb::BaseStatistics>(5, "statistics", m.statistics);
}

ColumnBlockMeta DeserializeColumnSegmentMeta(duckdb::Deserializer& d) {
  const auto compression_type = static_cast<duckdb::CompressionType>(
    d.ReadProperty<uint8_t>(0, "compression_type"));
  const auto tuple_count = d.ReadProperty<uint64_t>(2, "tuple_count");
  const auto file_offset = d.ReadProperty<uint64_t>(3, "file_offset");
  const auto byte_size = d.ReadProperty<uint64_t>(4, "byte_size");
  auto stats = d.ReadProperty<duckdb::BaseStatistics>(5, "statistics");
  return ColumnBlockMeta{std::move(stats), tuple_count, file_offset, byte_size,
                         compression_type};
}

}  // namespace

void SerializeColumnMeta(duckdb::Serializer& s, const ColumnMeta& meta) {
  s.WriteProperty<uint64_t>(0, "id", static_cast<uint64_t>(meta.id));
  s.WriteProperty(1, "type", meta.type);
  s.WriteList(2, "data", meta.data.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                list.WriteObject([&](duckdb::Serializer& so) {
                  SerializeColumnSegmentMeta(so, meta.data[j]);
                });
              });
  s.WriteList(3, "validity", meta.validity.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                list.WriteObject([&](duckdb::Serializer& so) {
                  SerializeColumnSegmentMeta(so, meta.validity[j]);
                });
              });
  s.WriteList(4, "children", meta.children.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                list.WriteObject([&](duckdb::Serializer& co) {
                  SerializeColumnMeta(co, meta.children[j]);
                });
              });
  s.WriteList(5, "variant_rgs", meta.variant_rgs.size(),
              [&](duckdb::Serializer::List& list, duckdb::idx_t j) {
                const auto& rg = meta.variant_rgs[j];
                list.WriteObject([&](duckdb::Serializer& ro) {
                  ro.WriteProperty<uint64_t>(1, "row_count", rg.row_count);
                  ro.WriteProperty<uint8_t>(
                    2, "shred_state", static_cast<uint8_t>(rg.shred_state));
                  ro.WriteObject(3, "unshredded", [&](duckdb::Serializer& uo) {
                    SerializeColumnMeta(uo, *rg.unshredded);
                  });
                  const bool has_shredded = rg.shredded != nullptr;
                  ro.WriteProperty<bool>(4, "has_shredded", has_shredded);
                  if (has_shredded) {
                    ro.WriteObject(5, "shredded", [&](duckdb::Serializer& so) {
                      SerializeColumnMeta(so, *rg.shredded);
                    });
                  }
                });
              });
  s.WritePropertyWithDefault<duckdb::shared_ptr<duckdb::HyperLogLog>>(
    6, "hyperloglog", meta.hyperloglog);
}

ColumnMeta DeserializeColumnMeta(duckdb::Deserializer& d) {
  ColumnMeta meta;
  meta.id = static_cast<field_id>(d.ReadProperty<uint64_t>(0, "id"));
  meta.type = d.ReadProperty<duckdb::LogicalType>(1, "type");
  const bool is_list_like = meta.type.id() == duckdb::LogicalTypeId::LIST ||
                            meta.type.id() == duckdb::LogicalTypeId::MAP;
  const duckdb::LogicalType data_stats_type =
    is_list_like ? duckdb::LogicalType::UBIGINT : meta.type;
  d.Set<const duckdb::LogicalType&>(data_stats_type);
  d.ReadList(2, "data",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& so) {
                 meta.data.push_back(DeserializeColumnSegmentMeta(so));
               });
             });
  d.Unset<const duckdb::LogicalType>();
  const duckdb::LogicalType validity_type{duckdb::LogicalTypeId::VALIDITY};
  d.Set<const duckdb::LogicalType&>(validity_type);
  d.ReadList(3, "validity",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& so) {
                 meta.validity.push_back(DeserializeColumnSegmentMeta(so));
               });
             });
  d.Unset<const duckdb::LogicalType>();
  d.ReadList(4, "children",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& co) {
                 meta.children.push_back(DeserializeColumnMeta(co));
               });
             });
  d.ReadList(5, "variant_rgs",
             [&](duckdb::Deserializer::List& list, duckdb::idx_t /*j*/) {
               list.ReadObject([&](duckdb::Deserializer& ro) {
                 VariantRgMeta rg;
                 rg.row_count = ro.ReadProperty<uint64_t>(1, "row_count");
                 rg.shred_state = static_cast<VariantShredState>(
                   ro.ReadProperty<uint8_t>(2, "shred_state"));
                 ro.ReadObject(3, "unshredded", [&](duckdb::Deserializer& uo) {
                   rg.unshredded =
                     std::make_unique<ColumnMeta>(DeserializeColumnMeta(uo));
                 });
                 const bool has_shredded =
                   ro.ReadProperty<bool>(4, "has_shredded");
                 if (has_shredded) {
                   ro.ReadObject(5, "shredded", [&](duckdb::Deserializer& so) {
                     rg.shredded =
                       std::make_unique<ColumnMeta>(DeserializeColumnMeta(so));
                   });
                 }
                 meta.variant_rgs.push_back(std::move(rg));
               });
             });
  meta.hyperloglog =
    d.ReadPropertyWithDefault<duckdb::shared_ptr<duckdb::HyperLogLog>>(
      6, "hyperloglog");
  return meta;
}

ColumnReader::ColumnReader(field_id id, duckdb::LogicalType type,
                           uint64_t row_count,
                           std::vector<ColumnBlockMeta> segments,
                           std::unique_ptr<ColumnReader> validity,
                           std::vector<std::unique_ptr<ColumnReader>> children)
  : _id{id},
    _type{std::move(type)},
    _kind{ClassifyKind(_type.id())},
    _segments{std::move(segments)},
    _codecs(_segments.size()),
    _offsets{BuildOffsets(_segments)},
    _row_count{row_count},
    _nulls_in_data{
      absl::c_any_of(_segments,
                     [](const ColumnBlockMeta& m) {
                       return m.compression_type ==
                              duckdb::CompressionType::COMPRESSION_DICT_FSST;
                     })},
    _validity{std::move(validity)},
    _children{std::move(children)},
    _array_size{_type.id() == duckdb::LogicalTypeId::ARRAY
                  ? duckdb::ArrayType::GetSize(_type)
                  : 0} {
  _stats = BuildMergedStatistics().ToUnique();
}

std::unique_ptr<ColumnReader> ColumnReader::Make(ColumnMeta&& meta) {
  const auto type_id = meta.type.id();

  std::unique_ptr<ColumnReader> validity;
  {
    uint64_t vrows = 0;
    bool any_non_empty = false;
    for (const auto& s : meta.validity) {
      vrows += s.tuple_count;
      any_non_empty |=
        s.compression_type != duckdb::CompressionType::COMPRESSION_EMPTY;
    }
    if (any_non_empty) {
      validity.reset(new ColumnReader(
        meta.id, duckdb::LogicalType(duckdb::LogicalTypeId::VALIDITY), vrows,
        std::move(meta.validity), nullptr, {}));
    }
  }

  std::vector<std::unique_ptr<ColumnReader>> children;
  children.reserve(meta.children.size());
  for (auto& c : meta.children) {
    children.push_back(Make(std::move(c)));
  }
  std::vector<VariantRg> variant_rgs;
  std::vector<uint64_t> variant_offsets;
  uint64_t row_count = 0;
  std::vector<ColumnBlockMeta> data;
  if (type_id == duckdb::LogicalTypeId::VARIANT) {
    variant_rgs.reserve(meta.variant_rgs.size());
    variant_offsets.reserve(meta.variant_rgs.size() + 1);
    variant_offsets.push_back(0);
    for (auto& rg : meta.variant_rgs) {
      VariantRg vrg;
      vrg.shred_state = rg.shred_state;
      vrg.unshredded = Make(std::move(*rg.unshredded));
      if (rg.shredded) {
        vrg.shredded = Make(std::move(*rg.shredded));
        duckdb::child_list_t<duckdb::LogicalType> intermediate_children;
        intermediate_children.emplace_back("unshredded",
                                           vrg.unshredded->Type());
        intermediate_children.emplace_back("shredded", vrg.shredded->Type());
        vrg.intermediate_type =
          duckdb::LogicalType::STRUCT(std::move(intermediate_children));
      }
      row_count += rg.row_count;
      variant_offsets.push_back(row_count);
      variant_rgs.push_back(std::move(vrg));
    }
  } else if (type_id == duckdb::LogicalTypeId::STRUCT) {
    row_count = !children.empty() ? children.front()->RowCount()
                                  : (validity ? validity->RowCount() : 0);
  } else if (type_id == duckdb::LogicalTypeId::ARRAY) {
    const auto arr_sz = duckdb::ArrayType::GetSize(meta.type);
    row_count = (arr_sz != 0 && !children.empty())
                  ? children.front()->RowCount() / arr_sz
                  : 0;
  } else {
    data = std::move(meta.data);
    for (const auto& s : data) {
      row_count += s.tuple_count;
    }
  }
  std::unique_ptr<ColumnReader> col(
    new ColumnReader(meta.id, std::move(meta.type), row_count, std::move(data),
                     std::move(validity), std::move(children)));
  col->_variant_rgs = std::move(variant_rgs);
  col->_variant_offsets = std::move(variant_offsets);
  col->_hyperloglog = std::move(meta.hyperloglog);
  if (type_id == duckdb::LogicalTypeId::VARIANT) {
    col->_stats = col->BuildMergedStatistics().ToUnique();
  }
  return col;
}

BlockWindow ColumnReader::Locate(uint64_t row,
                                 BlockWindow hint) const noexcept {
  if (hint.end != 0) {
    if (row >= hint.begin && row < hint.end) {
      return hint;
    }
    const auto next = hint.block + 1;
    if (next < _segments.size() && row >= _offsets[next] &&
        row < _offsets[next + 1]) {
      return BlockWindow{next, _offsets[next], _offsets[next + 1]};
    }
  }
  SDB_ASSERT(row < _row_count);
  const auto it = std::upper_bound(_offsets.begin(), _offsets.end(), row);
  const auto rg = static_cast<size_t>(it - _offsets.begin()) - 1;
  return BlockWindow{rg, _offsets[rg], _offsets[rg + 1]};
}

bool ColumnReader::NextSegment(BlockWindow& w) const noexcept {
  const auto next = w.block + 1;
  if (next >= _segments.size()) {
    return false;
  }
  w.block = next;
  w.begin = _offsets[next];
  w.end = _offsets[next + 1];
  return true;
}

const duckdb::CompressionFunction& ColumnReader::ResolveCodec(
  size_t block, ReadContext& ctx) const {
  auto& slot = _codecs[block];
  if (const auto* cached = slot.load(std::memory_order_acquire)) {
    return *cached;
  }
  const auto compression_type = _segments[block].compression_type;
  auto& cfg = duckdb::DBConfig::GetConfig(ctx.Database());
  auto codec =
    cfg.TryGetCompressionFunction(compression_type, _type.InternalType());
  SDB_ENSURE(codec, sdb::ERROR_INTERNAL,
             "ColumnReader: missing compression function for codec ",
             static_cast<uint8_t>(compression_type));
  slot.store(codec.get(), std::memory_order_release);
  return *codec;
}

std::unique_ptr<duckdb::ColumnSegment> ColumnReader::Open(
  const BlockWindow& w, ReadContext& ctx) const {
  const auto& m = _segments[w.block];
  auto& db = ctx.Database();
  const auto& codec = ResolveCodec(w.block, ctx);
  auto stats = m.statistics.Copy();
  const auto byte_size = static_cast<duckdb::idx_t>(m.byte_size);

  if (byte_size == 0) {
    return std::make_unique<duckdb::ColumnSegment>(
      db, /*block=*/nullptr, duckdb::ColumnSegmentType::PERSISTENT,
      static_cast<duckdb::idx_t>(m.tuple_count), codec, std::move(stats),
      /*block_id=*/0, /*offset=*/0, byte_size, /*segment_state=*/nullptr);
  }

  auto& bm = duckdb::BufferManager::GetBufferManager(db);
  auto handle = bm.RegisterTransientMemory(byte_size, ctx);
  auto buf = bm.Pin(handle);
  ctx.In().ReadData(m.file_offset, buf.GetDataMutable(), byte_size);
  auto segment = std::make_unique<duckdb::ColumnSegment>(
    db, std::move(handle), duckdb::ColumnSegmentType::PERSISTENT,
    static_cast<duckdb::idx_t>(m.tuple_count), codec, std::move(stats),
    /*block_id=*/0, /*offset=*/0, byte_size, /*segment_state=*/nullptr);
  if (_type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    if (auto seg_state = segment->GetSegmentState()) {
      seg_state->Cast<duckdb::UncompressedStringSegmentState>()
        .overflow_reader =
        duckdb::make_uniq<IndexInputOverflowReader>(ctx.In());
    }
  }
  return segment;
}

ColumnReader::ScanState ColumnReader::InitScan(ReadContext& ctx) const {
  ScanState s;
  s.ctx = &ctx;
  if (!_segments.empty()) {
    s.window = BlockWindow{0, _offsets[0], _offsets[1]};
  }
  s.st.offset_in_column = 0;
  s.st.internal_index = 0;
  s.initialized = false;
  s.child_states.reserve(_children.size() + 1);
  s.child_states.push_back(_validity ? _validity->InitScan(ctx) : ScanState{});
  for (const auto& child : _children) {
    s.child_states.push_back(child->InitScan(ctx));
  }
  if (!_variant_rgs.empty()) {
    s.variant_rgs.resize(_variant_rgs.size());
  }
  return s;
}

void ColumnReader::BeginScanVector(ScanState& s) const {
  if (s.segments.size() > 1) {
    s.segments.erase(s.segments.begin(), s.segments.end() - 1);
  }
  s.st.previous_states.clear();
  if (!s.initialized) {
    s.segments.clear();
    s.segments.emplace_back(Open(s.window, *s.ctx));
    s.segments.back()->InitializeScan(s.st);
    s.st.internal_index = 0;
    s.initialized = true;
  }
  SDB_ASSERT(s.st.internal_index <= s.st.offset_in_column);
  if (s.st.internal_index < s.st.offset_in_column) {
    s.segments.back()->Skip(s.st);
  }
}

void ColumnReader::AttachScanPin(ScanState& s, duckdb::Vector& result) const {
  if (_type.InternalType() != duckdb::PhysicalType::VARCHAR) {
    return;
  }
  auto& segment = *s.segments.back();
  auto& block = segment.GetBlockHandle();
  if (!block) {
    return;
  }
  duckdb::buffer_ptr<duckdb::DictionaryEntry> dict;
  if (_segments[s.window.block].compression_type ==
        duckdb::CompressionType::COMPRESSION_DICT_FSST &&
      s.st.scan_state) {
    dict = s.st.scan_state->Cast<duckdb::dict_fsst::CompressedStringScanState>()
             .dictionary;
  }
  auto& bm = duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  result.AddAuxiliaryData(
    std::make_unique<ScanBufferHolder>(bm.Pin(block), std::move(dict)));
}

duckdb::ScanVectorType ColumnReader::GetVectorScanType(
  ScanState& s, duckdb::idx_t count, duckdb::Vector& result) const {
  if (result.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
    return duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  }
  if ((s.window.end - s.window.begin) - s.st.offset_in_column < count) {
    return duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  }
  auto scan_type = duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  if (_validity &&
      _validity->GetVectorScanType(s.child_states[0], count, result) ==
        duckdb::ScanVectorType::SCAN_FLAT_VECTOR) {
    scan_type = duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  }
  return scan_type;
}

duckdb::idx_t ColumnReader::ScanVector(ScanState& s, duckdb::Vector& result,
                                       duckdb::idx_t remaining,
                                       duckdb::ScanVectorType scan_type,
                                       duckdb::idx_t base_result_offset) const {
  BeginScanVector(s);
  const auto initial = remaining;
  while (remaining > 0) {
    const auto scan_count = std::min<duckdb::idx_t>(
      remaining, (s.window.end - s.window.begin) - s.st.offset_in_column);
    const auto result_offset = base_result_offset + (initial - remaining);
    if (scan_count > 0) {
      s.segments.back()->Scan(s.st, scan_count, result, result_offset,
                              scan_type);
      AttachScanPin(s, result);
      s.st.offset_in_column += scan_count;
      remaining -= scan_count;
    }
    if (remaining > 0) {
      if (!NextSegment(s.window)) {
        break;
      }
      s.st.previous_states.emplace_back(std::move(s.st.scan_state));
      s.segments.emplace_back(Open(s.window, *s.ctx));
      s.segments.back()->InitializeScan(s.st);
      s.st.offset_in_column = 0;
      s.st.internal_index = 0;
    }
  }
  s.st.internal_index = s.st.offset_in_column;
  return initial - remaining;
}

duckdb::idx_t ColumnReader::ScanStruct(ScanState& s, duckdb::Vector& result,
                                       duckdb::idx_t count) const {
  duckdb::idx_t scan_count = count;
  if (_validity) {
    SDB_ASSERT(!s.child_states.empty());
    scan_count = _validity->Scan(s.child_states[0], result, count);
    if (result.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
      SDB_ASSERT(duckdb::ConstantVector::IsNull(result));
      return scan_count;
    }
  }
  auto& entries = duckdb::StructVector::GetEntries(result);
  SDB_ASSERT(entries.size() == _children.size());
  SDB_ASSERT(s.child_states.size() == _children.size() + 1);
  for (size_t fi = 0; fi < _children.size(); ++fi) {
    _children[fi]->Scan(s.child_states[fi + 1], entries[fi], count);
  }
  duckdb::FlatVector::SetSize(result, scan_count);
  return scan_count;
}

duckdb::idx_t ColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                 duckdb::idx_t count) const {
  switch (_kind) {
    case Kind::Variant:
      return ScanVariant(s, result, count, /*result_offset=*/0);
    case Kind::Struct:
      return ScanStruct(s, result, count);
    case Kind::Array:
    case Kind::List:
      return ScanCount(s, result, count, /*result_offset=*/0);
    case Kind::Primitive:
      break;
  }
  const auto scan_type = GetVectorScanType(s, count, result);
  const auto n = ScanVector(s, result, count, scan_type);
  if (_validity) {
    SDB_ASSERT(!s.child_states.empty());
    _validity->ScanVector(s.child_states[0], result, count, scan_type);
  }
  return n;
}

duckdb::idx_t ColumnReader::ScanCount(ScanState& s, duckdb::Vector& result,
                                      duckdb::idx_t count,
                                      duckdb::idx_t result_offset) const {
  if (_kind == Kind::Variant) {
    return ScanVariant(s, result, count, result_offset);
  }
  if (_kind == Kind::Struct) {
    duckdb::idx_t scan_count = count;
    if (_validity) {
      scan_count =
        _validity->ScanCount(s.child_states[0], result, count, result_offset);
    }
    auto& entries = duckdb::StructVector::GetEntries(result);
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->ScanCount(s.child_states[fi + 1], entries[fi], count,
                               result_offset);
    }
    return scan_count;
  }
  if (_kind == Kind::Array) {
    duckdb::idx_t scan_count = count;
    if (_validity) {
      scan_count =
        _validity->ScanCount(s.child_states[0], result, count, result_offset);
    }
    auto& child = duckdb::ArrayVector::GetChildMutable(result);
    _children[0]->ScanCount(s.child_states[1], child, count * _array_size,
                            result_offset * _array_size);
    return scan_count;
  }
  if (_kind == Kind::List) {
    if (count == 0) {
      return 0;
    }
    if (result_offset != 0) {
      auto& scratch = ScratchVector(s, result.GetType());
      const auto sc = ScanCount(s, scratch, count, 0);
      duckdb::VectorOperations::Copy(scratch, result, sc, 0, result_offset);
      return sc;
    }
    duckdb::Vector offsets{duckdb::LogicalType::UBIGINT, count};
    const auto scan_count =
      ScanVector(s, offsets, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    if (_validity) {
      _validity->ScanCount(s.child_states[0], result, count, 0);
    }
    const auto* odata = duckdb::FlatVector::GetData<uint64_t>(offsets);
    const uint64_t last_entry = odata[scan_count - 1];
    auto* list_entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    const uint64_t base = s.st.last_offset;
    uint64_t current = 0;
    for (duckdb::idx_t i = 0; i < scan_count; ++i) {
      const uint64_t length = odata[i] - current - base;
      list_entries[i] = duckdb::list_entry_t{current, length};
      current += length;
    }
    const uint64_t child_scan_count = last_entry - base;
    duckdb::ListVector::Reserve(result, child_scan_count);
    if (child_scan_count > 0) {
      auto& child = duckdb::ListVector::GetChildMutable(result);
      _children[0]->ScanCount(s.child_states[1], child,
                              static_cast<duckdb::idx_t>(child_scan_count), 0);
    }
    s.st.last_offset = last_entry;
    duckdb::ListVector::SetListSize(result, child_scan_count);
    return scan_count;
  }
  const auto scan_type = duckdb::ScanVectorType::SCAN_FLAT_VECTOR;
  const auto n = ScanVector(s, result, count, scan_type, result_offset);
  if (_validity) {
    SDB_ASSERT(!s.child_states.empty());
    _validity->ScanVector(s.child_states[0], result, count, scan_type,
                          result_offset);
  }
  return n;
}

void ColumnReader::SeekVariantRg(ScanState& s, size_t rg,
                                 duckdb::idx_t target_local) const {
  const auto& rg_meta = _variant_rgs[rg];
  auto& vstate = s.variant_rgs[rg];
  if (!vstate.unshredded) {
    vstate.unshredded =
      std::make_unique<ScanState>(rg_meta.unshredded->InitScan(*s.ctx));
    if (rg_meta.shredded) {
      vstate.shredded =
        std::make_unique<ScanState>(rg_meta.shredded->InitScan(*s.ctx));
    }
    vstate.local_pos = 0;
  }
  if (target_local > vstate.local_pos) {
    const auto skip =
      static_cast<duckdb::idx_t>(target_local - vstate.local_pos);
    rg_meta.unshredded->Skip(*vstate.unshredded, skip);
    if (rg_meta.shredded) {
      rg_meta.shredded->Skip(*vstate.shredded, skip);
    }
    vstate.local_pos = target_local;
  }
}

const ColumnReader* ColumnReader::FindShreddedLeaf(
  const ColumnReader& node, std::span<const std::string_view> path) const {
  if (!duckdb::VariantShreddedStats::IsFullyShredded(node.MergedStatistics())) {
    return nullptr;
  }
  if (path.empty()) {
    if (node.Type().id() == duckdb::LogicalTypeId::STRUCT) {
      const auto& tv =
        node.StructField(duckdb::VariantColumnData::TYPED_VALUE_INDEX);
      return tv.Type().IsNested() ? nullptr : &tv;
    }
    return node.Type().IsNested() ? nullptr : &node;
  }
  if (node.Type().id() != duckdb::LogicalTypeId::STRUCT) {
    return nullptr;
  }
  const auto& tv =
    node.StructField(duckdb::VariantColumnData::TYPED_VALUE_INDEX);
  if (tv.Type().id() != duckdb::LogicalTypeId::STRUCT) {
    return nullptr;
  }
  const auto& fields = duckdb::StructType::GetChildTypes(tv.Type());
  for (size_t fi = 0; fi < fields.size(); ++fi) {
    if (duckdb::StringUtil::CIEquals(std::string{path[0]}, fields[fi].first)) {
      return FindShreddedLeaf(tv.StructField(fi), path.subspan(1));
    }
  }
  return nullptr;
}

bool ColumnReader::ResolveUniformShreddedLeaf(
  std::span<const std::string_view> path,
  duckdb::LogicalType& leaf_type) const {
  if (_type.id() != duckdb::LogicalTypeId::VARIANT || path.empty() ||
      _variant_rgs.empty()) {
    return false;
  }
  bool have = false;
  for (const auto& rg : _variant_rgs) {
    const ColumnReader* leaf =
      rg.shred_state != VariantShredState::Unshredded && rg.shredded
        ? FindShreddedLeaf(*rg.shredded, path)
        : nullptr;
    if (!leaf || leaf->Type().IsNested()) {
      return false;
    }
    if (!have) {
      leaf_type = leaf->Type();
      have = true;
    } else if (leaf->Type() != leaf_type) {
      return false;
    }
  }
  return have;
}

void ColumnReader::ExtractShreddedLeafRun(
  ScanState& s, size_t rg, std::span<const std::string_view> path,
  duckdb::idx_t local_start, duckdb::idx_t run, duckdb::Vector& out,
  duckdb::idx_t out_off, bool allow_entire) const {
  auto& vstate = s.variant_rgs[rg];
  if (!vstate.leaf) {
    vstate.leaf_reader = FindShreddedLeaf(*_variant_rgs[rg].shredded, path);
    vstate.leaf =
      std::make_unique<ScanState>(vstate.leaf_reader->InitScan(*s.ctx));
    vstate.leaf_pos = 0;
  }
  if (local_start > vstate.leaf_pos) {
    vstate.leaf_reader->Skip(
      *vstate.leaf, static_cast<duckdb::idx_t>(local_start - vstate.leaf_pos));
    vstate.leaf_pos = local_start;
  }
  if (allow_entire) {
    vstate.leaf_reader->Scan(*vstate.leaf, out, run);
  } else {
    vstate.leaf_reader->ScanCount(*vstate.leaf, out, run, out_off);
  }
  vstate.leaf_pos += run;
}

duckdb::Vector& ColumnReader::ScratchVector(ScanState& s,
                                            const duckdb::LogicalType& type) {
  if (!s.scratch) {
    s.scratch = std::make_unique<VectorScratch>(type);
  }
  return s.scratch->Reset();
}

void ColumnReader::ReconstructVariantRun(ScanState& s, size_t rg,
                                         duckdb::idx_t local_start,
                                         duckdb::idx_t run,
                                         duckdb::Vector& result,
                                         duckdb::idx_t out_off) const {
  const auto& rg_meta = _variant_rgs[rg];
  SeekVariantRg(s, rg, local_start);
  auto& vstate = s.variant_rgs[rg];

  if (rg_meta.shred_state == VariantShredState::Unshredded) {
    if (out_off == 0) {
      rg_meta.unshredded->ScanCount(*vstate.unshredded, result, run, 0);
    } else {
      auto& scratch = ScratchVector(s, result.GetType());
      rg_meta.unshredded->ScanCount(*vstate.unshredded, scratch, run, 0);
      duckdb::VectorOperations::Copy(scratch, result, run, 0, out_off);
    }
  } else {
    SDB_ENSURE(
      rg_meta.shredded, sdb::ERROR_INTERNAL,
      "ColumnReader: shredded VARIANT row group missing shredded data");
    if (!vstate.intermediate) {
      vstate.intermediate =
        std::make_unique<VectorScratch>(rg_meta.intermediate_type);
    }
    auto& intermediate = vstate.intermediate->Reset();
    auto& entries = duckdb::StructVector::GetEntries(intermediate);
    rg_meta.unshredded->ScanCount(*vstate.unshredded, entries[0], run, 0);
    rg_meta.shredded->ScanCount(*vstate.shredded, entries[1], run, 0);
    duckdb::FlatVector::SetSize(intermediate, run);
    auto& scratch = ScratchVector(s, result.GetType());
    duckdb::VariantUtils::UnshredVariantData(intermediate, scratch, run);
    duckdb::VectorOperations::Copy(scratch, result, run, 0, out_off);
  }
  vstate.local_pos += run;
}

duckdb::idx_t ColumnReader::ScanVariant(ScanState& s, duckdb::Vector& result,
                                        duckdb::idx_t count,
                                        duckdb::idx_t result_offset) const {
  if (count == 0) {
    return 0;
  }
  ForEachVariantRun(s.variant_cursor, count,
                    [&](size_t rg, duckdb::idx_t local_start, duckdb::idx_t run,
                        duckdb::idx_t done) {
                      ReconstructVariantRun(s, rg, local_start, run, result,
                                            result_offset + done);
                    });
  if (_validity) {
    _validity->ScanCount(s.child_states[0], result, count, result_offset);
  }
  s.variant_cursor += count;
  return count;
}

void ColumnReader::SkipVariant(ScanState& s, duckdb::idx_t count) const {
  if (count == 0) {
    return;
  }
  if (_validity) {
    _validity->Skip(s.child_states[0], count);
  }
  ForEachVariantRun(
    s.variant_cursor, count,
    [&](size_t rg, duckdb::idx_t local_start, duckdb::idx_t run,
        duckdb::idx_t /*done*/) { SeekVariantRg(s, rg, local_start + run); });
  s.variant_cursor += count;
}

void ColumnReader::Skip(ScanState& s, duckdb::idx_t count) const {
  if (_kind == Kind::Variant) {
    SkipVariant(s, count);
    return;
  }
  if (_kind == Kind::Struct) {
    if (_validity) {
      _validity->Skip(s.child_states[0], count);
    }
    for (size_t fi = 0; fi < _children.size(); ++fi) {
      _children[fi]->Skip(s.child_states[fi + 1], count);
    }
    return;
  }
  if (_kind == Kind::Array) {
    if (_validity) {
      _validity->Skip(s.child_states[0], count);
    }
    _children[0]->Skip(s.child_states[1], count * _array_size);
    return;
  }
  if (_kind == Kind::List) {
    if (_validity) {
      _validity->Skip(s.child_states[0], count);
    }
    duckdb::Vector offsets{duckdb::LogicalType::UBIGINT, count};
    const auto scan_count =
      ScanVector(s, offsets, count, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    SDB_ASSERT(scan_count > 0);
    const auto* odata = duckdb::FlatVector::GetData<uint64_t>(offsets);
    const uint64_t last_entry = odata[scan_count - 1];
    const uint64_t child_skip = last_entry - s.st.last_offset;
    s.st.last_offset = last_entry;
    if (child_skip > 0) {
      _children[0]->Skip(s.child_states[1],
                         static_cast<duckdb::idx_t>(child_skip));
    }
    return;
  }
  duckdb::idx_t remaining = count;
  while (remaining > 0) {
    const auto avail = (s.window.end - s.window.begin) - s.st.offset_in_column;
    if (remaining < avail) {
      s.st.offset_in_column += remaining;
      break;
    }
    remaining -= avail;
    if (!NextSegment(s.window)) {
      s.st.offset_in_column += avail;
      break;
    }
    s.initialized = false;
    s.st.offset_in_column = 0;
    s.st.internal_index = 0;
  }
  if (_validity) {
    _validity->Skip(s.child_states[0], count);
  }
}

void ColumnReader::FetchRow(ReadContext& ctx,
                            duckdb::ColumnFetchState& fetch_state, uint64_t row,
                            duckdb::Vector& result,
                            duckdb::idx_t result_idx) const {
  SDB_ENSURE(_children.empty() && _variant_rgs.empty(),
             sdb::ERROR_NOT_IMPLEMENTED,
             "ColumnReader: FetchRow not supported for nested columns");
  const auto w = Locate(row, BlockWindow{});
  auto segment = Open(w, ctx);
  segment->FetchRow(fetch_state, static_cast<duckdb::row_t>(row - w.begin),
                    result, result_idx);
  if (_validity) {
    if (fetch_state.child_states.empty()) {
      fetch_state.child_states.push_back(
        duckdb::make_uniq<duckdb::ColumnFetchState>());
    }
    _validity->FetchRow(ctx, *fetch_state.child_states[0], row, result,
                        result_idx);
  }
}

duckdb::BaseStatistics ColumnReader::BuildMergedStatistics() const {
  const auto child_stats = [](const ColumnReader& child) {
    return child.MergedStatistics().ToUnique();
  };
  auto stats = [&] {
    switch (_type.id()) {
      case duckdb::LogicalTypeId::ARRAY: {
        auto s = duckdb::ArrayStats::CreateEmpty(_type);
        duckdb::ArrayStats::SetChildStats(s, child_stats(*_children.front()));
        return s;
      }
      case duckdb::LogicalTypeId::MAP:
      case duckdb::LogicalTypeId::LIST: {
        auto s = duckdb::ListStats::CreateEmpty(_type);
        duckdb::ListStats::SetChildStats(s, child_stats(*_children.front()));
        return s;
      }
      case duckdb::LogicalTypeId::STRUCT: {
        auto s = duckdb::StructStats::CreateEmpty(_type);
        for (size_t fi = 0; fi < _children.size(); ++fi) {
          duckdb::StructStats::SetChildStats(s, fi,
                                             child_stats(*_children[fi]));
        }
        return s;
      }
      case duckdb::LogicalTypeId::VARIANT: {
        auto s = duckdb::VariantStats::CreateEmpty(_type);
        for (const auto& vrg : _variant_rgs) {
          auto rg_s = duckdb::VariantStats::CreateEmpty(_type);
          if (vrg.unshredded) {
            duckdb::VariantStats::SetUnshreddedStats(
              rg_s, vrg.unshredded->MergedStatistics());
          }
          if (vrg.shred_state != VariantShredState::Unshredded &&
              vrg.shredded) {
            duckdb::VariantStats::SetShreddedStats(
              rg_s, vrg.shredded->MergedStatistics());
          } else {
            duckdb::VariantStats::MarkAsNotShredded(rg_s);
          }
          s.Merge(rg_s);
        }
        return s;
      }
      default: {
        auto s = duckdb::BaseStatistics::CreateEmpty(_type);
        for (const auto& m : _segments) {
          s.Merge(m.statistics);
        }
        return s;
      }
    }
  }();
  if (_validity) {
    stats.Merge(_validity->MergedStatistics());
  }
  return stats;
}

ColumnReader::PointReader::PointReader(const ColReader& col_reader,
                                       const ColumnReader& col)
  : _ctx{col_reader}, _reader{&col} {}

bool ColumnReader::PointReader::FetchRow(uint64_t row, duckdb::Vector& out,
                                         duckdb::idx_t out_offset) {
  if (row >= _reader->RowCount()) {
    duckdb::FlatVector::ValidityMutable(out).SetInvalid(out_offset);
    return false;
  }
  _window = _reader->Locate(row, _window);
  if (_window.block != _cached_block) {
    _block = _reader->Open(_window, _ctx);
    _fetch_state = duckdb::ColumnFetchState{};
    _cached_block = _window.block;
  }
  duckdb::FlatVector::ValidityMutable(out).SetValid(out_offset);
  _block->FetchRow(_fetch_state,
                   static_cast<duckdb::row_t>(row - _window.begin), out,
                   out_offset);
  const auto* validity = _reader->_validity.get();
  if (validity) {
    _validity_window = validity->Locate(row, _validity_window);
    if (_validity_window.block != _cached_validity_block) {
      _validity_block = validity->Open(_validity_window, _ctx);
      _validity_fetch_state = duckdb::ColumnFetchState{};
      _cached_validity_block = _validity_window.block;
    }
    _validity_block->FetchRow(
      _validity_fetch_state,
      static_cast<duckdb::row_t>(row - _validity_window.begin), out,
      out_offset);
  } else if (!_reader->_nulls_in_data) {
    return true;
  }
  return duckdb::FlatVector::Validity(out).RowIsValid(out_offset);
}

}  // namespace irs
