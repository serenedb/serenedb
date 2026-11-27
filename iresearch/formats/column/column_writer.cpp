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

#include "iresearch/formats/column/column_writer.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/variant/variant_shredding.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <duckdb/storage/statistics/string_stats.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/seal_bound.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {
namespace {

uint64_t AlignedPosition(duckdb::WriteStream& out) {
  constexpr duckdb::data_t kZeros[8]{};
  if (const auto misalign = out.Position() % sizeof(kZeros); misalign != 0) {
    out.WriteData(kZeros, sizeof(kZeros) - misalign);
  }
  return out.Position();
}

uint64_t AppendAligned(duckdb::WriteStream& out, std::string_view bytes) {
  const auto offset = AlignedPosition(out);
  out.WriteData(duckdb::const_data_ptr_cast(bytes.data()), bytes.size());
  return offset;
}

void WriteBytes(ReadContext& source, uint64_t offset, uint64_t size,
                duckdb::WriteStream& out) {
  if (const auto* bytes = source.TryReadStable(offset, size)) {
    out.WriteData(bytes, size);
    return;
  }
  const auto* file = source.File();
  SDB_ASSERT(file != nullptr);
  const auto& blocks = file->Blocks();
  for (const auto end = offset + size; offset != end;) {
    const auto piece =
      std::min<uint64_t>(end, blocks.BlockStart(blocks.BlockOf(offset) + 1)) -
      offset;
    const auto pin = file->Pin(offset, piece);
    out.WriteData(pin.Ptr(), piece);
    offset += piece;
  }
}

class BlockReadAhead {
 public:
  BlockReadAhead(const ReadContext& source,
                 std::span<const ColumnBlockMeta> blocks) noexcept
    : _file{source.File()}, _blocks{blocks} {}

  void At(size_t block) {
    if (_file == nullptr || block < _next || _end == _blocks.size()) {
      return;
    }
    std::vector<duckdb::FileRange> ranges;
    uint64_t bytes = 0;
    auto shared = std::numeric_limits<uint64_t>::max();
    const auto from = std::max(block, _end);
    auto end = from;
    for (; end != _blocks.size() && bytes < kWindow; ++end) {
      const auto& m = _blocks[end];
      if (m.shared_size != 0 && m.shared_offset != shared) {
        shared = m.shared_offset;
        ranges.push_back({.offset = m.shared_offset, .size = m.shared_size});
        bytes += m.shared_size;
      }
      if (m.byte_size != 0) {
        ranges.push_back({.offset = m.file_offset, .size = m.byte_size});
        bytes += m.byte_size;
      }
    }
    _next = from + (end - from) / 2;
    _end = end;
    _file->Prefetch(ranges);
  }

 private:
  static constexpr uint64_t kWindow = duckdb::ExternalFileCache::MAX_READ_AHEAD;

  const duckdb::InputFile* _file;
  std::span<const ColumnBlockMeta> _blocks;
  size_t _next = 0;
  size_t _end = 0;
};

void ZeroNullArrays(duckdb::Vector& arrays, duckdb::idx_t from,
                    duckdb::idx_t count) {
  const auto& validity = duckdb::FlatVector::Validity(arrays);
  auto& child = duckdb::ArrayVector::GetChildMutable(arrays);
  const auto child_type = child.GetType().InternalType();
  if (!duckdb::TypeIsConstantSize(child_type)) {
    return;
  }
  const auto bytes = duckdb::ArrayType::GetSize(arrays.GetType()) *
                     duckdb::GetTypeIdSize(child_type);
  auto* data = duckdb::FlatVector::GetDataMutable(child);
  for (auto row = from; row != from + count; ++row) {
    if (!validity.RowIsValid(row)) {
      std::memset(data + row * bytes, 0, bytes);
    }
  }
}

codecs::CodecType ForcedBySetting(const duckdb::DBConfig& config) {
  const auto type = codecs::FindCodec(
    duckdb::Settings::Get<duckdb::ForceCompressionSetting>(config));
  return type == codecs::kNoCodec ? codecs::CodecType::Auto : type;
}

constexpr uint64_t kNestedPerElement = 16;

using SealLeaves = TrackedVector<codecs::SealLeaf>;

codecs::SealLeaf& NextLeaf(SealLeaves& leaves, size_t& at,
                           duckdb::PhysicalType physical) {
  if (at == leaves.size()) {
    leaves.push_back({.physical = physical});
  }
  SDB_ASSERT(leaves[at].physical == physical);
  return leaves[at++];
}

uint64_t StringBytes(const duckdb::Vector& vec) {
  uint64_t bytes = 0;
  for (const auto entry : vec.ValidValues<duckdb::string_t>()) {
    bytes += entry.GetValue().GetSize();
  }
  return bytes;
}

void CollectLeaves(const duckdb::LogicalType& type, const duckdb::Vector& vec,
                   uint64_t count, SealLeaves& leaves, size_t& at) {
  NextLeaf(leaves, at, duckdb::PhysicalType::BIT).rows += count;
  if (type.id() == duckdb::LogicalTypeId::VARIANT) {
    auto& leaf = NextLeaf(leaves, at, duckdb::PhysicalType::VARCHAR);
    leaf.rows += count;
    leaf.heap += vec.GetAllocationSize();
    return;
  }
  switch (type.InternalType()) {
    case duckdb::PhysicalType::STRUCT: {
      const auto& children = duckdb::StructType::GetChildTypes(type);
      const auto& entries = duckdb::StructVector::GetEntries(vec);
      for (size_t i = 0; i < children.size(); ++i) {
        CollectLeaves(children[i].second, entries[i], count, leaves, at);
      }
      return;
    }
    case duckdb::PhysicalType::LIST:
      NextLeaf(leaves, at, duckdb::PhysicalType::LIST).rows += count;
      CollectLeaves(duckdb::ListType::GetChildType(type),
                    duckdb::ListVector::GetChild(vec),
                    duckdb::ListVector::GetListSize(vec), leaves, at);
      return;
    case duckdb::PhysicalType::ARRAY:
      CollectLeaves(duckdb::ArrayType::GetChildType(type),
                    duckdb::ArrayVector::GetChild(vec),
                    count * duckdb::ArrayType::GetSize(type), leaves, at);
      return;
    default: {
      auto& leaf = NextLeaf(leaves, at, type.InternalType());
      leaf.rows += count;
      if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
        leaf.heap += StringBytes(vec);
      }
      return;
    }
  }
}

uint64_t StoredBytes(std::span<const ColumnReader* const> columns) {
  uint64_t bytes = 0;
  for (const auto* column : columns) {
    for (const auto& block : column->DataBlocks()) {
      bytes += block.byte_size;
    }
  }
  return bytes;
}

uint64_t StringBytesOf(std::span<const ColumnReader* const> columns,
                       uint64_t rows) {
  uint64_t longest = 0;
  for (const auto* column : columns) {
    const auto& stats = column->MergedStatistics();
    if (stats.GetStatsType() != duckdb::StatisticsType::STRING_STATS ||
        !duckdb::StringStats::HasMaxStringLength(stats)) {
      return StoredBytes(columns);
    }
    longest =
      std::max<uint64_t>(longest, duckdb::StringStats::MaxStringLength(stats));
  }
  return rows * longest;
}

void CollectSourceLeaves(const duckdb::LogicalType& type,
                         std::span<const ColumnReader* const> columns,
                         uint64_t rows, SealLeaves& leaves, size_t& at) {
  NextLeaf(leaves, at, duckdb::PhysicalType::BIT).rows += rows;
  if (type.id() == duckdb::LogicalTypeId::VARIANT) {
    auto& leaf = NextLeaf(leaves, at, duckdb::PhysicalType::VARCHAR);
    leaf.rows += rows;
    leaf.heap += StoredBytes(columns);
    return;
  }
  TrackedVector<const ColumnReader*> children{leaves.get_allocator()};
  switch (type.InternalType()) {
    case duckdb::PhysicalType::STRUCT: {
      const auto& fields = duckdb::StructType::GetChildTypes(type);
      for (size_t i = 0; i < fields.size(); ++i) {
        children.clear();
        for (const auto* column : columns) {
          if (i < column->StructFieldCount()) {
            children.push_back(&column->StructField(i));
          }
        }
        CollectSourceLeaves(fields[i].second, children, rows, leaves, at);
      }
      return;
    }
    case duckdb::PhysicalType::LIST: {
      NextLeaf(leaves, at, duckdb::PhysicalType::LIST).rows += rows;
      uint64_t elements = 0;
      for (const auto* column : columns) {
        if (const auto* child = column->Child()) {
          children.push_back(child);
          elements += child->RowCount();
        }
      }
      CollectSourceLeaves(duckdb::ListType::GetChildType(type), children,
                          elements, leaves, at);
      return;
    }
    case duckdb::PhysicalType::ARRAY:
      for (const auto* column : columns) {
        if (const auto* child = column->Child()) {
          children.push_back(child);
        }
      }
      CollectSourceLeaves(duckdb::ArrayType::GetChildType(type), children,
                          rows * duckdb::ArrayType::GetSize(type), leaves, at);
      return;
    default: {
      auto& leaf = NextLeaf(leaves, at, type.InternalType());
      leaf.rows += rows;
      if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
        leaf.heap += StringBytesOf(columns, rows);
      }
      return;
    }
  }
}

uint64_t LeavesBound(const SealLeaves& leaves, codecs::CodecType forced,
                     const ColCodecParams& params, bool nested) {
  uint64_t peak = 0;
  uint64_t elements = 0;
  for (const auto& leaf : leaves) {
    const auto method =
      forced != codecs::CodecType::Auto && codecs::Writes(forced, leaf.physical)
        ? forced
        : codecs::CodecType::Auto;
    peak = std::max(peak, codecs::SealBytes(leaf, method, params));
    elements += leaf.rows;
  }
  return peak + (nested ? kNestedPerElement * elements : 0);
}

bool Nested(const duckdb::LogicalType& type) noexcept {
  switch (type.InternalType()) {
    case duckdb::PhysicalType::STRUCT:
    case duckdb::PhysicalType::LIST:
    case duckdb::PhysicalType::ARRAY:
      return true;
    default:
      return type.id() == duckdb::LogicalTypeId::VARIANT;
  }
}

struct SharedBlock {
  uint64_t offset = 0;
  uint64_t size = 0;
};

void CaptureBlock(const codecs::Codec& codec, duckdb::BaseStatistics stats,
                  uint64_t tuple_count, std::string_view bytes,
                  SharedBlock shared, duckdb::WriteStream& blocks,
                  std::vector<ColumnBlockMeta>& sink) {
  if (tuple_count == 0) {
    return;
  }
  ColumnBlockMeta m{
    .statistics = std::move(stats),
    .tuple_count = tuple_count,
    .codec = &codec,
  };
  if (!bytes.empty()) {
    SDB_ASSERT(bytes.size() <= std::numeric_limits<uint32_t>::max());
    m.file_offset = AppendAligned(blocks, bytes);
    m.byte_size = bytes.size();
    m.shared_offset = shared.offset;
    m.shared_size = shared.size;
  }
  sink.push_back(std::move(m));
}

void SetInvalidRows(duckdb::Vector& vec, duckdb::idx_t begin,
                    duckdb::idx_t count) {
  auto& validity = duckdb::FlatVector::ValidityMutable(vec);
  validity.EnsureWritable();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    validity.SetInvalidUnsafe(begin + i);
  }
  switch (vec.GetType().InternalType()) {
    case duckdb::PhysicalType::STRUCT:
      for (auto& field : duckdb::StructVector::GetEntries(vec)) {
        SetInvalidRows(field, begin, count);
      }
      break;
    case duckdb::PhysicalType::ARRAY: {
      const auto size = duckdb::ArrayType::GetSize(vec.GetType());
      SetInvalidRows(duckdb::ArrayVector::GetChildMutable(vec), begin * size,
                     count * size);
    } break;
    default:
      break;
  }
}

codecs::CodecType CodesCodec(const ListParts& parts, uint64_t rows) noexcept {
  uint64_t runs = 0;
  uint64_t run = 0;
  uint64_t longest_run = 0;
  int64_t least_step = std::numeric_limits<int64_t>::max();
  int64_t largest_step = std::numeric_limits<int64_t>::min();
  for (size_t i = 0; i < parts.codes.size(); ++i) {
    if (i != 0) {
      const auto step =
        static_cast<int64_t>(parts.codes[i] - parts.codes[i - 1]);
      least_step = std::min(least_step, step);
      largest_step = std::max(largest_step, step);
      if (step == 0) {
        ++run;
        continue;
      }
    }
    longest_run = std::max(longest_run, run);
    run = 1;
    ++runs;
  }
  longest_run = std::max(longest_run, run);
  const uint64_t width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(
      parts.distinct == 0 ? 0 : parts.distinct - 1);
  const uint64_t run_width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(longest_run);
  const uint64_t bitpacked = rows * width;
  const uint64_t run_bits = runs * (width + run_width);
  if (run_bits < bitpacked) {
    return codecs::CodecType::Rle;
  }
  const uint64_t step_width =
    parts.codes.size() < 2
      ? 0
      : duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(
          static_cast<uint64_t>(largest_step) -
          static_cast<uint64_t>(least_step));
  return step_width < width ? codecs::CodecType::Delta
                            : codecs::CodecType::Bitpacking;
}

bool SelfContained(const ColumnBlockMeta& block) noexcept {
  return block.codec->type != codecs::CodecType::Uncompressed ||
         block.statistics.GetType().InternalType() !=
           duckdb::PhysicalType::VARCHAR;
}

bool EmptyValidity(const ColumnBlockMeta& block) noexcept {
  return block.byte_size == 0 && block.codec->type == codecs::CodecType::Empty;
}

bool InCodec(codecs::CodecType forced, const ColumnBlockMeta& block) noexcept {
  return forced == codecs::CodecType::Auto || block.codec->type == forced ||
         block.codec->type == codecs::CodecType::Constant;
}

void RestartLists(ColumnWriteState& state) noexcept {
  state.list_running = 0;
  state.list_distinct = 0;
  for (auto& child : state.children) {
    RestartLists(child);
  }
}

void EmitEmptyValidity(uint64_t rows, std::vector<ColumnBlockMeta>& sink) {
  const duckdb::LogicalType type{duckdb::LogicalTypeId::VALIDITY};
  ColumnBlockMeta m{duckdb::BaseStatistics::CreateEmpty(type)};
  m.tuple_count = rows;
  m.codec = codecs::GetCodec(codecs::CodecType::Empty, type.InternalType());
  sink.push_back(std::move(m));
}

}  // namespace

void DropAllValidMask(WriteChunk& chunk) {
  auto& validity = duckdb::FlatVector::ValidityMutable(chunk.data);
  if (validity.CheckAllValid(chunk.count)) {
    validity.Reset();
  }
}

WriteContext& ColumnWriter::WriteCtx() const noexcept {
  return _owner->WriteCtx();
}

TrackedAllocator<> ColumnWriter::SealAllocator() const noexcept {
  return _owner->Allocator();
}

codecs::CodecType ColumnWriter::SealMethod() const {
  return _options.compression == codecs::CodecType::Auto
           ? ForcedBySetting(duckdb::DBConfig::GetConfig(_owner->Database()))
           : _options.compression;
}

uint64_t ColumnWriter::SealBound(const WriteRows& rows) const {
  SealLeaves leaves{_owner->Allocator()};
  if (IngestsList()) {
    leaves.push_back(
      {.physical = duckdb::PhysicalType::BIT, .rows = rows.count});
    leaves.push_back(
      {.physical = duckdb::PhysicalType::LIST, .rows = rows.count});
  } else {
    for (const auto& chunk : rows.chunks) {
      size_t at = 0;
      CollectLeaves(_type, chunk.data, chunk.count, leaves, at);
    }
  }
  for (size_t i = 0; i < _ingest.size(); ++i) {
    if (!_ingest[i]) {
      continue;
    }
    const auto& list = IngestsList()
                         ? _type
                         : duckdb::StructType::GetChildTypes(_type)[i].second;
    const auto& element = duckdb::ListType::GetChildType(list);
    const auto first = leaves.size();
    _ingest[i]->VisitElements(
      [&](const duckdb::Vector& vec, duckdb::idx_t count) {
        size_t at = first;
        CollectLeaves(element, vec, count, leaves, at);
      });
  }
  return LeavesBound(leaves, SealMethod(), _options.codec_params,
                     Nested(_type));
}

uint64_t ColumnWriter::PendingSealBound() const {
  if (_staged_rows == 0) {
    return 0;
  }
  const std::span<const WriteChunk> chunks{_staged.data(), _staged_chunks};
  if (!_deferred) {
    return SealBound({chunks, _staged_rows});
  }
  const size_t per_seal = std::max<size_t>(
    1, size_t{_options.row_group_size} / size_t{STANDARD_VECTOR_SIZE});
  uint64_t bound = 0;
  for (size_t begin = 0; begin < chunks.size(); begin += per_seal) {
    WriteRows slice{
      chunks.subspan(begin, std::min(per_seal, chunks.size() - begin))};
    for (const auto& chunk : slice.chunks) {
      slice.count += chunk.count;
    }
    bound = std::max(bound, SealBound(slice));
  }
  return bound;
}

uint64_t SealBoundOf(const duckdb::LogicalType& type,
                     std::span<const ColumnReader* const> columns,
                     uint64_t rows, codecs::CodecType forced,
                     const ColCodecParams& params, TrackedAllocator<> alloc) {
  SealLeaves leaves{alloc};
  size_t at = 0;
  CollectSourceLeaves(type, columns, rows, leaves, at);
  return LeavesBound(leaves, forced, params, Nested(type));
}

bool ColumnWriter::Compress(const duckdb::LogicalType& type,
                            const WriteRows& rows, codecs::CodecType method,
                            codecs::ColumnTuning& tuning,
                            std::vector<ColumnBlockMeta>& sink) {
  SDB_ASSERT(absl::c_accumulate(rows.chunks, uint64_t{0},
                                [](uint64_t sum, const WriteChunk& chunk) {
                                  return sum + chunk.data.size();
                                }) == rows.count);
  auto& ctx = WriteCtx();
  bool flushed = false;
  bool needs_validity = false;
  std::string shared;
  SharedBlock staged;
  const auto flush = [&](const codecs::Codec& codec,
                         duckdb::BaseStatistics stats, uint64_t block_rows,
                         std::string_view bytes,
                         std::string_view shared_bytes) {
    flushed = true;
    needs_validity =
      needs_validity || codec.decompression.validity ==
                          codecs::CompressionValidity::RequiresValidity;
    if (!shared_bytes.empty() && shared != shared_bytes) {
      staged = {AppendAligned(ctx.Out(), shared_bytes), shared_bytes.size()};
      shared = shared_bytes;
    }
    CaptureBlock(codec, std::move(stats), block_rows, bytes,
                 shared_bytes.empty() ? SharedBlock{} : staged, ctx.Out(),
                 sink);
  };
  codecs::ColumnDataCheckpointData data{
    .type = type,
    .ctx = &ctx,
    .alloc = SealAllocator(),
    .flush = flush,
    .params = _options.codec_params,
    .tuning = tuning,
    .disabled = _disabled,
    .rows = rows,
  };
  codecs::Compress(Enabled(method, type) ? method : codecs::CodecType::Auto,
                   data);
  return flushed && !needs_validity;
}

bool ColumnWriter::Enabled(codecs::CodecType method,
                           const duckdb::LogicalType& type) const noexcept {
  return !_disabled.test(std::to_underlying(method)) &&
         codecs::Writes(method, type.InternalType());
}

void ColumnWriter::SealValidity(const WriteRows& rows,
                                std::vector<ColumnBlockMeta>& sink) {
  if (rows.count == 0) {
    EmitEmptyValidity(rows.count, sink);
    return;
  }
  codecs::ColumnTuning tuning;
  Compress(duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY}, rows,
           _global_method, tuning, sink);
}

void ColumnWriter::Claim(ColumnMeta& meta,
                         const duckdb::LogicalType& type) const {
  if (meta.type.id() == duckdb::LogicalTypeId::INVALID) {
    meta.id = _id;
    meta.type = type;
  }
}

void ColumnWriter::SealNestedValidity(const WriteRows& rows, bool skip_validity,
                                      size_t child_count, ColumnMeta& meta,
                                      ColumnWriteState& state) {
  if (!skip_validity) {
    SealValidity(rows, meta.validity);
  }
  meta.children.resize(child_count);
  state.children.resize(child_count);
}

void ColumnWriter::SealStruct(
  const duckdb::LogicalType& type, const WriteRows& rows, bool skip_validity,
  ColumnMeta& meta, ColumnWriteState& state,
  std::span<const std::unique_ptr<ListIngest>> ingest) {
  const auto& child_types = duckdb::StructType::GetChildTypes(type);
  SealNestedValidity(rows, skip_validity, child_types.size(), meta, state);
  for (size_t i = 0; i < child_types.size(); ++i) {
    TrackedVector<WriteChunk> field_chunks{SealAllocator()};
    field_chunks.reserve(rows.chunks.size());
    for (auto& c : rows.chunks) {
      field_chunks.push_back(
        RefChunk(duckdb::StructVector::GetEntries(c.data)[i], c.count));
    }
    SealColumn(child_types[i].second, {field_chunks, rows.count},
               /*skip_validity=*/false, meta.children[i], state.children[i],
               i < ingest.size() && ingest[i] ? ingest.subspan(i, 1)
                                              : decltype(ingest){});
  }
}

void ColumnWriter::SealArray(const duckdb::LogicalType& type,
                             const WriteRows& rows, bool skip_validity,
                             ColumnMeta& meta, ColumnWriteState& state) {
  SealNestedValidity(rows, skip_validity, 1, meta, state);
  const auto array_size =
    static_cast<uint64_t>(duckdb::ArrayType::GetSize(type));
  TrackedVector<WriteChunk> elem_chunks{SealAllocator()};
  for (auto& c : rows.chunks) {
    SliceChunks(elem_chunks, duckdb::ArrayVector::GetChild(c.data), 0,
                c.count * array_size);
  }
  SealColumn(duckdb::ArrayType::GetChildType(type),
             {elem_chunks, rows.count * array_size}, /*skip_validity=*/false,
             meta.children[0], state.children[0]);
}

void ColumnWriter::SealList(const duckdb::LogicalType& type,
                            const WriteRows& rows, bool skip_validity,
                            ColumnMeta& meta, ColumnWriteState& state) {
  ListIngest ingest{SealAllocator(), type, /*borrow=*/true, state.list_distinct,
                    state.list_running};
  for (const auto& c : rows.chunks) {
    ingest.Add(c.data, 0, c.count);
    ingest.CheckDistinct();
  }
  SealListParts(type, rows, skip_validity, ingest, meta, state);
}

void ColumnWriter::SealListParts(const duckdb::LogicalType& type,
                                 const WriteRows& rows, bool skip_validity,
                                 ListIngest& ingest, ColumnMeta& meta,
                                 ColumnWriteState& state) {
  SealNestedValidity(rows, skip_validity, 2, meta, state);
  auto parts = ingest.Take();
  state.list_running = parts.running;
  state.list_distinct = parts.next_code;
  WriteChunk codes{
    duckdb::Vector{duckdb::LogicalType::UBIGINT,
                   reinterpret_cast<duckdb::data_ptr_t>(parts.codes.data()),
                   parts.codes.size()},
    parts.codes.size()};
  CompressPlain(type, CodesCodec(parts, rows.count),
                {{&codes, parts.codes.empty() ? 0U : 1U}, parts.codes.size()},
                meta.data);
  SealColumn(duckdb::ListType::GetChildType(type),
             {parts.elems, parts.elem_count}, /*skip_validity=*/false,
             meta.children[0], state.children[0]);
  auto& ends = meta.children[1];
  Claim(ends, duckdb::LogicalType::UBIGINT);
  WriteChunk end_chunk{
    duckdb::Vector{duckdb::LogicalType::UBIGINT,
                   reinterpret_cast<duckdb::data_ptr_t>(parts.ends.data()),
                   parts.ends.size()},
    parts.ends.size()};
  CompressPlain(duckdb::LogicalType::UBIGINT, codecs::CodecType::Delta,
                {{&end_chunk, parts.ends.empty() ? 0U : 1U}, parts.ends.size()},
                ends.data);
}

void ColumnWriter::CompressPlain(const duckdb::LogicalType& type,
                                 codecs::CodecType codec, const WriteRows& rows,
                                 std::vector<ColumnBlockMeta>& sink) {
  codecs::ColumnTuning tuning;
  Compress(type, rows, Enabled(_global_method, type) ? _global_method : codec,
           tuning, sink);
}

void ColumnWriter::SealVariant(const duckdb::LogicalType& type,
                               const WriteRows& rows, bool skip_validity,
                               ColumnMeta& meta, ColumnWriteState& state) {
  if (!skip_validity) {
    SealValidity(rows, meta.validity);
  }
  duckdb::LogicalType shredded_type;
  if (_variant_min_shred_size != -1 &&
      rows.count >= static_cast<uint64_t>(_variant_min_shred_size)) {
    if (_force_variant_shredding.id() != duckdb::LogicalTypeId::INVALID) {
      shredded_type = _force_variant_shredding;
    } else {
      duckdb::VariantShreddingStats stats;
      for (auto& c : rows.chunks) {
        stats.Update(c.data, c.count);
      }
      shredded_type = stats.GetShreddedType();
    }
  }
  ColumnMeta* prev_unshredded = nullptr;
  ColumnMeta* prev_shredded = nullptr;
  if (!meta.variant_rgs.empty()) {
    prev_unshredded = meta.variant_rgs.back().unshredded.get();
    prev_shredded = meta.variant_rgs.back().shredded.get();
  }
  state.children.resize(2);
  const auto next = [](ColumnWriteState& state, const ColumnMeta* prev,
                       const duckdb::LogicalType& type) -> ColumnWriteState& {
    if (prev && prev->type == type) {
      RestartLists(state);
    } else {
      state = {};
    }
    return state;
  };
  auto& layout = meta.variant_rgs.emplace_back();
  layout.row_count = rows.count;
  if (shredded_type.id() != duckdb::LogicalTypeId::STRUCT ||
      duckdb::StructType::GetChildCount(shredded_type) != 2) {
    const auto unshredded_type = duckdb::VariantShredding::GetUnshreddedType();
    layout.unshredded = std::make_unique<ColumnMeta>();
    SealColumn(unshredded_type, rows,
               /*skip_validity=*/true, *layout.unshredded,
               next(state.children[0], prev_unshredded, unshredded_type));
    return;
  }

  const auto alloc = SealAllocator();
  MemoryCharge shredded_charge{alloc};
  TrackedVector<duckdb::Vector> shredded_hold{alloc};
  shredded_hold.reserve(rows.chunks.size());
  TrackedVector<WriteChunk> unshredded_chunks{alloc};
  TrackedVector<WriteChunk> shredded_chunks{alloc};
  unshredded_chunks.reserve(rows.chunks.size());
  shredded_chunks.reserve(rows.chunks.size());
  for (auto& c : rows.chunks) {
    auto& shredded_out = shredded_hold.emplace_back(shredded_type, c.count);
    duckdb::VariantColumnData::ShredVariantData(c.data, shredded_out, c.count);
    shredded_charge.Resize(shredded_charge.Bytes() +
                           shredded_out.GetAllocationSize());
    auto& shred_entries = duckdb::StructVector::GetEntries(shredded_out);
    SDB_ASSERT(shred_entries.size() == 2);
    unshredded_chunks.push_back(RefChunk(shred_entries[0], c.count));
    shredded_chunks.push_back(RefChunk(shred_entries[1], c.count));
  }

  const auto& shredded_children =
    duckdb::StructType::GetChildTypes(shredded_type);
  const auto& unshredded_type = shredded_children[0].second;
  const auto& typed_type = shredded_children[1].second;
  layout.unshredded = std::make_unique<ColumnMeta>();
  layout.shredded = std::make_unique<ColumnMeta>();
  SealColumn(unshredded_type, {unshredded_chunks, rows.count},
             /*skip_validity=*/false, *layout.unshredded,
             next(state.children[0], prev_unshredded, unshredded_type));
  SealColumn(typed_type, {shredded_chunks, rows.count},
             /*skip_validity=*/false, *layout.shredded,
             next(state.children[1], prev_shredded, typed_type));
}

void ColumnWriter::SealColumn(
  const duckdb::LogicalType& type, const WriteRows& rows, bool skip_validity,
  ColumnMeta& meta, ColumnWriteState& state,
  std::span<const std::unique_ptr<ListIngest>> ingest) {
  Claim(meta, type);
  if (type.id() == duckdb::LogicalTypeId::VARIANT) {
    SealVariant(type, rows, skip_validity, meta, state);
    return;
  }
  if (duckdb::StructType::IsStruct(type) ||
      type.id() == duckdb::LogicalTypeId::UNION) {
    SealStruct(type, rows, skip_validity, meta, state, ingest);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::ARRAY) {
    SealArray(type, rows, skip_validity, meta, state);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::MAP) {
    if (ingest.empty()) {
      SealList(type, rows, skip_validity, meta, state);
    } else {
      SealListParts(type, rows, skip_validity, *ingest.front(), meta, state);
    }
    return;
  }

  const bool nulls_covered_by_data =
    Compress(type, rows, _method, state.tuning, meta.data);
  if (skip_validity) {
    return;
  }
  if (nulls_covered_by_data) {
    EmitEmptyValidity(rows.count, meta.validity);
    return;
  }
  SealValidity(rows, meta.validity);
}

ColumnWriter::ColumnWriter(ColWriter& owner, field_id id,
                           duckdb::LogicalType type,
                           const ColumnWriterOptions& options, bool deferred)
  : _owner{&owner},
    _id{id},
    _type{std::move(type)},
    _options{options},
    _staged_allocator{DuckAllocator(owner.Allocator())},
    _null_pad_charge{owner.Allocator()},
    _deferred{deferred},
    _distinct_charge{owner.Allocator()} {
  if (_type.id() == duckdb::LogicalTypeId::VARIANT) {
    const auto& config = duckdb::DBConfig::GetConfig(_owner->Database());
    _variant_min_shred_size =
      duckdb::Settings::Get<duckdb::VariantMinimumShreddingSizeSetting>(config);
    _force_variant_shredding = config.options.force_variant_shredding;
  }
  _meta.id = _id;
  _meta.type = _type;
  _meta.compression_level = _options.codec_params.compression_level;
  if (_options.distinct != DistinctStats::None) {
    _meta.distinct = duckdb::make_shared_ptr<duckdb::DistinctStatistics>();
  }
  OpenRowGroup();
  if (options.distinct != DistinctStats::None || deferred) {
    return;
  }
  if (_type.InternalType() == duckdb::PhysicalType::LIST) {
    _ingest.emplace_back(std::make_unique<ListIngest>(_owner->Allocator(), _type,
                                                      /*borrow=*/false));
    return;
  }
  if (_type.id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  const auto& fields = duckdb::StructType::GetChildTypes(_type);
  for (size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].second.InternalType() != duckdb::PhysicalType::LIST) {
      continue;
    }
    _ingest.resize(fields.size());
    _ingest[i] =
      std::make_unique<ListIngest>(_owner->Allocator(), fields[i].second,
                                   /*borrow=*/false);
  }
}

void ColumnWriter::OpenRowGroup() noexcept {
  _rg_rows = _owner->RowGroupRows(_row_start, _options.row_group_size);
}

WriteChunk& ColumnWriter::OpenChunk() {
  if (_staged_chunks != 0 &&
      _staged[_staged_chunks - 1].count < duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
    return _staged[_staged_chunks - 1];
  }
  if (_staged_chunks == _staged.size()) {
    auto& cache = _staged_caches.emplace_back(
      _staged_allocator, IngestsList() ? duckdb::LogicalType::BOOLEAN : _type,
      STANDARD_VECTOR_SIZE);
    _staged.push_back(WriteChunk{duckdb::Vector{cache}, 0});
  }
  auto& chunk = _staged[_staged_chunks];
  chunk.data.ResetFromCache(_staged_caches[_staged_chunks]);
  chunk.count = 0;
  duckdb::FlatVector::ValidityMutable(chunk.data)
    .SetAllValid(STANDARD_VECTOR_SIZE);
  ++_staged_chunks;
  return chunk;
}

template<typename Fill>
void ColumnWriter::Stage(uint64_t count, Fill&& fill) {
  uint64_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const uint64_t rg_room =
      _deferred ? STANDARD_VECTOR_SIZE : _rg_rows - _staged_rows;
    const auto take = static_cast<duckdb::idx_t>(std::min(
      {count - off, STANDARD_VECTOR_SIZE - uint64_t{back.count}, rg_room}));
    fill(back, static_cast<duckdb::idx_t>(off), take);
    back.count += take;
    if (back.count == duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
      DropAllValidMask(back);
      for (auto& ingest : _ingest) {
        if (ingest) {
          ingest->CheckDistinct();
        }
      }
    }
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (!_deferred && _staged_rows == _rg_rows) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::AppendList(const duckdb::Vector& vec, duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat rows;
  vec.ToUnifiedFormat(rows);
  Stage(count, [&](WriteChunk& back, duckdb::idx_t off, duckdb::idx_t take) {
    duckdb::FlatVector::ValidityMutable(back.data).CopySel(
      rows.validity, *rows.sel, off, back.count, take);
    _ingest.front()->Add(vec, off, take);
  });
}

void ColumnWriter::AppendStruct(const duckdb::Vector& vec,
                                duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat rows;
  vec.ToUnifiedFormat(rows);
  const auto& fields = duckdb::StructVector::GetEntries(vec);
  _field_rows.resize(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    if (_ingest[i]) {
      fields[i].ToUnifiedFormat(_field_rows[i]);
    }
  }
  Stage(count, [&](WriteChunk& back, duckdb::idx_t off, duckdb::idx_t take) {
    duckdb::FlatVector::ValidityMutable(back.data).CopySel(
      rows.validity, *rows.sel, off, back.count, take);
    auto& staged = duckdb::StructVector::GetEntries(back.data);
    for (size_t i = 0; i < fields.size(); ++i) {
      if (!_ingest[i]) {
        duckdb::ImmutableStrings::Copy(fields[i], staged[i], off + take,
                                       /*source_offset=*/off,
                                       /*target_offset=*/back.count);
        continue;
      }
      duckdb::FlatVector::ValidityMutable(staged[i]).CopySel(
        _field_rows[i].validity, *_field_rows[i].sel, off, back.count, take);
      _ingest[i]->Add(fields[i], off, take);
    }
  });
}

void ColumnWriter::Append(const duckdb::Vector& vec, duckdb::idx_t count) {
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  if (IngestsList()) {
    AppendList(vec, count);
    return;
  }
  if (!_ingest.empty()) {
    AppendStruct(vec, count);
    return;
  }
  Stage(count, [&](WriteChunk& back, duckdb::idx_t off, duckdb::idx_t take) {
    if (_deferred) {
      duckdb::VectorOperations::Copy(vec, back.data, off + take,
                                     /*source_offset=*/off,
                                     /*target_offset=*/back.count);
    } else {
      duckdb::ImmutableStrings::Copy(vec, back.data, off + take,
                                     /*source_offset=*/off,
                                     /*target_offset=*/back.count);
    }
    if (_type.id() == duckdb::LogicalTypeId::ARRAY) {
      ZeroNullArrays(back.data, back.count, take);
    }
  });
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          duckdb::idx_t count) {
  PadNullsTo(start_row);
  Append(vec, count);
}

void ColumnWriter::PadNullsTo(uint64_t target_row) {
  uint64_t current = _row_start + _staged_rows;
  if (current >= target_row) {
    return;
  }
  if (_type.IsNested()) {
    Stage(target_row - current,
          [&](WriteChunk& back, duckdb::idx_t, duckdb::idx_t take) {
            SetInvalidRows(back.data, back.count, take);
            for (auto& ingest : _ingest) {
              if (ingest) {
                ingest->AddNulls(take);
              }
            }
          });
    return;
  }
  if (!_null_pad) {
    _null_pad = std::make_unique<duckdb::Vector>(
      _type, STANDARD_VECTOR_SIZE,
      duckdb::VectorDataInitialization::ZERO_INITIALIZE);
    duckdb::FlatVector::ValidityMutable(*_null_pad)
      .SetAllInvalid(STANDARD_VECTOR_SIZE);
    _null_pad_charge.Resize(_null_pad->GetAllocationSize());
  }
  while (current < target_row) {
    const auto n =
      std::min<uint64_t>(target_row - current, STANDARD_VECTOR_SIZE);
    duckdb::FlatVector::SetSize(*_null_pad, static_cast<duckdb::idx_t>(n));
    Append(*_null_pad, static_cast<duckdb::idx_t>(n));
    current += n;
  }
}

bool ColumnWriter::LayoutMatches(const ColumnReader& column) const noexcept {
  return !_type.IsNested() && !_deferred &&
         _type.id() != duckdb::LogicalTypeId::VARIANT && !column.InMemory() &&
         !column.Patched() && column.Type() == _type &&
         !(_options.skip_validity && column.HasValidity()) &&
         column.CompressionLevel() == _options.codec_params.compression_level;
}

codecs::CodecType ColumnWriter::ForcedCodec() const {
  auto forced = _options.compression;
  if (forced == codecs::CodecType::Auto) {
    forced = ForcedBySetting(duckdb::DBConfig::GetConfig(_owner->Database()));
  }
  if (forced == codecs::CodecType::Auto) {
    return forced;
  }
  const auto physical = _type.InternalType();
  if (physical == duckdb::PhysicalType::VARCHAR &&
      forced != codecs::CodecType::Uncompressed) {
    return codecs::IsText(forced) ? forced : codecs::CodecType::Auto;
  }
  return codecs::GetCodec(forced, physical) ? forced : codecs::CodecType::Auto;
}

bool ColumnWriter::CopyableFrom(const ColumnReader& column) const {
  if (!LayoutMatches(column) || column.RowCount() < _options.row_group_size) {
    return false;
  }
  const auto forced = ForcedCodec();
  if (!absl::c_all_of(column.DataBlocks(), [&](const ColumnBlockMeta& block) {
        return SelfContained(block) && InCodec(forced, block);
      })) {
    return false;
  }
  const auto* validity = column.Validity();
  return !validity || absl::c_all_of(validity->DataBlocks(), SelfContained);
}

void ColumnWriter::CopyBlock(const ColumnBlockMeta& block, ReadContext& source,
                             std::vector<ColumnBlockMeta>& sink) {
  ColumnBlockMeta m{
    .statistics = block.statistics.Copy(),
    .tuple_count = block.tuple_count,
    .codec = block.codec,
  };
  auto& out = WriteCtx().Out();
  if (block.shared_size != 0) {
    if (_shared_source != &source || _shared_from != block.shared_offset) {
      _shared_to = AlignedPosition(out);
      WriteBytes(source, block.shared_offset, block.shared_size, out);
      _shared_source = &source;
      _shared_from = block.shared_offset;
    }
    m.shared_offset = _shared_to;
    m.shared_size = block.shared_size;
  }
  if (block.byte_size != 0) {
    m.file_offset = AlignedPosition(out);
    m.byte_size = block.byte_size;
    WriteBytes(source, block.file_offset, block.byte_size, out);
  }
  sink.push_back(std::move(m));
}

bool ColumnWriter::CopyBlocks(const ColumnReader& column, ReadContext& source,
                              absl::FunctionRef<bool()> progress) {
  SDB_ASSERT(CopyableFrom(column));
  SealRowGroup();
  _shared_source = nullptr;
  const auto blocks = column.DataBlocks();
  BlockReadAhead ahead{source, blocks};
  for (size_t b = 0; b != blocks.size(); ++b) {
    if (!progress()) {
      return false;
    }
    ahead.At(b);
    CopyBlock(blocks[b], source, _meta.data);
  }
  CopyValidity(column, source, 0, column.RowCount());
  _row_start += column.RowCount();
  OpenRowGroup();
  return true;
}

bool ColumnWriter::ValidityCopyable(const ColumnReader& column, uint64_t first,
                                    uint64_t end) const noexcept {
  const auto* validity = column.Validity();
  if (_options.skip_validity || !validity) {
    return true;
  }
  const auto blocks = validity->DataBlocks();
  for (auto v = validity->Locate(first).block; v < blocks.size(); ++v) {
    const auto v_first = validity->DataBlockFirstRow(v);
    if (v_first >= end) {
      break;
    }
    const auto v_end = v_first + blocks[v].tuple_count;
    if (!EmptyValidity(blocks[v]) &&
        (v_first < first || v_end > end || !SelfContained(blocks[v]))) {
      return false;
    }
  }
  return true;
}

void ColumnWriter::CopyValidity(const ColumnReader& column, ReadContext& source,
                                uint64_t first, uint64_t end) {
  if (_options.skip_validity) {
    return;
  }
  const auto* validity = column.Validity();
  if (!validity) {
    EmitEmptyValidity(end - first, _meta.validity);
    return;
  }
  const auto blocks = validity->DataBlocks();
  const auto begin = validity->Locate(first).block;
  auto stop = begin;
  while (stop < blocks.size() && validity->DataBlockFirstRow(stop) < end) {
    ++stop;
  }
  BlockReadAhead ahead{source, blocks.subspan(begin, stop - begin)};
  for (auto v = begin; v < stop; ++v) {
    ahead.At(v - begin);
    const auto v_first = validity->DataBlockFirstRow(v);
    const auto v_end = v_first + blocks[v].tuple_count;
    if (v_first >= first && v_end <= end) {
      CopyBlock(blocks[v], source, _meta.validity);
    } else {
      SDB_ASSERT(EmptyValidity(blocks[v]));
      EmitEmptyValidity(std::min(v_end, end) - std::max(v_first, first),
                        _meta.validity);
    }
  }
}

void ColumnWriter::DecodeRows(const ColumnReader& column, ReadContext& source,
                              uint64_t first, uint64_t count) {
  auto state = column.InitScan(source);
  if (first != 0) {
    column.Skip(state, first);
  }
  ColumnReader::VectorScratch scratch{column.Type(), source.Allocator()};
  for (uint64_t done = 0; done < count;) {
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(count - done, STANDARD_VECTOR_SIZE));
    auto& batch = scratch.Reset();
    column.Scan(state, batch, take);
    Append(batch, take);
    done += take;
  }
}

void ColumnWriter::CopyRows(uint64_t start_row, const ColumnReader& column,
                            ReadContext& source, uint64_t first,
                            uint64_t count) {
  SDB_ASSERT(_row_start + _staged_rows <= start_row);
  SDB_ASSERT(first + count <= column.RowCount());
  PadNullsTo(start_row);
  _shared_source = nullptr;
  const auto end = first + count;
  auto written = first;
  if (LayoutMatches(column) && _options.distinct == DistinctStats::None) {
    SDB_ASSERT(!_owner->HasAnn(_id));
    const auto forced = ForcedCodec();
    const auto blocks = column.DataBlocks();
    for (auto b = column.Locate(first).block; b < blocks.size(); ++b) {
      const auto block_first = column.DataBlockFirstRow(b);
      const auto block_end = block_first + blocks[b].tuple_count;
      if (block_first >= end) {
        break;
      }
      if (block_first < first || block_end > end || !SelfContained(blocks[b]) ||
          !InCodec(forced, blocks[b]) ||
          !ValidityCopyable(column, block_first, block_end)) {
        continue;
      }
      if (written < block_first) {
        DecodeRows(column, source, written, block_first - written);
        written = block_first;
      }
      if (blocks[b].tuple_count > _rg_rows - _staged_rows) {
        continue;
      }
      SealRowGroup();
      CopyBlock(blocks[b], source, _meta.data);
      CopyValidity(column, source, block_first, block_end);
      _row_start += blocks[b].tuple_count;
      OpenRowGroup();
      written = block_end;
    }
  }
  if (written < end) {
    DecodeRows(column, source, written, end - written);
  }
}

void ColumnWriter::SealStaged(const WriteRows& rows) {
  SealColumn(_type, rows, _options.skip_validity, _meta, _state, _ingest);
  _row_start += rows.count;
}

void ColumnWriter::SealRowGroup() {
  if (_staged_rows == 0) {
    return;
  }
  std::span<WriteChunk> chunks{_staged.data(), _staged_chunks};
  DropAllValidMask(chunks.back());
  if (_options.distinct != DistinctStats::None) {
    for (auto& chunk : chunks) {
      UpdateDistinct(*_meta.distinct, _options.distinct, chunk.data,
                     chunk.count, _distinct_hashes);
    }
    _distinct_charge.Resize(_distinct_hashes.GetAllocationSize());
  }
  const auto& config = duckdb::DBConfig::GetConfig(_owner->Database());
  _global_method = ForcedBySetting(config);
  _method = _options.compression == codecs::CodecType::Auto
              ? _global_method
              : _options.compression;
  _disabled.reset();
  for (const auto disabled : config.GetDisabledCompressionMethods()) {
    if (const auto type = codecs::FindCodec(disabled);
        type != codecs::kNoCodec) {
      _disabled.set(std::to_underlying(type));
    }
  }
  if (!_deferred) {
    SealStaged({chunks, _staged_rows});
  } else {
    const size_t per_seal = std::max<size_t>(
      1, size_t{_options.row_group_size} / size_t{STANDARD_VECTOR_SIZE});
    for (size_t begin = 0; begin < chunks.size(); begin += per_seal) {
      WriteRows slice{
        chunks.subspan(begin, std::min(per_seal, chunks.size() - begin))};
      for (const auto& chunk : slice.chunks) {
        slice.count += chunk.count;
      }
      SealStaged(slice);
    }
  }
  for (duckdb::idx_t c = 0; c != _staged_chunks; ++c) {
    _staged[c].data.ResetFromCache(_staged_caches[c]);
  }
  _staged_chunks = 0;
  _staged_rows = 0;
  OpenRowGroup();
}

void ColumnWriter::Finish() {
  SealRowGroup();
  _staged.clear();
  _staged_caches.clear();
}

std::vector<WriteChunk> ColumnWriter::Snapshot(uint64_t rows) const {
  SDB_ASSERT(_deferred && _row_start == 0);
  std::vector<WriteChunk> chunks;
  auto left = std::min(rows, _staged_rows);
  for (size_t i = 0; i < _staged_chunks && left != 0; ++i) {
    const auto& chunk = _staged[i];
    const auto take =
      static_cast<duckdb::idx_t>(std::min<uint64_t>(left, chunk.count));
    if (take == duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
      chunks.push_back(WriteChunk{duckdb::Vector::Ref(chunk.data), take});
    } else {
      duckdb::Vector copy{_type, take};
      duckdb::VectorOperations::Copy(chunk.data, copy, take, 0, 0);
      duckdb::FlatVector::SetSize(copy, take);
      chunks.push_back(WriteChunk{std::move(copy), take});
    }
    left -= take;
  }
  return chunks;
}

size_t ColumnWriter::StagedBytes() const noexcept {
  size_t bytes = 0;
  for (size_t i = 0; i < _staged_chunks; ++i) {
    bytes += _staged[i].data.GetAllocationSize();
  }
  return bytes;
}

void ColumnWriter::SetDistinct(
  duckdb::shared_ptr<duckdb::DistinctStatistics> distinct) {
  _meta.distinct = std::move(distinct);
  _options.distinct = DistinctStats::None;
}

void UpdateDistinct(duckdb::DistinctStatistics& distinct, DistinctStats mode,
                    const duckdb::Vector& values, duckdb::idx_t count,
                    duckdb::Vector& hashes) {
  SDB_ASSERT(mode != DistinctStats::None);
  if (!hashes.GetBufferRef()) {
    hashes.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                      STANDARD_VECTOR_SIZE);
  }
  if (mode == DistinctStats::Exact) {
    distinct.Update(values, count, hashes);
  } else {
    distinct.UpdateSample(values, count, hashes);
  }
}

}  // namespace irs
