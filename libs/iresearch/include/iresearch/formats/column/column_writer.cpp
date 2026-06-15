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

#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/variant/variant_shredding.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table/column_data.hpp>
#include <duckdb/storage/table/column_data_checkpointer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/internal/overflow_string_io.hpp"
#include "iresearch/formats/column/internal/persistent_column_data.hpp"
#include "iresearch/formats/column/internal/write_context.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {

ColumnWriter::ColumnWriter(field_id id, duckdb::LogicalType type,
                           uint32_t row_group_size, WriteContext& write_ctx,
                           FooterColumnEntry& entry, bool skip_validity)
  : _id{id},
    _type{std::move(type)},
    _row_group_size{row_group_size},
    _write_ctx{&write_ctx},
    _entry{&entry},
    _staging{_type, _row_group_size,
             duckdb::VectorDataInitialization::UNINITIALIZED},
    _skip_validity{skip_validity} {
  SDB_ASSERT(_row_group_size != 0);
}

void ColumnWriter::PadNullsTo(uint64_t start_row) {
  const uint64_t expected = _row_group_first_doc + _filled;
  SDB_ASSERT(start_row >= expected,
             "ColumnWriter::Append: start_row must be monotonic");
  if (start_row == expected) {
    return;
  }
  uint64_t gap = start_row - expected;
  while (gap > 0) {
    const uint64_t pad = std::min<uint64_t>(gap, _row_group_size - _filled);
    auto& validity = duckdb::FlatVector::ValidityMutable(_staging);
    validity.EnsureWritable();
    auto* mask = validity.GetData();
    using V = std::remove_pointer_t<decltype(mask)>;
    constexpr auto kBitsPerEntry = duckdb::ValidityMask::BITS_PER_VALUE;
    uint64_t i = _filled;
    const uint64_t end = _filled + pad;
    // Partial leading word: one masked AND covers all bits [i, word_end).
    if (const auto r = i % kBitsPerEntry; r != 0) {
      const auto take = std::min<uint64_t>(end - i, kBitsPerEntry - r);
      const V clear_mask = ((V{1} << take) - 1) << r;
      mask[i / kBitsPerEntry] &= ~clear_mask;
      i += take;
    }
    // Whole-word zero stores.
    if (i + kBitsPerEntry <= end) {
      const auto words = (end - i) / kBitsPerEntry;
      std::memset(mask + i / kBitsPerEntry, 0, words * sizeof(V));
      i += words * kBitsPerEntry;
    }
    // Trailing partial word: one masked AND covers bits [i, end).
    if (i < end) {
      const auto take = end - i;
      const V clear_mask = (V{1} << take) - 1;
      mask[i / kBitsPerEntry] &= ~clear_mask;
    }
    _filled += pad;
    gap -= pad;
    if (_filled == _row_group_size) {
      FlushRowGroup();
    }
  }
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  PadNullsTo(start_row);

  duckdb::idx_t consumed = 0;
  while (consumed < count) {
    const auto take =
      std::min<duckdb::idx_t>(count - consumed, _row_group_size - _filled);
    duckdb::VectorOperations::Copy(vec, _staging, consumed + take, consumed,
                                   _filled);
    _filled += take;
    consumed += take;
    if (_filled == _row_group_size) {
      FlushRowGroup();
    }
  }
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          const duckdb::SelectionVector& sel,
                          duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  PadNullsTo(start_row);

  duckdb::idx_t consumed = 0;
  while (consumed < count) {
    const auto take =
      std::min<duckdb::idx_t>(count - consumed, _row_group_size - _filled);
    duckdb::VectorOperations::Copy(vec, _staging, sel, consumed + take,
                                   consumed, _filled);
    _filled += take;
    consumed += take;
    if (_filled == _row_group_size) {
      FlushRowGroup();
    }
  }
}

void ColumnWriter::Finalize() {
  if (_filled > 0) {
    FlushRowGroup();
  }
}

namespace {

void CaptureSegment(duckdb::ColumnSegment& segment, duckdb::idx_t segment_size,
                    const byte_type* bytes, IndexOutput& out,
                    uint64_t& running_row_start,
                    std::vector<duckdb::DataPointer>& sink) {
  const auto tuple_count = segment.count.load();
  if (tuple_count == 0) {
    return;
  }

  duckdb::DataPointer ptr{segment.GetStats().Copy()};
  ptr.row_start = running_row_start;
  ptr.tuple_count = tuple_count;
  const auto& codec = segment.GetCompressionFunction();
  ptr.compression_type = codec.type;

  if (segment.GetStats().IsConstant() || !bytes || segment_size == 0) {
    ptr.compression_type = duckdb::CompressionType::COMPRESSION_CONSTANT;
    ptr.block_pointer.block_id = INVALID_BLOCK;
    ptr.block_pointer.offset = 0;
  } else {
    SDB_ASSERT(segment_size <= std::numeric_limits<uint32_t>::max(),
               ".col writer segment > 4GB; offset field too narrow");
    static constexpr uint64_t kAlign = 64;
    static constexpr byte_type kZeros[kAlign] = {};
    if (const uint64_t rem = out.Position() % kAlign; rem != 0) {
      out.WriteData(kZeros, kAlign - rem);
    }
    const uint64_t file_offset = out.Position();
    out.WriteData(bytes, segment_size);
    ptr.block_pointer.block_id = static_cast<duckdb::block_id_t>(file_offset);
    ptr.block_pointer.offset = static_cast<uint32_t>(segment_size);
  }

  if (codec.serialize_state) {
    ptr.segment_state = codec.serialize_state(segment);
  }
  running_row_start += tuple_count;
  sink.push_back(std::move(ptr));
}

void CopySlice(duckdb::Vector& dst, duckdb::Vector& src, duckdb::idx_t offset,
               duckdb::idx_t count) {
  duckdb::VectorOperations::Copy(src, dst, offset + count, offset, 0);
}

struct PickedCodec {
  duckdb::optional_ptr<const duckdb::CompressionFunction> function;
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
};

PickedCodec PickCodec(WriteContext& write_ctx,
                      const duckdb::LogicalType& codec_type,
                      duckdb::Vector& staging, duckdb::idx_t row_count,
                      duckdb::CompressionType forced) {
  auto& db = write_ctx.Database();
  const auto& config = duckdb::DBConfig::GetConfig(db);

  std::vector<duckdb::reference<const duckdb::CompressionFunction>> candidates;
  if (forced != duckdb::CompressionType::COMPRESSION_AUTO) {
    auto fn =
      config.TryGetCompressionFunction(forced, codec_type.InternalType());
    if (!fn || !fn->init_analyze) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER, ".col writer: compression '",
                duckdb::CompressionTypeToString(forced),
                "' is not supported for type ", codec_type.ToString());
    }
    candidates.emplace_back(*fn);
  } else {
    candidates = config.GetCompressionFunctions(codec_type.InternalType());
  }

  duckdb::CompressionAnalyzeContext ctx{write_ctx, db,
                                        duckdb::StorageVersion::V2_0_0};
  std::vector<duckdb::unique_ptr<duckdb::AnalyzeState>> states(
    candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (auto* init_analyze = candidates[i].get().init_analyze) {
      states[i] = init_analyze(ctx, codec_type.InternalType());
    }
  }

  // Feed analyze in native's transient-segment scan cadence
  // so the shape-sensitive analyzers pick the same codec native does.
  const auto type_size = duckdb::GetTypeIdSize(codec_type.InternalType());
  duckdb::Vector slice{staging.GetType(), STANDARD_VECTOR_SIZE};
  duckdb::idx_t consumed = 0;
  duckdb::idx_t seg_bytes = 0;
  while (consumed < row_count) {
    seg_bytes = duckdb::ColumnData::GetTransientSegmentSize(
      config, write_ctx, codec_type, seg_bytes);
    const auto seg_rows = std::max<duckdb::idx_t>(1, seg_bytes / type_size);
    duckdb::idx_t seg_consumed = 0;
    while (seg_consumed < seg_rows && consumed < row_count) {
      const auto take = std::min<duckdb::idx_t>(
        {seg_rows - seg_consumed, STANDARD_VECTOR_SIZE, row_count - consumed});
      CopySlice(slice, staging, consumed, take);
      duckdb::FlatVector::SetSize(slice, take);
      for (size_t i = 0; i < candidates.size(); ++i) {
        if (states[i] && !candidates[i].get().analyze(*states[i], slice)) {
          states[i].reset();
        }
      }
      seg_consumed += take;
      consumed += take;
    }
  }

  PickedCodec best;
  duckdb::idx_t best_score = std::numeric_limits<duckdb::idx_t>::max();
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!states[i]) {
      continue;
    }
    const auto score = candidates[i].get().final_analyze(*states[i]);
    if (score != duckdb::DConstants::INVALID_INDEX && score < best_score) {
      best_score = score;
      best = {&candidates[i].get(), std::move(states[i])};
    }
  }
  if (!best.function) {
    if (forced == duckdb::CompressionType::COMPRESSION_AUTO) {
      SDB_THROW(sdb::ERROR_INTERNAL,
                ".col writer: no codec accepted the row group for type ",
                codec_type.ToString());
    }
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, ".col writer: forced compression '",
              duckdb::CompressionTypeToString(forced),
              "' could not produce a plan for type ", codec_type.ToString());
  }
  return best;
}

void CompressColumn(WriteContext& write_ctx,
                    const duckdb::LogicalType& codec_type,
                    duckdb::Vector& staging, duckdb::idx_t row_count,
                    uint64_t row_start, std::vector<duckdb::DataPointer>& sink,
                    duckdb::CompressionType forced) {
  auto& db = write_ctx.Database();
  auto& out = write_ctx.Out();
  // VALIDITY all-valid short-circuit: synthesize a COMPRESSION_EMPTY
  // DataPointer instead of going through PickCodec. EMPTY is not in the
  // analyze tournament (null init_analyze) so without this short-circuit
  // Roaring (~26 bytes/RG) wins by default.
  if (codec_type.id() == duckdb::LogicalTypeId::VALIDITY) {
    duckdb::UnifiedVectorFormat fmt;
    staging.ToUnifiedFormat(row_count, fmt);
    if (fmt.validity.CountValid(row_count) == row_count) {
      duckdb::DataPointer dp{duckdb::BaseStatistics::CreateEmpty(codec_type)};
      dp.row_start = row_start;
      dp.tuple_count = row_count;
      dp.compression_type = duckdb::CompressionType::COMPRESSION_EMPTY;
      sink.push_back(std::move(dp));
      return;
    }
  }

  auto picked = PickCodec(write_ctx, codec_type, staging, row_count, forced);

  duckdb::ColumnDataCheckpointData::OverflowStringWriterFactory
    overflow_writer_factory;
  if (codec_type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    overflow_writer_factory = [&] {
      return duckdb::make_uniq<IndexOutputOverflowWriter>(out);
    };
  }

  uint64_t running_row_start = row_start;
  duckdb::ColumnDataCheckpointData::FlushSegmentFn flush_segment_fn =
    [&](duckdb::unique_ptr<duckdb::ColumnSegment> segment,
        duckdb::BufferHandle handle, duckdb::idx_t segment_size) {
      if (segment_size == 0) {
        CaptureSegment(*segment, segment_size, nullptr, out, running_row_start,
                       sink);
        return;
      }
      if (handle.IsValid()) {
        CaptureSegment(*segment, segment_size,
                       reinterpret_cast<const byte_type*>(handle.Ptr()), out,
                       running_row_start, sink);
        return;
      }
      if (!segment->GetBlockHandle()) {
        CaptureSegment(*segment, segment_size, nullptr, out, running_row_start,
                       sink);
        return;
      }
      auto& bm = duckdb::BufferManager::GetBufferManager(db);
      auto repinned = bm.Pin(segment->GetBlockHandle());
      CaptureSegment(*segment, segment_size,
                     reinterpret_cast<const byte_type*>(repinned.Ptr()), out,
                     running_row_start, sink);
    };

  duckdb::ColumnDataCheckpointData::FlushSegmentInternalFn
    flush_segment_internal_fn =
      [&](duckdb::unique_ptr<duckdb::ColumnSegment> segment,
          duckdb::idx_t segment_size) {
        if (segment_size == 0 || !segment->GetBlockHandle()) {
          CaptureSegment(*segment, segment_size, nullptr, out,
                         running_row_start, sink);
          return;
        }
        auto& bm = duckdb::BufferManager::GetBufferManager(db);
        auto handle = bm.Pin(segment->GetBlockHandle());
        CaptureSegment(*segment, segment_size,
                       reinterpret_cast<const byte_type*>(handle.Ptr()), out,
                       running_row_start, sink);
      };

  duckdb::ColumnDataCheckpointData ckp{
    codec_type,
    db,
    duckdb::StorageVersion::V2_0_0,
    std::move(overflow_writer_factory),
    std::move(flush_segment_fn),
    std::move(flush_segment_internal_fn),
    write_ctx,
  };

  auto comp_state =
    picked.function->init_compression(ckp, std::move(picked.state));

  duckdb::Vector slice{staging.GetType(), STANDARD_VECTOR_SIZE};
  duckdb::idx_t consumed = 0;
  while (consumed < row_count) {
    const auto take =
      std::min<duckdb::idx_t>(row_count - consumed, STANDARD_VECTOR_SIZE);
    CopySlice(slice, staging, consumed, take);
    duckdb::FlatVector::SetSize(slice, take);
    picked.function->compress(*comp_state, slice);
    consumed += take;
  }
  picked.function->compress_finalize(*comp_state);
}

bool VariantShreddingEnabled(int64_t minimum_size, uint64_t row_count) {
  if (minimum_size == -1) {
    return false;
  }
  return row_count >= static_cast<uint64_t>(minimum_size);
}

bool NoSpilledRows(duckdb::Vector& untyped_value_index, uint64_t row_count) {
  duckdb::UnifiedVectorFormat fmt;
  untyped_value_index.ToUnifiedFormat(row_count, fmt);
  const auto* indices = duckdb::UnifiedVectorFormat::GetData<uint32_t>(fmt);
  for (uint64_t row = 0; row < row_count; ++row) {
    const auto idx = fmt.sel->get_index(row);
    if (fmt.validity.RowIsValid(idx) && indices[idx] > 0) {
      return false;
    }
  }
  return true;
}

void MarkFullyShredded(duckdb::Vector& vec, PersistentColumnData& node,
                       uint64_t row_count) {
  if (vec.GetType().id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  auto& entries = duckdb::StructVector::GetEntries(vec);
  const auto& child_types = duckdb::StructType::GetChildTypes(vec.GetType());
  size_t typed_value_index = child_types.size();
  size_t untyped_value_index = child_types.size();
  for (size_t child = 0; child < child_types.size(); ++child) {
    if (child_types[child].first == "typed_value") {
      typed_value_index = child;
    } else if (child_types[child].first == "untyped_value_index") {
      untyped_value_index = child;
    }
  }

  if (typed_value_index == child_types.size()) {
    for (size_t child = 0; child < entries.size(); ++child) {
      MarkFullyShredded(entries[child], node.child_columns[child], row_count);
    }
    return;
  }

  if (untyped_value_index != child_types.size()) {
    node.fully_shredded =
      NoSpilledRows(entries[untyped_value_index], row_count);
  }
  MarkFullyShredded(entries[typed_value_index],
                    node.child_columns[typed_value_index], row_count);
}

void FlushNode(WriteContext& write_ctx, const duckdb::LogicalType& type,
               duckdb::Vector& vec, duckdb::idx_t row_count, uint64_t row_start,
               PersistentColumnData& node, bool skip_validity,
               duckdb::CompressionType forced) {
  // `forced` applies only to the leaf data column; validity bitmaps and
  // LIST length sub-columns always run the analyze tournament.
  const auto validity_type =
    duckdb::LogicalType(duckdb::LogicalTypeId::VALIDITY);
  switch (type.id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, vec, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      if (node.child_columns.empty()) {
        node.child_columns.emplace_back();
        node.child_columns.front().type = duckdb::ArrayType::GetChildType(type);
      }
      const auto array_size =
        static_cast<duckdb::idx_t>(duckdb::ArrayType::GetSize(type));
      auto& child = duckdb::ArrayVector::GetChildMutable(vec);
      FlushNode(write_ctx, duckdb::ArrayType::GetChildType(type), child,
                row_count * array_size, row_start * array_size,
                node.child_columns.front(),
                /*skip_validity=*/false, forced);
      return;
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      // MAP shares LIST's physical layout (PhysicalType::LIST + STRUCT<k,v>
      // element). ListType::GetChildType / ListVector accessors work for
      // both, so the on-disk shape is identical.
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, vec, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      // Store column-global cumulative offsets per row (matches
      // duckdb::ListColumnData::Append). Row i's element span is
      // [offsets[i-1], offsets[i]) in the child column's address
      // space, with the implicit offsets[-1] == 0 at column start.
      // Invalid parent rows contribute zero elements.
      const auto* entries =
        duckdb::FlatVector::GetData<duckdb::list_entry_t>(vec);
      const auto& parent_validity = duckdb::FlatVector::Validity(vec);
      duckdb::Vector offsets{duckdb::LogicalType::UBIGINT, row_count};
      auto* op = duckdb::FlatVector::GetDataMutable<uint64_t>(offsets);
      uint64_t running = node.list_global_running;
      for (duckdb::idx_t i = 0; i < row_count; ++i) {
        if (parent_validity.RowIsValid(i)) {
          running += entries[i].length;
        }
        op[i] = running;
      }
      const uint64_t total_elems = running - node.list_global_running;
      node.list_global_running = running;
      const auto offsets_type = duckdb::LogicalType::UBIGINT;
      CompressColumn(write_ctx, offsets_type, offsets, row_count, row_start,
                     node.pointers, duckdb::CompressionType::COMPRESSION_AUTO);
      if (node.child_columns.empty()) {
        node.child_columns.emplace_back();
        node.child_columns.front().type = duckdb::ListType::GetChildType(type);
      }
      auto& child = duckdb::ListVector::GetChildMutable(vec);
      FlushNode(write_ctx, duckdb::ListType::GetChildType(type), child,
                static_cast<duckdb::idx_t>(total_elems),
                /*row_start=*/0, node.child_columns.front(),
                /*skip_validity=*/false, forced);
      return;
    }
    case duckdb::LogicalTypeId::VARIANT: {
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, vec, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }

      auto& db = write_ctx.Database();
      const auto& config = duckdb::DBConfig::GetConfig(db);
      bool should_shred = VariantShreddingEnabled(
        duckdb::Settings::Get<duckdb::VariantMinimumShreddingSizeSetting>(
          config),
        row_count);

      duckdb::LogicalType shredded_type;
      if (should_shred) {
        if (config.options.force_variant_shredding.id() !=
            duckdb::LogicalTypeId::INVALID) {
          shredded_type = config.options.force_variant_shredding;
        } else {
          duckdb::VariantShreddingStats stats;
          stats.Update(vec, row_count);
          shredded_type = stats.GetShreddedType();
        }
        if (shredded_type.id() != duckdb::LogicalTypeId::STRUCT ||
            duckdb::StructType::GetChildCount(shredded_type) != 2) {
          should_shred = false;
        }
      }

      node.variant_layouts.emplace_back();
      auto& layout = node.variant_layouts.back();
      layout.row_start = row_start;
      layout.row_count = row_count;
      layout.unshredded = std::make_unique<PersistentColumnData>();

      if (!should_shred) {
        layout.unshredded->type = duckdb::VariantShredding::GetUnshreddedType();
        FlushNode(write_ctx, layout.unshredded->type, vec, row_count,
                  /*row_start=*/0, *layout.unshredded, /*skip_validity=*/true,
                  forced);
        return;
      }

      duckdb::Vector shredded_out{shredded_type, row_count};
      duckdb::VariantColumnData::ShredVariantData(vec, shredded_out, row_count);
      auto& shred_entries = duckdb::StructVector::GetEntries(shredded_out);
      SDB_ASSERT(shred_entries.size() == 2);

      {
        duckdb::UnifiedVectorFormat unshredded_fmt;
        shred_entries[0].ToUnifiedFormat(row_count, unshredded_fmt);
        layout.shred_state = unshredded_fmt.validity.CountValid(row_count) == 0
                               ? VariantShredState::Full
                               : VariantShredState::Partial;
      }

      layout.shredded_node = std::make_unique<PersistentColumnData>();
      layout.unshredded->type = shred_entries[0].GetType();
      layout.shredded_node->type = shred_entries[1].GetType();
      FlushNode(write_ctx, layout.unshredded->type, shred_entries[0], row_count,
                /*row_start=*/0, *layout.unshredded,
                /*skip_validity=*/false, forced);
      FlushNode(write_ctx, layout.shredded_node->type, shred_entries[1],
                row_count, /*row_start=*/0, *layout.shredded_node,
                /*skip_validity=*/false, forced);
      MarkFullyShredded(shred_entries[1], *layout.shredded_node, row_count);
      return;
    }
    case duckdb::LogicalTypeId::STRUCT: {
      // STRUCT has no top-level data of its own -- just parent validity and
      // per-field children. Matches duckdb::StructColumnData::Append.
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, vec, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      const auto& child_types = duckdb::StructType::GetChildTypes(type);
      auto& entries = duckdb::StructVector::GetEntries(vec);
      SDB_ASSERT(entries.size() == child_types.size());
      if (node.child_columns.size() != child_types.size()) {
        node.child_columns.clear();
        node.child_columns.resize(child_types.size());
        for (size_t i = 0; i < child_types.size(); ++i) {
          node.child_columns[i].type = child_types[i].second;
        }
      }
      for (size_t i = 0; i < child_types.size(); ++i) {
        FlushNode(write_ctx, child_types[i].second, entries[i], row_count,
                  row_start, node.child_columns[i],
                  /*skip_validity=*/false, forced);
      }
      return;
    }
    default: {
      CompressColumn(write_ctx, type, vec, row_count, row_start, node.pointers,
                     forced);
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, vec, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      return;
    }
  }
}

}  // namespace

void ColumnWriter::FlushRowGroup() {
  if (_filled == 0) {
    return;
  }

  FlushNode(*_write_ctx, _type, _staging, _filled, _row_group_first_doc,
            _entry->root, _skip_validity, _forced_compression);

  _row_group_first_doc += _filled;
  _filled = 0;
  _staging.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                      _row_group_size);
}

}  // namespace irs
