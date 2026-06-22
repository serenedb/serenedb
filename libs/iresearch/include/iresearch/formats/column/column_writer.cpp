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
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
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
#include <duckdb/storage/table/column_data_checkpointer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <limits>
#include <memory>
#include <span>
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
                           FooterColumnEntry& entry, bool skip_validity,
                           bool hyperloglog)
  : _id{id},
    _type{std::move(type)},
    _row_group_size{row_group_size},
    _write_ctx{&write_ctx},
    _entry{&entry},
    _skip_validity{skip_validity},
    _hyperloglog{hyperloglog} {
  SDB_ASSERT(_row_group_size != 0);
  if (hyperloglog) {
    _entry->root.hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
  }
}

bool ColumnWriter::HasHyperLogLog() const noexcept { return _hyperloglog; }

void ColumnWriter::SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll) {
  _entry->root.hyperloglog = std::move(hll);
}

void ColumnWriter::PadNullsTo(uint64_t target_row) {
  const uint64_t first_not_present =
    _data_ctx.first_rg_doc_id + _data_ctx.filled;
  SDB_ASSERT(target_row >= first_not_present,
             "ColumnWriter::PadNullsTo: target_row must be monotonic");
  uint64_t gap = target_row - first_not_present;

  while (gap > 0) {
    auto& chunk = OpenChunk();
    const duckdb::idx_t room =
      duckdb::FlatVector::GetCapacity(chunk.data) - chunk.Count();
    const duckdb::idx_t to_boundary =
      static_cast<duckdb::idx_t>(_row_group_size - _data_ctx.filled);
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(gap, std::min<duckdb::idx_t>(room, to_boundary)));
    auto& validity = duckdb::FlatVector::ValidityMutable(chunk.data);
    validity.EnsureWritable();
    for (duckdb::idx_t i = 0; i < take; ++i) {
      validity.SetInvalid(chunk.Count() + i);
    }
    chunk.SetCount(chunk.Count() + take);
    _data_ctx.filled += take;
    gap -= take;
    MaybeFlushRowGroup();
  }
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  PadNullsTo(start_row);

  duckdb::idx_t src = 0;
  while (src < count) {
    auto& chunk = OpenChunk();
    const duckdb::idx_t room =
      duckdb::FlatVector::GetCapacity(chunk.data) - chunk.Count();
    const duckdb::idx_t to_boundary = _row_group_size - _data_ctx.filled;
    const duckdb::idx_t take =
      std::min(count - src, std::min<duckdb::idx_t>(room, to_boundary));
    duckdb::VectorOperations::Copy(vec, chunk.data, src + take, src,
                                   chunk.Count());
    chunk.SetCount(chunk.Count() + take);
    _data_ctx.filled += take;
    src += take;
    MaybeFlushRowGroup();
  }
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          const duckdb::SelectionVector& sel,
                          duckdb::idx_t count) {
  if (count == 0) {
    return;
  }
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  PadNullsTo(start_row);

  duckdb::idx_t src = 0;
  while (src < count) {
    auto& chunk = OpenChunk();
    const duckdb::idx_t room =
      duckdb::FlatVector::GetCapacity(chunk.data) - chunk.Count();
    const duckdb::idx_t to_boundary = _row_group_size - _data_ctx.filled;
    const duckdb::idx_t take =
      std::min(count - src, std::min<duckdb::idx_t>(room, to_boundary));
    duckdb::VectorOperations::Copy(vec, chunk.data, sel, src + take, src,
                                   chunk.Count());
    chunk.SetCount(chunk.Count() + take);
    _data_ctx.filled += take;
    src += take;
    MaybeFlushRowGroup();
  }
}

void ColumnWriter::Finalize() {
  if (_data_ctx.filled > 0) {
    FlushChunks(_data_ctx.filled);
  }
}

namespace {

using ChunkSpan = std::span<Chunk>;

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

struct PickedCodec {
  duckdb::optional_ptr<const duckdb::CompressionFunction> function;
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
};

PickedCodec PickCodec(WriteContext& write_ctx,
                      const duckdb::LogicalType& codec_type, ChunkSpan chunks,
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

  for (auto& chunk : chunks) {
    SDB_ASSERT(chunk.Count() <= STANDARD_VECTOR_SIZE);
    for (size_t i = 0; i < candidates.size(); ++i) {
      if (states[i] && !candidates[i].get().analyze(*states[i], chunk.data)) {
        states[i].reset();
      }
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
                    const duckdb::LogicalType& codec_type, ChunkSpan chunks,
                    duckdb::idx_t row_count, uint64_t row_start,
                    std::vector<duckdb::DataPointer>& sink,
                    duckdb::CompressionType forced) {
  auto& db = write_ctx.Database();
  auto& out = write_ctx.Out();
  if (codec_type.id() == duckdb::LogicalTypeId::VALIDITY) {
    uint64_t valid = 0;
    for (auto& chunk : chunks) {
      valid +=
        duckdb::FlatVector::Validity(chunk.data).CountValid(chunk.Count());
    }
    if (valid == row_count) {
      duckdb::DataPointer dp{duckdb::BaseStatistics::CreateEmpty(codec_type)};
      dp.row_start = row_start;
      dp.tuple_count = row_count;
      dp.compression_type = duckdb::CompressionType::COMPRESSION_EMPTY;
      sink.push_back(std::move(dp));
      return;
    }
  }

  auto picked = PickCodec(write_ctx, codec_type, chunks, forced);

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

  for (auto& chunk : chunks) {
    SDB_ASSERT(chunk.Count() <= STANDARD_VECTOR_SIZE);
    picked.function->compress(*comp_state, chunk.data);
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
  const auto* indices =
    duckdb::FlatVector::GetData<uint32_t>(untyped_value_index);
  const auto& validity = duckdb::FlatVector::Validity(untyped_value_index);
  for (uint64_t row = 0; row < row_count; ++row) {
    if (validity.RowIsValid(row) && indices[row] > 0) {
      return false;
    }
  }
  return true;
}

bool NoSpilledRows(ChunkSpan chunks) {
  for (auto& chunk : chunks) {
    if (!NoSpilledRows(chunk.data, chunk.Count())) {
      return false;
    }
  }
  return true;
}

void PushSlicedChunks(std::vector<Chunk>& out, duckdb::Vector& vec,
                      duckdb::idx_t count) {
  duckdb::idx_t off = 0;
  while (off < count) {
    const auto take =
      std::min<duckdb::idx_t>(count - off, STANDARD_VECTOR_SIZE);
    if (off == 0 && take == count) {
      out.emplace_back(duckdb::Vector::Ref(vec), take);
    } else {
      duckdb::Vector owned{vec.GetType(), take};
      duckdb::VectorOperations::Copy(vec, owned, off + take, off, 0);
      out.emplace_back(std::move(owned), take);
    }
    off += take;
  }
}

std::vector<Chunk> StructChildChunks(ChunkSpan chunks, size_t child_index) {
  std::vector<Chunk> out;
  out.reserve(chunks.size());
  for (auto& chunk : chunks) {
    auto& entries = duckdb::StructVector::GetEntries(chunk.data);
    out.emplace_back(duckdb::Vector::Ref(entries[child_index]), chunk.Count());
  }
  return out;
}

void MarkFullyShredded(ChunkSpan vecs, PersistentColumnData& node) {
  if (vecs.empty()) {
    return;
  }
  const auto& type = vecs.front().data.GetType();
  if (type.id() != duckdb::LogicalTypeId::STRUCT) {
    return;
  }
  const auto& child_types = duckdb::StructType::GetChildTypes(type);
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
    for (size_t child = 0; child < child_types.size(); ++child) {
      auto child_chunks = StructChildChunks(vecs, child);
      MarkFullyShredded(child_chunks, node.child_columns[child]);
    }
    return;
  }
  if (untyped_value_index != child_types.size()) {
    auto untyped_chunks = StructChildChunks(vecs, untyped_value_index);
    node.fully_shredded = NoSpilledRows(untyped_chunks);
  }
  auto typed_chunks = StructChildChunks(vecs, typed_value_index);
  MarkFullyShredded(typed_chunks, node.child_columns[typed_value_index]);
}

void FlushNode(WriteContext& write_ctx, const duckdb::LogicalType& type,
               ChunkSpan chunks, duckdb::idx_t row_count, uint64_t row_start,
               PersistentColumnData& node, bool skip_validity,
               duckdb::CompressionType forced) {
  const auto validity_type =
    duckdb::LogicalType(duckdb::LogicalTypeId::VALIDITY);
  switch (type.id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, chunks, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      if (node.child_columns.empty()) {
        node.child_columns.emplace_back();
        node.child_columns.front().type = duckdb::ArrayType::GetChildType(type);
      }
      const auto array_size =
        static_cast<duckdb::idx_t>(duckdb::ArrayType::GetSize(type));
      std::vector<Chunk> child_chunks;
      for (auto& chunk : chunks) {
        PushSlicedChunks(child_chunks,
                         duckdb::ArrayVector::GetChildMutable(chunk.data),
                         chunk.Count() * array_size);
      }
      FlushNode(write_ctx, duckdb::ArrayType::GetChildType(type), child_chunks,
                row_count * array_size, row_start * array_size,
                node.child_columns.front(), false, forced);
      return;
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, chunks, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      duckdb::Vector offsets{duckdb::LogicalType::UBIGINT, row_count};
      auto* op = duckdb::FlatVector::GetDataMutable<uint64_t>(offsets);
      uint64_t running = node.list_global_running;
      duckdb::idx_t out_row = 0;
      std::vector<Chunk> child_chunks;
      for (auto& chunk : chunks) {
        const auto* entries =
          duckdb::FlatVector::GetData<duckdb::list_entry_t>(chunk.data);
        const auto& parent_validity = duckdb::FlatVector::Validity(chunk.data);
        uint64_t chunk_elems = 0;
        for (duckdb::idx_t i = 0; i < chunk.Count(); ++i) {
          if (parent_validity.RowIsValid(i)) {
            running += entries[i].length;
            chunk_elems += entries[i].length;
          }
          op[out_row++] = running;
        }
        PushSlicedChunks(child_chunks,
                         duckdb::ListVector::GetChildMutable(chunk.data),
                         chunk_elems);
      }
      const uint64_t total_elems = running - node.list_global_running;
      node.list_global_running = running;
      std::vector<Chunk> offset_chunks;
      PushSlicedChunks(offset_chunks, offsets, row_count);
      CompressColumn(write_ctx, duckdb::LogicalType::UBIGINT, offset_chunks,
                     row_count, row_start, node.pointers,
                     duckdb::CompressionType::COMPRESSION_AUTO);
      if (node.child_columns.empty()) {
        node.child_columns.emplace_back();
        node.child_columns.front().type = duckdb::ListType::GetChildType(type);
      }
      FlushNode(write_ctx, duckdb::ListType::GetChildType(type), child_chunks,
                static_cast<duckdb::idx_t>(total_elems), 0,
                node.child_columns.front(), false, forced);
      return;
    }
    case duckdb::LogicalTypeId::VARIANT: {
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, chunks, row_count, row_start,
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
          for (auto& chunk : chunks) {
            stats.Update(chunk.data, chunk.Count());
          }
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
        FlushNode(write_ctx, layout.unshredded->type, chunks, row_count, 0,
                  *layout.unshredded, true, forced);
        return;
      }

      std::vector<duckdb::Vector> shredded_hold;
      shredded_hold.reserve(chunks.size());
      std::vector<Chunk> unshredded_chunks;
      std::vector<Chunk> shredded_chunks;
      unshredded_chunks.reserve(chunks.size());
      shredded_chunks.reserve(chunks.size());
      uint64_t unshredded_valid = 0;
      for (auto& chunk : chunks) {
        auto& shredded_out =
          shredded_hold.emplace_back(shredded_type, chunk.Count());
        duckdb::VariantColumnData::ShredVariantData(chunk.data, shredded_out,
                                                    chunk.Count());
        auto& shred_entries = duckdb::StructVector::GetEntries(shredded_out);
        SDB_ASSERT(shred_entries.size() == 2);
        unshredded_valid += duckdb::FlatVector::Validity(shred_entries[0])
                              .CountValid(chunk.Count());
        unshredded_chunks.emplace_back(duckdb::Vector::Ref(shred_entries[0]),
                                       chunk.Count());
        shredded_chunks.emplace_back(duckdb::Vector::Ref(shred_entries[1]),
                                     chunk.Count());
      }

      layout.shred_state = unshredded_valid == 0 ? VariantShredState::Full
                                                 : VariantShredState::Partial;
      layout.shredded_node = std::make_unique<PersistentColumnData>();
      const auto& shredded_children =
        duckdb::StructType::GetChildTypes(shredded_type);
      layout.unshredded->type = shredded_children[0].second;
      layout.shredded_node->type = shredded_children[1].second;
      FlushNode(write_ctx, layout.unshredded->type, unshredded_chunks,
                row_count, 0, *layout.unshredded, false, forced);
      FlushNode(write_ctx, layout.shredded_node->type, shredded_chunks,
                row_count, 0, *layout.shredded_node, false, forced);
      MarkFullyShredded(shredded_chunks, *layout.shredded_node);
      return;
    }
    case duckdb::LogicalTypeId::STRUCT: {
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, chunks, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      const auto& child_types = duckdb::StructType::GetChildTypes(type);
      if (node.child_columns.size() != child_types.size()) {
        node.child_columns.clear();
        node.child_columns.resize(child_types.size());
        for (size_t i = 0; i < child_types.size(); ++i) {
          node.child_columns[i].type = child_types[i].second;
        }
      }
      for (size_t i = 0; i < child_types.size(); ++i) {
        auto child_chunks = StructChildChunks(chunks, i);
        FlushNode(write_ctx, child_types[i].second, child_chunks, row_count,
                  row_start, node.child_columns[i], false, forced);
      }
      return;
    }
    default: {
      CompressColumn(write_ctx, type, chunks, row_count, row_start,
                     node.pointers, forced);
      if (!skip_validity) {
        CompressColumn(write_ctx, validity_type, chunks, row_count, row_start,
                       node.validity_pointers,
                       duckdb::CompressionType::COMPRESSION_AUTO);
      }
      return;
    }
  }
}

}  // namespace

Chunk& ColumnWriter::OpenChunk() {
  if (_data_ctx.used_chunks != 0) {
    auto& cur = _data_ctx.chunks[_data_ctx.used_chunks - 1];
    if (cur.data.GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
        cur.Count() < duckdb::FlatVector::GetCapacity(cur.data)) {
      return cur;
    }
  }
  const size_t cap = _data_ctx.next_capacity;
  _data_ctx.next_capacity =
    std::min<size_t>(_data_ctx.next_capacity * 2, STANDARD_VECTOR_SIZE);
  const size_t idx = _data_ctx.used_chunks++;
  if (idx < _data_ctx.chunks.size()) {
    auto& slot = _data_ctx.chunks[idx];
    if (duckdb::FlatVector::GetCapacity(slot.data) != cap) {
      auto& alloc = duckdb::Allocator::Get(_write_ctx->Database());
      _data_ctx.chunk_caches[idx] = duckdb::VectorCache(alloc, _type, cap);
    }
    slot.data.ResetFromCache(_data_ctx.chunk_caches[idx]);
    duckdb::FlatVector::ValidityMutable(slot.data).Reset(cap);
    slot.SetCount(0);
    return slot;
  }
  auto& alloc = duckdb::Allocator::Get(_write_ctx->Database());
  _data_ctx.chunk_caches.emplace_back(alloc, _type, cap);
  _data_ctx.chunks.emplace_back(duckdb::Vector(_data_ctx.chunk_caches.back()),
                                0);
  return _data_ctx.chunks.back();
}

void ColumnWriter::MaybeFlushRowGroup() {
  if (_data_ctx.filled == _row_group_size) {
    FlushChunks(_row_group_size);
  }
}

void ColumnWriter::FlushChunks(uint64_t count) {
  if (_data_ctx.used_chunks == 0) {
    return;
  }
  const ChunkSpan group{_data_ctx.chunks.data(), _data_ctx.used_chunks};

  if (_hyperloglog) {
    duckdb::Vector hashes{duckdb::LogicalType::HASH, STANDARD_VECTOR_SIZE};
    for (auto& chunk : group) {
      duckdb::VectorOperations::Hash(chunk.data, hashes, chunk.Count());
      duckdb::FlatVector::SetSize(hashes, chunk.Count());
      _entry->root.hyperloglog->Update(chunk.data, hashes);
    }
  }

  FlushNode(*_write_ctx, _type, group, count, _data_ctx.first_rg_doc_id,
            _entry->root, _skip_validity, _forced_compression);

  _data_ctx.used_chunks = 0;
  _data_ctx.next_capacity = DataContext::kInitialSize;
  _data_ctx.filled -= count;
  _data_ctx.first_rg_doc_id += count;
}

}  // namespace irs
