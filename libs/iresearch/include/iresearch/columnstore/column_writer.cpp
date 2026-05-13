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
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/columnstore/column_writer.hpp"

#include <absl/strings/str_cat.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/partial_block_manager.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table/column_checkpoint_state.hpp>
#include <duckdb/storage/table/column_data_checkpointer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>
#include <utility>
#include <vector>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/internal/overflow_string_io.hpp"
#include "iresearch/columnstore/internal/persistent_column_data.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs::columnstore {

ColumnWriter::ColumnWriter(field_id id, duckdb::LogicalType type,
                           uint64_t row_group_size,
                           duckdb::DatabaseInstance& db, IndexOutput& out,
                           FooterColumnEntry& entry, bool skip_validity)
  : _id{id},
    _type{std::move(type)},
    _row_group_size{row_group_size != 0 ? row_group_size
                                        : kDefaultRowGroupSize},
    _db{&db},
    _out{&out},
    _entry{&entry},
    _staging{_type, _row_group_size,
             duckdb::VectorDataInitialization::UNINITIALIZED},
    _skip_validity{skip_validity} {}

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
    // Partial leading word: clear individual bits up to the next entry
    // boundary.
    while (i < end && (i % kBitsPerEntry) != 0) {
      mask[i / kBitsPerEntry] &= ~(static_cast<V>(1) << (i % kBitsPerEntry));
      ++i;
    }
    // Whole-word zero stores.
    while (i + kBitsPerEntry <= end) {
      mask[i / kBitsPerEntry] = 0;
      i += kBitsPerEntry;
    }
    // Trailing partial word.
    while (i < end) {
      mask[i / kBitsPerEntry] &= ~(static_cast<V>(1) << (i % kBitsPerEntry));
      ++i;
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

void ColumnWriter::AppendChunk(uint64_t start_row,
                               const duckdb::DataChunk& chunk,
                               duckdb::idx_t col_idx) {
  Append(start_row, chunk.data[col_idx], chunk.size());
}

void ColumnWriter::Finalize() {
  if (_filled > 0) {
    FlushRowGroup();
  }
}

namespace {

class CapturingCheckpointState final : public duckdb::ColumnCheckpointState {
 public:
  CapturingCheckpointState(duckdb::DatabaseInstance& db,
                           duckdb::PartialBlockManager& pbm, IndexOutput& out,
                           uint64_t row_start)
    : duckdb::ColumnCheckpointState(pbm),
      _db{db},
      _out{out},
      _running_row_start{row_start} {}

  void FlushSegment(duckdb::unique_ptr<duckdb::ColumnSegment> segment,
                    duckdb::BufferHandle handle,
                    duckdb::idx_t segment_size) override {
    // Some codecs (RLE, FSST) Destroy() the handle before this call to
    // release their pin -- the bytes still live on segment->block, so
    // re-pin in that case.
    if (segment_size == 0) {
      Capture(*segment, segment_size, nullptr);
      return;
    }
    if (handle.IsValid()) {
      Capture(*segment, segment_size,
              reinterpret_cast<const byte_type*>(handle.Ptr()));
      return;
    }
    if (!segment->block) {
      Capture(*segment, segment_size, nullptr);
      return;
    }
    auto& bm = duckdb::BufferManager::GetBufferManager(_db);
    auto repinned = bm.Pin(segment->block);
    Capture(*segment, segment_size,
            reinterpret_cast<const byte_type*>(repinned.Ptr()));
  }

  void FlushSegmentInternal(duckdb::unique_ptr<duckdb::ColumnSegment> segment,
                            duckdb::idx_t segment_size) override {
    if (segment_size == 0 || !segment->block) {
      Capture(*segment, segment_size, nullptr);
      return;
    }
    auto& bm = duckdb::BufferManager::GetBufferManager(_db);
    auto handle = bm.Pin(segment->block);
    Capture(*segment, segment_size,
            reinterpret_cast<const byte_type*>(handle.Ptr()));
  }

 private:
  void Capture(duckdb::ColumnSegment& segment, duckdb::idx_t segment_size,
               const byte_type* bytes) {
    const auto tuple_count = segment.count.load();
    if (tuple_count == 0) {
      return;
    }

    duckdb::DataPointer ptr{segment.stats.statistics.Copy()};
    ptr.row_start = _running_row_start;
    ptr.tuple_count = tuple_count;
    const auto& codec = segment.GetCompressionFunction();
    ptr.compression_type = codec.type;

    if (segment.stats.statistics.IsConstant() || !bytes || segment_size == 0) {
      // CONSTANT/EMPTY: value encoded in stats, no payload bytes. Mirror
      // DuckDB's ConvertToPersistent: on-disk codec becomes CONSTANT.
      ptr.compression_type = duckdb::CompressionType::COMPRESSION_CONSTANT;
      ptr.block_pointer.block_id = INVALID_BLOCK;
      ptr.block_pointer.offset = 0;
    } else {
      // block_pointer.block_id repurposed as the .cs file byte offset;
      // block_pointer.offset (uint32_t) carries the segment byte size.
      SDB_ASSERT(segment_size <= std::numeric_limits<uint32_t>::max(),
                 "columnstore segment > 4GB; offset field too narrow");
      const uint64_t file_offset = _out.Position();
      _out.WriteBytes(bytes, segment_size);
      ptr.block_pointer.block_id = static_cast<duckdb::block_id_t>(file_offset);
      ptr.block_pointer.offset = static_cast<uint32_t>(segment_size);
    }

    if (codec.serialize_state) {
      ptr.segment_state = codec.serialize_state(segment);
    }
    _running_row_start += tuple_count;
    data_pointers.push_back(std::move(ptr));
  }

  duckdb::DatabaseInstance& _db;
  IndexOutput& _out;
  uint64_t _running_row_start;
};

void CopySlice(duckdb::Vector& dst, duckdb::Vector& src, duckdb::idx_t offset,
               duckdb::idx_t count) {
  duckdb::VectorOperations::Copy(src, dst, offset + count, offset, 0);
}

struct PickedCodec {
  duckdb::optional_ptr<const duckdb::CompressionFunction> function;
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
};

PickedCodec PickCodec(duckdb::DatabaseInstance& db,
                      const duckdb::LogicalType& codec_type,
                      duckdb::Vector& staging, duckdb::idx_t row_count,
                      duckdb::CompressionType forced) {
  auto& bm = duckdb::BufferManager::GetBufferManager(db);
  auto& block_manager = bm.GetTemporaryBlockManager();
  const auto& config = duckdb::DBConfig::GetConfig(db);

  std::vector<duckdb::reference<const duckdb::CompressionFunction>> candidates;
  if (forced != duckdb::CompressionType::COMPRESSION_AUTO) {
    auto fn =
      config.TryGetCompressionFunction(forced, codec_type.InternalType());
    if (!fn || !fn->init_analyze) {
      SDB_THROW(sdb::ERROR_BAD_PARAMETER, "columnstore: compression '",
                duckdb::CompressionTypeToString(forced),
                "' is not supported for type ", codec_type.ToString());
    }
    candidates.emplace_back(*fn);
  } else {
    candidates = config.GetCompressionFunctions(codec_type.InternalType());
  }

  duckdb::CompressionAnalyzeContext ctx{block_manager, db,
                                        duckdb::VERSION_NUMBER_UPPER};
  std::vector<duckdb::unique_ptr<duckdb::AnalyzeState>> states(
    candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (auto* init_analyze = candidates[i].get().init_analyze) {
      states[i] = init_analyze(ctx, codec_type.InternalType());
    }
  }

  duckdb::Vector slice{staging.GetType(), STANDARD_VECTOR_SIZE};
  duckdb::idx_t consumed = 0;
  while (consumed < row_count) {
    const auto take =
      std::min<duckdb::idx_t>(row_count - consumed, STANDARD_VECTOR_SIZE);
    CopySlice(slice, staging, consumed, take);
    for (size_t i = 0; i < candidates.size(); ++i) {
      if (states[i] && !candidates[i].get().analyze(*states[i], slice, take)) {
        states[i].reset();
      }
    }
    consumed += take;
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
                "columnstore: no codec accepted the row group for type ",
                codec_type.ToString());
    }
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "columnstore: forced compression '",
              duckdb::CompressionTypeToString(forced),
              "' could not produce a plan for type ", codec_type.ToString());
  }
  return best;
}

void CompressColumn(duckdb::DatabaseInstance& db,
                    duckdb::PartialBlockManager& pbm, IndexOutput& out,
                    const duckdb::LogicalType& codec_type,
                    duckdb::Vector& staging, duckdb::idx_t row_count,
                    uint64_t row_start, std::vector<duckdb::DataPointer>& sink,
                    duckdb::CompressionType forced) {
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

  auto picked = PickCodec(db, codec_type, staging, row_count, forced);

  CapturingCheckpointState state{db, pbm, out, row_start};
  state.global_stats =
    duckdb::BaseStatistics::CreateEmpty(codec_type).ToUnique();

  duckdb::ColumnDataCheckpointData ckp{
    state, codec_type, db,
    /* matches duckdb::DEFAULT_STORAGE_VERSION_INFO */ 64};

  // VARCHAR UNCOMPRESSED spills long strings via OverflowStringWriter.
  // DuckDB's default writer wants a real .db file; install ours backed by
  // the .cs IndexOutput. Other VARCHAR codecs ignore this hook.
  if (codec_type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    ckp.SetOverflowStringWriterFactory(
      [&out]() { return duckdb::make_uniq<IndexOutputOverflowWriter>(out); });
  }

  auto comp_state =
    picked.function->init_compression(ckp, std::move(picked.state));

  duckdb::Vector slice{staging.GetType(), STANDARD_VECTOR_SIZE};
  duckdb::idx_t consumed = 0;
  while (consumed < row_count) {
    const auto take =
      std::min<duckdb::idx_t>(row_count - consumed, STANDARD_VECTOR_SIZE);
    CopySlice(slice, staging, consumed, take);
    picked.function->compress(*comp_state, slice, take);
    consumed += take;
  }
  picked.function->compress_finalize(*comp_state);

  for (auto& dp : state.data_pointers) {
    sink.push_back(std::move(dp));
  }
}

void FlushNode(duckdb::DatabaseInstance& db, duckdb::PartialBlockManager& pbm,
               IndexOutput& out, const duckdb::LogicalType& type,
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
        CompressColumn(db, pbm, out, validity_type, vec, row_count, row_start,
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
      FlushNode(db, pbm, out, duckdb::ArrayType::GetChildType(type), child,
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
        CompressColumn(db, pbm, out, validity_type, vec, row_count, row_start,
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
      CompressColumn(db, pbm, out, offsets_type, offsets, row_count, row_start,
                     node.pointers, duckdb::CompressionType::COMPRESSION_AUTO);
      if (node.child_columns.empty()) {
        node.child_columns.emplace_back();
        node.child_columns.front().type = duckdb::ListType::GetChildType(type);
      }
      auto& child = duckdb::ListVector::GetChildMutable(vec);
      FlushNode(db, pbm, out, duckdb::ListType::GetChildType(type), child,
                static_cast<duckdb::idx_t>(total_elems),
                /*row_start=*/0, node.child_columns.front(),
                /*skip_validity=*/false, forced);
      return;
    }
    case duckdb::LogicalTypeId::STRUCT: {
      // STRUCT has no top-level data of its own -- just parent validity and
      // per-field children. Matches duckdb::StructColumnData::Append.
      if (!skip_validity) {
        CompressColumn(db, pbm, out, validity_type, vec, row_count, row_start,
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
        FlushNode(db, pbm, out, child_types[i].second, entries[i], row_count,
                  row_start, node.child_columns[i],
                  /*skip_validity=*/false, forced);
      }
      return;
    }
    default: {
      CompressColumn(db, pbm, out, type, vec, row_count, row_start,
                     node.pointers, forced);
      if (!skip_validity) {
        CompressColumn(db, pbm, out, validity_type, vec, row_count, row_start,
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

  auto& bm = duckdb::BufferManager::GetBufferManager(*_db);
  duckdb::PartialBlockManager pbm{
    duckdb::QueryContext{}, bm.GetTemporaryBlockManager(),
    duckdb::PartialBlockType::IN_MEMORY_CHECKPOINT};

  FlushNode(*_db, pbm, *_out, _type, _staging, _filled, _row_group_first_doc,
            _entry->root, _skip_validity, _forced_compression);

  // Without this the PBM dtor leaves BlockHandles with an inconsistent
  // memory charge and the BlockHandle dtor asserts.
  pbm.ClearBlocks();

  _row_group_first_doc += _filled;
  _filled = 0;
  _staging.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                      _row_group_size);
}

}  // namespace irs::columnstore
