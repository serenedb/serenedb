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

#include <algorithm>
#include <cstring>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/function/variant/variant_shredding.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/table/column_data_checkpointer.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <limits>
#include <memory>
#include <utility>

#include "basics/assert.h"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/internal/overflow_string_io.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

constexpr auto kStorageVersion = duckdb::StorageVersion::V2_0_0;

void CaptureSegment(duckdb::ColumnSegment& segment, duckdb::idx_t segment_size,
                    const uint8_t* bytes, IndexOutput& out,
                    std::vector<ColumnBlockMeta>& sink) {
  const auto tuple_count = segment.count.load();
  if (tuple_count == 0) {
    return;
  }
  ColumnBlockMeta m{segment.GetStats().Copy()};
  m.tuple_count = tuple_count;
  m.codec = &segment.GetCompressionFunction();
  if (!bytes || segment_size == 0) {
    m.file_offset = 0;
    m.byte_size = 0;
  } else {
    SDB_ASSERT(segment_size <= std::numeric_limits<uint32_t>::max());
    if (const uint64_t misalign = out.Position() % 8; misalign != 0) {
      static constexpr byte_type kPad[8]{};
      out.WriteData(kPad, 8 - misalign);
    }
    m.file_offset = out.Position();
    out.WriteData(reinterpret_cast<const byte_type*>(bytes), segment_size);
    m.byte_size = segment_size;
  }
  sink.push_back(std::move(m));
}

// Slices vec[base, base+count) into <=STANDARD_VECTOR_SIZE write chunks. `base`
// is the child-element offset the chunk starts at: a nested collection's child
// is shared across sliced parent views (list entries keep absolute offsets), so
// the recursion must serialize from the view's real base, not 0.
void SliceChunks(std::vector<WriteChunk>& out, duckdb::Vector& vec,
                 duckdb::idx_t base, duckdb::idx_t count) {
  duckdb::idx_t off = 0;
  while (off < count) {
    const auto take =
      std::min<duckdb::idx_t>(count - off, STANDARD_VECTOR_SIZE);
    if (base == 0 && off == 0 && take == count) {
      auto v = duckdb::Vector::Ref(vec);
      duckdb::FlatVector::SetSize(v, take);
      out.push_back(WriteChunk{std::move(v), take});
    } else {
      duckdb::Vector view{vec, base + off, base + off + take};
      out.push_back(WriteChunk{std::move(view), take});
    }
    off += take;
  }
}

bool VariantShreddingEnabled(int64_t minimum_size, uint64_t row_count) {
  if (minimum_size == -1) {
    return false;
  }
  return row_count >= static_cast<uint64_t>(minimum_size);
}

void EmitEmptyValidity(const duckdb::LogicalType& validity_type,
                       uint64_t row_count, duckdb::DBConfig& cfg,
                       std::vector<ColumnBlockMeta>& sink) {
  ColumnBlockMeta m{duckdb::BaseStatistics::CreateEmpty(validity_type)};
  m.tuple_count = row_count;
  m.codec =
    cfg
      .TryGetCompressionFunction(duckdb::CompressionType::COMPRESSION_EMPTY,
                                 validity_type.InternalType())
      .get();
  sink.push_back(std::move(m));
}

}  // namespace

WriteContext& ColumnWriter::WriteCtx() const noexcept {
  return _owner->WriteCtx();
}

IndexOutput& ColumnWriter::Out() const noexcept { return _owner->Out(); }

duckdb::optional_ptr<const duckdb::CompressionFunction> ColumnWriter::PickCodec(
  const duckdb::LogicalType& codec_type, std::span<WriteChunk> chunks,
  duckdb::CompressionType forced,
  duckdb::unique_ptr<duckdb::AnalyzeState>& out_state) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  const auto& config = duckdb::DBConfig::GetConfig(db);

  std::vector<duckdb::reference<const duckdb::CompressionFunction>> candidates =
    config.GetCompressionFunctions(codec_type.InternalType());

  auto forced_method = forced;
  if (forced_method != duckdb::CompressionType::COMPRESSION_AUTO) {
    const bool available = std::ranges::any_of(
      candidates, [&](const auto& f) { return f.get().type == forced_method; });
    if (available) {
      std::erase_if(candidates, [&](const auto& f) {
        const auto t = f.get().type;
        return t != forced_method &&
               t != duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
      });
    } else {
      forced_method = duckdb::CompressionType::COMPRESSION_AUTO;
    }
  }

  duckdb::CompressionAnalyzeContext actx{ctx, db, kStorageVersion};
  std::vector<duckdb::unique_ptr<duckdb::AnalyzeState>> states(
    candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (auto* init_analyze = candidates[i].get().init_analyze) {
      states[i] = init_analyze(actx, codec_type.InternalType());
    }
  }
  for (auto& c : chunks) {
    for (size_t i = 0; i < candidates.size(); ++i) {
      if (states[i] && !candidates[i].get().analyze(*states[i], c.data)) {
        states[i].reset();
      }
    }
  }

  duckdb::optional_ptr<const duckdb::CompressionFunction> best;
  auto best_score = std::numeric_limits<duckdb::idx_t>::max();
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!states[i]) {
      continue;
    }
    const auto score = candidates[i].get().final_analyze(*states[i]);
    if (score == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const bool forced_found = candidates[i].get().type == forced_method;
    if (score < best_score || forced_found) {
      best_score = score;
      best = &candidates[i].get();
      out_state = std::move(states[i]);
    }
    if (forced_found) {
      break;
    }
  }
  SDB_ENSURE(best, "column writer: no codec accepted the row group for ",
             codec_type.ToString());
  return best;
}

void ColumnWriter::Compress(const duckdb::CompressionFunction& picked,
                            duckdb::unique_ptr<duckdb::AnalyzeState> state,
                            const duckdb::LogicalType& codec_type,
                            std::span<WriteChunk> chunks,
                            std::vector<ColumnBlockMeta>& sink) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  auto& out = Out();
  auto& bm = db.GetBufferManager();

  duckdb::ColumnDataCheckpointData::OverflowStringWriterFactory
    overflow_factory;
  if (codec_type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    overflow_factory = [&out]() {
      return duckdb::make_uniq<IndexOutputOverflowWriter>(out);
    };
  }

  auto capture = [&](duckdb::ColumnSegment& seg, duckdb::idx_t size,
                     const uint8_t* bytes) {
    CaptureSegment(seg, size, bytes, out, sink);
  };
  auto capture_from_block = [&](duckdb::ColumnSegment& seg,
                                duckdb::idx_t size) {
    if (size == 0 || !seg.GetBlockHandle()) {
      capture(seg, size, nullptr);
    } else {
      auto pin = bm.Pin(seg.GetBlockHandle());
      capture(seg, size, reinterpret_cast<const uint8_t*>(pin.Ptr()));
    }
  };
  auto flush_fn = [&](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
                      duckdb::BufferHandle handle, duckdb::idx_t size) {
    if (size != 0 && handle.IsValid()) {
      capture(*seg, size, reinterpret_cast<const uint8_t*>(handle.Ptr()));
    } else {
      capture_from_block(*seg, size);
    }
  };
  auto flush_internal_fn = [&](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
                               duckdb::idx_t size) {
    capture_from_block(*seg, size);
  };

  duckdb::ColumnDataCheckpointData ckp{
    codec_type,
    db,
    kStorageVersion,
    std::move(overflow_factory),
    std::move(flush_fn),
    std::move(flush_internal_fn),
    ctx,
  };
  auto comp_state = picked.init_compression(ckp, std::move(state));
  for (auto& c : chunks) {
    picked.compress(*comp_state, c.data);
  }
  picked.compress_finalize(*comp_state);
}

void ColumnWriter::SealValidity(std::span<WriteChunk> chunks,
                                uint64_t row_count,
                                std::vector<ColumnBlockMeta>& sink) {
  const duckdb::LogicalType validity_type{duckdb::LogicalTypeId::VALIDITY};
  uint64_t valid = 0;
  for (auto& c : chunks) {
    valid += duckdb::FlatVector::Validity(c.data).CountValid(c.count);
  }
  if (valid == row_count) {
    EmitEmptyValidity(validity_type, row_count,
                      duckdb::DBConfig::GetConfig(WriteCtx().Database()), sink);
    return;
  }
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  auto fn = PickCodec(validity_type, chunks,
                      duckdb::CompressionType::COMPRESSION_AUTO, state);
  Compress(*fn, std::move(state), validity_type, chunks, sink);
}

void ColumnWriter::SealNestedValidity(std::span<WriteChunk> chunks,
                                      uint64_t row_count, bool skip_validity,
                                      size_t child_count, ColumnMeta& meta) {
  if (!skip_validity) {
    SealValidity(chunks, row_count, meta.validity);
  }
  meta.children.resize(child_count);
}

void ColumnWriter::SealStruct(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks, uint64_t row_count,
                              bool skip_validity,
                              duckdb::CompressionType forced,
                              ColumnMeta& meta) {
  const auto& child_types = duckdb::StructType::GetChildTypes(type);
  SealNestedValidity(chunks, row_count, skip_validity, child_types.size(),
                     meta);
  for (size_t i = 0; i < child_types.size(); ++i) {
    std::vector<WriteChunk> field_chunks;
    field_chunks.reserve(chunks.size());
    for (auto& c : chunks) {
      auto& entries = duckdb::StructVector::GetEntries(c.data);
      auto fv = duckdb::Vector::Ref(entries[i]);
      duckdb::FlatVector::SetSize(fv, c.count);
      field_chunks.push_back(WriteChunk{std::move(fv), c.count});
    }
    SealColumn(child_types[i].second, field_chunks, row_count,
               /*skip_validity=*/false, forced, meta.children[i]);
  }
}

void ColumnWriter::SealArray(const duckdb::LogicalType& type,
                             std::span<WriteChunk> chunks, uint64_t row_count,
                             bool skip_validity, duckdb::CompressionType forced,
                             ColumnMeta& meta) {
  SealNestedValidity(chunks, row_count, skip_validity, 1, meta);
  const auto array_size =
    static_cast<uint64_t>(duckdb::ArrayType::GetSize(type));
  std::vector<WriteChunk> elem_chunks;
  for (auto& c : chunks) {
    auto& child = duckdb::ArrayVector::GetChildMutable(c.data);
    // Array children are positional (no per-row offset), so a sliced parent
    // view materialises them at 0 rather than sharing with a base offset.
    SliceChunks(elem_chunks, child, 0, c.count * array_size);
  }
  SealColumn(duckdb::ArrayType::GetChildType(type), elem_chunks,
             row_count * array_size, /*skip_validity=*/false, forced,
             meta.children[0]);
}

void ColumnWriter::SealList(const duckdb::LogicalType& type,
                            std::span<WriteChunk> chunks, uint64_t row_count,
                            bool skip_validity, duckdb::CompressionType forced,
                            ColumnMeta& meta) {
  SealNestedValidity(chunks, row_count, skip_validity, 1, meta);

  std::vector<WriteChunk> offset_chunks;
  uint64_t* op = nullptr;
  duckdb::idx_t chunk_row = 0;
  auto open_offsets = [&] {
    offset_chunks.push_back(WriteChunk{
      duckdb::Vector{duckdb::LogicalType::UBIGINT, STANDARD_VECTOR_SIZE}, 0});
    op =
      duckdb::FlatVector::GetDataMutable<uint64_t>(offset_chunks.back().data);
    chunk_row = 0;
  };
  auto seal_offsets = [&] {
    auto& back = offset_chunks.back();
    back.count = chunk_row;
    duckdb::FlatVector::SetSize(back.data, chunk_row);
  };

  uint64_t running = meta.write_list_running;
  const uint64_t elem_base = running;
  std::vector<WriteChunk> elem_chunks;
  open_offsets();
  for (auto& c : chunks) {
    const auto* entries =
      duckdb::FlatVector::GetData<duckdb::list_entry_t>(c.data);
    const auto& parent_validity = duckdb::FlatVector::Validity(c.data);
    auto& child = duckdb::ListVector::GetChildMutable(c.data);
    // A sliced parent view shares the original child with absolute list
    // offsets, so this chunk's elements start at the first valid row's child
    // offset (a null row's list_entry is undefined), not 0.
    uint64_t child_base = 0;
    bool have_child_base = false;
    uint64_t chunk_elems = 0;
    for (duckdb::idx_t i = 0; i < c.count; ++i) {
      if (parent_validity.RowIsValid(i)) {
        if (!have_child_base) {
          child_base = entries[i].offset;
          have_child_base = true;
        }
        running += entries[i].length;
        chunk_elems += entries[i].length;
      }
      op[chunk_row++] = running;
      if (chunk_row == duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
        seal_offsets();
        open_offsets();
      }
    }
    SliceChunks(elem_chunks, child, static_cast<duckdb::idx_t>(child_base),
                static_cast<duckdb::idx_t>(chunk_elems));
  }
  seal_offsets();
  if (offset_chunks.back().count == 0) {
    offset_chunks.pop_back();
  }
  const uint64_t total_elems = running - elem_base;
  meta.write_list_running = running;

  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  auto fn = PickCodec(duckdb::LogicalType::UBIGINT, offset_chunks,
                      duckdb::CompressionType::COMPRESSION_AUTO, state);
  Compress(*fn, std::move(state), duckdb::LogicalType::UBIGINT, offset_chunks,
           meta.data);

  SealColumn(duckdb::ListType::GetChildType(type), elem_chunks, total_elems,
             /*skip_validity=*/false, forced, meta.children[0]);
}

void ColumnWriter::SealVariant(const duckdb::LogicalType& type,
                               std::span<WriteChunk> chunks, uint64_t row_count,
                               bool skip_validity,
                               duckdb::CompressionType forced,
                               ColumnMeta& meta) {
  if (!skip_validity) {
    SealValidity(chunks, row_count, meta.validity);
  }

  bool should_shred =
    VariantShreddingEnabled(_variant_min_shred_size, row_count);

  duckdb::LogicalType shredded_type;
  if (should_shred) {
    if (_force_variant_shredding.id() != duckdb::LogicalTypeId::INVALID) {
      shredded_type = _force_variant_shredding;
    } else {
      duckdb::VariantShreddingStats stats;
      for (auto& c : chunks) {
        stats.Update(c.data, c.count);
      }
      shredded_type = stats.GetShreddedType();
    }
    if (shredded_type.id() != duckdb::LogicalTypeId::STRUCT ||
        duckdb::StructType::GetChildCount(shredded_type) != 2) {
      should_shred = false;
    }
  }

  meta.variant_rgs.emplace_back();
  auto& layout = meta.variant_rgs.back();
  layout.row_count = row_count;
  layout.unshredded = std::make_unique<ColumnMeta>();

  if (!should_shred) {
    layout.unshredded->id = _id;
    layout.unshredded->type = duckdb::VariantShredding::GetUnshreddedType();
    SealColumn(layout.unshredded->type, chunks, row_count,
               /*skip_validity=*/true, forced, *layout.unshredded);
    return;
  }

  std::vector<duckdb::Vector> shredded_hold;
  shredded_hold.reserve(chunks.size());
  std::vector<WriteChunk> unshredded_chunks;
  std::vector<WriteChunk> shredded_chunks;
  unshredded_chunks.reserve(chunks.size());
  shredded_chunks.reserve(chunks.size());
  for (auto& c : chunks) {
    auto& shredded_out = shredded_hold.emplace_back(shredded_type, c.count);
    duckdb::VariantColumnData::ShredVariantData(c.data, shredded_out, c.count);
    auto& shred_entries = duckdb::StructVector::GetEntries(shredded_out);
    SDB_ASSERT(shred_entries.size() == 2);
    auto u = duckdb::Vector::Ref(shred_entries[0]);
    duckdb::FlatVector::SetSize(u, c.count);
    unshredded_chunks.push_back(WriteChunk{std::move(u), c.count});
    auto sh = duckdb::Vector::Ref(shred_entries[1]);
    duckdb::FlatVector::SetSize(sh, c.count);
    shredded_chunks.push_back(WriteChunk{std::move(sh), c.count});
  }

  layout.shredded = std::make_unique<ColumnMeta>();
  const auto& shredded_children =
    duckdb::StructType::GetChildTypes(shredded_type);
  layout.unshredded->id = _id;
  layout.shredded->id = _id;
  layout.unshredded->type = shredded_children[0].second;
  layout.shredded->type = shredded_children[1].second;
  SealColumn(layout.unshredded->type, unshredded_chunks, row_count,
             /*skip_validity=*/false, forced, *layout.unshredded);
  SealColumn(layout.shredded->type, shredded_chunks, row_count,
             /*skip_validity=*/false, forced, *layout.shredded);
}

void ColumnWriter::SealColumn(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks, uint64_t row_count,
                              bool skip_validity,
                              duckdb::CompressionType forced,
                              ColumnMeta& meta) {
  if (meta.type.id() == duckdb::LogicalTypeId::INVALID) {
    meta.id = _id;
    meta.type = type;
  }

  if (type.id() == duckdb::LogicalTypeId::VARIANT) {
    SealVariant(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::STRUCT) {
    SealStruct(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::ARRAY) {
    SealArray(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::MAP) {
    SealList(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }

  duckdb::unique_ptr<duckdb::AnalyzeState> data_state;
  auto data_fn = PickCodec(type, chunks, forced, data_state);
  const bool nulls_covered_by_data =
    data_fn->validity == duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
  Compress(*data_fn, std::move(data_state), type, chunks, meta.data);

  if (skip_validity) {
    return;
  }
  if (nulls_covered_by_data) {
    EmitEmptyValidity(
      duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY}, row_count,
      duckdb::DBConfig::GetConfig(WriteCtx().Database()), meta.validity);
    return;
  }
  SealValidity(chunks, row_count, meta.validity);
}

ColumnWriter::ColumnWriter(ColWriter& owner, field_id id,
                           duckdb::LogicalType type, bool skip_validity,
                           uint32_t row_group_size,
                           duckdb::CompressionType forced, bool hyperloglog)
  : _owner{&owner},
    _id{id},
    _type{std::move(type)},
    _skip_validity{skip_validity},
    _row_group_size{row_group_size},
    _forced{forced} {
  const auto pt = _type.InternalType();
  _is_nested = pt == duckdb::PhysicalType::STRUCT ||
               pt == duckdb::PhysicalType::LIST ||
               pt == duckdb::PhysicalType::ARRAY;
  if (_type.id() == duckdb::LogicalTypeId::VARIANT) {
    const auto& config = duckdb::DBConfig::GetConfig(WriteCtx().Database());
    _variant_min_shred_size =
      duckdb::Settings::Get<duckdb::VariantMinimumShreddingSizeSetting>(config);
    _force_variant_shredding = config.options.force_variant_shredding;
  }
  _meta.id = _id;
  _meta.type = _type;
  if (hyperloglog) {
    _meta.hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    _hll_auto = true;
  }
}

WriteChunk& ColumnWriter::OpenChunk() {
  if (_staged_chunks != 0 &&
      _staged[_staged_chunks - 1].count < duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
    return _staged[_staged_chunks - 1];
  }
  if (_staged_chunks == _staged.size()) {
    auto& alloc = duckdb::Allocator::Get(WriteCtx().Database());
    auto& cache =
      _staged_caches.emplace_back(alloc, _type, STANDARD_VECTOR_SIZE);
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

void ColumnWriter::AppendDense(const duckdb::Vector& vec, duckdb::idx_t count) {
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min(
      {count - off, duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    duckdb::VectorOperations::Copy(vec, back.data, off + take,
                                   /*source_offset=*/off,
                                   /*target_offset=*/back.count);
    back.count += take;
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::Append(const duckdb::Vector& vec, duckdb::idx_t count) {
  AppendDense(vec, count);
}

void ColumnWriter::Append(uint64_t start_row, const duckdb::Vector& vec,
                          duckdb::idx_t count) {
  PadNullsTo(start_row);
  AppendDense(vec, count);
}

void ColumnWriter::PadNestedNulls(uint64_t count) {
  duckdb::idx_t off = 0;
  while (off < count) {
    auto& back = OpenChunk();
    const auto rg_room =
      static_cast<duckdb::idx_t>(_row_group_size - _staged_rows);
    const auto take = std::min<duckdb::idx_t>(
      {static_cast<duckdb::idx_t>(count - off),
       duckdb::idx_t{STANDARD_VECTOR_SIZE} - back.count, rg_room});
    auto& validity = duckdb::FlatVector::ValidityMutable(back.data);
    validity.EnsureWritable();
    for (duckdb::idx_t i = 0; i < take; ++i) {
      validity.SetInvalidUnsafe(back.count + i);
    }
    back.count += take;
    duckdb::FlatVector::SetSize(back.data, back.count);
    _staged_rows += take;
    off += take;
    if (_staged_rows == _row_group_size) {
      SealRowGroup();
    }
  }
}

void ColumnWriter::PadNullsTo(uint64_t target_row) {
  uint64_t current = _row_start + _staged_rows;
  if (current >= target_row) {
    return;
  }
  if (_is_nested) {
    PadNestedNulls(target_row - current);
    return;
  }
  if (!_null_pad) {
    _null_pad = std::make_unique<duckdb::Vector>(_type, STANDARD_VECTOR_SIZE);
    _null_pad->SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    const auto pt = _type.InternalType();
    if (pt == duckdb::PhysicalType::VARCHAR) {
      std::memset(
        duckdb::FlatVector::GetDataMutable(*_null_pad), 0,
        static_cast<size_t>(STANDARD_VECTOR_SIZE) * sizeof(duckdb::string_t));
    } else if (duckdb::TypeIsConstantSize(pt)) {
      std::memset(
        duckdb::FlatVector::GetDataMutable(*_null_pad), 0,
        static_cast<size_t>(STANDARD_VECTOR_SIZE) * duckdb::GetTypeIdSize(pt));
    }
    duckdb::FlatVector::ValidityMutable(*_null_pad)
      .SetAllInvalid(STANDARD_VECTOR_SIZE);
  }
  while (current < target_row) {
    const auto n =
      std::min<uint64_t>(target_row - current, STANDARD_VECTOR_SIZE);
    duckdb::FlatVector::SetSize(*_null_pad, static_cast<duckdb::idx_t>(n));
    AppendDense(*_null_pad, static_cast<duckdb::idx_t>(n));
    current += n;
  }
}

void ColumnWriter::SealRowGroup() {
  if (_staged_rows == 0) {
    return;
  }
  std::span<WriteChunk> chunks{_staged.data(), _staged_chunks};
  if (_hll_auto && _meta.hyperloglog) {
    if (!_hll_hashes.GetBufferRef()) {
      _hll_hashes.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                             STANDARD_VECTOR_SIZE);
    }
    for (auto& chunk : chunks) {
      duckdb::VectorOperations::Hash(chunk.data, _hll_hashes, chunk.count);
      duckdb::FlatVector::SetSize(_hll_hashes, chunk.count);
      _meta.hyperloglog->Update(chunk.data, _hll_hashes);
    }
  }
  SealColumn(_type, chunks, _staged_rows, _skip_validity, _forced, _meta);
  _row_start += _staged_rows;
  _staged_chunks = 0;
  _staged_rows = 0;
}

void ColumnWriter::SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll) {
  _meta.hyperloglog = std::move(hll);
  _hll_auto = false;
}

}  // namespace irs
