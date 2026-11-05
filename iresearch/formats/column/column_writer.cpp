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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
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
#include <string_view>
#include <utility>

#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/string_writer.hpp"
#include "iresearch/formats/column/immutable_payloads.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void CaptureBlock(duckdb::DatabaseInstance& db,
                  const duckdb::CompressionFunction& codec,
                  duckdb::BaseStatistics stats, uint64_t tuple_count,
                  std::span<const std::string_view> parts,
                  ColFileWriter& blocks, std::vector<ColumnBlockMeta>& sink) {
  if (tuple_count == 0) {
    return;
  }
  ColumnBlockMeta m{
    .statistics = std::move(stats),
    .tuple_count = tuple_count,
    .codec = &codec,
  };
  if (m.statistics.IsConstant()) {
    auto& cfg = duckdb::DBConfig::GetConfig(db);
    if (auto fn = cfg.TryGetCompressionFunction(
          duckdb::CompressionType::COMPRESSION_CONSTANT,
          m.statistics.GetType().InternalType())) {
      m.codec = fn.get();
      sink.push_back(std::move(m));
      return;
    }
  }
  uint64_t size = 0;
  for (const auto part : parts) {
    size += part.size();
  }
  if (size != 0) {
    SDB_ASSERT(size <= std::numeric_limits<uint32_t>::max());
    m.file_offset = blocks.AppendAligned(parts);
    m.byte_size = size;
  }
  sink.push_back(std::move(m));
}

void CertifyStaged(duckdb::Vector& vector) {
  switch (vector.GetType().InternalType()) {
    case duckdb::PhysicalType::VARCHAR:
      duckdb::StringVector::AddAuxiliaryData(
        vector, std::make_unique<ImmutablePayloads>());
      return;
    case duckdb::PhysicalType::STRUCT:
      for (auto& child : duckdb::StructVector::GetEntries(vector)) {
        CertifyStaged(child);
      }
      return;
    case duckdb::PhysicalType::LIST:
      CertifyStaged(duckdb::ListVector::GetChildMutable(vector));
      return;
    default:
      return;
  }
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

duckdb::CompressionType CodesCodec(const ListParts& parts,
                                   uint64_t rows) noexcept {
  const uint64_t width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(
      parts.distinct == 0 ? 0 : parts.distinct - 1);
  const uint64_t run_width =
    duckdb::BitpackingPrimitives::MinimumBitWidth<uint64_t>(parts.longest_run);
  const uint64_t bitpacked = rows * width;
  const uint64_t runs = parts.runs * (width + run_width);
  return runs < bitpacked ? duckdb::CompressionType::COMPRESSION_RLE
                          : duckdb::CompressionType::COMPRESSION_BITPACKING;
}

duckdb::CompressionType ForcedMethod(duckdb::DatabaseInstance& db,
                                     duckdb::CompressionType forced) {
  if (forced != duckdb::CompressionType::COMPRESSION_AUTO) {
    return forced;
  }
  return duckdb::Settings::Get<duckdb::ForceCompressionSetting>(
    duckdb::DBConfig::GetConfig(db));
}

bool Applies(duckdb::DatabaseInstance& db, duckdb::CompressionType method,
             duckdb::PhysicalType physical) {
  return method != duckdb::CompressionType::COMPRESSION_AUTO &&
         duckdb::DBConfig::GetConfig(db).TryGetCompressionFunction(method,
                                                                   physical);
}

}  // namespace

void EmitEmptyValidity(duckdb::DatabaseInstance& db, uint64_t rows,
                       std::vector<ColumnBlockMeta>& sink) {
  const duckdb::LogicalType type{duckdb::LogicalTypeId::VALIDITY};
  ColumnBlockMeta m{duckdb::BaseStatistics::CreateEmpty(type)};
  m.tuple_count = rows;
  m.codec = duckdb::DBConfig::GetConfig(db)
              .TryGetCompressionFunction(
                duckdb::CompressionType::COMPRESSION_EMPTY, type.InternalType())
              .get();
  sink.push_back(std::move(m));
}

duckdb::optional_ptr<const duckdb::CompressionFunction> ColumnWriter::PickCodec(
  const duckdb::LogicalType& codec_type, std::span<WriteChunk> chunks,
  duckdb::CompressionType forced,
  duckdb::unique_ptr<duckdb::AnalyzeState>& out_state,
  duckdb::idx_t* out_score) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  const auto& config = duckdb::DBConfig::GetConfig(db);

  std::vector<duckdb::reference<const duckdb::CompressionFunction>> candidates =
    config.GetCompressionFunctions(codec_type.InternalType());

  auto forced_method = ForcedMethod(db, forced);
  if (forced_method != duckdb::CompressionType::COMPRESSION_AUTO) {
    const bool available = absl::c_any_of(
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

  duckdb::CompressionAnalyzeContext actx{
    ctx, db, duckdb::StorageVersion::SERENEDB_LATEST};
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
  if (out_score) {
    *out_score = best_score;
  }
  return best;
}

bool ColumnWriter::CompressData(const duckdb::LogicalType& type,
                                std::span<WriteChunk> chunks,
                                duckdb::CompressionType forced,
                                std::vector<ColumnBlockMeta>& sink) {
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  auto fn = PickCodec(type, chunks, forced, state);
  return Compress(*fn, std::move(state), type, chunks, sink);
}

bool ColumnWriter::SealString(const duckdb::LogicalType& type,
                              std::span<WriteChunk> chunks, uint64_t row_count,
                              duckdb::CompressionType forced,
                              ColumnMeta& meta) {
  auto& db = WriteCtx().Database();
  const auto forced_method = ForcedMethod(db, forced);
  SDB_IF_FAILURE("irs::ColumnWriter::DuckDBStrings") {
    return CompressData(type, chunks, forced_method, meta.data);
  }
  if (forced_method == duckdb::CompressionType::COMPRESSION_UNCOMPRESSED) {
    return CompressData(type, chunks, forced_method, meta.data);
  }
  const auto* named = codecs::ChoiceOf(forced_method);
  codecs::StringAccumulator acc{!named || named->shape == codecs::Shape::Dedup};
  if (!meta.write_string_tuning) {
    meta.write_string_tuning = std::make_unique<codecs::StringTuning>();
  }
  auto& tuning = *meta.write_string_tuning;
  acc.Reserve(row_count, tuning.last_distinct);
  for (auto& c : chunks) {
    acc.Add(c.data);
  }
  tuning.last_distinct = acc.entries.size();
  auto& blocks = WriteCtx().Blocks();
  if (auto dictionary =
        codecs::TrainDictionary(acc, named, _codec_params, tuning)) {
    const auto bytes = dictionary->Bytes();
    meta.dictionaries.push_back(
      {.file_offset = blocks.Append(bytes), .byte_size = bytes.size()});
    codecs::Adopt(tuning, std::move(dictionary),
                  static_cast<uint16_t>(meta.dictionaries.size()));
  }
  const auto outcome = codecs::SealSegments(
    acc, named, _codec_params, type, tuning,
    [&](codecs::StringChoice choice, duckdb::BaseStatistics stats,
        uint64_t rows, std::span<const std::string_view> parts) {
      const auto& codec = *codecs::GetCodec(db, codecs::TypeOf(choice),
                                            duckdb::PhysicalType::VARCHAR);
      CaptureBlock(db, codec, std::move(stats), rows, parts, blocks, meta.data);
    });
  if (outcome.sealed) {
    return outcome.all_dedup;
  }
  return CompressData(
    type, chunks, duckdb::CompressionType::COMPRESSION_UNCOMPRESSED, meta.data);
}

bool ColumnWriter::SealNumeric(const duckdb::LogicalType& type,
                               std::span<WriteChunk> chunks, uint64_t row_count,
                               duckdb::CompressionType forced,
                               ColumnMeta& meta) {
  auto& db = WriteCtx().Database();
  if (Applies(db, ForcedMethod(db, forced), type.InternalType())) {
    return CompressData(type, chunks, forced, meta.data);
  }
  if (!meta.write_numeric_tuning) {
    meta.write_numeric_tuning = std::make_unique<codecs::NumericTuning>();
  }
  auto& tuning = *meta.write_numeric_tuning;
  const auto rows = static_cast<double>(std::max<uint64_t>(row_count, 1));
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
  duckdb::optional_ptr<const duckdb::CompressionFunction> fn;
  duckdb::idx_t score = codecs::kNoRival;
  const auto measure = [&] {
    fn = PickCodec(type, chunks, forced, state, &score);
    tuning.rival = fn->type;
  };
  bool due = tuning.Due();
  const bool flush = _codec_params.tier == WriteTier::Flush;
  if (flush && !due && !tuning.pick) {
    fn = PickCodec(type, chunks, tuning.rival, state, &score);
    if (fn->type != tuning.rival) {
      due = true;
    } else if (tuning.Drifted(static_cast<double>(score), rows)) {
      tuning.clock.Restart();
    }
  }
  if (due || !flush) {
    measure();
  }
  if (due || tuning.pick) {
    const auto sealer = codecs::NumericSealer::Make(type, row_count);
    for (const auto& c : chunks) {
      sealer->Add(c.data);
    }
    auto segment = sealer->Seal(_codec_params, score, tuning, due);
    if (segment.bytes.empty() && !fn) {
      measure();
      segment = sealer->Seal(_codec_params, score, tuning, true);
    }
    if (!segment.bytes.empty()) {
      const std::string_view part{segment.bytes};
      CaptureBlock(
        db,
        *codecs::GetCodec(db, duckdb::CompressionType::COMPRESSION_COL_NUMERIC,
                          type.InternalType()),
        std::move(segment.stats), row_count,
        std::span<const std::string_view>{&part, 1}, WriteCtx().Blocks(),
        meta.data);
      return false;
    }
  }
  return Compress(*fn, std::move(state), type, chunks, meta.data);
}

bool ColumnWriter::Compress(const duckdb::CompressionFunction& picked,
                            duckdb::unique_ptr<duckdb::AnalyzeState> state,
                            const duckdb::LogicalType& codec_type,
                            std::span<WriteChunk> chunks,
                            std::vector<ColumnBlockMeta>& sink) {
  auto& ctx = WriteCtx();
  auto& db = ctx.Database();
  auto* strings =
    codec_type.InternalType() == duckdb::PhysicalType::VARCHAR ? &ctx : nullptr;
  const auto capture = [&](duckdb::ColumnSegment& seg,
                           duckdb::BufferHandle handle, duckdb::idx_t size) {
    if (size != 0 && !handle.IsValid() && seg.GetBlockHandle()) {
      handle = db.GetBufferManager().Pin(seg.GetBlockHandle());
    }
    const std::string_view part =
      handle.IsValid()
        ? std::string_view{reinterpret_cast<const char*>(handle.Ptr()), size}
        : std::string_view{};
    CaptureBlock(db, seg.GetCompressionFunction(),
                 std::move(seg.GetStatsMutable()), seg.count.load(),
                 std::span<const std::string_view>{&part, 1}, ctx.Blocks(),
                 sink);
  };
  duckdb::ColumnDataCheckpointData ckp{
    codec_type,
    db,
    duckdb::StorageVersion::SERENEDB_LATEST,
    strings,
    strings,
    [&](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
        duckdb::BufferHandle handle,
        duckdb::idx_t size) { capture(*seg, std::move(handle), size); },
    [&](duckdb::unique_ptr<duckdb::ColumnSegment> seg, duckdb::idx_t size) {
      capture(*seg, duckdb::BufferHandle{}, size);
    },
    ctx,
  };
  auto comp_state = picked.init_compression(ckp, std::move(state));
  for (auto& c : chunks) {
    picked.compress(*comp_state, c.data);
  }
  picked.compress_finalize(*comp_state);
  return picked.validity == duckdb::CompressionValidity::NO_VALIDITY_REQUIRED;
}

void ColumnWriter::SealValidity(std::span<WriteChunk> chunks,
                                uint64_t row_count,
                                std::vector<ColumnBlockMeta>& sink) {
  uint64_t valid = 0;
  for (auto& c : chunks) {
    valid += duckdb::FlatVector::Validity(c.data).CountValid(c.count);
  }
  if (valid == row_count) {
    EmitEmptyValidity(WriteCtx().Database(), row_count, sink);
    return;
  }
  CompressData(duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY}, chunks,
               duckdb::CompressionType::COMPRESSION_AUTO, sink);
}

void ColumnWriter::Claim(ColumnMeta& meta,
                         const duckdb::LogicalType& type) const {
  if (meta.type.id() == duckdb::LogicalTypeId::INVALID) {
    meta.id = _id;
    meta.type = type;
  }
}

void ColumnWriter::SealNestedValidity(std::span<WriteChunk> chunks,
                                      uint64_t row_count, bool skip_validity,
                                      size_t child_count, ColumnMeta& meta) {
  if (!skip_validity) {
    SealValidity(chunks, row_count, meta.validity);
  }
  meta.children.resize(child_count);
}

void ColumnWriter::SealStruct(
  const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
  uint64_t row_count, bool skip_validity, duckdb::CompressionType forced,
  ColumnMeta& meta, std::span<const std::unique_ptr<ListIngest>> ingest) {
  const auto& child_types = duckdb::StructType::GetChildTypes(type);
  SealNestedValidity(chunks, row_count, skip_validity, child_types.size(),
                     meta);
  for (size_t i = 0; i < child_types.size(); ++i) {
    std::vector<WriteChunk> field_chunks;
    field_chunks.reserve(chunks.size());
    for (auto& c : chunks) {
      field_chunks.push_back(
        RefChunk(duckdb::StructVector::GetEntries(c.data)[i], c.count));
    }
    SealColumn(child_types[i].second, field_chunks, row_count,
               /*skip_validity=*/false, forced, meta.children[i],
               i < ingest.size() && ingest[i] ? ingest.subspan(i, 1)
                                              : decltype(ingest){});
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
    SliceChunks(elem_chunks, duckdb::ArrayVector::GetChildMutable(c.data), 0,
                c.count * array_size);
  }
  SealColumn(duckdb::ArrayType::GetChildType(type), elem_chunks,
             row_count * array_size, /*skip_validity=*/false, forced,
             meta.children[0]);
}

void ColumnWriter::SealList(const duckdb::LogicalType& type,
                            std::span<WriteChunk> chunks, uint64_t row_count,
                            bool skip_validity, duckdb::CompressionType forced,
                            ColumnMeta& meta) {
  ListIngest ingest{type, /*borrow=*/true, meta.write_list_distinct,
                    meta.write_list_running};
  for (const auto& c : chunks) {
    ingest.Add(c.data, 0, c.count);
    ingest.CheckDistinct();
  }
  SealListParts(type, chunks, row_count, skip_validity, ingest, forced, meta);
}

void ColumnWriter::SealListParts(const duckdb::LogicalType& type,
                                 std::span<WriteChunk> chunks,
                                 uint64_t row_count, bool skip_validity,
                                 ListIngest& ingest,
                                 duckdb::CompressionType forced,
                                 ColumnMeta& meta) {
  SealNestedValidity(chunks, row_count, skip_validity, 2, meta);
  auto parts = ingest.Take();
  meta.write_list_running = parts.running;
  meta.write_list_distinct = parts.next_code;
  CompressPlain(type, CodesCodec(parts, row_count), parts.codes, meta.data);
  SealColumn(duckdb::ListType::GetChildType(type), parts.elems,
             parts.elem_count, /*skip_validity=*/false, forced,
             meta.children[0]);
  auto& ends = meta.children[1];
  Claim(ends, duckdb::LogicalType::UBIGINT);
  CompressPlain(duckdb::LogicalType::UBIGINT,
                duckdb::CompressionType::COMPRESSION_BITPACKING, parts.ends,
                ends.data);
}

void ColumnWriter::CompressPlain(const duckdb::LogicalType& type,
                                 duckdb::CompressionType codec,
                                 std::span<WriteChunk> chunks,
                                 std::vector<ColumnBlockMeta>& sink) {
  auto& db = WriteCtx().Database();
  const auto physical = type.InternalType();
  const auto plain =
    duckdb::DBConfig::GetConfig(db).TryGetCompressionFunction(codec, physical);
  if (!plain ||
      Applies(db, ForcedMethod(db, duckdb::CompressionType::COMPRESSION_AUTO),
              physical)) {
    CompressData(type, chunks, duckdb::CompressionType::COMPRESSION_AUTO, sink);
    return;
  }
  Compress(*plain, nullptr, type, chunks, sink);
}

void ColumnWriter::SealVariant(const duckdb::LogicalType& type,
                               std::span<WriteChunk> chunks, uint64_t row_count,
                               bool skip_validity,
                               duckdb::CompressionType forced,
                               ColumnMeta& meta) {
  if (!skip_validity) {
    SealValidity(chunks, row_count, meta.validity);
  }
  duckdb::LogicalType shredded_type;
  if (_variant_min_shred_size != -1 &&
      row_count >= static_cast<uint64_t>(_variant_min_shred_size)) {
    if (_force_variant_shredding.id() != duckdb::LogicalTypeId::INVALID) {
      shredded_type = _force_variant_shredding;
    } else {
      duckdb::VariantShreddingStats stats;
      for (auto& c : chunks) {
        stats.Update(c.data, c.count);
      }
      shredded_type = stats.GetShreddedType();
    }
  }
  auto& layout = meta.variant_rgs.emplace_back();
  layout.row_count = row_count;
  layout.unshredded = std::make_unique<ColumnMeta>();
  if (shredded_type.id() != duckdb::LogicalTypeId::STRUCT ||
      duckdb::StructType::GetChildCount(shredded_type) != 2) {
    SealColumn(duckdb::VariantShredding::GetUnshreddedType(), chunks, row_count,
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
    unshredded_chunks.push_back(RefChunk(shred_entries[0], c.count));
    shredded_chunks.push_back(RefChunk(shred_entries[1], c.count));
  }

  layout.shredded = std::make_unique<ColumnMeta>();
  const auto& shredded_children =
    duckdb::StructType::GetChildTypes(shredded_type);
  SealColumn(shredded_children[0].second, unshredded_chunks, row_count,
             /*skip_validity=*/false, forced, *layout.unshredded);
  SealColumn(shredded_children[1].second, shredded_chunks, row_count,
             /*skip_validity=*/false, forced, *layout.shredded);
}

void ColumnWriter::SealColumn(
  const duckdb::LogicalType& type, std::span<WriteChunk> chunks,
  uint64_t row_count, bool skip_validity, duckdb::CompressionType forced,
  ColumnMeta& meta, std::span<const std::unique_ptr<ListIngest>> ingest) {
  Claim(meta, type);
  if (type.id() == duckdb::LogicalTypeId::VARIANT) {
    SealVariant(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (duckdb::StructType::IsStruct(type) ||
      type.id() == duckdb::LogicalTypeId::UNION) {
    SealStruct(type, chunks, row_count, skip_validity, forced, meta, ingest);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::ARRAY) {
    SealArray(type, chunks, row_count, skip_validity, forced, meta);
    return;
  }
  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::MAP) {
    if (ingest.empty()) {
      SealList(type, chunks, row_count, skip_validity, forced, meta);
    } else {
      SealListParts(type, chunks, row_count, skip_validity, *ingest.front(),
                    forced, meta);
    }
    return;
  }

  bool nulls_covered_by_data = false;
  if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
    nulls_covered_by_data = SealString(type, chunks, row_count, forced, meta);
  } else if (codecs::NumericFunction(type.InternalType())) {
    nulls_covered_by_data = SealNumeric(type, chunks, row_count, forced, meta);
  } else {
    nulls_covered_by_data = CompressData(type, chunks, forced, meta.data);
  }

  if (skip_validity) {
    return;
  }
  if (nulls_covered_by_data) {
    EmitEmptyValidity(WriteCtx().Database(), row_count, meta.validity);
    return;
  }
  SealValidity(chunks, row_count, meta.validity);
}

ColumnWriter::ColumnWriter(WriteContext& ctx, field_id id,
                           duckdb::LogicalType type,
                           const ColumnWriterOptions& options, bool stage_all)
  : _ctx{&ctx},
    _id{id},
    _type{std::move(type)},
    _skip_validity{options.skip_validity},
    _ingest_lists{!options.hyperloglog},
    _row_group_size{options.row_group_size},
    _seal_at{stage_all ? std::numeric_limits<uint64_t>::max()
                       : options.row_group_size},
    _forced{options.compression},
    _codec_params{options.codec_params} {
  if (_type.id() == duckdb::LogicalTypeId::VARIANT) {
    const auto& config = duckdb::DBConfig::GetConfig(WriteCtx().Database());
    _variant_min_shred_size =
      duckdb::Settings::Get<duckdb::VariantMinimumShreddingSizeSetting>(config);
    _force_variant_shredding = config.options.force_variant_shredding;
  }
  _meta.id = _id;
  _meta.type = _type;
  if (options.hyperloglog) {
    _meta.hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    _hll_auto = true;
  }
  if (!stage_all) {
    InitIngest();
  }
}

void ColumnWriter::InitIngest() {
  if (!_ingest_lists) {
    return;
  }
  if (_type.InternalType() == duckdb::PhysicalType::LIST) {
    _ingest.emplace_back(std::make_unique<ListIngest>(_type, /*borrow=*/false));
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
      std::make_unique<ListIngest>(fields[i].second, /*borrow=*/false);
  }
}

void ColumnWriter::Reorder(std::span<const uint32_t> new2old) {
  SDB_ASSERT(_seal_at == std::numeric_limits<uint64_t>::max());
  SDB_ASSERT(_row_start == 0);
  SDB_ASSERT(new2old.empty() || _staged_rows == new2old.size());
  const auto rows = static_cast<duckdb::idx_t>(_staged_rows);
  auto staged = std::exchange(_staged, {});
  auto caches = std::exchange(_staged_caches, {});
  const auto chunks = std::exchange(_staged_chunks, 0);
  _staged_rows = 0;
  _staged_bytes = 0;
  _open_bytes = 0;
  _seal_at = _row_group_size;
  InitIngest();
  for (duckdb::idx_t c = 0; c != chunks; ++c) {
    CertifyStaged(staged[c].data);
  }
  if (new2old.empty()) {
    for (duckdb::idx_t c = 0; c != chunks; ++c) {
      Append(staged[c].data, staged[c].count);
    }
    return;
  }
  duckdb::Vector all{_type, rows};
  duckdb::idx_t at = 0;
  for (duckdb::idx_t c = 0; c != chunks; ++c) {
    duckdb::ImmutableStrings::Copy(staged[c].data, all, staged[c].count, 0, at);
    at += staged[c].count;
  }
  duckdb::FlatVector::SetSize(all, rows);
  staged.clear();
  caches.clear();
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  for (duckdb::idx_t first = 0; first < rows; first += STANDARD_VECTOR_SIZE) {
    const auto n = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows - first);
    for (duckdb::idx_t i = 0; i != n; ++i) {
      sel.set_index(i, new2old[first + i]);
    }
    if (_ingest.empty()) {
      Append(duckdb::Vector{all, sel, n}, n);
      continue;
    }
    duckdb::Vector out{_type, n};
    duckdb::ImmutableStrings::Copy(all, out, sel, n, 0, 0);
    duckdb::FlatVector::SetSize(out, n);
    Append(out, n);
  }
}

WriteChunk& ColumnWriter::OpenChunk() {
  if (_staged_chunks != 0 &&
      _staged[_staged_chunks - 1].count < duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
    return _staged[_staged_chunks - 1];
  }
  if (_staged_chunks != 0 && _seal_at == std::numeric_limits<uint64_t>::max()) {
    const auto& full = _staged[_staged_chunks - 1];
    _staged_bytes += full.data.GetDataSize(full.count);
    _open_bytes = 0;
  }
  if (_staged_chunks == _staged.size()) {
    auto& alloc = duckdb::Allocator::Get(WriteCtx().Database());
    auto& cache = _staged_caches.emplace_back(
      alloc, IngestsList() ? duckdb::LogicalType::BOOLEAN : _type,
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
    const auto take = static_cast<duckdb::idx_t>(
      std::min({count - off, STANDARD_VECTOR_SIZE - uint64_t{back.count},
                _seal_at - _staged_rows}));
    fill(back, static_cast<duckdb::idx_t>(off), take);
    back.count += take;
    if (back.count == duckdb::idx_t{STANDARD_VECTOR_SIZE}) {
      for (auto& ingest : _ingest) {
        if (ingest) {
          ingest->CheckDistinct();
        }
      }
    }
    duckdb::FlatVector::SetSize(back.data, back.count);
    if (_seal_at == std::numeric_limits<uint64_t>::max()) {
      _open_bytes = back.data.GetDataSize(back.count);
    }
    _staged_rows += take;
    off += take;
    if (_staged_rows == _seal_at) {
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
    duckdb::ImmutableStrings::Copy(vec, back.data, off + take,
                                   /*source_offset=*/off,
                                   /*target_offset=*/back.count);
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
  }
  while (current < target_row) {
    const auto n =
      std::min<uint64_t>(target_row - current, STANDARD_VECTOR_SIZE);
    duckdb::FlatVector::SetSize(*_null_pad, static_cast<duckdb::idx_t>(n));
    Append(*_null_pad, static_cast<duckdb::idx_t>(n));
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
      _meta.hyperloglog->Update(chunk.data, _hll_hashes);
    }
  }
  SealColumn(_type, chunks, _staged_rows, _skip_validity, _forced, _meta,
             _ingest);
  for (duckdb::idx_t c = 0; c != _staged_chunks; ++c) {
    _staged[c].data.ResetFromCache(_staged_caches[c]);
  }
  _row_start += _staged_rows;
  _staged_chunks = 0;
  _staged_rows = 0;
}

void ColumnWriter::SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll) {
  _meta.hyperloglog = std::move(hll);
  _hll_auto = false;
}

bool TrainsColumnDictionary(duckdb::DatabaseInstance& db,
                            const duckdb::LogicalType& type,
                            duckdb::CompressionType forced,
                            const ColCodecParams& params) {
  if (type.InternalType() != duckdb::PhysicalType::VARCHAR) {
    return false;
  }
  const auto forced_method = ForcedMethod(db, forced);
  if (forced_method == duckdb::CompressionType::COMPRESSION_UNCOMPRESSED) {
    return false;
  }
  return codecs::TrainsDictionary(codecs::ChoiceOf(forced_method), params);
}

bool ColumnWriter::TrainsDictionary() const noexcept {
  return TrainsColumnDictionary(WriteCtx().Database(), _type, _forced,
                                _codec_params);
}

void ColumnWriter::SampleDictionary(std::span<const std::string_view> entries) {
  if (!_meta.write_string_tuning) {
    _meta.write_string_tuning = std::make_unique<codecs::StringTuning>();
  }
  _meta.write_string_tuning->sampler.Add(entries);
}

void ColumnWriter::ShareLeafDictionary(
  std::span<const uint32_t> path,
  std::shared_ptr<const codecs::TrainedDictionary> dictionary, uint16_t index) {
  auto* meta = &_meta;
  for (const auto child : path) {
    if (meta->children.size() <= child) {
      meta->children.resize(child + 1);
    }
    meta = &meta->children[child];
  }
  meta->write_string_tuning = std::make_unique<codecs::StringTuning>();
  codecs::Adopt(*meta->write_string_tuning, std::move(dictionary), index);
}

}  // namespace irs
