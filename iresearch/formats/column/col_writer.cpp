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

#include "iresearch/formats/column/col_writer.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/database.hpp>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "iresearch/formats/ann_writer.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

#ifndef IRESEARCH_NO_VECTOR
#include "iresearch/formats/hnsw/hnsw_writer.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#endif

namespace irs {
namespace {

uint64_t BlockRows(const std::vector<ColumnBlockMeta>& blocks) {
  uint64_t rows = 0;
  for (const auto& block : blocks) {
    rows += block.tuple_count;
  }
  return rows;
}

uint64_t MetaRows(const ColumnMeta& meta) {
  return BlockRows(meta.data.empty() ? meta.validity : meta.data);
}

void DropTuning(ColumnMeta& meta) {
  meta.write_string_tuning.reset();
  meta.write_numeric_tuning.reset();
  for (auto& child : meta.children) {
    DropTuning(child);
  }
  for (auto& layout : meta.variant_rgs) {
    if (layout.unshredded) {
      DropTuning(*layout.unshredded);
    }
    if (layout.shredded) {
      DropTuning(*layout.shredded);
    }
  }
}

void AppendPart(ColumnMeta& meta, ColumnMeta& part) {
  SDB_ASSERT(part.list_parts.empty() && part.variant_rgs.empty() &&
             !part.hyperloglog);
  SDB_ENSURE(part.dictionaries.empty(), "column ", part.id,
             ": a merged part trained its own dictionary");
  if (meta.type.id() == duckdb::LogicalTypeId::INVALID) {
    meta.id = part.id;
    meta.type = std::move(part.type);
  }
  if (meta.children.empty()) {
    meta.children.resize(part.children.size());
  }
  SDB_ASSERT(meta.children.size() == part.children.size());
  if (meta.type.InternalType() == duckdb::PhysicalType::LIST &&
      !meta.data.empty()) {
    meta.list_parts.push_back(ListPart{
      .row = BlockRows(meta.data),
      .code = MetaRows(meta.children[1]),
      .elem = MetaRows(meta.children[0]),
    });
  }
  absl::c_move(part.data, std::back_inserter(meta.data));
  absl::c_move(part.validity, std::back_inserter(meta.validity));
  for (size_t i = 0; i != part.children.size(); ++i) {
    AppendPart(meta.children[i], part.children[i]);
  }
}

void SerializeNormRegion(duckdb::BinarySerializer& s, const NormRegionMeta& r) {
  s.WriteProperty(0, "rows", r.stats.rows);
  s.WriteProperty(1, "sum", r.stats.sum);
  s.WriteProperty(2, "non_zero", r.stats.non_zero);
  s.WriteProperty(3, "wide8", r.stats.wide8);
  s.WriteProperty(4, "wide16", r.stats.wide16);
  s.WriteProperty(5, "min", r.stats.min);
  s.WriteProperty(6, "max", r.stats.max);
  s.WriteProperty(7, "bits", r.bits);
  if (r.bits == 0) {
    s.WriteProperty(8, "value", r.value);
    return;
  }
  s.WriteProperty(9, "file_offset", r.file_offset);
  s.WritePropertyWithDefault<uint32_t>(10, "exceptions", r.exceptions, 0);
  if (r.exceptions != 0) {
    s.WriteProperty(11, "overflow", r.overflow);
    s.WriteProperty(12, "shift", r.shift);
    s.WriteProperty(13, "exception_bytes", r.exception_bytes);
    s.WriteProperty(14, "table_offset", r.table_offset);
  }
}

void SerializeNormColumn(duckdb::BinarySerializer& s,
                         const NormColumnWriter& nw) {
  const auto& meta = nw.Meta();
  s.WriteProperty(0, "id", static_cast<uint64_t>(nw.Id()));
  s.WriteList(1, "regions", meta.regions.size(),
              [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                list.WriteObject([&](duckdb::BinarySerializer& obj) {
                  SerializeNormRegion(obj, meta.regions[i]);
                });
              });
}

bool PatchDictionary(std::string_view block,
                     std::span<const uint16_t> dictionaries,
                     std::array<char, sizeof(codecs::Header)>& head) {
  const auto header = codecs::Header::Parse(
    reinterpret_cast<duckdb::const_data_ptr_t>(block.data()), block.size());
  if (header.flags != codecs::FrameDictionary::Trained) {
    return false;
  }
  SDB_ENSURE(header.dictionary <= dictionaries.size(),
             "col codec: missing trained dictionary ", header.dictionary);
  const uint16_t dictionary = dictionaries[header.dictionary - 1];
  std::memcpy(head.data(), block.data(), head.size());
  std::memcpy(head.data() + offsetof(codecs::Header, dictionary), &dictionary,
              sizeof(dictionary));
  return true;
}

bool CopyBlocks(ReadContext& ctx, std::span<const ColumnBlockMeta> blocks,
                std::span<const uint16_t> dictionaries, ColFileWriter& sink,
                std::vector<ColumnBlockMeta>& out,
                absl::FunctionRef<bool()> progress) {
  std::string bytes;
  for (const auto& block : blocks) {
    if (!progress()) {
      return false;
    }
    auto& copy = out.emplace_back(block.statistics.Copy(), block.tuple_count,
                                  uint64_t{0}, block.byte_size, block.codec);
    if (block.byte_size == 0) {
      continue;
    }
    std::string_view view;
    if (const auto* stable =
          ctx.TryReadStable(block.file_offset, block.byte_size)) {
      view = {reinterpret_cast<const char*>(stable), block.byte_size};
    } else {
      bytes.resize(block.byte_size);
      ctx.Read(block.file_offset,
               reinterpret_cast<duckdb::data_ptr_t>(bytes.data()),
               block.byte_size);
      view = bytes;
    }
    std::array<char, sizeof(codecs::Header)> head{};
    if (!dictionaries.empty() &&
        duckdb::IsSereneDBCompressionType(block.codec->type) &&
        PatchDictionary(view, dictionaries, head)) {
      const std::array<std::string_view, 2> parts{
        std::string_view{head.data(), head.size()},
        view.substr(head.size()),
      };
      copy.file_offset = sink.AppendAligned(parts);
    } else {
      copy.file_offset = sink.AppendAligned({&view, 1});
    }
  }
  return true;
}

}  // namespace

ColWriter::ColWriter(Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db, WriteTier tier,
                     bool stage_all)
  : _dir{&dir},
    _segment_name{segment_name},
    _db{&db},
    _tier{tier},
    _stage_all{stage_all} {}

ColWriter::~ColWriter() {
  if (_file && !_committed) {
    Rollback();
  }
}

void ColWriter::EnsureOut() {
  if (_file) {
    return;
  }
  _file =
    std::make_unique<ColFileWriter>(_dir->create(FileName(_segment_name)));
  _write_ctx = std::make_unique<WriteContext>(*_db, *_file);
}

void ColWriter::SetFieldOptions(
  const IndexFieldOptions* field_options) noexcept {
  SDB_ASSERT(
    !_field_options || CompatibleFieldOptions(_field_options, field_options),
    "ColWriter::SetFieldOptions: encodings differ mid-segment");
  _field_options = field_options;
}

ColumnWriter* ColWriter::StagedColumn(field_id id) noexcept {
  SDB_ASSERT(_stage_all);
  const auto it = _by_id.find(id);
  return it == _by_id.end() ? nullptr
                            : _columns[it->second]->parts.front().writer.get();
}

uint64_t ColWriter::StagedBytes() const noexcept {
  if (!_stage_all) {
    return 0;
  }
  uint64_t bytes = 0;
  for (const auto& column : _columns) {
    bytes += column->parts.front().writer->StagedBytes();
  }
  for (const auto& norm : _norms) {
    bytes += norm->StagedBytes();
  }
  return bytes;
}

ColWriter::Column& ColWriter::AddColumn(field_id id, uint32_t parts,
                                        bool whole) {
  EnsureOut();
  const bool added = _by_id.try_emplace(id, _columns.size()).second;
  SDB_ASSERT(added);
  return *_columns.emplace_back(std::make_unique<Column>(parts, whole));
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type,
                                    const ColumnWriterOptions& options) {
  SDB_ASSERT(options.row_group_size != 0);
  if (const auto it = _by_id.find(id); it != _by_id.end()) {
    auto& column = *_columns[it->second];
    SDB_ASSERT(column.whole && column.parts.front().writer);
    auto& existing = *column.parts.front().writer;
    SDB_ASSERT(existing._type == type &&
               existing._row_group_size == options.row_group_size &&
               existing._skip_validity == options.skip_validity &&
               existing._forced == options.compression &&
               existing._codec_params == options.codec_params &&
               (existing._meta.hyperloglog != nullptr) == options.hyperloglog);
    return existing;
  }
  auto& writer = AddColumn(id, 1, true).parts.front().writer;
  writer = std::make_unique<ColumnWriter>(*_write_ctx, id, std::move(type),
                                          options, _stage_all);
  return *writer;
}

ColumnWriter& ColWriter::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnWriterOptions options;
  if (_field_options) {
    const auto opts = _field_options->GetColumnOptions(id);
    options = {
      .skip_validity = opts.skip_validity,
      .row_group_size = _field_options->row_group_size,
      .compression = opts.compression,
      .hyperloglog = opts.hyperloglog,
      .codec_params = _field_options->CodecParams(opts),
    };
    options.codec_params.tier = _tier;
  }
  auto& cw = OpenColumn(id, std::move(type), options);
  if (_field_options) {
    for (const auto ann : _field_options->AnnFields(id)) {
      AttachAnn(id, *_field_options->GetColumnOptions(ann).ann_info);
    }
  }
  return cw;
}

ColumnWriter& ColWriter::OpenedColumn(field_id id) noexcept {
  auto& column = ColumnOf(id);
  SDB_ASSERT(column.whole && column.parts.front().writer);
  return *column.parts.front().writer;
}

void ColWriter::CloseColumn(field_id id) {
  auto& column = ColumnOf(id);
  SDB_ASSERT(column.whole);
  Close(column.parts.front());
}

void ColWriter::PlanParts(field_id id, uint32_t parts) {
  AddColumn(id, parts, false);
}

ColWriter::Column& ColWriter::ColumnOf(field_id id) {
  const auto it = _by_id.find(id);
  SDB_ASSERT(it != _by_id.end());
  return *_columns[it->second];
}

ColWriter::ColumnEntry& ColWriter::PartOf(field_id id, uint32_t part) {
  auto& column = ColumnOf(id);
  SDB_ASSERT(!column.whole && part < column.parts.size());
  return column.parts[part];
}

void ColWriter::Close(ColumnEntry& entry) {
  SDB_ASSERT(entry.writer);
  entry.writer->Finish();
  entry.meta = std::move(entry.writer->_meta);
  entry.writer.reset();
  DropTuning(entry.meta);
}

ColumnWriter& ColWriter::OpenColumnPart(field_id id, uint32_t part,
                                        duckdb::LogicalType type,
                                        const ColumnWriterOptions& options) {
  SDB_ASSERT(options.row_group_size != 0 && !options.hyperloglog);
  auto& writer = PartOf(id, part).writer;
  SDB_ASSERT(!writer);
  writer = std::make_unique<ColumnWriter>(*_write_ctx, id, std::move(type),
                                          options, false);
  return *writer;
}

void ColWriter::CloseColumnPart(field_id id, uint32_t part) {
  Close(PartOf(id, part));
}

bool ColWriter::CopyColumnPart(field_id id, uint32_t part, ReadContext& ctx,
                               const ColumnReader& column, bool validity,
                               std::span<const uint16_t> dictionaries,
                               absl::FunctionRef<bool()> progress) {
  auto& slot = PartOf(id, part);
  SDB_ASSERT(!slot.writer);
  auto& meta = slot.meta;
  meta.id = column.Id();
  meta.type = column.Type();
  if (!CopyBlocks(ctx, column.DataBlocks(), dictionaries, *_file, meta.data,
                  progress)) {
    return false;
  }
  if (!validity) {
    return true;
  }
  if (const auto* stored = column.Validity()) {
    return CopyBlocks(ctx, stored->DataBlocks(), {}, *_file, meta.validity,
                      progress);
  }
  for (const auto& block : meta.data) {
    EmitEmptyValidity(*_db, block.tuple_count, meta.validity);
  }
  return true;
}

uint16_t ColWriter::AddColumnDictionary(field_id id, std::string_view bytes) {
  auto& column = ColumnOf(id);
  absl::MutexLock lock{&column.mutex};
  if (column.own == 0) {
    SDB_ASSERT(column.dictionaries.size() < kMaxColumnDictionaries);
    column.dictionaries.push_back(
      {.file_offset = _file->Append(bytes), .byte_size = bytes.size()});
    column.own = static_cast<uint16_t>(column.dictionaries.size());
  }
  return column.own;
}

void ColWriter::AddLeafDictionary(field_id id, std::span<const uint32_t> path,
                                  std::string_view bytes) {
  const bool added =
    ColumnOf(id)
      .leaf_dictionaries
      .try_emplace(std::vector<uint32_t>{path.begin(), path.end()},
                   ColumnDictionaryMeta{.file_offset = _file->Append(bytes),
                                        .byte_size = bytes.size()})
      .second;
  SDB_ASSERT(added);
}

bool ColWriter::AddCopiedDictionaries(
  field_id id,
  std::span<const std::shared_ptr<const codecs::TrainedDictionary>>
    dictionaries,
  std::vector<uint16_t>& indexes) {
  auto& column = ColumnOf(id);
  absl::MutexLock lock{&column.mutex};
  const size_t reserved = column.own == 0 ? 1 : 0;
  if (column.dictionaries.size() + reserved + dictionaries.size() >
      kMaxColumnDictionaries) {
    return false;
  }
  for (const auto& dictionary : dictionaries) {
    const auto bytes = dictionary->Bytes();
    column.dictionaries.push_back(
      {.file_offset = _file->Append(bytes), .byte_size = bytes.size()});
    indexes.emplace_back(static_cast<uint16_t>(column.dictionaries.size()));
  }
  return true;
}

ColumnMeta ColWriter::Assemble(Column& column) {
  if (column.whole) {
    auto& entry = column.parts.front();
    SDB_ASSERT(!entry.writer);
    return std::move(entry.meta);
  }
  ColumnMeta meta;
  for (auto& part : column.parts) {
    SDB_ASSERT(!part.writer &&
               part.meta.type.id() != duckdb::LogicalTypeId::INVALID);
    AppendPart(meta, part.meta);
  }
  {
    absl::MutexLock lock{&column.mutex};
    meta.dictionaries = std::move(column.dictionaries);
  }
  for (auto& [path, dictionary] : column.leaf_dictionaries) {
    auto* leaf = &meta;
    for (const auto child : path) {
      SDB_ASSERT(child < leaf->children.size());
      leaf = &leaf->children[child];
    }
    leaf->dictionaries = {dictionary};
  }
  return meta;
}

NormColumnWriter& ColWriter::AddNormColumn(field_id id,
                                           uint32_t row_group_size) {
  auto& writer = *_norms.emplace_back(std::make_unique<NormColumnWriter>(
    id, _stage_all ? 0 : row_group_size, [this]() -> ColFileWriter& {
      EnsureOut();
      return *_file;
    }));
  _norm_by_id.emplace(id, &writer);
  return writer;
}

NormColumnWriter& ColWriter::OpenNormColumn(field_id id,
                                            uint32_t row_group_size) {
  SDB_ASSERT(row_group_size != 0);
  if (auto it = _norm_by_id.find(id); it != _norm_by_id.end()) {
    return *it->second;
  }
  return AddNormColumn(id, row_group_size);
}

NormColumnWriter& ColWriter::StreamNormColumn(field_id id) {
  SDB_ASSERT(!_norm_by_id.contains(id), "ColWriter::StreamNormColumn: column ",
             id, " already open");
  return AddNormColumn(id, 0);
}

AnnWriter& ColWriter::AttachAnn(field_id column_id, AnnInfo info) {
  if (auto it = _ann_by_id.find(info.index_id); it != _ann_by_id.end()) {
    auto& existing = _ann_writers[it->second];
    SDB_ASSERT(existing.info == info && existing.column_id == column_id,
               "ColWriter::AttachAnn: re-attach with mismatched AnnInfo on "
               "column ",
               column_id);
    return *existing.writer;
  }
  SDB_ASSERT(_by_id.contains(column_id), "ColWriter::AttachAnn: column ",
             column_id, " must be opened first");
  std::unique_ptr<AnnWriter> writer;
  switch (info.kind) {
#ifndef IRESEARCH_NO_VECTOR
    case AnnKind::Ivf:
      writer = std::make_unique<IvfWriter>(info);
      break;
    case AnnKind::Hnsw:
      writer = std::make_unique<HnswWriter>(info, _hnsw_bases);
      break;
#endif
    default:
      SDB_UNREACHABLE();
  }
  auto& entry =
    _ann_writers.emplace_back(column_id, std::move(info), std::move(writer));
  _ann_by_id.emplace(entry.info.index_id, _ann_writers.size() - 1);
  return *entry.writer;
}

void ColWriter::SetIdxWriter(IdxWriter& idx) noexcept {
  for (auto& entry : _ann_writers) {
    if (entry.writer) {
      entry.writer->SetIdxWriter(idx);
    }
  }
}

std::vector<std::unique_ptr<AnnWriter>> ColWriter::TakeAnnWriters() noexcept {
  std::vector<std::unique_ptr<AnnWriter>> out;
  out.reserve(_ann_writers.size());
  for (auto& entry : _ann_writers) {
    if (entry.writer) {
      out.push_back(std::move(entry.writer));
    }
  }
  return out;
}

void ColWriter::Rollback() noexcept {
  _write_ctx.reset();
  _file.reset();
}

constexpr auto kNoCancel = [] { return true; };

bool ColWriter::Commit(uint64_t target_row, std::span<const uint32_t> new2old) {
  return Commit(target_row, kNoCancel, new2old);
}

bool ColWriter::Commit(uint64_t target_row, absl::FunctionRef<bool()> progress,
                       std::span<const uint32_t> new2old) {
  if (_committed) {
    return true;
  }
  SDB_ASSERT(new2old.empty() || (_stage_all && new2old.size() == target_row));
  std::vector<const NormColumnWriter*> norms;
  for (auto& writer : _norms) {
    writer->PadTo(target_row);
    if (_stage_all) {
      SDB_ASSERT(_field_options);
      writer->Reorder(new2old, _field_options->row_group_size);
    }
    writer->Finalize();
    SDB_ASSERT(writer->Meta().row_count == target_row,
               "ColWriter::Commit: norm column ", writer->Id(), " holds ",
               writer->Meta().row_count, " rows, segment ", target_row);
    if (!writer->Meta().regions.empty()) {
      norms.push_back(writer.get());
    }
  }
  if (!_file && norms.empty()) {
    _committed = true;
    return true;
  }
  EnsureOut();
  for (auto& column : _columns) {
    auto& entry = column->parts.front();
    if (!column->whole || !entry.writer) {
      continue;
    }
    if (!progress()) {
      return false;
    }
    if (_stage_all) {
      entry.writer->PadNullsTo(target_row);
      entry.writer->Reorder(new2old);
    }
    Close(entry);
  }
  std::vector<ColumnMeta> metas;
  metas.reserve(_columns.size());
  for (auto& column : _columns) {
    metas.emplace_back(Assemble(*column));
  }
  _file->Finish([&](duckdb::BinarySerializer& footer) {
    if (!metas.empty()) {
      footer.WriteList(
        kColFieldColumns, "columns", metas.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            SerializeColumnMeta(obj, metas[i]);
          });
        });
    }
    if (!norms.empty()) {
      footer.WriteList(
        kColFieldNormColumns, "norm_columns", norms.size(),
        [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
          list.WriteObject([&](duckdb::BinarySerializer& obj) {
            SerializeNormColumn(obj, *norms[i]);
          });
        });
    }
  });
  _committed = true;
  return true;
}

yaclib::Task<> ColWriter::ComputeAnn(const AnnBuildEnv* env) {
  if (!_committed || _ann_writers.empty()) {
    co_return {};
  }
  ColReader reader{*_dir, _segment_name, *_db, IOAdvice::SEQUENTIAL};
  for (auto& entry : _ann_writers) {
    const auto* col = reader.Column(entry.column_id);
    if (!col) {
      continue;
    }
    co_await entry.writer->Compute(*col, reader.Ctx(), env);
  }
  co_return {};
}

yaclib::Task<> ColWriter::ComputeAnn(field_id column, ColReader& reader,
                                     const AnnBuildEnv* env) {
  SDB_ASSERT(_committed);
  const auto* col = reader.Column(column);
  if (!col) {
    co_return {};
  }
  for (auto& entry : _ann_writers) {
    if (entry.column_id == column) {
      co_await entry.writer->Compute(*col, reader.Ctx(), env);
    }
  }
  co_return {};
}

}  // namespace irs
