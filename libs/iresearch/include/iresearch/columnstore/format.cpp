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

#include "iresearch/columnstore/format.hpp"

#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_allocator.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/data_pointer.hpp>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/hnsw.hpp"
#include "iresearch/columnstore/internal/persistent_column_data.hpp"
#include "iresearch/columnstore/internal/write_context.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/columnstore/norm_writer.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"

namespace irs::columnstore {
namespace {

// Footer slot ids; stable, never reuse.
constexpr duckdb::field_id_t kFooterSlotColumns = 100;
constexpr duckdb::field_id_t kFooterSlotNormColumns = 101;
constexpr duckdb::field_id_t kFooterSlotHnswColumns = 102;

void SerializeColumnData(duckdb::Serializer& obj,
                         const PersistentColumnData& node) {
  obj.WriteProperty(0, "type", node.type);
  obj.WriteList(1, "data", node.pointers.size(),
                [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
                  plist.WriteObject([&](duckdb::Serializer& p) {
                    node.pointers[j].Serialize(p);
                  });
                });
  obj.WriteList(2, "validity", node.validity_pointers.size(),
                [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
                  plist.WriteObject([&](duckdb::Serializer& p) {
                    node.validity_pointers[j].Serialize(p);
                  });
                });
  obj.WriteList(3, "child_columns", node.child_columns.size(),
                [&](duckdb::Serializer::List& clist, duckdb::idx_t j) {
                  clist.WriteObject([&](duckdb::Serializer& child) {
                    SerializeColumnData(child, node.child_columns[j]);
                  });
                });
}

std::unique_ptr<ColumnReader> MakeColumnReader(field_id id,
                                               PersistentColumnData&& node) {
  std::unique_ptr<ColumnReader> element_child;
  std::vector<std::unique_ptr<ColumnReader>> struct_children;
  uint64_t array_size = 0;

  switch (node.type.id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      SDB_ASSERT(node.child_columns.size() == 1);
      array_size = static_cast<uint64_t>(duckdb::ArrayType::GetSize(node.type));
      element_child = MakeColumnReader(field_limits::invalid(),
                                       std::move(node.child_columns.front()));
      node.pointers.clear();  // ARRAY carries no self data on disk.
      break;
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      // MAP rides on the LIST path: PhysicalType::LIST with a
      // STRUCT<key, value> element.
      SDB_ASSERT(node.child_columns.size() == 1);
      element_child = MakeColumnReader(field_limits::invalid(),
                                       std::move(node.child_columns.front()));
      break;
    }
    case duckdb::LogicalTypeId::VARIANT:
    case duckdb::LogicalTypeId::STRUCT: {
      struct_children.reserve(node.child_columns.size());
      for (auto& cn : node.child_columns) {
        struct_children.push_back(
          MakeColumnReader(field_limits::invalid(), std::move(cn)));
      }
      node.pointers.clear();
      break;
    }
    default:
      break;  // primitive leaf: keep pointers, no children.
  }

  return std::make_unique<ColumnReader>(
    id, std::move(node.type), std::move(node.pointers),
    std::move(node.validity_pointers), std::move(element_child),
    std::move(struct_children), array_size);
}

PersistentColumnData DeserializeColumnData(duckdb::Deserializer& obj) {
  PersistentColumnData node;
  node.type = obj.ReadProperty<duckdb::LogicalType>(0, "type");
  // DataPointer::Deserialize reads the LogicalType from the deserializer
  // context. For LIST/MAP the codec data type is UBIGINT (per-row lengths),
  // not the node's nested type; for STRUCT there is no own data; primitives
  // use node.type directly.
  static const duckdb::LogicalType kListLengthsType =
    duckdb::LogicalType::UBIGINT;
  const bool is_list_like = node.type.id() == duckdb::LogicalTypeId::LIST ||
                            node.type.id() == duckdb::LogicalTypeId::MAP;
  const auto& data_codec_type = is_list_like ? kListLengthsType : node.type;
  obj.Set<const duckdb::LogicalType&>(data_codec_type);
  obj.ReadList(1, "data",
               [&](duckdb::Deserializer::List& plist, duckdb::idx_t /*j*/) {
                 plist.ReadObject([&](duckdb::Deserializer& p) {
                   node.pointers.push_back(duckdb::DataPointer::Deserialize(p));
                 });
               });
  obj.Unset<const duckdb::LogicalType>();
  static const duckdb::LogicalType kValidityType{
    duckdb::LogicalTypeId::VALIDITY};
  obj.Set<const duckdb::LogicalType&>(kValidityType);
  obj.ReadList(
    2, "validity", [&](duckdb::Deserializer::List& plist, duckdb::idx_t /*j*/) {
      plist.ReadObject([&](duckdb::Deserializer& p) {
        node.validity_pointers.push_back(duckdb::DataPointer::Deserialize(p));
      });
    });
  obj.Unset<const duckdb::LogicalType>();
  obj.ReadList(3, "child_columns",
               [&](duckdb::Deserializer::List& clist, duckdb::idx_t /*j*/) {
                 clist.ReadObject([&](duckdb::Deserializer& child) {
                   node.child_columns.push_back(DeserializeColumnData(child));
                 });
               });
  return node;
}

}  // namespace
namespace {

class IndexOutputWriteStream final : public duckdb::WriteStream {
 public:
  explicit IndexOutputWriteStream(IndexOutput& out) noexcept : _out{&out} {}

  void WriteData(duckdb::const_data_ptr_t buffer, duckdb::idx_t size) final {
    _out->WriteBytes(reinterpret_cast<const byte_type*>(buffer), size);
  }

 private:
  IndexOutput* _out;
};

class MemoryReadStream final : public duckdb::ReadStream {
 public:
  MemoryReadStream(const byte_type* data, uint64_t size) noexcept
    : _cur{data}, _end{data + size} {}

  void ReadData(duckdb::data_ptr_t buffer, duckdb::idx_t size) final {
    SDB_ENSURE(_cur + size <= _end, sdb::ERROR_INTERNAL,
               "columnstore: short read in footer (need ", size, " bytes, ",
               (_end - _cur), " remaining)");
    std::memcpy(buffer, _cur, size);
    _cur += size;
  }

  void ReadData(duckdb::QueryContext, duckdb::data_ptr_t buffer,
                duckdb::idx_t size) final {
    ReadData(buffer, size);
  }

 private:
  const byte_type* _cur;
  const byte_type* _end;
};

}  // namespace

struct HNSWWriterEntry {
  field_id column_id;
  std::string name;
  HNSWInfo info;
  std::unique_ptr<HNSWWriter> writer;
  // Filled at Finalize() (just before Commit serializes the footer): the
  // graph bytes are emitted as a separate inline payload at this offset.
  uint64_t graph_offset = 0;
  uint64_t graph_byte_size = 0;
};

struct Writer::Impl {
  Directory* dir;
  std::string segment_name;
  std::string filename;
  duckdb::DatabaseInstance* db;
  const ColumnOptionsProvider* column_options;
  const NormColumnOptionsProvider* norm_column_options;
  IndexOutput::ptr out;
  std::unique_ptr<WriteContext> write_ctx;
  std::vector<std::unique_ptr<ColumnWriter>> column_writers;
  sdb::containers::FlatHashMap<field_id, ColumnWriter*> column_by_id;
  std::vector<std::unique_ptr<FooterColumnEntry>> column_entries;
  std::vector<std::unique_ptr<NormColumnWriter>> norm_writers;
  sdb::containers::FlatHashMap<field_id, NormColumnWriter*> norm_by_id;
  std::vector<std::unique_ptr<HNSWWriterEntry>> hnsw_writers;
  sdb::containers::FlatHashMap<field_id, HNSWWriterEntry*> hnsw_by_id;
  bool committed = false;
};

Writer::Writer(Directory& dir, std::string_view segment_name,
               duckdb::DatabaseInstance& db,
               const ColumnOptionsProvider* column_options,
               const NormColumnOptionsProvider* norm_column_options)
  : _impl{std::make_unique<Impl>()} {
  _impl->dir = &dir;
  _impl->segment_name = std::string{segment_name};
  _impl->db = &db;
  _impl->column_options = column_options;
  _impl->norm_column_options = norm_column_options;
  _impl->filename = absl::StrCat(segment_name, ".", kFormatExt);
  _impl->out = dir.create(_impl->filename);
  if (!_impl->out) {
    throw IoError{
      absl::StrCat("failed to create columnstore file: ", _impl->filename)};
  }
  format_utils::WriteHeader(*_impl->out, kFormatName, kFormatVersion);
  _impl->write_ctx = std::make_unique<WriteContext>(db, *_impl->out);
}

Writer::~Writer() {
  if (_impl && !_impl->committed) {
    Rollback();
  }
}

ColumnWriter& Writer::OpenColumn(field_id id, duckdb::LogicalType type) {
  ColumnOptions opts{};
  if (_impl->column_options && *_impl->column_options) {
    opts = (*_impl->column_options)(id);
  }
  auto& cw = OpenColumn(id, std::move(type), opts.skip_validity,
                        opts.row_group_size, opts.compression);
  if (opts.hnsw_info) {
    AttachHNSW(id, *opts.hnsw_info);
  }
  return cw;
}

ColumnWriter& Writer::OpenColumn(field_id id, duckdb::LogicalType type,
                                 bool skip_validity, uint32_t row_group_size,
                                 duckdb::CompressionType compression) {
  SDB_ASSERT(row_group_size != 0);
  // Per-batch SearchSink may re-open the same id; return the existing
  // writer so batches accumulate into one footer entry.
  if (auto it = _impl->column_by_id.find(id); it != _impl->column_by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(existing.Type() == type &&
                 existing.RowGroupSize() == row_group_size &&
                 existing.SkipValidity() == skip_validity &&
                 existing.Compression() == compression,
               "columnstore::Writer::OpenColumn: re-opened id ", id,
               " with mismatched settings (type ", type.ToString(), " vs ",
               existing.Type().ToString(), ", row_group_size ", row_group_size,
               " vs ", existing.RowGroupSize(), ", skip_validity ",
               skip_validity, " vs ", existing.SkipValidity(), ", compression ",
               duckdb::CompressionTypeToString(compression), " vs ",
               duckdb::CompressionTypeToString(existing.Compression()), ")");
    return existing;
  }
  auto entry = std::make_unique<FooterColumnEntry>();
  entry->id = id;
  entry->root.type = type;
  auto cw = std::make_unique<ColumnWriter>(
    id, type, row_group_size, *_impl->write_ctx, *entry, skip_validity);
  cw->SetCompression(compression);
  _impl->column_entries.push_back(std::move(entry));
  auto& back = *_impl->column_writers.emplace_back(std::move(cw));
  _impl->column_by_id.emplace(id, &back);
  return back;
}

HNSWWriter& Writer::AttachHNSW(field_id column_id, HNSWInfo info) {
  // Per-batch SearchSink may call AttachHNSW multiple times for the same
  // column; return the existing writer.
  if (auto it = _impl->hnsw_by_id.find(column_id);
      it != _impl->hnsw_by_id.end()) {
    auto& existing = *it->second;
    SDB_ASSERT(existing.info.d == info.d &&
                 existing.info.metric == info.metric &&
                 existing.info.m == info.m &&
                 existing.info.ef_construction == info.ef_construction,
               "columnstore::Writer::AttachHNSW: re-attach with mismatched "
               "HNSWInfo on column ",
               column_id);
    return *existing.writer;
  }
  SDB_ASSERT(_impl->column_by_id.contains(column_id),
             "columnstore::Writer::AttachHNSW: column ", column_id,
             " must be opened first");
  auto entry = std::make_unique<HNSWWriterEntry>();
  entry->column_id = column_id;
  entry->info = info;
  entry->writer = std::make_unique<HNSWWriter>(info);
  auto& back = *_impl->hnsw_writers.emplace_back(std::move(entry));
  _impl->hnsw_by_id.emplace(column_id, &back);
  return *back.writer;
}

std::span<const std::unique_ptr<NormColumnWriter>> Writer::NormWriters()
  const noexcept {
  return _impl->norm_writers;
}

NormColumnWriter* Writer::OpenNormColumn(std::string_view field_name) {
  if (!_impl->norm_column_options || !*_impl->norm_column_options) {
    return nullptr;
  }
  const auto opts = (*_impl->norm_column_options)(field_name);
  if (!field_limits::valid(opts.id)) {
    return nullptr;
  }
  return &OpenNormColumn(opts.id, opts.row_group_size);
}

NormColumnWriter& Writer::OpenNormColumn(field_id id, uint32_t row_group_size) {
  if (auto it = _impl->norm_by_id.find(id); it != _impl->norm_by_id.end()) {
    return *it->second;
  }
  auto cw = std::make_unique<NormColumnWriter>(id, row_group_size, *_impl->out);
  auto& back = *_impl->norm_writers.emplace_back(std::move(cw));
  _impl->norm_by_id.emplace(id, &back);
  return back;
}

std::string Writer::Commit(uint64_t target_row) {
  for (auto& cw : _impl->column_writers) {
    cw->Finalize();
  }
  for (auto& nw : _impl->norm_writers) {
    nw->PadTo(target_row);
    nw->Finalize();
  }
  // HNSW graphs build after column data is durable on disk so the writer
  // doesn't carry an in-memory vector cache during ingest.
  if (!_impl->hnsw_writers.empty()) {
    _impl->out->Flush();
    // TODO(mbkkt) measure seq vs rand for hnsw construction
    auto in = _impl->dir->open(_impl->filename, IOAdvice::RANDOM);
    if (!in) {
      throw IoError{absl::StrCat("failed to open columnstore for HNSW build: ",
                                 _impl->filename)};
    }
    ReadContext hnsw_ctx{*_impl->db, std::move(in)};
    for (auto& entry : _impl->hnsw_writers) {
      const FooterColumnEntry* col_entry = nullptr;
      for (auto& e : _impl->column_entries) {
        if (e->id == entry->column_id) {
          col_entry = e.get();
          break;
        }
      }
      SDB_ASSERT(
        col_entry,
        "columnstore::Writer::Commit: HNSW entry references missing column id ",
        entry->column_id);
      auto col_reader = MakeColumnReader(col_entry->id, Clone(col_entry->root));
      entry->writer->Build(*col_reader, hnsw_ctx);
    }
  }
  for (auto& entry : _impl->hnsw_writers) {
    entry->graph_offset = _impl->out->Position();
    entry->writer->Serialize(*_impl->out);
    entry->graph_byte_size = _impl->out->Position() - entry->graph_offset;
  }

  const uint64_t footer_offset = _impl->out->Position();

  IndexOutputWriteStream stream{*_impl->out};
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteList(
    kFooterSlotColumns, "columns", _impl->column_entries.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& e = *_impl->column_entries[i];
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", e.id);
        obj.WriteObject(1, "root", [&](duckdb::Serializer& root_obj) {
          SerializeColumnData(root_obj, e.root);
        });
      });
    });
  serializer.WriteList(
    kFooterSlotNormColumns, "norm_columns", _impl->norm_writers.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& nw = *_impl->norm_writers[i];
      SDB_ASSERT(!nw.Pointers().empty());
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", nw.Id());
        obj.WriteList(
          1, "row_groups", nw.Pointers().size(),
          [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
            const auto& p = nw.Pointers()[j];
            plist.WriteObject([&](duckdb::Serializer& pe) {
              pe.WriteProperty<uint8_t>(0, "byte_size", p.byte_size);
              pe.WriteProperty<uint32_t>(1, "row_count", p.row_count);
              pe.WriteProperty<uint32_t>(2, "max", p.max);
              pe.WriteProperty<uint64_t>(3, "sum", p.sum);
              pe.WriteProperty<uint64_t>(4, "non_zero_count", p.non_zero_count);
              pe.WriteProperty<uint64_t>(5, "file_offset", p.file_offset);
            });
          });
      });
    });
  serializer.WriteList(
    kFooterSlotHnswColumns, "hnsw_columns", _impl->hnsw_writers.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& e = *_impl->hnsw_writers[i];
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", e.column_id);
        obj.WriteProperty<uint64_t>(1, "graph_offset", e.graph_offset);
        obj.WriteProperty<uint64_t>(2, "graph_byte_size", e.graph_byte_size);
        obj.WriteProperty<uint64_t>(3, "max_doc", e.info.max_doc);
        obj.WriteProperty<uint8_t>(4, "metric",
                                   static_cast<uint8_t>(e.info.metric));
      });
    });
  serializer.End();

  _impl->out->WriteU64(footer_offset);
  format_utils::WriteFooter(*_impl->out);
  _impl->out.reset();
  _impl->committed = true;
  return _impl->filename;
}

PreloadedHnswGraphs Writer::TakeBuiltHnswGraphs() {
  PreloadedHnswGraphs out;
  if (!_impl) {
    return out;
  }
  out.reserve(_impl->hnsw_writers.size());
  for (auto& entry : _impl->hnsw_writers) {
    if (!entry || !entry->writer) {
      continue;
    }
    auto graph = entry->writer->Graph();
    if (!graph) {
      continue;
    }
    out.emplace(entry->column_id, std::move(graph));
  }
  return out;
}

void Writer::Rollback() noexcept {
  _impl->out.reset();
  _impl->committed = true;
}

struct Reader::Impl {
  duckdb::DatabaseInstance* db;
  IndexInput::ptr in;
  std::vector<std::unique_ptr<ColumnReader>> readers;
  sdb::containers::FlatHashMap<field_id, const ColumnReader*> by_id;
  std::vector<std::unique_ptr<NormColumnReader>> norm_readers;
  sdb::containers::FlatHashMap<field_id, const NormColumnReader*> norm_by_id;
  std::vector<std::unique_ptr<HNSWReader>> hnsw_readers;
  sdb::containers::FlatHashMap<field_id, const HNSWReader*> hnsw_by_id;
};

namespace {

IndexInput::ptr OpenAndCheckHeader(const Directory& dir,
                                   std::string_view filename) {
  auto in = dir.open(filename, IOAdvice::SEQUENTIAL);
  if (!in) {
    throw IoError{
      absl::StrCat("Failed to open columnstore file, path: ", filename)};
  }
  format_utils::CheckHeader(*in, kFormatName, kFormatVersion, kFormatVersion);
  return in;
}

std::span<const byte_type> ReadFooterBytes(
  IndexInput& in, std::string_view filename,
  std::vector<byte_type>& fallback_storage) {
  const auto file_len = in.Length();
  const uint64_t header_len =
    static_cast<uint64_t>(format_utils::HeaderLength(kFormatName));
  SDB_ENSURE(
    file_len > header_len + sizeof(uint64_t) + format_utils::kFooterLen,
    sdb::ERROR_SERVER_CORRUPTED_DATAFILE, "columnstore: truncated `.cs` file ",
    filename, " (length ", file_len,
    " is not large enough to contain header + footer offset + "
    "iresearch footer)");
  const uint64_t footer_offset_pos =
    file_len - format_utils::kFooterLen - sizeof(uint64_t);
  in.Seek(footer_offset_pos);
  const uint64_t footer_offset = in.ReadI64();
  SDB_ENSURE(footer_offset >= header_len && footer_offset < footer_offset_pos,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             "columnstore: corrupted `.cs` file ", filename, ": footer offset ",
             footer_offset, " is out of range [", header_len, ", ",
             footer_offset_pos, ")");
  const uint64_t footer_size = footer_offset_pos - footer_offset;
  if (const auto* view = in.ReadData(footer_offset, footer_size)) {
    return {view, static_cast<size_t>(footer_size)};
  }
  fallback_storage.resize(footer_size);
  in.ReadBytes(footer_offset, fallback_storage.data(), footer_size);
  return {fallback_storage.data(), fallback_storage.size()};
}

}  // namespace

void Reader::BuildColumnReaders(duckdb::BinaryDeserializer& deserializer,
                                duckdb::DatabaseInstance& /*db*/) {
  deserializer.ReadList(
    kFooterSlotColumns, "columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto id = obj.ReadProperty<uint64_t>(0, "id");
        PersistentColumnData root;
        obj.ReadObject(1, "root", [&](duckdb::Deserializer& robj) {
          root = DeserializeColumnData(robj);
        });
        auto reader = MakeColumnReader(id, std::move(root));
        _impl->readers.push_back(std::move(reader));
        _impl->by_id.emplace(id, _impl->readers.back().get());
      });
    });
}

void Reader::BuildNormReaders(duckdb::BinaryDeserializer& deserializer) {
  deserializer.ReadList(
    kFooterSlotNormColumns, "norm_columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto id = obj.ReadProperty<uint64_t>(0, "id");
        std::vector<NormRowGroupPointer> pointers;
        obj.ReadList(
          1, "row_groups",
          [&](duckdb::Deserializer::List& plist, duckdb::idx_t /*j*/) {
            plist.ReadObject([&](duckdb::Deserializer& pe) {
              NormRowGroupPointer p;
              p.byte_size = pe.ReadProperty<uint8_t>(0, "byte_size");
              p.row_count = pe.ReadProperty<uint32_t>(1, "row_count");
              p.max = pe.ReadProperty<uint32_t>(2, "max");
              p.sum = pe.ReadProperty<uint64_t>(3, "sum");
              p.non_zero_count = pe.ReadProperty<uint64_t>(4, "non_zero_count");
              p.file_offset = pe.ReadProperty<uint64_t>(5, "file_offset");
              SDB_ASSERT(
                (p.byte_size == 1 || p.byte_size == 2 || p.byte_size == 4) &&
                  p.row_count != 0,
                "columnstore: corrupt norm row-group (byte_size=", p.byte_size,
                ", row_count=", p.row_count, ") on column id ", id);
              pointers.push_back(p);
            });
          });
        if (pointers.empty()) {
          return;
        }
        auto nr = std::make_unique<NormColumnReader>(id, std::move(pointers),
                                                     *_impl->in);
        _impl->norm_readers.push_back(std::move(nr));
        _impl->norm_by_id.emplace(id, _impl->norm_readers.back().get());
      });
    });
}

void Reader::BuildHnswReaders(duckdb::BinaryDeserializer& deserializer,
                              const PreloadedHnswGraphs& preloaded) {
  deserializer.ReadList(
    kFooterSlotHnswColumns, "hnsw_columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto id = obj.ReadProperty<uint64_t>(0, "id");
        auto graph_offset = obj.ReadProperty<uint64_t>(1, "graph_offset");
        auto graph_byte_size = obj.ReadProperty<uint64_t>(2, "graph_byte_size");
        HNSWInfo info;
        info.max_doc =
          static_cast<doc_id_t>(obj.ReadProperty<uint64_t>(3, "max_doc"));
        info.metric =
          static_cast<HNSWMetric>(obj.ReadProperty<uint8_t>(4, "metric"));

        std::shared_ptr<const faiss::HNSW> hnsw;
        if (auto pit = preloaded.find(id); pit != preloaded.end()) {
          hnsw = pit->second;
        } else {
          // Footer is read from a MemoryReadStream, so seeking the
          // IndexInput here doesn't disturb the in-flight footer walk.
          _impl->in->Seek(graph_offset);
          auto mutable_hnsw = std::make_shared<faiss::HNSW>();
          irs::ReadHNSW(*_impl->in, *mutable_hnsw);
          SDB_ASSERT(_impl->in->Position() - graph_offset == graph_byte_size,
                     "ReadHNSW must consume exactly graph_byte_size bytes");
          hnsw = std::move(mutable_hnsw);
        }

        auto col_it = _impl->by_id.find(id);
        if (col_it == _impl->by_id.end()) {
          // Corrupted footer; HNSW(field_id) returns nullptr.
          return;
        }
        const auto* col_reader = col_it->second;
        const auto& col_type = col_reader->Type();
        if (col_type.id() == duckdb::LogicalTypeId::ARRAY) {
          info.d = static_cast<int>(duckdb::ArrayType::GetSize(col_type));
        }
        info.m = hnsw->nb_neighbors(1);
        info.ef_construction = hnsw->efConstruction;

        auto hnsw_reader =
          std::make_unique<HNSWReader>(id, std::move(hnsw), info, *col_reader);
        _impl->hnsw_readers.push_back(std::move(hnsw_reader));
        _impl->hnsw_by_id.emplace(id, _impl->hnsw_readers.back().get());
      });
    });
}

Reader::Reader(const Directory& dir, std::string_view segment_name,
               duckdb::DatabaseInstance& db,
               const PreloadedHnswGraphs& preloaded)
  : _impl{std::make_unique<Impl>()} {
  _impl->db = &db;
  const auto filename = absl::StrCat(segment_name, ".", kFormatExt);

  bool exists = false;
  if (!dir.exists(exists, filename)) {
    throw IoError{
      absl::StrCat("Failed to check existence of file, path: ", filename)};
  }
  if (!exists) {
    return;
  }

  _impl->in = OpenAndCheckHeader(dir, filename);
  std::vector<byte_type> footer_storage;  // unused when ReadView succeeds
  const auto footer_view =
    ReadFooterBytes(*_impl->in, filename, footer_storage);

  MemoryReadStream stream{footer_view.data(), footer_view.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::DatabaseInstance&>(db);
  deserializer.Begin();
  BuildColumnReaders(deserializer, db);
  BuildNormReaders(deserializer);
  BuildHnswReaders(deserializer, preloaded);
  deserializer.End();
}

Reader::~Reader() = default;

bool Reader::HasColumn(field_id id) const noexcept {
  return _impl->by_id.contains(id);
}

const ColumnReader* Reader::Column(field_id id) const noexcept {
  auto it = _impl->by_id.find(id);
  return it == _impl->by_id.end() ? nullptr : it->second;
}

std::span<const std::unique_ptr<ColumnReader>> Reader::Columns()
  const noexcept {
  return _impl->readers;
}

bool Reader::HasNormColumn(field_id id) const noexcept {
  return _impl->norm_by_id.contains(id);
}

const NormColumnReader* Reader::NormColumn(field_id id) const noexcept {
  auto it = _impl->norm_by_id.find(id);
  return it == _impl->norm_by_id.end() ? nullptr : it->second;
}

bool Reader::HasHNSW(field_id id) const noexcept {
  return _impl->hnsw_by_id.contains(id);
}

const HNSWReader* Reader::HNSW(field_id id) const noexcept {
  auto it = _impl->hnsw_by_id.find(id);
  return it == _impl->hnsw_by_id.end() ? nullptr : it->second;
}

IndexInput::ptr Reader::ReopenIn() const { return _impl->in->Reopen(); }

duckdb::DatabaseInstance& Reader::Database() const noexcept {
  return *_impl->db;
}

}  // namespace irs::columnstore
