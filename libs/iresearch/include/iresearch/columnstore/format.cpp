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

#include "iresearch/columnstore/format.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/data_pointer.hpp>
#include <utility>
#include <vector>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/hnsw.hpp"
#include "iresearch/columnstore/internal/persistent_column_data.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/columnstore/norm_writer.hpp"
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

std::unique_ptr<ColumnReader> MakeColumnReader(field_id id, std::string name,
                                               PersistentColumnData&& node,
                                               IndexInput& in,
                                               duckdb::DatabaseInstance& db) {
  switch (node.type.id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      SDB_ASSERT(node.child_columns.size() == 1);
      const auto array_size =
        static_cast<uint64_t>(duckdb::ArrayType::GetSize(node.type));
      auto child =
        MakeColumnReader(field_limits::invalid(), {},
                         std::move(node.child_columns.front()), in, db);
      return std::make_unique<ColumnReader>(
        id, std::move(name), std::move(node.type),
        std::move(node.validity_pointers), std::move(child), array_size, in,
        db);
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      // MAP rides on the LIST path: PhysicalType::LIST with a
      // STRUCT<key, value> element. ListType::GetChildType returns the
      // STRUCT, which recurses through the STRUCT case below.
      SDB_ASSERT(node.child_columns.size() == 1);
      auto child =
        MakeColumnReader(field_limits::invalid(), {},
                         std::move(node.child_columns.front()), in, db);
      return std::make_unique<ColumnReader>(
        id, std::move(name), std::move(node.type), std::move(node.pointers),
        std::move(node.validity_pointers), std::move(child), in, db);
    }
    case duckdb::LogicalTypeId::STRUCT: {
      std::vector<std::unique_ptr<ColumnReader>> fields;
      fields.reserve(node.child_columns.size());
      for (auto& cn : node.child_columns) {
        fields.push_back(
          MakeColumnReader(field_limits::invalid(), {}, std::move(cn), in, db));
      }
      return std::make_unique<ColumnReader>(
        id, std::move(name), std::move(node.type),
        std::move(node.validity_pointers), std::move(fields), in, db);
    }
    default: {
      return std::make_unique<ColumnReader>(
        id, std::move(name), std::move(node.type), std::move(node.pointers),
        std::move(node.validity_pointers), in, db);
    }
  }
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
  IndexOutput::ptr out;
  std::vector<std::unique_ptr<ColumnWriter>> column_writers;
  absl::flat_hash_map<field_id, size_t> column_by_id;
  // unique_ptr to keep FooterColumnEntry* handed to ColumnWriter stable
  // across vector emplaces.
  std::vector<std::unique_ptr<FooterColumnEntry>> column_entries;
  std::vector<std::unique_ptr<NormColumnWriter>> norm_writers;
  absl::flat_hash_map<field_id, size_t> norm_by_id;
  std::vector<std::unique_ptr<HNSWWriterEntry>> hnsw_writers;
  absl::flat_hash_map<field_id, size_t> hnsw_by_id;
  bool committed = false;
  field_id next_id = 0;
};

Writer::Writer(Directory& dir, std::string_view segment_name,
               duckdb::DatabaseInstance& db)
  : _impl{std::make_unique<Impl>()} {
  _impl->dir = &dir;
  _impl->segment_name = std::string{segment_name};
  _impl->db = &db;
  _impl->filename = absl::StrCat(segment_name, ".", kFormatExt);
  _impl->out = dir.create(_impl->filename);
  if (!_impl->out) {
    throw IoError{
      absl::StrCat("failed to create columnstore file: ", _impl->filename)};
  }
  format_utils::WriteHeader(*_impl->out, kFormatName, kFormatVersion);
}

Writer::~Writer() {
  if (_impl && !_impl->committed) {
    Rollback();
  }
}

field_id Writer::AllocateColumnId() noexcept { return _impl->next_id++; }

ColumnWriter& Writer::OpenColumn(field_id id, std::string_view name,
                                 duckdb::LogicalType type,
                                 uint64_t row_group_size, bool skip_validity,
                                 duckdb::CompressionType compression) {
  // Per-batch SearchSink may re-open the same id; return the existing
  // writer so batches accumulate into one footer entry.
  if (auto it = _impl->column_by_id.find(id); it != _impl->column_by_id.end()) {
    auto& existing = *_impl->column_writers[it->second];
    const auto normalized_row_group_size =
      row_group_size != 0 ? row_group_size : kDefaultRowGroupSize;
    SDB_ASSERT(existing.Type() == type &&
                 existing.RowGroupSize() == normalized_row_group_size &&
                 existing.SkipValidity() == skip_validity &&
                 existing.Compression() == compression,
               "columnstore::Writer::OpenColumn: re-opened id ", id,
               " with mismatched settings (type ", type.ToString(), " vs ",
               existing.Type().ToString(), ", row_group_size ",
               normalized_row_group_size, " vs ", existing.RowGroupSize(),
               ", skip_validity ", skip_validity, " vs ",
               existing.SkipValidity(), ", compression ",
               duckdb::CompressionTypeToString(compression), " vs ",
               duckdb::CompressionTypeToString(existing.Compression()), ")");
    return existing;
  }
  auto entry = std::make_unique<FooterColumnEntry>();
  entry->id = id;
  entry->name = std::string{name};
  entry->root.type = type;
  auto cw = std::make_unique<ColumnWriter>(id, std::string{name}, type,
                                           row_group_size, *_impl->db,
                                           *_impl->out, *entry, skip_validity);
  cw->SetCompression(compression);
  _impl->column_entries.push_back(std::move(entry));
  _impl->column_by_id.emplace(id, _impl->column_writers.size());
  _impl->column_writers.push_back(std::move(cw));
  return *_impl->column_writers.back();
}

HNSWWriter& Writer::AttachHNSW(field_id column_id, HNSWInfo info) {
  // Per-batch SearchSink may call AttachHNSW multiple times for the same
  // column; return the existing writer.
  if (auto it = _impl->hnsw_by_id.find(column_id);
      it != _impl->hnsw_by_id.end()) {
    auto& existing = *_impl->hnsw_writers[it->second];
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
  _impl->hnsw_by_id.emplace(column_id, _impl->hnsw_writers.size());
  _impl->hnsw_writers.push_back(std::move(entry));
  return *_impl->hnsw_writers.back()->writer;
}

NormColumnWriter& Writer::OpenNormColumn(field_id id, std::string_view name,
                                         uint64_t row_group_size) {
  // Same dedup contract as OpenColumn; norm + typed maps are separate
  // in the Reader so the same id may appear in both.
  if (auto it = _impl->norm_by_id.find(id); it != _impl->norm_by_id.end()) {
    return *_impl->norm_writers[it->second];
  }
  auto cw = std::make_unique<NormColumnWriter>(
    id, std::string{name},
    row_group_size != 0 ? row_group_size : kDefaultRowGroupSize, *_impl->out);
  _impl->norm_by_id.emplace(id, _impl->norm_writers.size());
  _impl->norm_writers.push_back(std::move(cw));
  return *_impl->norm_writers.back();
}

std::string Writer::Commit() {
  for (auto& cw : _impl->column_writers) {
    cw->Finalize();
  }
  for (auto& nw : _impl->norm_writers) {
    nw->Finalize();
  }
  // HNSW graphs build after column data is durable on disk so the writer
  // doesn't carry an in-memory vector cache during ingest.
  if (!_impl->hnsw_writers.empty()) {
    _impl->out->Flush();
    auto in = _impl->dir->open(_impl->filename, IOAdvice::RANDOM);
    if (!in) {
      throw IoError{absl::StrCat("failed to open columnstore for HNSW build: ",
                                 _impl->filename)};
    }
    // DataPointer is move-only; clone the metadata via a serialize /
    // deserialize round-trip so the footer write still has the original
    // pointers to emit. TODO(perf): add a real Clone().
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
      duckdb::MemoryStream mem_out;
      duckdb::BinarySerializer ser{mem_out};
      ser.Begin();
      ser.WriteObject(0, "root", [&](duckdb::Serializer& obj) {
        SerializeColumnData(obj, col_entry->root);
      });
      ser.End();
      MemoryReadStream mem_in{
        reinterpret_cast<const byte_type*>(mem_out.GetData()),
        static_cast<uint64_t>(mem_out.GetPosition())};
      duckdb::BinaryDeserializer deser{mem_in};
      deser.Set<duckdb::DatabaseInstance&>(*_impl->db);
      deser.Begin();
      PersistentColumnData root_clone;
      deser.ReadObject(0, "root", [&](duckdb::Deserializer& obj) {
        root_clone = DeserializeColumnData(obj);
      });
      deser.End();
      auto col_reader = MakeColumnReader(
        col_entry->id, col_entry->name, std::move(root_clone), *in, *_impl->db);
      entry->writer->Build(*col_reader);
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
        obj.WriteProperty(1, "name", e.name);
        obj.WriteObject(2, "root", [&](duckdb::Serializer& root_obj) {
          SerializeColumnData(root_obj, e.root);
        });
      });
    });
  serializer.WriteList(
    kFooterSlotNormColumns, "norm_columns", _impl->norm_writers.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& nw = *_impl->norm_writers[i];
      list.WriteObject([&](duckdb::Serializer& obj) {
        obj.WriteProperty<uint64_t>(0, "id", nw.Id());
        obj.WriteProperty(1, "name", nw.Name());
        obj.WriteList(
          2, "row_groups", nw.Pointers().size(),
          [&](duckdb::Serializer::List& plist, duckdb::idx_t j) {
            const auto& p = nw.Pointers()[j];
            plist.WriteObject([&](duckdb::Serializer& pe) {
              pe.WriteProperty<uint8_t>(0, "byte_size", p.byte_size);
              pe.WriteProperty<uint64_t>(1, "row_count", p.row_count);
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
        obj.WriteProperty<int32_t>(4, "d", e.info.d);
        obj.WriteProperty<int32_t>(5, "m", e.info.m);
        obj.WriteProperty<uint8_t>(6, "metric",
                                   static_cast<uint8_t>(e.info.metric));
        obj.WriteProperty<int32_t>(7, "ef_construction",
                                   e.info.ef_construction);
      });
    });
  serializer.End();

  _impl->out->WriteU64(footer_offset);
  format_utils::WriteFooter(*_impl->out);
  _impl->out.reset();
  _impl->committed = true;
  return _impl->filename;
}

void Writer::Rollback() noexcept {
  _impl->out.reset();
  if (!_impl->filename.empty()) {
    try {
      _impl->dir->remove(_impl->filename);
    } catch (...) {
    }
  }
  _impl->committed = true;
}

struct Reader::Impl {
  duckdb::DatabaseInstance* db;
  IndexInput::ptr in;
  std::vector<std::unique_ptr<ColumnReader>> readers;
  absl::flat_hash_map<field_id, size_t> by_id;
  std::vector<std::unique_ptr<NormColumnReader>> norm_readers;
  absl::flat_hash_map<field_id, size_t> norm_by_id;
  std::vector<std::unique_ptr<HNSWReader>> hnsw_readers;
  absl::flat_hash_map<field_id, size_t> hnsw_by_id;
};

Reader::Reader(const Directory& dir, std::string_view segment_name,
               duckdb::DatabaseInstance& db)
  : _impl{std::make_unique<Impl>()} {
  _impl->db = &db;
  const auto filename = absl::StrCat(segment_name, ".", kFormatExt);
  _impl->in = dir.open(filename, IOAdvice::RANDOM);
  if (!_impl->in) {
    return;  // Segment has no `.cs` file.
  }

  format_utils::CheckHeader(*_impl->in, kFormatName, kFormatVersion,
                            kFormatVersion);

  const auto file_len = _impl->in->Length();
  const uint64_t kIrsFooterLen = format_utils::kFooterLen;
  const uint64_t header_len =
    static_cast<uint64_t>(format_utils::HeaderLength(kFormatName));
  SDB_ENSURE(file_len > header_len + sizeof(uint64_t) + kIrsFooterLen,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             "columnstore: truncated `.cs` file ", filename, " (length ",
             file_len,
             " is not large enough to contain header + footer offset + "
             "iresearch footer)");
  const uint64_t footer_offset_pos =
    file_len - kIrsFooterLen - sizeof(uint64_t);
  _impl->in->Seek(footer_offset_pos);
  const uint64_t footer_offset = _impl->in->ReadI64();

  SDB_ENSURE(footer_offset >= header_len && footer_offset < footer_offset_pos,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             "columnstore: corrupted `.cs` file ", filename, ": footer offset ",
             footer_offset, " is out of range [", header_len, ", ",
             footer_offset_pos, ")");
  const uint64_t footer_size = footer_offset_pos - footer_offset;
  std::vector<byte_type> footer_buf(footer_size);
  _impl->in->Seek(footer_offset);
  _impl->in->ReadBytes(footer_buf.data(), footer_size);

  MemoryReadStream stream{footer_buf.data(), footer_size};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::DatabaseInstance&>(db);

  deserializer.Begin();
  deserializer.ReadList(
    kFooterSlotColumns, "columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto id = obj.ReadProperty<uint64_t>(0, "id");
        auto name = obj.ReadProperty<std::string>(1, "name");
        PersistentColumnData root;
        obj.ReadObject(2, "root", [&](duckdb::Deserializer& robj) {
          root = DeserializeColumnData(robj);
        });
        auto reader = MakeColumnReader(id, std::move(name), std::move(root),
                                       *_impl->in, db);
        _impl->by_id.emplace(id, _impl->readers.size());
        _impl->readers.push_back(std::move(reader));
      });
    });
  deserializer.ReadList(
    kFooterSlotNormColumns, "norm_columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto id = obj.ReadProperty<uint64_t>(0, "id");
        auto name = obj.ReadProperty<std::string>(1, "name");
        std::vector<NormRowGroupPointer> pointers;
        obj.ReadList(
          2, "row_groups",
          [&](duckdb::Deserializer::List& plist, duckdb::idx_t /*j*/) {
            plist.ReadObject([&](duckdb::Deserializer& pe) {
              NormRowGroupPointer p;
              p.byte_size = pe.ReadProperty<uint8_t>(0, "byte_size");
              p.row_count = pe.ReadProperty<uint64_t>(1, "row_count");
              p.max = pe.ReadProperty<uint32_t>(2, "max");
              p.sum = pe.ReadProperty<uint64_t>(3, "sum");
              p.non_zero_count = pe.ReadProperty<uint64_t>(4, "non_zero_count");
              p.file_offset = pe.ReadProperty<uint64_t>(5, "file_offset");
              pointers.push_back(p);
            });
          });
        auto nr = std::make_unique<NormColumnReader>(
          id, std::move(name), std::move(pointers), *_impl->in);
        _impl->norm_by_id.emplace(id, _impl->norm_readers.size());
        _impl->norm_readers.push_back(std::move(nr));
      });
    });
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
        info.d = obj.ReadProperty<int32_t>(4, "d");
        info.m = obj.ReadProperty<int32_t>(5, "m");
        info.metric =
          static_cast<HNSWMetric>(obj.ReadProperty<uint8_t>(6, "metric"));
        info.ef_construction = obj.ReadProperty<int32_t>(7, "ef_construction");
        // Footer is read from a MemoryReadStream, so seeking the IndexInput
        // here doesn't disturb the in-flight footer walk.
        _impl->in->Seek(graph_offset);
        faiss::HNSW hnsw;
        irs::ReadHNSW(*_impl->in, hnsw);
        SDB_ASSERT(_impl->in->Position() - graph_offset == graph_byte_size,
                   "ReadHNSW must consume exactly graph_byte_size bytes");

        const ColumnReader* col_reader = nullptr;
        auto col_it = _impl->by_id.find(id);
        if (col_it != _impl->by_id.end()) {
          col_reader = _impl->readers[col_it->second].get();
        }
        if (!col_reader) {
          // Corrupted footer; HNSW(field_id) returns nullptr.
          return;
        }
        auto hr =
          std::make_unique<HNSWReader>(id, std::string{col_reader->Name()},
                                       std::move(hnsw), info, *col_reader);
        _impl->hnsw_by_id.emplace(id, _impl->hnsw_readers.size());
        _impl->hnsw_readers.push_back(std::move(hr));
      });
    });
  deserializer.End();
}

Reader::~Reader() = default;

bool Reader::HasColumn(field_id id) const noexcept {
  return _impl->by_id.contains(id);
}

const ColumnReader* Reader::Column(field_id id) const noexcept {
  auto it = _impl->by_id.find(id);
  if (it == _impl->by_id.end()) {
    return nullptr;
  }
  return _impl->readers[it->second].get();
}

std::vector<const ColumnReader*> Reader::Columns() const {
  std::vector<const ColumnReader*> out;
  out.reserve(_impl->readers.size());
  for (auto& r : _impl->readers) {
    out.push_back(r.get());
  }
  return out;
}

bool Reader::HasNormColumn(field_id id) const noexcept {
  return _impl->norm_by_id.contains(id);
}

const NormColumnReader* Reader::NormColumn(field_id id) const noexcept {
  auto it = _impl->norm_by_id.find(id);
  if (it == _impl->norm_by_id.end()) {
    return nullptr;
  }
  return _impl->norm_readers[it->second].get();
}

bool Reader::HasHNSW(field_id id) const noexcept {
  return _impl->hnsw_by_id.contains(id);
}

const HNSWReader* Reader::HNSW(field_id id) const noexcept {
  auto it = _impl->hnsw_by_id.find(id);
  if (it == _impl->hnsw_by_id.end()) {
    return nullptr;
  }
  return _impl->hnsw_readers[it->second].get();
}

std::vector<const NormColumnReader*> Reader::NormColumns() const {
  std::vector<const NormColumnReader*> out;
  out.reserve(_impl->norm_readers.size());
  for (auto& r : _impl->norm_readers) {
    out.push_back(r.get());
  }
  return out;
}

}  // namespace irs::columnstore
