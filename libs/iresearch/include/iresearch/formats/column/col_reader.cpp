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

#include "iresearch/formats/column/col_reader.hpp"

#include <absl/strings/str_cat.h>

#include <cstring>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/data_pointer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/error/error.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/persistent_column_data.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/format_utils.hpp"
#include "iresearch/formats/serializer_stream.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/directory.hpp"

namespace irs {
namespace {

// Footer slot ids; stable, never reuse. Must match col_writer.cpp.
constexpr duckdb::field_id_t kFooterSlotColumns = 100;
constexpr duckdb::field_id_t kFooterSlotNormColumns = 101;

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
  obj.ReadList(
    4, "variant_layouts",
    [&](duckdb::Deserializer::List& vlist, duckdb::idx_t /*j*/) {
      vlist.ReadObject([&](duckdb::Deserializer& vo) {
        auto& layout = node.variant_layouts.emplace_back();
        layout.row_start = vo.ReadProperty<uint64_t>(0, "row_start");
        layout.row_count = vo.ReadProperty<uint64_t>(1, "row_count");
        const auto shred_state = magic_enum::enum_cast<VariantShredState>(
          vo.ReadProperty<uint8_t>(2, "shred_state"));
        SDB_ENSURE(shred_state, sdb::ERROR_INTERNAL,
                   "corrupt columnstore footer: invalid variant shred_state");
        layout.shred_state = *shred_state;
        vo.ReadObject(3, "unshredded", [&](duckdb::Deserializer& u) {
          layout.unshredded =
            std::make_unique<PersistentColumnData>(DeserializeColumnData(u));
        });
        if (layout.shred_state != VariantShredState::Unshredded) {
          vo.ReadObject(4, "shredded_node", [&](duckdb::Deserializer& s) {
            layout.shredded_node =
              std::make_unique<PersistentColumnData>(DeserializeColumnData(s));
          });
        }
      });
    });
  node.fully_shredded =
    obj.ReadPropertyWithExplicitDefault<bool>(5, "fully_shredded", true);
  return node;
}

IndexInput::ptr OpenAndCheckHeader(const Directory& dir,
                                   std::string_view filename) {
  auto in = dir.open(filename, IOAdvice::SEQUENTIAL);
  if (!in) {
    throw IoError{absl::StrCat("Failed to open .col file, path: ", filename)};
  }
  format_utils::CheckHeader(*in, kColFormatName, kColFormatVersion,
                            kColFormatVersion);
  return in;
}

std::span<const byte_type> ReadFooterBytes(
  IndexInput& in, std::string_view filename,
  std::vector<byte_type>& fallback_storage) {
  const auto file_len = in.Length();
  const uint64_t header_len =
    static_cast<uint64_t>(format_utils::HeaderLength(kColFormatName));
  SDB_ENSURE(
    file_len > header_len + sizeof(uint64_t) + format_utils::kFooterLen,
    sdb::ERROR_SERVER_CORRUPTED_DATAFILE, ".col reader: truncated `.col` file ",
    filename, " (length ", file_len,
    " is not large enough to contain header + footer offset + "
    "iresearch footer)");
  const uint64_t footer_offset_pos =
    file_len - format_utils::kFooterLen - sizeof(uint64_t);
  in.Seek(footer_offset_pos);
  const uint64_t footer_offset = in.ReadI64();
  SDB_ENSURE(footer_offset >= header_len && footer_offset < footer_offset_pos,
             sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
             ".col reader: corrupted `.col` file ", filename,
             ": footer offset ", footer_offset, " is out of range [",
             header_len, ", ", footer_offset_pos, ")");
  const uint64_t footer_size = footer_offset_pos - footer_offset;
  if (const auto* view = in.ReadData(footer_offset, footer_size)) {
    return {view, static_cast<size_t>(footer_size)};
  }
  fallback_storage.resize(footer_size);
  in.ReadBytes(footer_offset, fallback_storage.data(), footer_size);
  return {fallback_storage.data(), fallback_storage.size()};
}

}  // namespace

struct ColReader::Impl {
  duckdb::DatabaseInstance* db;
  IndexInput::ptr in;
  std::vector<std::unique_ptr<ColumnReader>> readers;
  sdb::containers::FlatHashMap<field_id, const ColumnReader*> by_id;
  std::vector<std::unique_ptr<NormColumnReader>> norm_readers;
  sdb::containers::FlatHashMap<field_id, const NormColumnReader*> norm_by_id;
};

void ColReader::BuildColumnReaders(duckdb::BinaryDeserializer& deserializer,
                                   duckdb::DatabaseInstance& db) {
  // Shared anchor for the zero-copy block wrappers: one duplicate of the
  // mmap'd input keeps the mapping alive for as long as any reader block
  // (or a Vector pinning it) is around.
  const ColumnBlockSource source{&db,
                                 std::shared_ptr<IndexInput>{_impl->in->Dup()}};
  deserializer.ReadList(
    kFooterSlotColumns, "columns",
    [&](duckdb::Deserializer::List& list, duckdb::idx_t /*i*/) {
      list.ReadObject([&](duckdb::Deserializer& obj) {
        auto id = obj.ReadProperty<uint64_t>(0, "id");
        PersistentColumnData root;
        obj.ReadObject(1, "root", [&](duckdb::Deserializer& robj) {
          root = DeserializeColumnData(robj);
        });
        auto reader = MakeColumnReader(id, std::move(root), source);
        _impl->readers.push_back(std::move(reader));
        auto [it, ok] = _impl->by_id.emplace(id, _impl->readers.back().get());
        SDB_ENSURE(ok, sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
                   ".col footer: duplicate column field_id ", id);
      });
    });
}

void ColReader::BuildNormReaders(duckdb::BinaryDeserializer& deserializer) {
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
                ".col reader: corrupt norm row-group (byte_size=", p.byte_size,
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
        auto [it, ok] =
          _impl->norm_by_id.emplace(id, _impl->norm_readers.back().get());
        SDB_ENSURE(ok, sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
                   ".col footer: duplicate norm field_id ", id);
      });
    });
}

ColReader::ColReader(const Directory& dir, std::string_view segment_name,
                     duckdb::DatabaseInstance& db)
  : _impl{std::make_unique<Impl>()} {
  _impl->db = &db;
  const auto filename = absl::StrCat(segment_name, ".", kColFormatExt);

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
  deserializer.End();
}

ColReader::~ColReader() = default;

bool ColReader::HasColumn(field_id id) const noexcept {
  return _impl->by_id.contains(id);
}

const ColumnReader* ColReader::Column(field_id id) const noexcept {
  auto it = _impl->by_id.find(id);
  return it == _impl->by_id.end() ? nullptr : it->second;
}

std::span<const std::unique_ptr<ColumnReader>> ColReader::Columns()
  const noexcept {
  return _impl->readers;
}

bool ColReader::HasNormColumn(field_id id) const noexcept {
  return _impl->norm_by_id.contains(id);
}

const NormColumnReader* ColReader::NormColumn(field_id id) const noexcept {
  auto it = _impl->norm_by_id.find(id);
  return it == _impl->norm_by_id.end() ? nullptr : it->second;
}

IndexInput::ptr ColReader::ReopenIn() const { return _impl->in->Reopen(); }

duckdb::DatabaseInstance& ColReader::Database() const noexcept {
  return *_impl->db;
}

}  // namespace irs
