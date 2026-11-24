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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void SerializeColumnBlockMeta(duckdb::BinarySerializer& s,
                              const ColumnBlockMeta& m) {
  s.WriteProperty(0, "compression_type", static_cast<uint8_t>(m.codec->type));
  s.WriteProperty(1, "tuple_count", m.tuple_count);
  s.WriteProperty(2, "file_offset", m.file_offset);
  s.WriteProperty(3, "byte_size", m.byte_size);
  s.WriteProperty(4, "statistics", m.statistics);
  s.WritePropertyWithDefault<uint64_t>(5, "shared_offset", m.shared_offset, 0);
  s.WritePropertyWithDefault<uint64_t>(6, "shared_size", m.shared_size, 0);
}

ColumnBlockMeta DeserializeColumnBlockMeta(duckdb::BinaryDeserializer& d,
                                           duckdb::PhysicalType physical) {
  const auto compression_type = d.ReadProperty<uint8_t>(0, "compression_type");
  const auto tuple_count = d.ReadProperty<uint64_t>(1, "tuple_count");
  const auto file_offset = d.ReadProperty<uint64_t>(2, "file_offset");
  const auto byte_size = d.ReadProperty<uint64_t>(3, "byte_size");
  auto stats = d.ReadProperty<duckdb::BaseStatistics>(4, "statistics");
  const auto shared_offset =
    d.ReadPropertyWithExplicitDefault<uint64_t>(5, "shared_offset", 0);
  const auto shared_size =
    d.ReadPropertyWithExplicitDefault<uint64_t>(6, "shared_size", 0);
  const auto* codec =
    codecs::GetCodec(codecs::CodecType{compression_type}, physical);
  if (!codec) [[unlikely]] {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("Column block uses compression ",
              static_cast<uint32_t>(compression_type),
              ", which this release of SereneDB does not have; it was written "
              "by a newer release"));
  }
  return ColumnBlockMeta{std::move(stats), tuple_count, file_offset,
                         byte_size,        codec,       shared_offset,
                         shared_size};
}

template<typename T, typename Write>
void WriteObjects(duckdb::BinarySerializer& s, duckdb::field_id_t id,
                  const char* tag, const std::vector<T>& items, Write&& write) {
  s.WriteOptionalList(
    id, tag, items.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      list.WriteObject(
        [&](duckdb::BinarySerializer& obj) { write(obj, items[i]); });
    });
}

template<typename Read>
void ReadObjects(duckdb::BinaryDeserializer& d, duckdb::field_id_t id,
                 const char* tag, Read&& read) {
  d.ReadOptionalList(
    id, tag, [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
      list.ReadObject(read);
    });
}

void ReadBlockMetas(duckdb::BinaryDeserializer& d, duckdb::field_id_t id,
                    const char* tag, const duckdb::LogicalType& type,
                    std::vector<ColumnBlockMeta>& out) {
  d.Set<const duckdb::LogicalType&>(type);
  ReadObjects(d, id, tag, [&](duckdb::BinaryDeserializer& obj) {
    out.push_back(DeserializeColumnBlockMeta(obj, type.InternalType()));
  });
  d.Unset<const duckdb::LogicalType>();
}

}  // namespace

void SerializeColumnMeta(duckdb::BinarySerializer& s, const ColumnMeta& meta) {
  s.WriteProperty(0, "id", meta.id);
  s.WriteProperty(1, "type", meta.type);
  WriteObjects(s, 2, "data", meta.data, SerializeColumnBlockMeta);
  WriteObjects(s, 3, "validity", meta.validity, SerializeColumnBlockMeta);
  WriteObjects(s, 4, "children", meta.children, SerializeColumnMeta);
  WriteObjects(s, 5, "variant_rgs", meta.variant_rgs,
               [](duckdb::BinarySerializer& obj, const VariantRgMeta& rg) {
                 obj.WriteProperty(0, "row_count", rg.row_count);
                 obj.WriteObject(
                   1, "unshredded", [&](duckdb::BinarySerializer& unshredded) {
                     SerializeColumnMeta(unshredded, *rg.unshredded);
                   });
                 obj.WriteOptionalObject(
                   2, "shredded", rg.shredded != nullptr,
                   [&](duckdb::BinarySerializer& shredded) {
                     SerializeColumnMeta(shredded, *rg.shredded);
                   });
               });
  s.WritePropertyWithDefault(7, "distinct", meta.distinct);
  s.WritePropertyWithDefault<uint8_t>(8, "compression_level",
                                      meta.compression_level, 0);
  WriteObjects(s, 9, "list_parts", meta.list_parts,
               [](duckdb::BinarySerializer& obj, const ListPart& part) {
                 obj.WriteProperty(0, "row", part.row);
                 obj.WriteProperty(1, "code", part.code);
                 obj.WriteProperty(2, "elem", part.elem);
               });
}

ColumnMeta DeserializeColumnMeta(duckdb::BinaryDeserializer& d) {
  ColumnMeta meta;
  d.ReadProperty(0, "id", meta.id);
  d.ReadProperty(1, "type", meta.type);
  ReadBlockMetas(d, 2, "data", meta.type, meta.data);
  ReadBlockMetas(d, 3, "validity", duckdb::LogicalTypeId::VALIDITY,
                 meta.validity);
  ReadObjects(d, 4, "children", [&](duckdb::BinaryDeserializer& obj) {
    meta.children.push_back(DeserializeColumnMeta(obj));
  });
  ReadObjects(d, 5, "variant_rgs", [&](duckdb::BinaryDeserializer& obj) {
    auto& rg = meta.variant_rgs.emplace_back();
    obj.ReadProperty(0, "row_count", rg.row_count);
    obj.ReadObject(1, "unshredded", [&](duckdb::BinaryDeserializer& in) {
      rg.unshredded = std::make_unique<ColumnMeta>(DeserializeColumnMeta(in));
    });
    obj.ReadOptionalObject(2, "shredded", [&](duckdb::BinaryDeserializer& in) {
      rg.shredded = std::make_unique<ColumnMeta>(DeserializeColumnMeta(in));
    });
  });
  d.ReadPropertyWithDefault(7, "distinct", meta.distinct);
  meta.compression_level =
    d.ReadPropertyWithExplicitDefault<uint8_t>(8, "compression_level", 0);
  ReadObjects(d, 9, "list_parts", [&](duckdb::BinaryDeserializer& obj) {
    auto& part = meta.list_parts.emplace_back();
    obj.ReadProperty(0, "row", part.row);
    obj.ReadProperty(1, "code", part.code);
    obj.ReadProperty(2, "elem", part.elem);
  });
  return meta;
}

}  // namespace irs
