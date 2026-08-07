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

#include "catalog/store/store_op.h"

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <utility>

#include "basics/serialization.h"

namespace sdb::catalog::store_op {
namespace {

// Which duckdb entry point reads the payload back. The two halves of the parse
// hierarchy do not share one -- a CreateInfo writes its CatalogType where a
// ParseInfo writes its ParseInfoType, and neither dispatch knows the other --
// so the record names the half it holds, the way duckdb's WAL types do.
enum class Payload : uint8_t {
  None = 0,
  Parse = 1,
  Create = 2,
};

Payload PayloadOf(const duckdb::ParseInfo* info) noexcept {
  if (info == nullptr) {
    return Payload::None;
  }
  return info->info_type == duckdb::ParseInfoType::CREATE_INFO ? Payload::Create
                                                               : Payload::Parse;
}

void WriteId(duckdb::MemoryStream& s, ObjectId id) {
  s.Write<uint64_t>(id.id());
}

ObjectId ReadId(duckdb::MemoryStream& s) {
  return ObjectId{s.Read<uint64_t>()};
}

}  // namespace

bool IsDestructive(const Targeted& op) noexcept {
  if (!op.info) {
    return false;
  }
  if (op.info->info_type == duckdb::ParseInfoType::DROP_INFO) {
    return true;
  }
  if (op.info->info_type != duckdb::ParseInfoType::ALTER_INFO) {
    return false;
  }
  const auto& alter = op.info->Cast<duckdb::AlterInfo>();
  if (alter.type != duckdb::AlterType::ALTER_TABLE) {
    return false;
  }
  switch (alter.Cast<duckdb::AlterTableInfo>().alter_table_type) {
    using enum duckdb::AlterTableType;
    case REMOVE_COLUMN:
    case DROP_NOT_NULL:
    case DROP_CONSTRAINT:
      return true;
    default:
      return false;
  }
}

void SerializeOps(std::span<const Targeted> ops, duckdb::MemoryStream& stream) {
  stream.Write<uint32_t>(static_cast<uint32_t>(ops.size()));
  for (const auto& op : ops) {
    WriteId(stream, op.relation_id);
    stream.Write<uint8_t>(static_cast<uint8_t>(PayloadOf(op.info.get())));
    if (op.info != nullptr) {
      duckdb::BinarySerializer::Serialize(*op.info, stream,
                                          duckdb::VersionStorageOptions());
    }
  }
}

std::vector<Targeted> DeserializeOps(ObjectId database_id,
                                     duckdb::MemoryStream& stream) {
  const auto count = stream.Read<uint32_t>();
  std::vector<Targeted> ops;
  ops.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    Targeted op{.database_id = database_id, .relation_id = ReadId(stream)};
    switch (static_cast<Payload>(stream.Read<uint8_t>())) {
      case Payload::None:
        break;
      case Payload::Parse:
        op.info =
          duckdb::BinaryDeserializer::Deserialize<duckdb::ParseInfo>(stream);
        break;
      case Payload::Create:
        op.info =
          duckdb::BinaryDeserializer::Deserialize<duckdb::CreateInfo>(stream);
        break;
    }
    ops.push_back(std::move(op));
  }
  return ops;
}

}  // namespace sdb::catalog::store_op
