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
#include <utility>

#include "basics/assert.h"
#include "basics/log.h"
#include "basics/serialization.h"

namespace sdb::catalog::store_op {
namespace {

// The tag an op is written under. Explicit values: the log outlives the
// process, so a reordered variant must not silently redefine what is on disk.
enum class Tag : uint8_t {
  CreateTable = 0,
  DropTable = 1,
  AddColumn = 2,
  DropColumn = 3,
  ChangeColumnType = 4,
  AddNotNull = 5,
  DropNotNull = 6,
  AddCheck = 7,
  DropCheck = 8,
  AddPrimaryKey = 9,
  AddUnique = 10,
  CreateIndex = 12,
  DropIndex = 13,
  RenameColumn = 14,
  RenameIndex = 15,
};

Tag TagOf(const Op& op) noexcept {
  return std::visit(
    [](const auto& o) {
      using T = std::decay_t<decltype(o)>;
      if constexpr (std::is_same_v<T, store_op::CreateTable>) {
        return Tag::CreateTable;
      } else if constexpr (std::is_same_v<T, store_op::DropTable>) {
        return Tag::DropTable;
      } else if constexpr (std::is_same_v<T, store_op::AddColumn>) {
        return Tag::AddColumn;
      } else if constexpr (std::is_same_v<T, store_op::DropColumn>) {
        return Tag::DropColumn;
      } else if constexpr (std::is_same_v<T, store_op::RenameColumn>) {
        return Tag::RenameColumn;
      } else if constexpr (std::is_same_v<T, store_op::ChangeColumnType>) {
        return Tag::ChangeColumnType;
      } else if constexpr (std::is_same_v<T, store_op::AddNotNull>) {
        return Tag::AddNotNull;
      } else if constexpr (std::is_same_v<T, store_op::DropNotNull>) {
        return Tag::DropNotNull;
      } else if constexpr (std::is_same_v<T, store_op::AddCheck>) {
        return Tag::AddCheck;
      } else if constexpr (std::is_same_v<T, store_op::DropCheck>) {
        return Tag::DropCheck;
      } else if constexpr (std::is_same_v<T, store_op::AddPrimaryKey>) {
        return Tag::AddPrimaryKey;
      } else if constexpr (std::is_same_v<T, store_op::AddUnique>) {
        return Tag::AddUnique;
      } else if constexpr (std::is_same_v<T, store_op::CreateIndex>) {
        return Tag::CreateIndex;
      } else if constexpr (std::is_same_v<T, store_op::DropIndex>) {
        return Tag::DropIndex;
      } else if constexpr (std::is_same_v<T, store_op::RenameIndex>) {
        return Tag::RenameIndex;
      } else {
        static_assert(false, "store op has no tag");
      }
    },
    op);
}

void WriteId(duckdb::MemoryStream& s, ObjectId id) {
  s.Write<uint64_t>(id.id());
}

ObjectId ReadId(duckdb::MemoryStream& s) {
  return ObjectId{s.Read<uint64_t>()};
}

void WriteString(duckdb::MemoryStream& s, const std::string& value) {
  s.Write<uint32_t>(static_cast<uint32_t>(value.size()));
  if (!value.empty()) {
    s.WriteData(reinterpret_cast<const duckdb::data_t*>(value.data()),
                value.size());
  }
}

std::string ReadString(duckdb::MemoryStream& s) {
  const auto size = s.Read<uint32_t>();
  std::string value;
  value.resize(size);
  if (size != 0) {
    s.ReadData(reinterpret_cast<duckdb::data_ptr_t>(value.data()), size);
  }
  return value;
}

void WriteStrings(duckdb::MemoryStream& s,
                  const std::vector<std::string>& values) {
  s.Write<uint32_t>(static_cast<uint32_t>(values.size()));
  for (const auto& value : values) {
    WriteString(s, value);
  }
}

std::vector<std::string> ReadStrings(duckdb::MemoryStream& s) {
  const auto count = s.Read<uint32_t>();
  std::vector<std::string> values;
  values.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    values.push_back(ReadString(s));
  }
  return values;
}

void WriteIndexDef(duckdb::MemoryStream& s, const StoreIndexDef& def) {
  WriteId(s, def.table_id);
  WriteId(s, def.index_id);
  WriteString(s, def.name);
  WriteStrings(s, def.keys);
  s.Write<uint8_t>(static_cast<uint8_t>(def.kind));
  s.Write<uint8_t>(static_cast<uint8_t>(def.unique));
  s.Write<uint8_t>(static_cast<uint8_t>(def.defer_injection));
}

StoreIndexDef ReadIndexDef(duckdb::MemoryStream& s) {
  StoreIndexDef def;
  def.table_id = ReadId(s);
  def.index_id = ReadId(s);
  def.name = ReadString(s);
  def.keys = ReadStrings(s);
  def.kind = static_cast<StoreIndexDef::Kind>(s.Read<uint8_t>());
  def.unique = s.Read<uint8_t>() != 0;
  def.defer_injection = s.Read<uint8_t>() != 0;
  return def;
}

}  // namespace

bool IsDestructive(const Op& op) noexcept {
  return std::holds_alternative<DropTable>(op) ||
         std::holds_alternative<DropColumn>(op) ||
         std::holds_alternative<DropNotNull>(op) ||
         std::holds_alternative<DropCheck>(op) ||
         std::holds_alternative<DropIndex>(op);
}

void SerializeOps(std::span<const Targeted> ops, duckdb::MemoryStream& stream) {
  stream.Write<uint32_t>(static_cast<uint32_t>(ops.size()));
  for (const auto& targeted : ops) {
    const auto& op = targeted.op;
    stream.Write<uint8_t>(static_cast<uint8_t>(TagOf(op)));
    std::visit(
      [&](const auto& o) {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, store_op::CreateTable> ||
                      std::is_same_v<T, store_op::DropTable>) {
          WriteId(stream, o.table_id);
        } else if constexpr (std::is_same_v<T, store_op::AddColumn>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.column);
          WriteString(stream, o.type_sql);
          WriteString(stream, o.default_sql);
          stream.Write<uint8_t>(static_cast<uint8_t>(o.compression));
        } else if constexpr (std::is_same_v<T, store_op::DropColumn>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.column);
          WriteId(stream, o.column_id);
        } else if constexpr (std::is_same_v<T, store_op::RenameColumn>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.column);
          WriteString(stream, o.new_name);
        } else if constexpr (std::is_same_v<T, store_op::AddNotNull> ||
                             std::is_same_v<T, store_op::DropNotNull>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.column);
        } else if constexpr (std::is_same_v<T, store_op::ChangeColumnType>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.column);
          WriteString(stream, o.type_sql);
          WriteString(stream, o.using_sql);
        } else if constexpr (std::is_same_v<T, store_op::AddCheck> ||
                             std::is_same_v<T, store_op::DropCheck>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.expr);
        } else if constexpr (std::is_same_v<T, store_op::AddPrimaryKey> ||
                             std::is_same_v<T, store_op::AddUnique>) {
          WriteId(stream, o.table_id);
          WriteString(stream, o.constraint);
          WriteStrings(stream, o.columns);
        } else if constexpr (std::is_same_v<T, store_op::CreateIndex>) {
          WriteIndexDef(stream, o.def);
        } else if constexpr (std::is_same_v<T, store_op::DropIndex>) {
          WriteIndexDef(stream, o.def);
        } else if constexpr (std::is_same_v<T, store_op::RenameIndex>) {
          WriteId(stream, o.table_id);
          WriteId(stream, o.index_id);
          WriteString(stream, o.from);
          WriteString(stream, o.to);
        } else {
          static_assert(false, "store op is not serialized");
        }
      },
      op);
  }
}

namespace {

std::vector<Op> DeserializeOpList(duckdb::MemoryStream& stream) {
  const auto count = stream.Read<uint32_t>();
  std::vector<Op> ops;
  ops.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    const auto tag = static_cast<Tag>(stream.Read<uint8_t>());
    switch (tag) {
      case Tag::CreateTable: {
        ops.emplace_back(store_op::CreateTable{.table_id = ReadId(stream)});
        break;
      }
      case Tag::DropTable: {
        ops.emplace_back(store_op::DropTable{.table_id = ReadId(stream)});
        break;
      }
      case Tag::AddColumn: {
        store_op::AddColumn o;
        o.table_id = ReadId(stream);
        o.column = ReadString(stream);
        o.type_sql = ReadString(stream);
        o.default_sql = ReadString(stream);
        o.compression =
          static_cast<duckdb::CompressionType>(stream.Read<uint8_t>());
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::DropColumn: {
        store_op::DropColumn o;
        o.table_id = ReadId(stream);
        o.column = ReadString(stream);
        o.column_id = ReadId(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::RenameColumn: {
        store_op::RenameColumn o;
        o.table_id = ReadId(stream);
        o.column = ReadString(stream);
        o.new_name = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::ChangeColumnType: {
        store_op::ChangeColumnType o;
        o.table_id = ReadId(stream);
        o.column = ReadString(stream);
        o.type_sql = ReadString(stream);
        o.using_sql = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::AddNotNull: {
        store_op::AddNotNull o;
        o.table_id = ReadId(stream);
        o.column = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::DropNotNull: {
        store_op::DropNotNull o;
        o.table_id = ReadId(stream);
        o.column = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::AddCheck: {
        store_op::AddCheck o;
        o.table_id = ReadId(stream);
        o.expr = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::DropCheck: {
        store_op::DropCheck o;
        o.table_id = ReadId(stream);
        o.expr = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::AddPrimaryKey: {
        store_op::AddPrimaryKey o;
        o.table_id = ReadId(stream);
        o.constraint = ReadString(stream);
        o.columns = ReadStrings(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::AddUnique: {
        store_op::AddUnique o;
        o.table_id = ReadId(stream);
        o.constraint = ReadString(stream);
        o.columns = ReadStrings(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::CreateIndex: {
        store_op::CreateIndex o;
        o.def = ReadIndexDef(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::DropIndex: {
        store_op::DropIndex o;
        o.def = ReadIndexDef(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      case Tag::RenameIndex: {
        store_op::RenameIndex o;
        o.table_id = ReadId(stream);
        o.index_id = ReadId(stream);
        o.from = ReadString(stream);
        o.to = ReadString(stream);
        ops.emplace_back(std::move(o));
        break;
      }
      default:
        SDB_FATAL(STARTUP, "catalog wal: unknown store op tag ",
                  static_cast<int>(tag));
    }
  }
  return ops;
}

}  // namespace

std::vector<Targeted> DeserializeOps(ObjectId database_id,
                                     duckdb::MemoryStream& stream) {
  auto ops = DeserializeOpList(stream);
  std::vector<Targeted> targeted;
  targeted.reserve(ops.size());
  for (auto& op : ops) {
    targeted.emplace_back(database_id, std::move(op));
  }
  return targeted;
}

}  // namespace sdb::catalog::store_op
