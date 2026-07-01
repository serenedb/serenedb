////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <absl/hash/hash.h>

#include <atomic>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "basics/identifier.h"
#include "basics/serialization.h"
#include "basics/serializer.h"
#include "catalog/identifiers/identifier.h"

namespace sdb::catalog {

enum class ObjectType : uint8_t {
  Invalid = 0,

  // Tombstone must be scanned first so deleted objects are known
  // before live objects are loaded.
  Tombstone = 1,
  // Catalog objects start at 128.
  // Order matters: within the same parent, objects are scanned in enum order.
  // Indexes must come after their table so the table exists when the index
  // is loaded.
  // Top-level (parent = instance)
  Database = 128,
  Role,
  // Under database
  Schema,
  // Under schema - dependencies first
  Tokenizer,
  PgSqlFunction,
  PgSqlType,
  PgSqlView,
  Sequence,
  Table,
  SecondaryIndex,
  InvertedIndex,

  Column,           // loaded as a part of a Table, not as a separate object
  CheckConstraint,  // loaded as a part of a Table, not as a separate object

  // Runtime only, not persisted.
  Virtual = 255,
};

constexpr bool IsIndex(ObjectType t) noexcept {
  return t == ObjectType::SecondaryIndex || t == ObjectType::InvertedIndex;
}

// https://www.postgresql.org/docs/current/sql-grant.html
enum class AclMode : uint64_t {
  NoRights = 0U,
  Insert = 1U << 0,
  Select = 1U << 1,
  Update = 1U << 2,
  Delete = 1U << 3,
  Truncate = 1U << 4,
  References = 1U << 5,
  Trigger = 1U << 6,
  Execute = 1U << 7,
  Usage = 1U << 8,
  Create = 1U << 9,
  CreateTemp = 1U << 10,
  Connect = 1U << 11,
  Set = 1U << 12,
  AlterSystem = 1U << 13,
  Maintain = 1U << 14,
};

ENABLE_BITMASK_ENUM(AclMode);

// AclMode is a privilege bitmask, so combined values (e.g. Select|Insert) are
// not named enumerators. The reflection serializer validates plain enums
// against their enumerators, which would reject those combinations -- serialize
// the raw underlying integer instead (ADL-found by basics::WriteTuple).
template<typename Context>
void SerdeWrite(Context ctx, AclMode mode) {
  ctx.io().WriteValue(std::to_underlying(mode));
}

template<typename Context>
void SerdeRead(Context ctx, AclMode& mode) {
  mode = static_cast<AclMode>(ctx.io().ReadUnsignedInt64());
}

// PUBLIC pseudo-grantee (PG's OID 0); an AclItem with this grantee applies to
// every role.
inline constexpr ObjectId kPublicGrantee{0};

struct AclItem {
  ObjectId grantee = id::kInvalid;  // who got the privileges
  ObjectId grantor = id::kInvalid;  // who granted them
  AclMode privs = AclMode::NoRights;
  AclMode grant_option = AclMode::NoRights;  // privs the grantee may re-grant
};

using Acl = std::vector<AclItem>;
using AclView = std::span<const AclItem>;

inline constexpr AclItem kSystemPublicSelect{.grantee = kPublicGrantee,
                                             .grantor = id::kRootUser,
                                             .privs = AclMode::Select};

struct Permissions {
  ObjectId owner;
  Acl acl;

  Permissions() = default;
  Permissions(ObjectId owner) : owner{owner} {}
  Permissions(ObjectId owner, Acl acl) : owner{owner}, acl{std::move(acl)} {}
};

template<typename Context>
void SerdeWrite(Context ctx, const Permissions& perm) {
  ctx.io().OnListBegin(2);
  basics::WriteTuple(ctx.io(), perm.owner, ctx.arg());
  basics::WriteTuple(ctx.io(), perm.acl, ctx.arg());
  ctx.io().OnListEnd();
}

template<typename Context>
void SerdeRead(Context ctx, Permissions& perm) {
  ctx.io().OnListBegin();
  basics::ReadTuple(ctx.io(), perm.owner, ctx.arg());
  basics::ReadTuple(ctx.io(), perm.acl, ctx.arg());
  ctx.io().OnListEnd();
}

class Object {
 public:
  virtual ~Object();

  Object(const Object& other)
    : _name{other._name},
      _id{other._id},
      _parent_id{other._parent_id},
      _perm{other._perm},
      _type{other._type},
      _tombstoned{other._tombstoned.load(std::memory_order_acquire)} {}

  Object& operator=(const Object& other) {
    if (this != &other) {
      _name = other._name;
      _id = other._id;
      _parent_id = other._parent_id;
      _perm = other._perm;
      _type = other._type;
      _tombstoned.store(other._tombstoned.load(std::memory_order_acquire),
                        std::memory_order_release);
    }
    return *this;
  }

  ObjectId GetParentId() const noexcept { return _parent_id; }
  auto GetAcl() const noexcept { return std::span{_perm.acl}; }
  ObjectType GetType() const noexcept { return _type; }
  const auto& GetName() const noexcept { return _name; }
  ObjectId GetId() const noexcept { return _id; }

  ObjectId GetOwner() const noexcept { return _perm.owner; }
  const Permissions& GetPermissions() const noexcept { return _perm; }

  void SetPermissions(Permissions perm) { _perm = std::move(perm); }

  bool Tombstoned() const noexcept {
    return _tombstoned.load(std::memory_order_acquire);
  }
  void SetTombstoned(bool v) noexcept {
    _tombstoned.store(v, std::memory_order_release);
  }

  virtual void Serialize(duckdb::Serializer&) const = 0;

  virtual std::shared_ptr<Object> Clone() const = 0;

  void SetName(std::string_view name) { _name = name; }
  void SetParentId(ObjectId parent_id) noexcept { _parent_id = parent_id; }
  void SetId(ObjectId id) noexcept { _id = id; }

 protected:
  // `perm` is the object's owner + ACL (grouped, so they are set together).
  // 'id' is autogenerated IFF 'id' == 0. Pass an empty Permissions for
  // non-owned objects (columns, indexes, system objects).
  Object(Permissions perm, ObjectId parent_id, ObjectId id,
         std::string_view name, ObjectType type);

  std::string _name;
  ObjectId _id;
  ObjectId _parent_id;
  Permissions _perm;
  ObjectType _type;
  std::atomic_bool _tombstoned = false;
};

ObjectId NextId();

ObjectId NextNIds(uint64_t n);

struct ReadContext {
  ObjectId id;
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId relation_id;
};

inline std::string_view SerializeObject(const Object& obj,
                                        duckdb::MemoryStream& stream) {
  stream.Rewind();
  duckdb::BinarySerializer serializer{stream, duckdb::VersionStorageOptions()};
  obj.Serialize(serializer);
  return {reinterpret_cast<const char*>(stream.GetData()),
          stream.GetPosition()};
}

inline duckdb::MemoryStream ReadStream(std::string_view bytes) {
  return duckdb::MemoryStream{
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(bytes.data())),
    bytes.size()};
}

template<typename T>
std::shared_ptr<T> DeserializeObject(std::string_view bytes, ReadContext ctx) {
  auto stream = ReadStream(bytes);
  duckdb::BinaryDeserializer deserializer{stream};
  return T::Deserialize(deserializer, ctx);
}

struct ObjectByName {
  using is_transparent = void;

  size_t operator()(std::string_view str) const { return absl::HashOf(str); }

  size_t operator()(const std::shared_ptr<Object>& obj) const {
    SDB_ASSERT(obj);
    return (*this)(obj->GetName());
  }

  bool operator()(const std::shared_ptr<Object>& obj,
                  std::string_view str) const {
    return obj->GetName() == str;
  }

  bool operator()(const std::shared_ptr<Object>& r,
                  const std::shared_ptr<Object>& l) const {
    SDB_ASSERT(l);
    SDB_ASSERT(r);
    return l->GetName() == r->GetName();
  }
};

struct ObjectById {
  using is_transparent = void;

  size_t operator()(ObjectId id) const { return absl::HashOf(id); }

  size_t operator()(const auto& obj) const {
    SDB_ASSERT(obj);
    return (*this)(obj->GetId());
  }

  bool operator()(const auto& obj, ObjectId id) const {
    return obj->GetId() == id;
  }

  bool operator()(const auto& r, const auto& l) const {
    SDB_ASSERT(l);
    SDB_ASSERT(r);
    return l->GetId() == r->GetId();
  }
};

}  // namespace sdb::catalog
