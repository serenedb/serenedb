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

#include <algorithm>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/identifier.h"
#include "basics/serialization.h"
#include "basics/serializer.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/identifier.h"

namespace sdb::basics {

class JsonSink;

}  // namespace sdb::basics
namespace duckdb {

class DatabaseManager;

// AclMode is a privilege bitmask, so combined values (e.g. Select|Insert) are
// not named enumerators. The reflection serializer validates plain enums
// against their enumerators, which would reject those combinations --
// serialize the raw underlying integer instead. In duckdb's namespace because
// that is where ADL looks for the type it serializes.
template<typename Context>
void SerdeWrite(Context ctx, AclMode mode) {
  ctx.io().WriteValue(std::to_underlying(mode));
}

template<typename Context>
void SerdeRead(Context ctx, AclMode& mode) {
  mode = static_cast<AclMode>(ctx.io().ReadUnsignedInt64());
}

}  // namespace duckdb
namespace sdb::catalog {

// Hangs off the instance rather than another object, so it stores no parent.
constexpr bool IsRoot(duckdb::CatalogType t) noexcept {
  return t == duckdb::CatalogType::DATABASE_ENTRY ||
         t == duckdb::CatalogType::ROLE_ENTRY;
}

// https://www.postgresql.org/docs/current/sql-grant.html
//
// The owner, the grants and the privilege bitmask are duckdb's storage
// (CatalogEntry::permissions), so that every version of an entry carries the
// ACL its committing transaction saw without a second MVCC beside the set.
// What the bits mean -- which kind accepts which, the closure, the grant
// options -- is here.
using AclMode = duckdb::AclMode;
using AclItem = duckdb::AclItem;
using Acl = duckdb::vector<AclItem>;
using AclView = std::span<const AclItem>;
using Permissions = duckdb::CatalogPermissions;
// Column-level grants, keyed by ColumnDefinition::CatalogOid(). They sit on the
// entry's permissions rather than on its definition: a grant is not a change to
// what the table is, and a column has no entry of its own to keep an ACL on.
using ColumnAcls = decltype(Permissions::column_acl);

// PUBLIC pseudo-grantee (PG's OID 0); an AclItem with this grantee applies to
// every role.
inline constexpr ObjectId kPublicGrantee{0};

inline constexpr AclItem kSystemPublicSelect{.grantee = kPublicGrantee.id(),
                                             .grantor = id::kRootUser.id(),
                                             .privs = AclMode::Select};

// duckdb stores the principals as raw host ids; every SereneDB reader wants the
// typed one.
inline ObjectId OwnerOf(const Permissions& perm) noexcept {
  return ObjectId{perm.owner};
}

// One column's grants out of an entry's permissions, which a reader resolves
// once and then indexes -- almost every table has no column grant at all.
inline AclView ColumnAclOf(const ColumnAcls& acls,
                           ObjectId column_id) noexcept {
  for (const auto& entry : acls) {
    if (entry.catalog_oid == column_id.id()) {
      return AclView{entry.acl};
    }
  }
  return {};
}

// Sets (or, when `acl` is empty, removes) one column's grants, keeping the list
// ordered by column so one catalog state writes one frame.
inline void SetColumnAcl(ColumnAcls& acls, ObjectId column_id, Acl acl) {
  const auto at = std::ranges::lower_bound(
    acls, column_id.id(), {}, [](const auto& e) { return e.catalog_oid; });
  if (at != acls.end() && at->catalog_oid == column_id.id()) {
    if (acl.empty()) {
      acls.erase(at);
    } else {
      at->acl = std::move(acl);
    }
    return;
  }
  if (!acl.empty()) {
    acls.insert(at, duckdb::ColumnAclItem{column_id.id(), std::move(acl)});
  }
}

// Drops a column's grants, for the ALTER that drops the column.
inline void EraseColumnAcl(ColumnAcls& acls, ObjectId column_id) {
  SetColumnAcl(acls, column_id, Acl{});
}

inline AclView ColumnAclOf(const ColumnAcls* acls,
                           ObjectId column_id) noexcept {
  return acls == nullptr ? AclView{} : ColumnAclOf(*acls, column_id);
}

inline ObjectId GranteeOf(const AclItem& item) noexcept {
  return ObjectId{item.grantee};
}

inline ObjectId GrantorOf(const AclItem& item) noexcept {
  return ObjectId{item.grantor};
}

// The identity a host catalog stamped on a CreateInfo, and its parent's. Every
// kind whose entry is a duckdb catalog entry keeps them there rather than in a
// wrapper beside the info: the info is the one object every version of an entry
// carries, and duckdb's own Copy() propagates them.
inline ObjectId IdOf(const duckdb::CreateInfo& info) noexcept {
  return ObjectId{info.oid};
}

inline ObjectId ParentIdOf(const duckdb::CreateInfo& info) noexcept {
  return ObjectId{info.parent_oid};
}

inline void SetIdentity(duckdb::CreateInfo& info, ObjectId id,
                        ObjectId parent_id) noexcept {
  info.oid = id.id();
  info.parent_oid = parent_id.id();
}

// The same identity, off the entry the info built. duckdb's oid is it: the
// entry is built from a definition that names the id, CatalogSet carries the
// oid across every version of the entry, and boot rebuilds the entry from the
// record -- so it survives a restart by construction rather than by being
// written down a second time.
inline ObjectId IdOf(const duckdb::CatalogEntry& entry) noexcept {
  return ObjectId{entry.oid};
}

// The schema an entry hangs off. A cluster-global kind (a role, a database) and
// a foreign server hang off no schema and answer none.
inline ObjectId ParentIdOf(const duckdb::StandardEntry& entry) noexcept {
  return ObjectId{entry.schema.oid};
}

inline ObjectId ParentIdOf(const duckdb::CatalogEntry& entry) noexcept {
  const auto* standard = dynamic_cast<const duckdb::StandardEntry*>(&entry);
  return standard == nullptr ? ObjectId{} : ParentIdOf(*standard);
}

// What makes a duckdb catalog entry a SereneDB one: the stable id it answers
// to and the owner and ACL every access check reads. Both come off the version
// the writing transaction commits -- including the one boot builds from the
// catalog log.
inline void AdoptEntryIdentity(duckdb::CatalogEntry& entry, ObjectId id,
                               Permissions perm = {}) noexcept {
  entry.oid = id.id();
  // The catalog log owns durability and reclamation for everything with a
  // serenedb id; duckdb must neither write it to a WAL nor free blocks for it.
  entry.duck_managed = false;
  entry.permissions = std::move(perm);
}

// The name the entry answers to. Not valid for a schema, whose name duckdb
// keeps in the qualified name's schema slot instead.
inline std::string_view NameOf(const duckdb::CreateInfo& info) noexcept {
  return info.GetQualifiedName().Name().GetIdentifierName();
}

// duckdb's own object-id allocator, which is the identity space: an entry is
// built from a definition that names the id, CatalogSet carries the oid across
// every version, and the durable horizon that keeps ids from being reissued
// after a crash lives beside the counter.
duckdb::DatabaseManager& IdAllocator();

ObjectId NextId();

ObjectId NextNIds(uint64_t n);

// Raises the allocator past an id read back rather than handed out.
void RestoreId(uint64_t id);

struct ReadContext {
  ObjectId id;
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId relation_id;
};

}  // namespace sdb::catalog
