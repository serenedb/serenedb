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

#pragma once

#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <memory>
#include <optional>
#include <utility>

#include "catalog/database.h"
#include "catalog/duckdb_entry.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/role.h"
#include "catalog/sequence.h"
#include "catalog/tokenizer.h"
#include "catalog/user_type.h"

namespace sdb::catalog {

// A user-defined type. duckdb owns the entry shape; the stable id, the owner
// and the ACL are serenedb's, and the entry is the object -- nothing else holds
// a type, and the mutators write this set directly.
class SereneDBTypeEntry final : public duckdb::TypeCatalogEntry {
 public:
  // The duckdb set family this kind is looked up under; Find<> reads it so
  // there is no per-kind lookup function to write.
  static constexpr auto kCatalogType = duckdb::CatalogType::TYPE_ENTRY;

 public:
  SereneDBTypeEntry(duckdb::Catalog& catalog,
                    duckdb::SchemaCatalogEntry& schema,
                    duckdb::CreateTypeInfo& info, catalog::Permissions perm)
    : duckdb::TypeCatalogEntry(catalog, schema, info) {
    catalog::AdoptEntryIdentity(*this, ObjectId{info.oid}, std::move(perm));
  }
};

// A SQL function, in whichever of duckdb's two macro sets its declaration puts
// it. Like a type, the entry is the object: nothing else holds a function, and
// the mutators write these sets directly.
class SereneDBScalarMacroEntry final : public duckdb::ScalarMacroCatalogEntry {
 public:
  // The duckdb set family this kind is looked up under; Find<> reads it so
  // there is no per-kind lookup function to write.
  static constexpr auto kCatalogType = duckdb::CatalogType::MACRO_ENTRY;

 public:
  SereneDBScalarMacroEntry(duckdb::Catalog& catalog,
                           duckdb::SchemaCatalogEntry& schema,
                           duckdb::CreateMacroInfo& info,
                           catalog::Permissions perm)
    : duckdb::ScalarMacroCatalogEntry(catalog, schema, info) {
    catalog::AdoptEntryIdentity(*this, ObjectId{info.oid}, std::move(perm));
  }
};

class SereneDBTableMacroEntry final : public duckdb::TableMacroCatalogEntry {
 public:
  // The duckdb set family this kind is looked up under; Find<> reads it so
  // there is no per-kind lookup function to write.
  static constexpr auto kCatalogType = duckdb::CatalogType::MACRO_ENTRY;

 public:
  SereneDBTableMacroEntry(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema,
                          duckdb::CreateMacroInfo& info,
                          catalog::Permissions perm)
    : duckdb::TableMacroCatalogEntry(catalog, schema, info) {
    catalog::AdoptEntryIdentity(*this, ObjectId{info.oid}, std::move(perm));
  }
};

// A sequence. duckdb owns the entry shape; the CACHE, the owning table, the
// counter with its durable horizon, and the grants are serenedb's.
//
// Like a type or a view, the entry is the object: nothing else holds a
// sequence, and the mutators write this set directly. The counter is the one
// thing that is not the definition -- it is shared by every version of it.
class SereneDBSequenceEntry final : public duckdb::SequenceCatalogEntry {
 public:
  // The duckdb set family this kind is looked up under; Find<> reads it so
  // there is no per-kind lookup function to write.
  static constexpr auto kCatalogType = duckdb::CatalogType::SEQUENCE_ENTRY;

 public:
  SereneDBSequenceEntry(duckdb::Catalog& catalog,
                        duckdb::SchemaCatalogEntry& schema,
                        duckdb::CreateSequenceInfo& info,
                        std::shared_ptr<catalog::SequenceCounter> counter,
                        catalog::Permissions perm)
    : duckdb::SequenceCatalogEntry(catalog, schema, info),
      _options{catalog::SequenceOptionsOf(info)},
      _counter{std::move(counter)} {
    catalog::AdoptEntryIdentity(*this, ObjectId{info.oid}, std::move(perm));
  }

  // The bounds and the CACHE, copied rather than read back through duckdb's
  // GetData(): that takes its counter lock, and nextval runs off the durable
  // counter below instead. Neither changes within a version.
  const catalog::SequenceOptions& Options() const noexcept { return _options; }
  // Set for SERIAL and the auto-PK sequence: the table this goes down with.
  ObjectId GetOwnerTableId() const noexcept {
    return ObjectId{_options.owner_table_id};
  }
  std::string_view Comment() const noexcept { return _options.comment; }

  // This version as a definition again. Rebuilt rather than held: duckdb's own
  // GetInfo() has nowhere to put the CACHE or the owning table, and only a
  // write ever needs the whole thing back.
  std::shared_ptr<const duckdb::CreateSequenceInfo> Definition() const {
    return catalog::MakeSequenceInfo(ObjectId{oid},
                                     ObjectId{ParentSchema().oid}, _options);
  }

  // The live counter, shared by every version of this sequence: a value a
  // retired version handed out is the same value. Bound after the entry is
  // placed, so a rewrite inherits its predecessor's rather than reseeding.
  const std::shared_ptr<catalog::SequenceCounter>& Counter() const noexcept {
    return _counter;
  }
  void AdoptCounter(std::shared_ptr<catalog::SequenceCounter> counter) const {
    _counter = std::move(counter);
  }
  uint64_t Reserve(uint64_t count) const { return _counter->Reserve(count); }
  uint64_t Read() const { return _counter->Read(); }
  void Write(uint64_t value) const { _counter->Write(value); }

 private:
  catalog::SequenceOptions _options;
  mutable std::shared_ptr<catalog::SequenceCounter> _counter;
};

// A text-search tokenizer. duckdb has no counterpart, so CatalogType gained
// TOKENIZER_ENTRY: the alternative was borrowing a kind whose upstream
// machinery would then mishandle it.
//
// Like a role, a database and a schema, the entry is the object: nothing else
// holds a tokenizer, and the mutators write this set directly.
class SereneDBTokenizerEntry final : public duckdb::StandardEntry {
 public:
  // The duckdb set family this kind is looked up under; Find<> reads it so
  // there is no per-kind lookup function to write.
  static constexpr auto kCatalogType = duckdb::CatalogType::TOKENIZER_ENTRY;

 public:
  SereneDBTokenizerEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         catalog::TokenizerRef tokenizer,
                         catalog::Permissions perm)
    : duckdb::StandardEntry(
        duckdb::CatalogType::TOKENIZER_ENTRY, schema, catalog,
        duckdb::Identifier{std::string{tokenizer->GetName()}}),
      _tokenizer(std::move(tokenizer)) {
    catalog::AdoptEntryIdentity(*this, _tokenizer->GetId(), std::move(perm));
  }

  const catalog::CreateTokenizerInfo& Tokenizer() const noexcept {
    return *_tokenizer;
  }
  const catalog::TokenizerRef& Definition() const noexcept {
    return _tokenizer;
  }

 private:
  catalog::TokenizerRef _tokenizer;
};

// A foreign server. Database-scoped, not schema-scoped -- PG's shape -- so it
// is an InCatalogEntry in a free-standing set on SereneDBCatalog rather than a
// StandardEntry under a schema.
//
// Like a role, a database and a schema, the entry is the object: nothing else
// holds a foreign server, and the mutators write this set directly.
class SereneDBForeignServerEntry final : public duckdb::InCatalogEntry {
 public:
  SereneDBForeignServerEntry(duckdb::Catalog& catalog,
                             catalog::ForeignServerRef server,
                             catalog::Permissions perm)
    : duckdb::InCatalogEntry(
        duckdb::CatalogType::FOREIGN_SERVER_ENTRY, catalog,
        duckdb::Identifier{std::string{server->GetName()}}),
      _server(std::move(server)) {
    catalog::AdoptEntryIdentity(*this, _server->GetId(), std::move(perm));
  }

  const catalog::CreateForeignServerInfo& ForeignServer() const noexcept {
    return *_server;
  }
  const catalog::ForeignServerRef& Definition() const noexcept {
    return _server;
  }

 private:
  catalog::ForeignServerRef _server;
};

// A role. Cluster-global, so it hangs off the storage-less __sdb_global
// attachment rather than any one database, and it is an InCatalogEntry for the
// same reason a foreign server is: there is no schema above it. duckdb has no
// counterpart, so CatalogType gained ROLE_ENTRY.
class SereneDBRoleEntry final : public duckdb::InCatalogEntry {
 public:
  SereneDBRoleEntry(duckdb::Catalog& catalog,
                    std::shared_ptr<const catalog::CreateRoleInfo> role)
    : duckdb::InCatalogEntry(duckdb::CatalogType::ROLE_ENTRY, catalog,
                             duckdb::Identifier{std::string{role->GetName()}}),
      _role(std::move(role)) {
    catalog::AdoptEntryIdentity(*this, _role->GetId());
  }

  // A role owns no other role, so the entry has no owner or ACL of its own;
  // what a role carries is its attributes, its memberships and its default
  // privileges, all of which live here.
  const catalog::CreateRoleInfo& Role() const noexcept { return *_role; }
  const std::shared_ptr<const catalog::CreateRoleInfo>& RoleInfo()
    const noexcept {
    return _role;
  }

 private:
  std::shared_ptr<const catalog::CreateRoleInfo> _role;
};

// A database. The other cluster-global kind, in the set beside the roles.
// duckdb's own DATABASE_ENTRY names an attachment; this one names the SereneDB
// database the attachment carries, and the two never share a set.
//
// Like a role, the entry is the object: nothing else holds a database, and the
// mutators write this set directly.
class SereneDBDatabaseEntry final : public duckdb::InCatalogEntry {
 public:
  SereneDBDatabaseEntry(
    duckdb::Catalog& catalog,
    std::shared_ptr<const catalog::CreateDatabaseInfo> database,
    catalog::Permissions perm)
    : duckdb::InCatalogEntry(
        duckdb::CatalogType::DATABASE_ENTRY, catalog,
        duckdb::Identifier{std::string{database->GetName()}}),
      _database(std::move(database)) {
    catalog::AdoptEntryIdentity(*this, _database->GetId(), std::move(perm));
  }

  const catalog::CreateDatabaseInfo& Database() const noexcept {
    return *_database;
  }
  const std::shared_ptr<const catalog::CreateDatabaseInfo>& DatabaseInfo()
    const noexcept {
    return _database;
  }

 private:
  std::shared_ptr<const catalog::CreateDatabaseInfo> _database;
};

}  // namespace sdb::catalog
