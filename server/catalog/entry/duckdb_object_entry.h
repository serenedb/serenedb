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

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <memory>
#include <utility>

#include "catalog/database.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/persistence/role.h"
#include "catalog/role.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"

namespace sdb::catalog {

// A user-defined type. duckdb owns the entry shape; the stable id, the owner
// and the ACL are serenedb's, and the entry is the object -- nothing else holds
// a type, and the mutators write this set directly.
using SereneDBTypeEntry = duckdb::TypeCatalogEntry;

// One version of one SQL function as PlaceEntry (and the static pg_catalog
// schema) builds it: the info's own type picks which of the two macro entry
// classes holds it.
inline duckdb::unique_ptr<duckdb::CatalogEntry> MakeMacroEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, bool internal,
  const duckdb::CreateMacroInfo& func, catalog::Permissions perm) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      func.Copy());
  info->SetSchema(schema.name);
  info->SetFunctionName(duckdb::Identifier{entry_name});
  info->temporary = false;
  info->internal = internal;
  const auto id = ObjectId{info->oid};
  auto entry = info->type == duckdb::CatalogType::TABLE_MACRO_ENTRY
                 ? duckdb::make_uniq_base<duckdb::CatalogEntry,
                                          duckdb::TableMacroCatalogEntry>(
                     catalog, schema, *info)
                 : duckdb::make_uniq_base<duckdb::CatalogEntry,
                                          duckdb::ScalarMacroCatalogEntry>(
                     catalog, schema, *info);
  catalog::AdoptEntryIdentity(*entry, id, std::move(perm));
  return entry;
}

// A sequence. duckdb owns the entry shape; the CACHE, the owning table, the
// counter with its durable horizon, and the grants are serenedb's. The counter
// is the one thing that is not the definition -- it is shared by every version
// of it.
class SereneDBSequenceEntry final : public duckdb::SequenceCatalogEntry {
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

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext&) const override {
    const auto info = GetInfo();
    return duckdb::make_uniq<SereneDBSequenceEntry>(
      catalog, Schema(), info->Cast<duckdb::CreateSequenceInfo>(), Counter(),
      permissions);
  }

  // One version of one sequence as PlaceEntry builds it. A rewrite is the same
  // sequence behind the same counter: a value the superseded version handed
  // out must never be handed out again. The counter and its durable horizon
  // are serenedb's, written to the catalog log; duckdb neither persists nor
  // reclaims anything for this entry.
  static duckdb::unique_ptr<duckdb::CatalogEntry> Make(
    duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
    const duckdb::CreateSequenceInfo& sequence, catalog::Permissions perm,
    duckdb::optional_ptr<duckdb::CatalogEntry> superseded) {
    std::shared_ptr<catalog::SequenceCounter> counter;
    if (const auto* previous =
          dynamic_cast<const SereneDBSequenceEntry*>(superseded.get())) {
      counter = previous->Counter();
    }
    auto info =
      duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateSequenceInfo>(
        sequence.Copy());
    info->SetSchema(schema.name);
    return duckdb::make_uniq<SereneDBSequenceEntry>(
      catalog, schema, *info, std::move(counter), std::move(perm));
  }

  // The bounds and the CACHE, copied rather than read back through duckdb's
  // GetData(): that takes its counter lock, and nextval runs off the durable
  // counter below instead. Neither changes within a version.
  const catalog::SequenceOptions& Options() const noexcept { return _options; }
  // Set for SERIAL and the auto-PK sequence: the table this goes down with.
  ObjectId GetOwnerTableId() const noexcept {
    return ObjectId{_options.owner_table_id};
  }
  // Off the entry field, not the options mirror: duckdb's SET_COMMENT alter
  // writes the field on a copy, where the mirror still says what the copy was
  // built from.
  std::string_view Comment() const noexcept {
    return catalog::CommentText(comment);
  }

  // This version as a definition again, for the readers that want the options
  // as a struct. The durable shape is GetInfo(): the CACHE and the owning table
  // ride the tags, which duckdb's own GetInfo() carries.
  duckdb::unique_ptr<duckdb::CreateSequenceInfo> Definition() const {
    auto options = _options;
    options.comment = std::string{Comment()};
    return catalog::MakeSequenceInfo(ObjectId{oid},
                                     ObjectId{ParentSchema().oid}, options);
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
// TOKENIZER_ENTRY.
class SereneDBTokenizerEntry final : public duckdb::StandardEntry {
 public:
  static constexpr auto Type = duckdb::CatalogType::TOKENIZER_ENTRY;

 public:
  SereneDBTokenizerEntry(duckdb::Catalog& catalog,
                         duckdb::SchemaCatalogEntry& schema,
                         const catalog::CreateTokenizerInfo& info,
                         catalog::Permissions perm)
    : duckdb::StandardEntry(duckdb::CatalogType::TOKENIZER_ENTRY, schema,
                            catalog,
                            duckdb::Identifier{std::string{info.GetName()}}),
      _tokenizer{std::make_shared<const catalog::Tokenizer>(
        info.GetId(), info.GetFeatures(),
        irs::analysis::Clone(info.Config()))} {
    catalog::AdoptEntryIdentity(*this, info.GetId(), std::move(perm));
  }

  // The analyzer config and its pool. Handed out by shared pointer because a
  // feed worker tokenizes on a thread with no catalog transaction of its own:
  // what it resolved stays alive for as long as it is still writing.
  const catalog::TokenizerRef& GetTokenizer() const noexcept {
    return _tokenizer;
  }

  // The durable shape of this version: the catalog log writes it, boot reads it
  // back, and nothing else holds one.
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final {
    return duckdb::make_uniq<catalog::CreateTokenizerInfo>(
      ObjectId{oid}, ObjectId{ParentSchema().oid}, name.GetIdentifierName(),
      _tokenizer->GetFeatures(), irs::analysis::Clone(_tokenizer->Config()));
  }

 private:
  catalog::TokenizerRef _tokenizer;
};

// A foreign server. Database-scoped, not schema-scoped -- PG's shape -- so it
// is an InCatalogEntry in a free-standing set on SereneDBCatalog rather than a
// StandardEntry under a schema.
class SereneDBForeignServerEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr auto Type = duckdb::CatalogType::FOREIGN_SERVER_ENTRY;

  SereneDBForeignServerEntry(duckdb::Catalog& catalog,
                             const catalog::CreateForeignServerInfo& server,
                             catalog::Permissions perm)
    : duckdb::InCatalogEntry(duckdb::CatalogType::FOREIGN_SERVER_ENTRY, catalog,
                             duckdb::Identifier{std::string{server.GetName()}}),
      _database_id{server.GetDatabaseId()},
      _fdw_name{server.GetFdwName()},
      _option_keys{server.OptionKeys().begin(), server.OptionKeys().end()},
      _option_values{server.OptionValues().begin(),
                     server.OptionValues().end()} {
    catalog::AdoptEntryIdentity(*this, server.GetId(), std::move(perm));
  }

  ObjectId GetDatabaseId() const noexcept { return _database_id; }
  std::string_view GetFdwName() const noexcept { return _fdw_name; }
  std::span<const std::string> OptionKeys() const noexcept {
    return _option_keys;
  }
  std::span<const std::string> OptionValues() const noexcept {
    return _option_values;
  }

  std::string_view GetName() const noexcept { return name.GetIdentifierName(); }

  // "key=value" strings in insertion order, the pg_foreign_server text[] shape.
  // Unredacted -- that view is superuser-only.
  std::vector<std::string> GetStringOptions() const {
    std::vector<std::string> out;
    out.reserve(_option_keys.size());
    for (size_t i = 0; i < _option_keys.size(); ++i) {
      out.push_back(absl::StrCat(_option_keys[i], "=", _option_values[i]));
    }
    return out;
  }

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final {
    return duckdb::make_uniq<catalog::CreateForeignServerInfo>(
      ObjectId{oid}, _database_id, name.GetIdentifierName(), _fdw_name,
      _option_keys, _option_values);
  }

 private:
  ObjectId _database_id;
  std::string _fdw_name;
  std::vector<std::string> _option_keys;
  std::vector<std::string> _option_values;
};

// A role. Cluster-global, so it hangs off the storage-less __sdb_global
// attachment rather than any one database, and it is an InCatalogEntry for the
// same reason a foreign server is: there is no schema above it. duckdb has no
// counterpart, so CatalogType gained ROLE_ENTRY.
class SereneDBRoleEntry final : public duckdb::InCatalogEntry {
 public:
  SereneDBRoleEntry(duckdb::Catalog& catalog,
                    std::shared_ptr<const catalog::Role> role)
    : duckdb::InCatalogEntry(duckdb::CatalogType::ROLE_ENTRY, catalog,
                             duckdb::Identifier{std::string{role->GetName()}}),
      _role{std::move(role)} {
    catalog::AdoptEntryIdentity(*this, _role->GetId());
  }

  // A role owns no other role, so the entry has no owner or ACL of its own;
  // it carries its attributes, memberships and default privileges. Readers
  // take names and grants straight out of it, so it has to be the entry's own
  // storage and not a copy handed out per ask.
  const catalog::Role& Role() const noexcept { return *_role; }

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final {
    return duckdb::make_uniq<catalog::CreateRoleInfo>(_role);
  }

 private:
  const std::shared_ptr<const catalog::Role> _role;
};

// A database. The other cluster-global kind, in the set beside the roles.
// duckdb's own DATABASE_ENTRY names an attachment; this one names the SereneDB
// database the attachment carries, and the two never share a set.
class SereneDBDatabaseEntry final : public duckdb::InCatalogEntry {
 public:
  SereneDBDatabaseEntry(duckdb::Catalog& catalog,
                        const catalog::CreateDatabaseInfo& database,
                        catalog::Permissions perm)
    : duckdb::InCatalogEntry(
        duckdb::CatalogType::DATABASE_ENTRY, catalog,
        duckdb::Identifier{std::string{database.GetName()}}),
      _public_schema_id{database.PublicSchemaId()} {
    catalog::AdoptEntryIdentity(*this, database.GetId(), std::move(perm));
  }

  // The schema every database has from the moment it exists.
  ObjectId PublicSchemaId() const noexcept { return _public_schema_id; }

  // This version as a definition again, for the mutators that author the next
  // one from it.
  duckdb::unique_ptr<catalog::CreateDatabaseInfo> Definition() const {
    return duckdb::make_uniq<catalog::CreateDatabaseInfo>(
      ObjectId{oid}, name.GetIdentifierName(), _public_schema_id);
  }

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final {
    return Definition();
  }

 private:
  ObjectId _public_schema_id;
};

}  // namespace sdb::catalog
