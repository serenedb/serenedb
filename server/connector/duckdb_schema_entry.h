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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <memory>
#include <span>

#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/schema.h"
#include "catalog/table_options.h"

namespace sdb::connector {

class SereneDBSchemaEntry final : public duckdb::DuckSchemaEntry {
 public:
  SereneDBSchemaEntry(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info);

  ObjectId GetDatabaseId() const;
  // This schema's id, re-resolved by name, with `role`'s CREATE privilege on
  // it enforced. Every DDL this entry performs starts here.
  ObjectId RequireSchemaId(duckdb::ClientContext* context, ObjectId role) const;

  // The SereneDB definition this entry stands for, or null for pg_catalog and
  // information_schema, which are generated rather than created.
  //
  // Published copy-on-write with std::atomic_store rather than by chaining an
  // entry version: this entry owns the CatalogSets of everything the schema
  // holds, so replacing it would take the version chains of its whole contents
  // with it. Its own definition is shared side state instead -- the pattern
  // Table::_data and the sequence counter already use. An owner or ACL change
  // on a schema therefore becomes visible when it commits rather than when the
  // reader's snapshot advances.
  // The definition and the owner/ACL beside it -- a schema entry is mutated in
  // place rather than versioned, so its permissions live here instead of on
  // CatalogEntry, where every other kind keeps them.
  // Both together, and by value: the cell is replaced whole on an ACL change,
  // so a reader that wants the permissions has to own the version it read --
  // handing out a reference into the cell leaves it dangling the moment another
  // session's GRANT drops the last other holder.
  catalog::HeldSchema Held() const;
  catalog::SchemaRef Definition() const;
  void SetDefinition(catalog::SchemaRef schema, catalog::Permissions perm);

  // pg_catalog and information_schema, whose content is fixed at startup.
  bool IsStatic() const noexcept { return _static_content; }

  // duckdb's ten sets plus the one kind it has no concept of.
  duckdb::CatalogSet& GetCatalogSet(duckdb::CatalogType type) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(
    duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
    duckdb::TableCatalogEntry& table) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(
    duckdb::CatalogTransaction transaction,
    duckdb::BoundCreateTableInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(
    duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSequenceInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTableFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCopyFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreatePragmaFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCollationInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(
    duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& info) final;

  void DropEntry(duckdb::ClientContext& context, duckdb::DropInfo& info) final;

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

 private:
  duckdb::optional_ptr<duckdb::CatalogEntry> LookupBuiltinFunction(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& info);

  std::shared_ptr<const catalog::HeldSchema> _definition;
  duckdb::CatalogSet _tokenizers;
  // pg_catalog and information_schema, whose content is fixed at startup and
  // generated into the sets on demand rather than projected per catalog
  // version. No transaction can add to it, so the sets answer for these two
  // schemas even while the statement holds an overlay.
  bool _static_content;
};

// The sets one kind occupies at once, and the ones a lookup of its own entry
// goes to -- GetCatalogSet above, for the two kinds that are not one set each.
// A function is in whichever of duckdb's two macro sets its own declaration
// puts it; an index is in two at once, its own and the relation-namespace
// wrapper behind SELECT * FROM <idx>, which is the one slot a lookup skips.
// The kind's own slot comes first: a wrapper projects what the primary entry
// says, and the walk that builds it reads the set that entry has just landed
// in.
std::span<const duckdb::CatalogType> EntrySlots(duckdb::CatalogType type);
std::span<const duckdb::CatalogType> LookupSlots(duckdb::CatalogType type);

// Whether serenedb put `entry` in the catalog, rather than duckdb owning it
// outright: only a serenedb entry carries a stable id, an owner and an ACL.
bool IsHostedEntry(const duckdb::CatalogEntry& entry) noexcept;

}  // namespace sdb::connector
