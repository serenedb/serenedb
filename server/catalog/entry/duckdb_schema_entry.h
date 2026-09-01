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

namespace sdb::catalog {

// duckdb's ten sets plus the one kind it has no concept of. Shared by every
// version of the schema entry, as duckdb's ten are.
class SereneDBSchemaSets final : public duckdb::SchemaCatalogSets {
 public:
  explicit SereneDBSchemaSets(duckdb::Catalog& catalog);

  duckdb::CatalogSet& Get(duckdb::CatalogType type) final;

 private:
  duckdb::CatalogSet _tokenizers;
};

class SereneDBSchemaEntry final : public duckdb::DuckSchemaEntry {
 public:
  SereneDBSchemaEntry(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info,
                      ObjectId id, catalog::Permissions perm);

  ObjectId GetDatabaseId() const;
  // This schema's id, re-resolved by name, with `role`'s CREATE privilege on
  // it enforced. Every DDL this entry performs starts here.
  ObjectId RequireSchemaId(duckdb::ClientContext* context, ObjectId role) const;

  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;

  // pg_catalog and information_schema, whose content is fixed at startup.
  bool IsStatic() const noexcept { return _static_content; }

  // The version this alter produces, for CatalogSet::AlterEntry to chain. It
  // takes the schema's whole contents over, exactly as a renamed table takes
  // over its predecessor's DataTable.
  duckdb::unique_ptr<duckdb::CatalogEntry> AlteredEntry(
    duckdb::CreateSchemaInfo& info, ObjectId id,
    catalog::Permissions perm) const;

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
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(
    duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& info) final;

  // A "did you mean" suggestion is only as visible as the entry it names:
  // the base scans the raw set, which would let the hint leak a name the
  // role has no right to see.
  duckdb::SimilarCatalogEntry GetSimilarEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& info) final;

  void DropEntry(duckdb::ClientContext& context, duckdb::DropInfo& info) final;

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

 private:
  SereneDBSchemaEntry(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info,
                      const duckdb::shared_ptr<duckdb::SchemaCatalogSets>& sets,
                      ObjectId id, catalog::Permissions perm);

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupBuiltinFunction(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& info);

  // pg_catalog and information_schema, whose content is fixed at startup and
  // generated into the sets on demand rather than projected per catalog
  // version. No transaction can add to it, so the sets answer for these two
  // schemas even while the statement holds an overlay.
  bool _static_content;
};

// Both function kinds share one set, as postgres has one function namespace;
// an index is in two sets at once, its own and the relation-namespace wrapper
// behind SELECT * FROM <idx>. The kind's own slot comes first: the walk that
// builds a wrapper reads the set the primary entry has just landed in.
std::span<const duckdb::CatalogType> EntrySlots(duckdb::CatalogType type);

// Whether serenedb put `entry` in the catalog, rather than duckdb owning it
// outright: only a serenedb entry carries a stable id, an owner and an ACL.
bool IsHostedEntry(const duckdb::CatalogEntry& entry) noexcept;

}  // namespace sdb::catalog
