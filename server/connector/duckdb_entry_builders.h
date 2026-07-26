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

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <memory>
#include <optional>
#include <string_view>

#include "catalog/entry.h"
#include "catalog/fwd.h"
#include "catalog/table.h"
#include "connector/duckdb_entry.h"

namespace sdb::connector {

class SereneDBSchemaEntry;

// The duckdb entry for one SereneDB object. One builder per kind, used by the
// mutators and by boot's seeding alike -- so a table looks the same whichever
// side asked for it. The peers a definition names (an index's relation, a
// foreign key's referent) are resolved by identity in `catalog`, whose sets
// already hold them: their entries are placed before the ones that project
// them.
// `storage` is the rows this version owns: the DataTable the version it
// replaces held, the reshaped one a reshaping statement produced, or null --
// which makes the entry build its own, and is what a create and a search table
// both want.
duckdb::unique_ptr<duckdb::CatalogEntry> MakeTableEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, catalog::TableInfoRef table,
  catalog::Permissions perm, duckdb::ClientContext* context,
  duckdb::shared_ptr<duckdb::DataTable> storage = nullptr,
  duckdb::shared_ptr<duckdb::CatalogSet> inherited_triggers = nullptr);

duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndexEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  catalog::IndexInfoRef index, duckdb::ClientContext* context);

// The "index name as table" wrapper behind `SELECT * FROM <index>`: an index
// name is in the relation namespace, so it also answers a TABLE_ENTRY lookup.
// Null when the index has no scannable shape -- a secondary index on a view.
duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndexScanEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  std::string_view entry_name, catalog::IndexInfoRef index,
  duckdb::ClientContext* context);

// `perm` is the owner and ACL the entry carries: they are the entry's own, not
// the definition's, so every builder for a kind that has them takes them beside
// it.
duckdb::unique_ptr<duckdb::CatalogEntry> MakeSequenceEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const catalog::CreateSequenceInfo& sequence,
  std::shared_ptr<catalog::SequenceCounter> counter, catalog::Permissions perm);

duckdb::unique_ptr<duckdb::CatalogEntry> MakeViewEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, const duckdb::CreateViewInfo& view,
  catalog::Permissions perm);

// `internal` marks the entry as a builtin, which is what the pg_catalog and
// information_schema functions are; a schema's own functions are never that.
duckdb::unique_ptr<duckdb::CatalogEntry> MakeMacroEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, bool internal,
  const duckdb::CreateMacroInfo& function, catalog::Permissions perm);

// The builder for one slot of one version, dispatched by the kind the version's
// own CreateInfo names -- duckdb's Catalog::CreateEntry shape, over the
// builders above. Null for a slot this definition does not occupy: a scalar
// macro is not in the table-macro set, and a secondary index on a view has no
// scannable shape to wrap.
//
// `superseded` is the version this one replaces, under the name it is still
// filed by: a table hands its rows and its triggers over.
duckdb::unique_ptr<duckdb::StandardEntry> MakeEntry(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  const std::shared_ptr<const duckdb::CreateInfo>& info,
  const catalog::Permissions& perm, duckdb::CatalogType slot,
  duckdb::ClientContext* context,
  duckdb::optional_ptr<duckdb::CatalogEntry> superseded);

// The definition an entry holds -- what GRANT, REVOKE and ALTER ... OWNER TO
// all write back, since none of them changes it. The one place that knows
// which class a kind's entry is, which is the switch duckdb spells as
// GetCatalogSet on its own side.
std::shared_ptr<const duckdb::CreateInfo> EntryDefinition(
  const duckdb::CatalogEntry& entry);

// The next version of the object `entry` holds under a new name or comment,
// with the resolution its body implies taken now. An empty `name` keeps the
// one it has; an unset `comment` keeps the comment. Null when the definition
// already says that, which postgres treats as a no-op rather than a new
// version.
std::shared_ptr<const duckdb::CreateInfo> RewrittenDefinition(
  duckdb::ClientContext* context, const duckdb::CatalogEntry& entry,
  std::string_view name, std::optional<std::string_view> comment);

// The sibling entries the version `entry` holds reshapes. Nothing here touches
// the kind's own set, so it cannot come back round.
void RefreshEntrySiblings(duckdb::ClientContext* context,
                          const duckdb::CatalogEntry& entry);

// The cluster-global kinds have no builder: a role, a database and a foreign
// server are built where their entry is put (PutRole, PlaceEntry).

}  // namespace sdb::connector
