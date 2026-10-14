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

#include <array>
#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/utils/oids.h"
#include "pg/catalog/utils/session.h"

namespace sdb {
namespace catalog {

class RoleCatalogEntry;

}  // namespace catalog
namespace pg {

inline constexpr std::array kSchemaObjectTypes{
  duckdb::CatalogType::TABLE_ENTRY,       duckdb::CatalogType::VIEW_ENTRY,
  duckdb::CatalogType::INDEX_ENTRY,       duckdb::CatalogType::SEQUENCE_ENTRY,
  duckdb::CatalogType::TYPE_ENTRY,        duckdb::CatalogType::MACRO_ENTRY,
  duckdb::CatalogType::TABLE_MACRO_ENTRY,
};
inline constexpr auto kRelationTypes = std::span{kSchemaObjectTypes}.first<5>();
inline constexpr auto kTableTypes = std::span{kSchemaObjectTypes}.first<1>();
inline constexpr auto kTableViewTypes =
  std::span{kSchemaObjectTypes}.first<2>();
inline constexpr auto kViewTypes =
  std::span{kSchemaObjectTypes}.subspan<1, 1>();
inline constexpr auto kSequenceTypes =
  std::span{kSchemaObjectTypes}.subspan<3, 1>();
inline constexpr auto kTypeTypes =
  std::span{kSchemaObjectTypes}.subspan<4, 1>();
inline constexpr auto kFunctionTypes = std::span{kSchemaObjectTypes}.last<2>();
inline constexpr std::array kSchemaTypes{duckdb::CatalogType::SCHEMA_ENTRY};
inline constexpr std::array kIndexTypes{duckdb::CatalogType::TABLE_ENTRY,
                                        duckdb::CatalogType::INDEX_ENTRY};
inline constexpr std::array kTokenizerTypes{
  duckdb::CatalogType::TOKENIZER_ENTRY};

duckdb::idx_t CatalogClassOid(duckdb::CatalogType type);

struct ObjectName {
  std::string_view schema;
  std::string_view name;
};

inline ObjectName NameOf(duckdb::CatalogTransaction transaction,
                         const duckdb::CatalogEntry& entry) {
  return {.schema = entry.ParentSchema(transaction).name.GetIdentifierName(),
          .name = entry.name.GetIdentifierName()};
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntryByOid(const Session& session,
                                                      uint64_t oid);
inline duckdb::optional_ptr<duckdb::CatalogEntry> FindMember(
  duckdb::CatalogTransaction transaction, duckdb::SchemaCatalogEntry& schema,
  duckdb::CatalogType type, const duckdb::Identifier& name) {
  return schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(type).GetEntry(
    transaction, name);
}
duckdb::optional_ptr<duckdb::TableCatalogEntry> OwnerTable(
  const Session& session, uint64_t oid);
std::optional<ObjectName> TypeObject(const Session& session, uint64_t oid);
uint64_t ArrayElementOid(const Session& session, uint64_t oid);

struct ReplicationOrigin {
  duckdb::idx_t id;
  std::string name;
  uint64_t remote_lsn;
};

std::vector<ReplicationOrigin> ReplicationOrigins(duckdb::DatabaseInstance& db);
std::string ArrayTypeName(duckdb::CatalogTransaction transaction,
                          const duckdb::CatalogEntry& element);

class ArrayTypeNames final {
 public:
  explicit ArrayTypeNames(const SystemScan& scan) : _scan{scan} {}

  SystemText Of(const duckdb::CatalogEntry& element);

 private:
  const SystemScan& _scan;
  duckdb::idx_t _schema = kInvalidOid;
  bool _unprefixed = false;
};

const catalog::RoleCatalogEntry* FindRole(duckdb::ClientContext& context,
                                          std::string_view name);
std::optional<duckdb::idx_t> RoleOrPublic(duckdb::ClientContext& context,
                                          std::string_view name);
duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabase(
  duckdb::ClientContext& context, std::string_view name);
duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabaseById(
  duckdb::ClientContext& context, duckdb::idx_t id);

struct SubObject {
  duckdb::TableCatalogEntry* table = nullptr;
  const duckdb::UniqueConstraint* key_index = nullptr;
};

SubObject FindKeyIndex(const Session& session, uint64_t oid);
SubObject FindKeyIndex(duckdb::ClientContext& context,
                       duckdb::SchemaCatalogEntry& schema,
                       std::string_view name);

struct StaticView;

struct Relation {
  duckdb::optional_ptr<duckdb::CatalogEntry> entry;
  const duckdb::UniqueConstraint* key;
  const SystemTable* system;
  const StaticView* view;
};

std::optional<Relation> FindRelation(const Session& session, uint64_t oid);
ObjectName NameOf(duckdb::CatalogTransaction transaction,
                  const Relation& relation);
const duckdb::ColumnList* TableColumns(const Relation& relation);

struct RelationAccess : Relation {
  const duckdb::Permissions* permissions;
};

std::optional<RelationAccess> RelationByOid(const Session& session,
                                            uint64_t oid);

}  // namespace pg
}  // namespace sdb
