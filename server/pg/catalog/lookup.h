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

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>

#include <array>
#include <cstdint>
#include <deque>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/oids.h"

namespace duckdb {

class IndexCatalogEntry;

}  // namespace duckdb
namespace sdb {
namespace auth {

struct RoleGraph;

}  // namespace auth
namespace catalog {

class RoleCatalogEntry;
class SereneDBCatalog;

}  // namespace catalog
namespace pg {

struct AclPrivilege {
  duckdb::AclMode mode;
  char chr;
  std::string_view name;
};
inline constexpr std::array kAclPrivileges{
  AclPrivilege{duckdb::AclMode::Insert, 'a', "INSERT"},
  AclPrivilege{duckdb::AclMode::Select, 'r', "SELECT"},
  AclPrivilege{duckdb::AclMode::Update, 'w', "UPDATE"},
  AclPrivilege{duckdb::AclMode::Delete, 'd', "DELETE"},
  AclPrivilege{duckdb::AclMode::Truncate, 'D', "TRUNCATE"},
  AclPrivilege{duckdb::AclMode::References, 'x', "REFERENCES"},
  AclPrivilege{duckdb::AclMode::Trigger, 't', "TRIGGER"},
  AclPrivilege{duckdb::AclMode::Execute, 'X', "EXECUTE"},
  AclPrivilege{duckdb::AclMode::Usage, 'U', "USAGE"},
  AclPrivilege{duckdb::AclMode::Create, 'C', "CREATE"},
  AclPrivilege{duckdb::AclMode::CreateTemp, 'T', "TEMPORARY"},
  AclPrivilege{duckdb::AclMode::Connect, 'c', "CONNECT"},
  AclPrivilege{duckdb::AclMode::Set, 's', "SET"},
  AclPrivilege{duckdb::AclMode::AlterSystem, 'A', "ALTER SYSTEM"},
  AclPrivilege{duckdb::AclMode::Maintain, 'm', "MAINTAIN"},
};

inline constexpr std::array kForeignDataWrapperAcl{duckdb::AclItem{
  .grantee = kPublicGrantee,
  .grantor = kRootUser,
  .privs = duckdb::AclMode::Usage,
}};

void AppendAcl(std::string& out, const duckdb::AclItem& item,
               const auth::RoleGraph& roles);

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

duckdb::shared_ptr<duckdb::ViewColumnInfo> ViewColumns(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view);
int16_t ViewAttnum(duckdb::ClientContext& context,
                   duckdb::ViewCatalogEntry& view,
                   const duckdb::Identifier& name);

std::vector<bool> NotNullColumns(const duckdb::TableCatalogEntry& table);

inline int16_t Attnum(const duckdb::ColumnDefinition& column) {
  return static_cast<int16_t>(column.Logical().index + 1);
}

template<typename Keys>
auto Attnums(const duckdb::ColumnList& columns, const Keys& keys) {
  return keys | std::views::transform([&columns](auto key) {
           return Attnum(columns.GetColumn(key));
         });
}

inline auto KeyAttnums(const duckdb::ColumnList& columns,
                       const duckdb::UniqueConstraint& key) {
  return std::views::iota(size_t{0}, key.HasIndex()
                                       ? size_t{1}
                                       : key.GetColumnNames().size()) |
         std::views::transform([&columns, &key](size_t i) {
           return Attnum(key.HasIndex()
                           ? columns.GetColumn(key.GetIndex())
                           : columns.GetColumn(key.GetColumnNames()[i]));
         });
}

bool HasDefault(const duckdb::ColumnDefinition& column);

inline bool HasAttrdef(const duckdb::ColumnDefinition& column) {
  return (HasDefault(column) || column.Generated()) && column.CatalogOid() != 0;
}

inline const duckdb::ParsedExpression& DefaultExpression(
  const duckdb::ColumnDefinition& column) {
  return column.Generated() ? column.GeneratedExpression()
                            : column.DefaultValue();
}

inline bool IsPgConstraint(
  const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
  return constraint->oid != 0 &&
         (constraint->type != duckdb::ConstraintType::FOREIGN_KEY ||
          constraint->Cast<duckdb::ForeignKeyConstraint>().info.type !=
            duckdb::ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE);
}

inline auto KeyIndexes(const duckdb::TableCatalogEntry& table) {
  return table.GetConstraints() |
         std::views::filter(
           [](const duckdb::unique_ptr<duckdb::Constraint>& constraint) {
             return constraint->type == duckdb::ConstraintType::UNIQUE &&
                    constraint->Cast<duckdb::UniqueConstraint>().index_oid != 0;
           }) |
         std::views::transform(
           [](const duckdb::unique_ptr<duckdb::Constraint>& constraint)
             -> const duckdb::UniqueConstraint& {
             return constraint->Cast<duckdb::UniqueConstraint>();
           });
}

std::vector<const duckdb::ParsedExpression*> IndexExpressions(
  const duckdb::IndexCatalogEntry& index);

std::vector<int16_t> IndexAttnums(duckdb::ClientContext& context,
                                  const duckdb::IndexCatalogEntry& index,
                                  duckdb::CatalogEntry& relation);

struct IndexColumn {
  std::string_view name;
  const duckdb::LogicalType& type;
};

inline IndexColumn IndexColumnAt(const duckdb::CatalogEntry& relation,
                                 const duckdb::ViewColumnInfo* columns,
                                 int16_t attnum) {
  static const duckdb::LogicalType kExpression{duckdb::LogicalTypeId::VARCHAR};
  if (attnum == 0) {
    return {"expr", kExpression};
  }
  const auto column = static_cast<duckdb::idx_t>(attnum - 1);
  if (relation.type == duckdb::CatalogType::VIEW_ENTRY) {
    return {relation.Cast<duckdb::ViewCatalogEntry>()
              .ColumnName(*columns, column)
              .GetIdentifierName(),
            columns->types[column]};
  }
  const auto& definition = relation.Cast<duckdb::TableCatalogEntry>().GetColumn(
    duckdb::LogicalIndex{column});
  return {definition.Name().GetIdentifierName(), definition.Type()};
}

std::vector<int16_t> ExpressionAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::ParsedExpression& expression);

std::string ExpressionText(const duckdb::ParsedExpression& expression);

duckdb::optional_ptr<const duckdb::TableCatalogEntry> SiblingTable(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& member,
  const duckdb::Identifier& name);

duckdb::optional_ptr<const duckdb::TableCatalogEntry> ReferencedTable(
  duckdb::CatalogTransaction transaction,
  const duckdb::TableCatalogEntry& table,
  const duckdb::ForeignKeyConstraint& fk);

template<typename R>
const duckdb::UniqueConstraint* ReferencedKey(
  const duckdb::TableCatalogEntry& target, const R& attnums) {
  for (const auto& key : KeyIndexes(target)) {
    if (absl::c_equal(KeyAttnums(target.GetColumns(), key), attnums)) {
      return &key;
    }
  }
  return nullptr;
}

SystemConcatText ConstraintName(const duckdb::TableCatalogEntry& table,
                                const duckdb::Constraint& constraint);

bool NumbersRows(const duckdb::CatalogEntry& sequence);

void VisitDefaultAcls(
  duckdb::ClientContext& context, const duckdb::CatalogEntry& database,
  absl::FunctionRef<void(duckdb::idx_t oid, const duckdb::DefaultAcl& row)>
    visitor);

struct ObjectName {
  std::string_view schema;
  std::string_view name;
};

inline ObjectName NameOf(duckdb::CatalogTransaction transaction,
                         const duckdb::CatalogEntry& entry) {
  return {.schema = entry.ParentSchema(transaction).name.GetIdentifierName(),
          .name = entry.name.GetIdentifierName()};
}

struct SessionSchema {
  std::string name;
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> entry;
};

struct RegOutRecent {
  uint64_t oid = 0;
  uint8_t kind = 0;
  std::string_view text;
};

inline constexpr size_t kRegOutLimit = 4096;

struct Session {
  duckdb::ClientContext* context;
  catalog::SereneDBCatalog* database;
  std::optional<duckdb::CatalogTransaction> transaction;
  mutable std::optional<duckdb::CatalogEntryRetriever> retriever;
  std::vector<SessionSchema> search_path;
  mutable irs::containers::FlatHashMap<std::pair<uint8_t, uint64_t>,
                                       std::string_view>
    reg_out;
  mutable std::deque<std::string> reg_out_texts;
  mutable std::array<RegOutRecent, 64> reg_out_recent;
};

duckdb::optional_ptr<catalog::SereneDBCatalog> SessionCatalog(
  duckdb::ClientContext& context);
Session MakeSession(duckdb::ClientContext* context);

struct SessionState final : duckdb::FunctionLocalState {
  explicit SessionState(duckdb::ClientContext* context)
    : session{MakeSession(context)} {}

  Session session;
};
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

std::optional<std::string_view> NextvalArgument(
  const duckdb::ParsedExpression& expression);

}  // namespace pg
}  // namespace sdb
