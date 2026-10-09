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

#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>

#include <array>
#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

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

class SystemScan;

struct PrivChar {
  duckdb::AclMode mode;
  char chr;
};
inline constexpr std::array kPrivChars{
  PrivChar{duckdb::AclMode::Insert, 'a'},
  PrivChar{duckdb::AclMode::Select, 'r'},
  PrivChar{duckdb::AclMode::Update, 'w'},
  PrivChar{duckdb::AclMode::Delete, 'd'},
  PrivChar{duckdb::AclMode::Truncate, 'D'},
  PrivChar{duckdb::AclMode::References, 'x'},
  PrivChar{duckdb::AclMode::Trigger, 't'},
  PrivChar{duckdb::AclMode::Maintain, 'm'},
  PrivChar{duckdb::AclMode::Execute, 'X'},
  PrivChar{duckdb::AclMode::Usage, 'U'},
  PrivChar{duckdb::AclMode::Create, 'C'},
  PrivChar{duckdb::AclMode::CreateTemp, 'T'},
  PrivChar{duckdb::AclMode::Connect, 'c'},
  PrivChar{duckdb::AclMode::Set, 's'},
  PrivChar{duckdb::AclMode::AlterSystem, 'A'},
};

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

duckdb::idx_t CatalogClassOid(duckdb::CatalogType type);

duckdb::shared_ptr<duckdb::ViewColumnInfo> ViewColumns(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view);

std::vector<bool> NotNullColumns(const duckdb::TableCatalogEntry& table);

inline int16_t Attnum(const duckdb::ColumnDefinition& column) {
  return static_cast<int16_t>(column.Logical().index + 1);
}

template<typename Keys>
std::vector<int16_t> Attnums(const duckdb::ColumnList& columns, Keys&& keys) {
  return keys | std::views::transform([&](auto key) {
           return Attnum(columns.GetColumn(key));
         }) |
         std::ranges::to<std::vector>();
}

inline bool HasAttrdef(const duckdb::ColumnDefinition& column) {
  return (column.HasDefaultValue() || column.Generated()) &&
         column.CatalogOid() != 0;
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

std::vector<int16_t> ExpressionAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::ParsedExpression& expression);

std::string ExpressionText(const duckdb::ParsedExpression& expression);

duckdb::optional_ptr<const duckdb::TableCatalogEntry> SiblingTable(
  duckdb::CatalogTransaction transaction, const duckdb::CatalogEntry& member,
  const duckdb::Identifier& name);

duckdb::optional_ptr<const duckdb::TableCatalogEntry> ReferencedTable(
  const SystemScan& scan, const duckdb::TableCatalogEntry& table,
  const duckdb::ForeignKeyConstraint& fk);

const duckdb::UniqueConstraint* ReferencedKey(
  const duckdb::TableCatalogEntry& target, std::span<const int16_t> attnums);

std::string ConstraintName(const duckdb::TableCatalogEntry& table,
                           const duckdb::Constraint& constraint);

bool NumbersRows(const duckdb::CatalogEntry& sequence);

void VisitDefaultAcls(
  duckdb::ClientContext& context, const duckdb::CatalogEntry& database,
  absl::FunctionRef<void(duckdb::idx_t oid, const duckdb::DefaultAcl& row)>
    visitor);

struct ObjectName {
  std::string schema;
  std::string name;
};

struct SessionSchema {
  std::string name;
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> entry;
};

struct Session {
  duckdb::ClientContext* context;
  catalog::SereneDBCatalog* database;
  std::optional<duckdb::CatalogTransaction> transaction;
  std::vector<SessionSchema> search_path;
  mutable irs::containers::NodeHashMap<std::pair<uint8_t, uint64_t>,
                                       std::string>
    reg_out;
};

duckdb::optional_ptr<catalog::SereneDBCatalog> SessionCatalog(
  duckdb::ClientContext& context);
Session MakeSession(duckdb::ClientContext* context);
duckdb::optional_ptr<duckdb::CatalogEntry> EntryByOid(const Session& session,
                                                      uint64_t oid);
duckdb::optional_ptr<duckdb::CatalogEntry> FindMember(
  duckdb::CatalogTransaction transaction, duckdb::SchemaCatalogEntry& schema,
  duckdb::CatalogType type, const duckdb::Identifier& name);
duckdb::optional_ptr<duckdb::TableCatalogEntry> OwnerTable(
  const Session& session, uint64_t oid);
std::optional<ObjectName> RelationObject(const Session& session, uint64_t oid);
std::optional<ObjectName> TypeObject(const Session& session, uint64_t oid);
uint64_t ArrayElementOid(const Session& session, uint64_t oid);
std::string ArrayTypeName(duckdb::CatalogTransaction transaction,
                          const duckdb::CatalogEntry& element);

class ArrayTypeNames final {
 public:
  explicit ArrayTypeNames(const SystemScan& scan) : _scan{scan} {}

  std::string Of(const duckdb::CatalogEntry& element);

 private:
  static constexpr uint32_t kLookupsBeforeScan = 64;

  struct SchemaNames {
    uint32_t looked_up = 0;
    std::optional<bool> unprefixed;
  };

  bool Unprefixed(const duckdb::CatalogEntry& element);

  const SystemScan& _scan;
  absl::flat_hash_map<duckdb::idx_t, SchemaNames> _schemas;
};

const catalog::RoleCatalogEntry* FindRole(duckdb::ClientContext& context,
                                          std::string_view name);
std::optional<duckdb::idx_t> RoleOrPublic(duckdb::ClientContext& context,
                                          std::string_view name);

struct SubObject {
  duckdb::TableCatalogEntry* table = nullptr;
  const duckdb::UniqueConstraint* key_index = nullptr;
};

SubObject FindKeyIndex(const Session& session, uint64_t oid);
SubObject FindKeyIndex(duckdb::ClientContext& context,
                       duckdb::SchemaCatalogEntry& schema,
                       std::string_view name);

std::optional<std::string> NextvalArgument(
  const duckdb::ParsedExpression& expression);

}  // namespace pg
}  // namespace sdb
