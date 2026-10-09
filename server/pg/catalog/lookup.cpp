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

#include "pg/catalog/lookup.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <limits>
#include <mutex>
#include <ranges>
#include <span>
#include <tuple>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/role.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"

namespace sdb::pg {

duckdb::idx_t CatalogClassOid(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  switch (type) {
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
      return kPgClassTable;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return kPgProcTable;
    case TYPE_ENTRY:
      return kPgTypeTable;
    case SCHEMA_ENTRY:
      return kPgNamespaceTable;
    case DATABASE_ENTRY:
      return kPgDatabaseTable;
    case ROLE_ENTRY:
      return kPgAuthidTable;
    case FOREIGN_SERVER_ENTRY:
      return kPgForeignServerTable;
    case TOKENIZER_ENTRY:
      return kPgTsDictTable;
    case INVALID:
    case PREPARED_STATEMENT:
    case COLLATION_ENTRY:
    case COORDINATE_SYSTEM_ENTRY:
    case TRIGGER_ENTRY:
    case JOB_ENTRY:
    case TABLE_FUNCTION_ENTRY:
    case SCALAR_FUNCTION_ENTRY:
    case AGGREGATE_FUNCTION_ENTRY:
    case PRAGMA_FUNCTION_ENTRY:
    case COPY_FUNCTION_ENTRY:
    case WINDOW_FUNCTION_ENTRY:
    case DELETED_ENTRY:
    case RENAMED_ENTRY:
    case SECRET_ENTRY:
    case SECRET_TYPE_ENTRY:
    case SECRET_FUNCTION_ENTRY:
    case DEPENDENCY_ENTRY:
      return kInvalidOid;
  }
}

duckdb::shared_ptr<duckdb::ViewColumnInfo> ViewColumns(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view) {
  if (auto columns = view.GetColumnInfo()) {
    return columns;
  }
  try {
    view.BindView(context);
  } catch (const std::exception&) {
  }
  return view.GetColumnInfo();
}

std::vector<bool> NotNullColumns(const duckdb::TableCatalogEntry& table) {
  std::vector<bool> not_null(table.GetColumns().LogicalColumnCount());
  for (const auto& constraint :
       table.GetConstraints() | std::views::filter([](const auto& constraint) {
         return constraint->type == duckdb::ConstraintType::NOT_NULL;
       })) {
    not_null[constraint->Cast<duckdb::NotNullConstraint>().index.index] = true;
  }
  return not_null;
}

namespace {

template<typename T, typename F>
std::vector<T> MapIndexExpressions(const duckdb::IndexCatalogEntry& index,
                                   F&& map) {
  std::vector<T> result;
  result.reserve(index.parsed_expressions.size());
  for (const bool included : {false, true}) {
    for (size_t i = 0; i < index.parsed_expressions.size(); ++i) {
      if ((i < index.column_opclasses.size() &&
           index.column_opclasses[i] == catalog::kIncludedKind) == included) {
        result.emplace_back(map(*index.parsed_expressions[i]));
      }
    }
  }
  return result;
}

}  // namespace

std::vector<const duckdb::ParsedExpression*> IndexExpressions(
  const duckdb::IndexCatalogEntry& index) {
  return MapIndexExpressions<const duckdb::ParsedExpression*>(
    index,
    [](const duckdb::ParsedExpression& expression) { return &expression; });
}

std::vector<int16_t> IndexAttnums(duckdb::ClientContext& context,
                                  const duckdb::IndexCatalogEntry& index,
                                  duckdb::CatalogEntry& relation) {
  auto* view = relation.type == duckdb::CatalogType::VIEW_ENTRY
                 ? &relation.Cast<duckdb::ViewCatalogEntry>()
                 : nullptr;
  const auto columns = view ? ViewColumns(context, *view) : nullptr;
  const auto attnum = [&](const duckdb::Identifier& name) -> int16_t {
    if (!view) {
      const auto& table = relation.Cast<duckdb::TableCatalogEntry>();
      return table.ColumnExists(name) ? Attnum(table.GetColumn(name)) : 0;
    }
    if (!columns) {
      return 0;
    }
    const auto it = absl::c_find(columns->names, name);
    return it == columns->names.end()
             ? 0
             : static_cast<int16_t>(it - columns->names.begin() + 1);
  };
  return MapIndexExpressions<int16_t>(
    index, [&](const duckdb::ParsedExpression& expression) -> int16_t {
      return expression.GetExpressionClass() ==
                 duckdb::ExpressionClass::COLUMN_REF
               ? attnum(expression.Cast<duckdb::ColumnRefExpression>()
                          .GetColumnName())
               : 0;
    });
}

std::vector<int16_t> ExpressionAttnums(
  const duckdb::TableCatalogEntry& table,
  const duckdb::ParsedExpression& expression) {
  std::vector<int16_t> attnums;
  duckdb::ParsedExpressionIterator::VisitExpression<
    duckdb::ColumnRefExpression>(
    expression, [&](const duckdb::ColumnRefExpression& column) {
      const auto& name = column.GetColumnName();
      if (!table.ColumnExists(name)) {
        return;
      }
      const auto attnum = Attnum(table.GetColumn(name));
      if (!absl::c_linear_search(attnums, attnum)) {
        attnums.emplace_back(attnum);
      }
    });
  return attnums;
}

std::string ExpressionText(const duckdb::ParsedExpression& expression) {
  auto copy = expression.Copy();
  duckdb::ParsedExpressionIterator::VisitExpressionMutable<
    duckdb::ColumnRefExpression>(*copy, [](duckdb::ColumnRefExpression& ref) {
    auto& names = ref.ColumnNamesMutable();
    if (names.size() > 1) {
      names.erase(names.begin(), names.end() - 1);
    }
  });
  return copy->ToString();
}

duckdb::optional_ptr<const duckdb::TableCatalogEntry> ReferencedTable(
  const SystemScan& scan, const duckdb::TableCatalogEntry& table,
  const duckdb::ForeignKeyConstraint& fk) {
  if (fk.info.type == duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
    return &table;
  }
  auto entry =
    FindMember(scan.Transaction(), table.ParentSchema(scan.Transaction()),
               duckdb::CatalogType::TABLE_ENTRY, fk.info.table);
  if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return &entry->Cast<duckdb::TableCatalogEntry>();
}

duckdb::optional_ptr<const duckdb::TableCatalogEntry> TriggerTable(
  const SystemScan& scan, const duckdb::TriggerCatalogEntry& trigger) {
  auto entry =
    FindMember(scan.Transaction(), trigger.ParentSchema(scan.Transaction()),
               duckdb::CatalogType::TABLE_ENTRY, trigger.base_table->Table());
  if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return &entry->Cast<duckdb::TableCatalogEntry>();
}

const duckdb::UniqueConstraint* ReferencedKey(
  const duckdb::TableCatalogEntry& target, std::span<const int16_t> attnums) {
  for (const auto& key : KeyIndexes(target)) {
    if (absl::c_equal(Attnums(target.GetColumns(),
                              key.GetLogicalIndexes(target.GetColumns())),
                      attnums)) {
      return &key;
    }
  }
  return nullptr;
}

std::string ConstraintName(const duckdb::TableCatalogEntry& table,
                           const duckdb::Constraint& constraint) {
  if (!constraint.constraint_name.empty() ||
      constraint.type != duckdb::ConstraintType::NOT_NULL) {
    return constraint.constraint_name;
  }
  return absl::StrCat(
    table.name.GetIdentifierName(), "_",
    table.GetColumn(constraint.Cast<duckdb::NotNullConstraint>().index)
      .Name()
      .GetIdentifierName(),
    "_not_null");
}

bool NumbersRows(const duckdb::CatalogEntry& sequence) {
  return sequence.tags.contains(std::string{catalog::kGeneratedPkSequenceTag});
}

namespace {

void PutId(std::string& out, std::string_view name) {
  const bool safe = absl::c_all_of(name, [](unsigned char c) {
    return !(c & 0x80) && (absl::ascii_isalnum(c) || c == '_');
  });
  if (safe) {
    out.append(name);
    return;
  }
  out.append(duckdb::KeywordHelper::WriteQuotedAndEscaped(name, '"'));
}

void PutRole(std::string& out, duckdb::idx_t role,
             const auth::RoleGraph& roles) {
  if (const auto name = roles.NameOf(role); !name.empty()) {
    PutId(out, name);
    return;
  }
  absl::StrAppend(&out, role);
}

}  // namespace

void AppendAcl(std::string& out, const duckdb::AclItem& item,
               const auth::RoleGraph& roles) {
  if (item.grantee != kPublicGrantee) {
    PutRole(out, item.grantee, roles);
  }
  out.push_back('=');
  for (const auto& p : kPrivChars) {
    if ((item.privs & p.mode) != duckdb::AclMode::NoRights) {
      out.push_back(p.chr);
      if ((item.grant_option & p.mode) != duckdb::AclMode::NoRights) {
        out.push_back('*');
      }
    }
  }
  out.push_back('/');
  PutRole(out, item.grantor, roles);
}

void VisitDefaultAcls(
  duckdb::ClientContext& context, const duckdb::CatalogEntry& database,
  absl::FunctionRef<void(duckdb::idx_t oid, const duckdb::DefaultAcl& row)>
    visitor) {
  auto attached =
    duckdb::DatabaseManager::Get(context).GetDatabase(context, database.name);
  if (!attached || attached->oid != database.oid) {
    return;
  }
  auto& catalog = attached->GetCatalog().Cast<duckdb::DuckCatalog>();
  const auto view = catalog.GetCatalogTransaction(context).view;
  for (const auto& [oid, row] :
       std::views::zip(std::views::iota(duckdb::idx_t{1}),
                       database.permissions.defaults) |
         std::views::filter([&](const auto& item) {
           const auto scope = std::get<1>(item).scope;
           return scope == kInvalidOid ||
                  catalog.GetOidIndex().GetVisible(scope, view);
         })) {
    visitor(oid, row);
  }
}

duckdb::optional_ptr<catalog::SereneDBCatalog> SessionCatalog(
  duckdb::ClientContext& context) {
  if (!connector::GetSereneDBContextPtr(context)) {
    return nullptr;
  }
  return duckdb::Catalog::GetCatalog(
           context, duckdb::DatabaseManager::GetDefaultDatabase(context))
    .Cast<catalog::SereneDBCatalog>();
}

Session MakeSession(duckdb::ClientContext* context) {
  Session session{
    .context = context,
    .database = context ? SessionCatalog(*context).get() : nullptr,
    .transaction = std::nullopt,
    .search_path = {},
    .reg_out = {}};
  if (!context) {
    return session;
  }
  if (session.database) {
    session.transaction.emplace(
      session.database->GetCatalogTransaction(*context));
  }
  const auto add = [&](std::string_view name) {
    if (absl::c_any_of(session.search_path, [&](const SessionSchema& schema) {
          return schema.name == name;
        })) {
      return;
    }
    session.search_path.emplace_back(SessionSchema{
      .name = std::string{name},
      .entry = session.database
                 ? session.database->GetSchema(
                     *session.transaction, duckdb::Identifier{name},
                     duckdb::OnEntryNotFound::RETURN_NULL)
                 : nullptr});
  };
  const auto& search_path =
    *duckdb::ClientData::Get(*context).catalog_search_path;
  if (absl::c_none_of(search_path.GetResolvedSetPaths(),
                      [](const duckdb::CatalogSearchEntry& entry) {
                        return entry.GetSchema() ==
                               irs::StaticStrings::kPgCatalogSchema;
                      })) {
    add(irs::StaticStrings::kPgCatalogSchema);
  }
  const auto database =
    duckdb::DatabaseManager::TryGetDefaultDatabase(*context);
  for (const auto& entry : search_path.Get()) {
    const auto& catalog = entry.GetCatalog();
    if (catalog.empty() || catalog == database.GetIdentifierName()) {
      add(entry.GetSchema().GetIdentifierName());
    }
  }
  return session;
}

duckdb::optional_ptr<duckdb::CatalogEntry> EntryByOid(const Session& session,
                                                      uint64_t oid) {
  if (!session.database || oid == kInvalidOid) {
    return nullptr;
  }
  auto entry =
    session.database->GetOidIndex().GetVisible(oid, session.transaction->view);
  if (!entry || entry->internal) {
    return nullptr;
  }
  return entry;
}

duckdb::optional_ptr<duckdb::CatalogEntry> FindMember(
  duckdb::CatalogTransaction transaction, duckdb::SchemaCatalogEntry& schema,
  duckdb::CatalogType type, const duckdb::Identifier& name) {
  return schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(type).GetEntry(
    transaction, name);
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> OwnerTable(
  const Session& session, uint64_t oid) {
  if (!session.database || oid == kInvalidOid) {
    return nullptr;
  }
  auto owner = session.database->GetOidIndex().GetVisibleOwner(
    oid, session.transaction->view);
  if (!owner || owner->type != duckdb::CatalogType::TABLE_ENTRY) {
    return nullptr;
  }
  return owner->Cast<duckdb::TableCatalogEntry>();
}

const catalog::RoleCatalogEntry* FindRole(duckdb::ClientContext& context,
                                          std::string_view name) {
  auto& cluster = catalog::ClusterOf(context);
  auto entry = cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
                 .GetEntry(cluster.GetCatalogTransaction(context),
                           duckdb::Identifier{name});
  return entry ? &entry->Cast<catalog::RoleCatalogEntry>() : nullptr;
}

std::optional<duckdb::idx_t> RoleOrPublic(duckdb::ClientContext& context,
                                          std::string_view name) {
  if (absl::EqualsIgnoreCase(name, irs::StaticStrings::kPublic)) {
    return kPublicGrantee;
  }
  if (const auto* role = FindRole(context, name)) {
    return role->oid;
  }
  return std::nullopt;
}

std::optional<ObjectName> RelationObject(const Session& session, uint64_t oid) {
  if (auto entry = EntryByOid(session, oid)) {
    if (CatalogClassOid(entry->type) != kPgClassTable) {
      return std::nullopt;
    }
    return ObjectName{entry->ParentSchemaName().GetIdentifierName(),
                      entry->name.GetIdentifierName()};
  }
  if (auto table = OwnerTable(session, oid)) {
    for (const auto& key : KeyIndexes(*table)) {
      if (key.index_oid == oid) {
        return ObjectName{table->ParentSchemaName().GetIdentifierName(),
                          key.constraint_name};
      }
    }
  }
  if (const auto* table = FindSystemTable(oid)) {
    return ObjectName{std::string{table->Sql().schema},
                      std::string{table->Sql().name}};
  }
  if (const auto* view = FindSystemView(oid)) {
    return ObjectName{std::string{view->schema}, std::string{view->name}};
  }
  return std::nullopt;
}

std::string ArrayTypeName(duckdb::CatalogTransaction transaction,
                          const duckdb::CatalogEntry& element) {
  using enum duckdb::CatalogType;
  const auto& name = element.name.GetIdentifierName();
  auto& schema = element.ParentSchema(transaction);
  auto array = absl::StrCat("_", name);
  const auto taken = [&](std::string_view candidate) {
    return absl::c_any_of(std::array{TYPE_ENTRY, TABLE_ENTRY}, [&](auto type) {
      return static_cast<bool>(schema.LookupEntry(
        transaction,
        duckdb::EntryLookupInfo{type, duckdb::Identifier{candidate}}));
    });
  };
  if (!name.starts_with('_') && !taken(array)) {
    return array;
  }
  const auto stem = [](std::string_view text) {
    const auto first = text.find_first_not_of('_');
    return first == std::string_view::npos ? std::string_view{}
                                           : text.substr(first);
  };
  std::vector<std::pair<duckdb::idx_t, std::string_view>> family;
  for (const auto type : {TYPE_ENTRY, TABLE_ENTRY}) {
    schema.Scan(transaction, type, [&](duckdb::CatalogEntry& entry) {
      if (!entry.internal &&
          stem(entry.name.GetIdentifierName()) == stem(name)) {
        family.emplace_back(entry.oid, entry.name.GetIdentifierName());
      }
    });
  }
  absl::c_sort(family);
  irs::containers::FlatHashSet<std::string> names;
  for (const auto& member : family) {
    names.emplace(member.second);
  }
  for (const auto& [oid, member] : family) {
    auto candidate = absl::StrCat("_", member);
    while (names.contains(candidate)) {
      candidate.insert(0, "_");
    }
    names.insert(candidate);
    if (oid == element.oid) {
      return candidate;
    }
  }
  return array;
}

std::string ArrayTypeNames::Of(const duckdb::CatalogEntry& element) {
  const auto& name = element.name.GetIdentifierName();
  if (!name.starts_with('_') && Unprefixed(element)) {
    return absl::StrCat("_", name);
  }
  return ArrayTypeName(_scan.Transaction(), element);
}

bool ArrayTypeNames::Unprefixed(const duckdb::CatalogEntry& element) {
  auto& names = _schemas[element.ParentSchemaOid()];
  if (!names.unprefixed && ++names.looked_up > kLookupsBeforeScan) {
    const auto transaction = _scan.Transaction();
    auto& schema = element.ParentSchema(transaction);
    bool prefixed = false;
    for (const auto type :
         {duckdb::CatalogType::TYPE_ENTRY, duckdb::CatalogType::TABLE_ENTRY}) {
      schema.Scan(transaction, type, [&](duckdb::CatalogEntry& entry) {
        prefixed = prefixed || entry.name.GetIdentifierName().starts_with('_');
      });
    }
    names.unprefixed = !prefixed;
  }
  return names.unprefixed.value_or(false);
}

std::optional<ObjectName> TypeObject(const Session& session, uint64_t oid) {
  if (oid & kRowTypeOidBit) {
    const auto relation = RowTypeRelation(oid);
    auto object = RelationObject(session, relation);
    if (oid & kRowArrayTypeOidBit) {
      if (auto entry = EntryByOid(session, relation); object && entry) {
        object->name = ArrayTypeName(*session.transaction, *entry);
      }
    }
    return object;
  }
  if (oid <= std::numeric_limits<int32_t>::max()) {
    if (const auto* builtin = FindBuiltinType(static_cast<int32_t>(oid))) {
      return ObjectName{std::string{FindSystemNamespace(builtin->nsp)->name},
                        std::string{builtin->name}};
    }
  }
  if (auto entry = EntryByOid(session, oid);
      entry && entry->type == duckdb::CatalogType::TYPE_ENTRY) {
    return ObjectName{entry->ParentSchemaName().GetIdentifierName(),
                      entry->name.GetIdentifierName()};
  }
  if (const auto element = ArrayElementOid(session, oid);
      element != kInvalidOid) {
    const auto entry = EntryByOid(session, element);
    return ObjectName{entry->ParentSchemaName().GetIdentifierName(),
                      ArrayTypeName(*session.transaction, *entry)};
  }
  return std::nullopt;
}

uint64_t ArrayElementOid(const Session& session, uint64_t oid) {
  if (oid & kRowTypeOidBit) {
    return oid & kRowArrayTypeOidBit ? oid & ~kRowArrayTypeOidBit : kInvalidOid;
  }
  const auto entry = EntryByOid(session, oid + 1);
  return entry && entry->type == duckdb::CatalogType::TYPE_ENTRY ? oid + 1
                                                                 : kInvalidOid;
}

SubObject FindKeyIndex(duckdb::ClientContext& context,
                       duckdb::SchemaCatalogEntry& schema,
                       std::string_view name) {
  SubObject found;
  const auto match = [&](duckdb::CatalogEntry& entry) {
    if (found.table || entry.type != duckdb::CatalogType::TABLE_ENTRY) {
      return;
    }
    auto& table = entry.Cast<duckdb::TableCatalogEntry>();
    for (const auto& key : KeyIndexes(table)) {
      if (key.constraint_name == name) {
        found = {.table = &table, .key_index = &key};
        return;
      }
    }
  };
  const auto transaction =
    schema.ParentCatalog().GetCatalogTransaction(context);
  for (auto end = name.rfind('_'); end != 0 && end != std::string_view::npos;
       end = name.rfind('_', end - 1)) {
    if (auto entry =
          FindMember(transaction, schema, duckdb::CatalogType::TABLE_ENTRY,
                     duckdb::Identifier{name.substr(0, end)})) {
      match(*entry);
      if (found.table) {
        return found;
      }
    }
  }
  auto* serene =
    dynamic_cast<catalog::SereneDBCatalog*>(&schema.ParentCatalog());
  const auto snapshot =
    serene ? serene->Snapshot(
               context, schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(
                          duckdb::CatalogType::TABLE_ENTRY))
           : nullptr;
  if (!snapshot) {
    schema.Scan(context, duckdb::CatalogType::TABLE_ENTRY, match);
    return found;
  }
  std::call_once(snapshot->keys_once, [&] {
    for (auto* entry : snapshot->entries) {
      if (entry->type != duckdb::CatalogType::TABLE_ENTRY) {
        continue;
      }
      auto& table = entry->Cast<duckdb::TableCatalogEntry>();
      for (const auto& key : KeyIndexes(table)) {
        snapshot->keys.try_emplace(key.constraint_name, &table, &key);
      }
    }
  });
  if (const auto it = snapshot->keys.find(name); it != snapshot->keys.end()) {
    found = {.table = it->second.first, .key_index = it->second.second};
  }
  return found;
}

std::optional<std::string> NextvalArgument(
  const duckdb::ParsedExpression& expression) {
  if (expression.GetExpressionClass() != duckdb::ExpressionClass::FUNCTION) {
    return std::nullopt;
  }
  const auto& function = expression.Cast<duckdb::FunctionExpression>();
  const auto& arguments = function.GetArguments();
  if (function.FunctionName() != duckdb::Identifier{"nextval"} ||
      arguments.size() != 1) {
    return std::nullopt;
  }
  const auto* argument = &arguments[0].GetExpression();
  if (argument->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
    argument = &argument->Cast<duckdb::CastExpression>().Child();
  }
  if (argument->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
    return std::nullopt;
  }
  const auto value =
    argument->Cast<duckdb::ConstantExpression>().GetLiteral().ToValue();
  if (value.IsNull()) {
    return std::nullopt;
  }
  return value.ToString();
}

}  // namespace sdb::pg
