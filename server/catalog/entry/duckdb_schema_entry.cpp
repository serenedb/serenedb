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

#include "catalog/entry/duckdb_schema_entry.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

#include "auth/role_closure.h"
#include "basics/static_strings.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_system_table_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/index.h"
#include "catalog/log/data_store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/read/duckdb_dependency.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/virtual_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_index_options_util.h"
#include "connector/pg_logical_types.h"
#include "connector/search_table_dispatch.h"
#include "connector/with_option_resolver.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/system_catalog.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::catalog {
namespace {

// The single column a CHECK expression references, or empty if it references
// zero or multiple distinct columns. Drives PostgreSQL-style auto naming
// (<table>_<col>_check vs <table>_check), shared by CREATE TABLE and
// ALTER TABLE ADD CONSTRAINT.
std::string FindConstraintColumn(const duckdb::ParsedExpression& root) {
  std::string result;
  bool multiple = false;
  std::function<void(const duckdb::ParsedExpression&)> visit;
  visit = [&](const duckdb::ParsedExpression& expr) {
    if (multiple) {
      return;
    }
    if (expr.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      const auto& name = expr.Cast<duckdb::ColumnRefExpression>()
                           .GetColumnName()
                           .GetIdentifierName();
      if (result.empty()) {
        result = name;
      } else if (result != name) {
        multiple = true;
      }
      return;
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](const duckdb::ParsedExpression& child) { visit(child); });
  };
  visit(root);
  return multiple ? std::string{} : result;
}

[[noreturn]] void ThrowRelationMissing(std::string_view name) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                  ERR_MSG("relation \"", name, "\" does not exist"));
}

// PG's spelling of an ALTER TABLE action, for the errors that name it. Empty
// for the actions PG has no name for, where the caller falls back to a generic
// refusal.
std::string_view AlterActionName(duckdb::AlterTableType type) noexcept {
  switch (type) {
    case duckdb::AlterTableType::RENAME_COLUMN:
      return "RENAME COLUMN";
    case duckdb::AlterTableType::ADD_COLUMN:
      return "ADD COLUMN";
    case duckdb::AlterTableType::REMOVE_COLUMN:
      return "DROP COLUMN";
    case duckdb::AlterTableType::ALTER_COLUMN_TYPE:
      return "ALTER COLUMN TYPE";
    case duckdb::AlterTableType::SET_DEFAULT:
      return "ALTER COLUMN SET DEFAULT";
    case duckdb::AlterTableType::SET_NOT_NULL:
      return "SET NOT NULL";
    case duckdb::AlterTableType::DROP_NOT_NULL:
      return "DROP NOT NULL";
    case duckdb::AlterTableType::ADD_CONSTRAINT:
    case duckdb::AlterTableType::FOREIGN_KEY_CONSTRAINT:
      return "ADD CONSTRAINT";
    case duckdb::AlterTableType::DROP_CONSTRAINT:
      return "DROP CONSTRAINT";
    case duckdb::AlterTableType::RENAME_CONSTRAINT:
      return "RENAME CONSTRAINT";
    default:
      return {};
  }
}

// PG accepts ALTER TABLE against a view for the actions a view can take and
// refuses the rest by naming the action and the relkind.
[[noreturn]] void ThrowAlterNotSupportedOnView(duckdb::AlterTableType type,
                                               std::string_view name) {
  const auto action = AlterActionName(type);
  if (action.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", name, "\" is not a table"),
                    ERR_DETAIL("This operation is not supported for views."));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                  ERR_MSG("ALTER action ", action,
                          " cannot be performed on relation \"", name, "\""),
                  ERR_DETAIL("This operation is not supported for views."));
}

[[noreturn]] void ThrowObjectMissing(duckdb::CatalogType type,
                                     std::string_view name) {
  switch (type) {
    case duckdb::CatalogType::MACRO_ENTRY:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
        ERR_MSG("could not find a function named \"", name, "\""));
    case duckdb::CatalogType::TYPE_ENTRY:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("type \"", name, "\" does not exist"));
    default:
      ThrowRelationMissing(name);
  }
}

// Resolving a name is itself a privileged act for the two kinds PG guards that
// way: EXECUTE on a function, USAGE on a type.
catalog::AclMode AccessNeededFor(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      return catalog::AclMode::Execute;
    case duckdb::CatalogType::TYPE_ENTRY:
      return catalog::AclMode::Usage;
    default:
      return catalog::AclMode::NoRights;
  }
}

// The lookup types that carry a privilege, so the relation path -- every bind
// of every query -- pays no cast for a check that could not fire.
bool LookupIsPrivileged(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
    case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY:
      return true;
    default:
      return false;
  }
}

// The owner and ACL come off the entry: it was built from the version its
// transaction committed, so what the lookup found is what the check runs
// against.
void RequireEntryAccess(duckdb::ClientContext& context, ObjectId role,
                        const duckdb::CatalogEntry& entry) {
  const auto type = entry.type;
  const auto need = AccessNeededFor(type);
  if (need == catalog::AclMode::NoRights ||
      auth::ClosureFor(&context, role)->Can(type, entry.permissions, need)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied for ", pg::ToPgObjectTypeName(type), " ",
            entry.name.GetIdentifierName()));
}

bool IsFunctionLookup(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
    case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
      return true;
    default:
      return false;
  }
}

// ALTER VIEW / ALTER TABLE ... RENAME TO on a name a view holds: a view's entry
// is the object, so the rename is its own rewrite rather than a relation
// reshape.
void RenameViewObject(const catalog::AccessContext& ax, ObjectId database_id,
                      std::string_view schema, std::string_view name,
                      std::string_view new_name, duckdb::AlterInfo& info) {
  const auto schema_id = FindSchemaId(ax.context, database_id, schema);
  const auto target = FindRelationTarget(ax.context, schema_id, name);
  if (target.kind != duckdb::CatalogType::VIEW_ENTRY) {
    // The other halves of the relation namespace still answer for the name, and
    // PG reports the kind mismatch rather than a missing relation.
    if (target.entry) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", name, "\" is not a view"));
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }
  catalog::RequireOwner(ax.context, ax.role, target.entry->permissions, "view",
                        target.entry->name.GetIdentifierName());
  if (FindRelation(ax.context, schema_id, new_name)) {
    catalog::ThrowDuplicateName(catalog::NameKind::Relation, new_name);
  }
  catalog::ApplyEntryAlter(ax, duckdb::CatalogType::VIEW_ENTRY, schema_id, name,
                           info);
}

// ALTER FUNCTION ... RENAME TO. Returns false for the IF EXISTS no-op.
bool RenameFunctionObject(const catalog::AccessContext& ax,
                          ObjectId database_id, std::string_view schema,
                          std::string_view name, std::string_view new_name,
                          bool missing_ok, duckdb::AlterInfo& info) {
  const auto schema_id = FindSchemaId(ax.context, database_id, schema);
  const auto* function =
    schema_id.isSet() ? FindFunction(ax.context, schema_id, name) : nullptr;
  if (!function) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                    ERR_MSG("could not find a function named \"", name, "\""));
  }
  catalog::RequireOwner(ax.context, ax.role, function->permissions, "function",
                        function->name.GetIdentifierName());
  if (FindFunction(ax.context, schema_id, new_name)) {
    catalog::ThrowDuplicateName(catalog::NameKind::Relation, new_name);
  }
  catalog::ApplyEntryAlter(ax, duckdb::CatalogType::MACRO_ENTRY, schema_id,
                           name, info);
  return true;
}

// ALTER INDEX ... SET/RESET (...): the persisted options are rewritten and
// pushed into the running storage, where the writer limits and the task
// settings take effect live.
void AlterInvertedIndexOptions(
  const catalog::AccessContext& ax, const catalog::CreateIndexInfo& index,
  absl::AnyInvocable<void(catalog::InvertedIndexOptions&)> mutate) {
  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  RequireIndexOwner(ax, index);
  const auto current =
    Find<SereneDBIndexEntry>(ax.context, index.GetSchemaId(), index.GetId());
  if (current == nullptr) {
    catalog::ThrowConcurrentlyDropped(index.GetId());
  }
  const auto record = current->GetInfo();
  const auto& current_record = record->Cast<catalog::CreateIndexInfo>();
  auto options = catalog::InvertedInfo(*current_record.GetIndex()).GetOptions();
  mutate(options);
  if (options.reindex_interval_ms &&
      !Find<duckdb::ViewCatalogEntry>(ax.context, index.GetSchemaId(),
                                      index.GetRelationId())) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("option \"", kReindexIntervalSetting,
                            "\" only applies to view-backed inverted indexes"));
  }
  auto updated =
    catalog::ReoptionedIndexRecord(current_record, std::move(options));
  const auto& next = updated->Cast<catalog::CreateIndexInfo>();
  // Off the version being superseded: the handle is the object's, and every
  // version of it publishes through the same one.
  const auto storage = current->GetInvertedData();
  const auto applied = catalog::InvertedInfo(*next.GetIndex()).GetOptions();
  PutEntry(ax.context, next.GetName(), std::move(updated));
  if (storage) {
    storage->ApplyOptions(applied);
  }
}

// The comment text an ALTER carries. NULL and the empty string both mean "no
// comment", which is what every kind's Commented() compares against.
std::string CommentString(const duckdb::Value& value) {
  return value.IsNull() ? std::string{}
                        : value.DefaultCastAs(duckdb::LogicalType::VARCHAR)
                            .GetValue<std::string>();
}

[[noreturn]] void ThrowNotOfKind(std::string_view name, std::string_view kind) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                  ERR_MSG("\"", name, "\" is not a ", kind));
}

// COMMENT ON <kind> <name>. Every kind but a table keeps its comment in its own
// definition, so the comment is a new version of the object rather than a
// reshape; a table's is one field of the table it changes.
void SetObjectComment(const catalog::AccessContext& ax, ObjectId database_id,
                      std::string_view schema_name, duckdb::CatalogType kind,
                      std::string_view target, const std::string& comment,
                      bool missing_ok, duckdb::AlterInfo& info) {
  auto* context = ax.context;
  const auto schema_id = FindSchemaId(context, database_id, schema_name);
  if (!schema_id.isSet()) {
    if (missing_ok) {
      return;
    }
    ThrowObjectMissing(kind, target);
  }
  // Whatever the relation namespace holds under the name, resolved once: which
  // of its kinds the statement accepts is all that is left to decide.
  const auto relation = FindRelationTarget(context, schema_id, target);
  switch (kind) {
    case duckdb::CatalogType::TABLE_ENTRY:
      // PostgreSQL accepts COMMENT ON TABLE naming a view, so the kind the
      // statement said only decides which answers count.
      if (relation.kind == duckdb::CatalogType::TABLE_ENTRY ||
          relation.kind == duckdb::CatalogType::VIEW_ENTRY) {
        catalog::ApplyEntryAlter(ax, relation.kind, schema_id, target, info);
        return;
      }
      break;
    case duckdb::CatalogType::VIEW_ENTRY:
      if (relation.kind == duckdb::CatalogType::VIEW_ENTRY) {
        catalog::ApplyEntryAlter(ax, duckdb::CatalogType::VIEW_ENTRY, schema_id,
                                 target, info);
        return;
      }
      if (relation.kind == duckdb::CatalogType::TABLE_ENTRY) {
        ThrowNotOfKind(target, "view");
      }
      break;
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      if (relation.kind == duckdb::CatalogType::SEQUENCE_ENTRY) {
        catalog::ApplyEntryAlter(ax, duckdb::CatalogType::SEQUENCE_ENTRY,
                                 schema_id, target, info);
        return;
      }
      if (relation.kind == duckdb::CatalogType::TABLE_ENTRY ||
          relation.kind == duckdb::CatalogType::VIEW_ENTRY) {
        ThrowNotOfKind(target, "sequence");
      }
      break;
    case duckdb::CatalogType::INDEX_ENTRY:
      if (relation.kind == duckdb::CatalogType::INDEX_ENTRY) {
        // An index keeps its comment in its own definition, not the entry
        // field duckdb's SET_COMMENT alter writes.
        catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
        SetEntryComment(ax, duckdb::CatalogType::INDEX_ENTRY, schema_id, target,
                        comment);
        return;
      }
      break;
    case duckdb::CatalogType::TYPE_ENTRY:
      if (Find<duckdb::TypeCatalogEntry>(context, schema_id, target)) {
        catalog::ApplyEntryAlter(ax, duckdb::CatalogType::TYPE_ENTRY, schema_id,
                                 target, info);
        return;
      }
      break;
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      if (FindFunction(context, schema_id, target)) {
        catalog::ApplyEntryAlter(ax, duckdb::CatalogType::MACRO_ENTRY,
                                 schema_id, target, info);
        return;
      }
      kind = duckdb::CatalogType::MACRO_ENTRY;
      break;
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("COMMENT ON is not supported for this object type"));
  }
  if (missing_ok) {
    return;
  }
  ThrowObjectMissing(kind, target);
}

}  // namespace

bool IsHostedEntry(const duckdb::CatalogEntry& entry) noexcept {
  switch (entry.type) {
    // No schema above these three, and nothing duckdb owns shares a kind
    // with them.
    case duckdb::CatalogType::ROLE_ENTRY:
    case duckdb::CatalogType::DATABASE_ENTRY:
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
      return true;
    case duckdb::CatalogType::TABLE_ENTRY:
    case duckdb::CatalogType::VIEW_ENTRY:
    case duckdb::CatalogType::INDEX_ENTRY:
    case duckdb::CatalogType::SEQUENCE_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY:
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
    case duckdb::CatalogType::TOKENIZER_ENTRY:
      break;
    default:
      return false;
  }
  const auto* standard = dynamic_cast<const duckdb::StandardEntry*>(&entry);
  return standard != nullptr && dynamic_cast<const SereneDBSchemaEntry*>(
                                  &standard->Schema()) != nullptr;
}

// Case-sensitive: serenedb folds an unquoted identifier at parse time and then
// matches exactly, as postgres does, so "Foo" and "foo" are two relations and
// duckdb's case-insensitive keying would collapse them.
SereneDBSchemaSets::SereneDBSchemaSets(duckdb::Catalog& catalog)
  : duckdb::SchemaCatalogSets{catalog, /*case_sensitive=*/true},
    _tokenizers{catalog, nullptr, /*case_sensitive=*/true} {
  _tokenizers.EnableOidLookup(*GetIdentity(),
                              duckdb::CatalogType::TOKENIZER_ENTRY);
}

duckdb::CatalogSet& SereneDBSchemaSets::Get(duckdb::CatalogType type) {
  if (type == duckdb::CatalogType::TOKENIZER_ENTRY) {
    return _tokenizers;
  }
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    type = duckdb::CatalogType::TABLE_ENTRY;
  }
  // One set for both function kinds, as postgres has one namespace for
  // functions whether or not they return a set. TABLE_FUNCTION is the slot a
  // FROM bind asks through, and it has to reach the same home.
  if (type == duckdb::CatalogType::TABLE_MACRO_ENTRY ||
      type == duckdb::CatalogType::TABLE_FUNCTION_ENTRY) {
    type = duckdb::CatalogType::MACRO_ENTRY;
  }
  return duckdb::SchemaCatalogSets::Get(type);
}

namespace {

// pg_catalog and information_schema. Their content -- the system tables, the
// system views and the builtin macros -- is fixed when the process starts and
// is the same in every database, so it is not a projection of anything.
duckdb::unique_ptr<duckdb::CatalogEntry> MakeSystemTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view entry_name, const catalog::VirtualTable& table) {
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
  // serenedb matches column names exactly; duckdb's default keying folds.
  info->columns = duckdb::ColumnList(/*allow_duplicate_names=*/false,
                                     /*case_sensitive=*/true);
  info->SetTableName(duckdb::Identifier{entry_name});
  info->SetSchema(schema.name);
  for (auto& [column_name, column_type] :
       duckdb::StructType::GetChildTypes(table.RowType())) {
    info->columns.AddColumn(duckdb::ColumnDefinition(column_name, column_type));
  }
  return duckdb::make_uniq<SystemTableEntry>(catalog, schema, *info, table);
}

class StaticRelationGenerator final : public duckdb::DefaultGenerator {
 public:
  StaticRelationGenerator(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema)
    : duckdb::DefaultGenerator{catalog},
      _schema{schema},
      _info_schema{schema.name.GetIdentifierName() ==
                   StaticStrings::kInformationSchema} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction /*transaction*/,
    const duckdb::Identifier& entry_name) override {
    const auto name = entry_name.GetIdentifierName();
    if (const auto* table =
          pg::GetSystemTable(_schema.name.GetIdentifierName(), name)) {
      return MakeSystemTableEntry(catalog, _schema, name, *table);
    }
    auto view = _info_schema ? pg::GetInfoSchemaView(name) : pg::GetView(name);
    if (view.first) {
      return MakeViewEntry(catalog, _schema, name, *view.first,
                           std::move(view.second));
    }
    return nullptr;
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() override {
    duckdb::vector<duckdb::Identifier> names;
    const auto add = [&](std::string_view name) { names.emplace_back(name); };
    if (_info_schema) {
      pg::VisitInfoSchemaTables(
        [&](const catalog::VirtualTable& table) { add(table.GetName()); });
      pg::VisitInfoSchemaViews([&](const pg::StaticView& view) {
        add(view.first->GetViewName().GetIdentifierName());
      });
    } else {
      pg::VisitPgCatalogTables(
        [&](const catalog::VirtualTable& table) { add(table.GetName()); });
      pg::VisitPgCatalogViews([&](const pg::StaticView& view) {
        add(view.first->GetViewName().GetIdentifierName());
      });
    }
    return names;
  }

 private:
  duckdb::SchemaCatalogEntry& _schema;
  bool _info_schema;
};

class StaticFunctionGenerator final : public duckdb::DefaultGenerator {
 public:
  StaticFunctionGenerator(duckdb::Catalog& catalog,
                          duckdb::SchemaCatalogEntry& schema)
    : duckdb::DefaultGenerator{catalog},
      _schema{schema},
      _info_schema{schema.name.GetIdentifierName() ==
                   StaticStrings::kInformationSchema} {}

  duckdb::unique_ptr<duckdb::CatalogEntry> CreateDefaultEntry(
    duckdb::CatalogTransaction /*transaction*/,
    const duckdb::Identifier& entry_name) override {
    const auto name = entry_name.GetIdentifierName();
    auto function = _info_schema ? pg::GetInfoSchemaFunction(name)
                                 : pg::GetPgCatalogFunction(name);
    if (!function.first) {
      return nullptr;
    }
    return MakeMacroEntry(catalog, _schema, name, /*internal=*/true,
                          *function.first, std::move(function.second));
  }

  duckdb::vector<duckdb::Identifier> GetDefaultEntries() override {
    duckdb::vector<duckdb::Identifier> names;
    const auto add = [&](const pg::StaticFunction& function) {
      names.emplace_back(function.first->GetFunctionName().GetIdentifierName());
    };
    if (_info_schema) {
      pg::VisitInfoSchemaFunctions(add);
    } else {
      pg::VisitPgCatalogFunctions(add);
    }
    return names;
  }

 private:
  duckdb::SchemaCatalogEntry& _schema;
  bool _info_schema;
};

bool IsStaticSchema(std::string_view schema_name) noexcept {
  return schema_name == StaticStrings::kPgCatalogSchema ||
         schema_name == StaticStrings::kInformationSchema;
}

duckdb::unique_ptr<duckdb::DefaultGenerator> MakeStaticRelationGenerator(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema) {
  return duckdb::make_uniq_base<duckdb::DefaultGenerator,
                                StaticRelationGenerator>(catalog, schema);
}

duckdb::unique_ptr<duckdb::DefaultGenerator> MakeStaticFunctionGenerator(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema) {
  return duckdb::make_uniq_base<duckdb::DefaultGenerator,
                                StaticFunctionGenerator>(catalog, schema);
}

}  // namespace

SereneDBSchemaEntry::SereneDBSchemaEntry(duckdb::Catalog& catalog,
                                         duckdb::CreateSchemaInfo& info,
                                         ObjectId id, catalog::Permissions perm)
  : SereneDBSchemaEntry{catalog, info,
                        duckdb::make_shared_ptr<SereneDBSchemaSets>(catalog),
                        id, std::move(perm)} {
  // The pg types are projected from our own catalog, not generated per schema.
  GetCatalogSet(duckdb::CatalogType::TYPE_ENTRY).SetDefaultGenerator(nullptr);
  if (_static_content) {
    GetCatalogSet(duckdb::CatalogType::TABLE_ENTRY)
      .SetDefaultGenerator(MakeStaticRelationGenerator(catalog, *this));
    GetCatalogSet(duckdb::CatalogType::MACRO_ENTRY)
      .SetDefaultGenerator(MakeStaticFunctionGenerator(catalog, *this));
  }
}

SereneDBSchemaEntry::SereneDBSchemaEntry(
  duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info,
  const duckdb::shared_ptr<duckdb::SchemaCatalogSets>& sets, ObjectId id,
  catalog::Permissions perm)
  : duckdb::DuckSchemaEntry{catalog, info, sets},
    _static_content{IsStaticSchema(name.GetIdentifierName())} {
  // The schema is a record in the serenedb catalog log; duckdb neither writes
  // it to a data file nor to a data WAL. What that buys here is the checkpoint:
  // it now sees this entry, and the flag is what tells it to take only the rows
  // and leave the definition alone.
  duck_managed = false;
  if (id.isSet()) {
    // The id is the entry's oid, as it is for every other serenedb entry, so
    // pg_namespace.oid and duckdb_schemas().oid are one number.
    catalog::AdoptEntryIdentity(*this, id, std::move(perm));
  }
}

duckdb::unique_ptr<duckdb::CreateInfo> SereneDBSchemaEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateSchemaInfo>();
  info->SetSchema(name);
  info->oid = oid;
  info->parent_oid = GetDatabaseId().id();
  info->comment = comment;
  info->tags = tags;
  return info;
}

ObjectId SereneDBSchemaEntry::RequireSchemaId(duckdb::ClientContext* context,
                                              ObjectId role) const {
  // Resolved by name rather than off this entry: a concurrent DROP SCHEMA that
  // committed while the statement was open means the create has nowhere to go,
  // and PG says that as an undefined schema.
  const auto schema_id =
    FindSchemaId(context, GetDatabaseId(), name.GetIdentifierName());
  if (!schema_id.isSet()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
      ERR_MSG("schema \"", name.GetIdentifierName(), "\" does not exist"));
  }
  catalog::RequireCreateOn(context, role, schema_id);
  return schema_id;
}

ObjectId SereneDBSchemaEntry::GetDatabaseId() const {
  return catalog.Cast<SereneDBCatalog>().GetDatabaseId();
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::AlteredEntry(
  duckdb::CreateSchemaInfo& info, ObjectId id,
  catalog::Permissions perm) const {
  return duckdb::unique_ptr<SereneDBSchemaEntry>{
    new SereneDBSchemaEntry{catalog, info, sets, id, std::move(perm)}};
}

std::span<const duckdb::CatalogType> EntrySlots(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  static constexpr std::array kRelation{TABLE_ENTRY};
  static constexpr std::array kType{TYPE_ENTRY};
  static constexpr std::array kSequence{SEQUENCE_ENTRY};
  static constexpr std::array kTokenizer{TOKENIZER_ENTRY};
  static constexpr std::array kFunction{MACRO_ENTRY};
  static constexpr std::array kIndex{INDEX_ENTRY, TABLE_ENTRY};
  switch (type) {
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return kFunction;
    case TYPE_ENTRY:
      return kType;
    case TOKENIZER_ENTRY:
      return kTokenizer;
    case SEQUENCE_ENTRY:
      return kSequence;
    case INDEX_ENTRY:
      return kIndex;
    default:
      return kRelation;
  }
}

// The pg_catalog builtins resolve from every schema, as they do in postgres,
// and the one set that holds them is pg_catalog's own.
duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::LookupBuiltinFunction(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  const auto type = lookup_info.GetCatalogType();
  if (!IsFunctionLookup(type) ||
      name.GetIdentifierName() == StaticStrings::kPgCatalogSchema) {
    return nullptr;
  }
  auto pg_catalog = catalog.Cast<SereneDBCatalog>().TryGetSchemaEntry(
    StaticStrings::kPgCatalogSchema);
  if (!pg_catalog) {
    return nullptr;
  }
  return pg_catalog->GetCatalogSet(type).GetEntry(
    transaction, duckdb::Identifier{lookup_info.GetEntryName()});
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  // The engine asking on its own account -- the WAL replay and the checkpoint
  // reader, which carry no session at all, and the data store, which carries
  // one only so that its index builds resolve names the way a session does.
  // Neither is a role, so neither is access-checked.
  auto* conn_ctx =
    transaction.HasContext()
      ? connector::GetSereneDBContextPtr(transaction.GetContext())
      : nullptr;
  if (conn_ctx != nullptr && conn_ctx->IsStorageConnection()) {
    conn_ctx = nullptr;
  }
  const auto type = lookup_info.GetCatalogType();
  auto entry = GetCatalogSet(type).GetEntry(
    transaction, duckdb::Identifier{lookup_info.GetEntryName()});
  if (entry && (type == duckdb::CatalogType::SEQUENCE_ENTRY) !=
                 (entry->type == duckdb::CatalogType::SEQUENCE_ENTRY)) {
    entry = nullptr;
  }
  // Both function kinds share the set and answer each other's lookups on
  // purpose: the binder normalizes -- a scalar found by a FROM bind is
  // wrapped, a table-bodied procedure found by a scalar one names CALL.
  if (entry) {
    if (conn_ctx != nullptr && LookupIsPrivileged(type)) {
      RequireEntryAccess(transaction.GetContext(), conn_ctx->GetRoleId(),
                         *entry);
    }
    return entry;
  }

  if (auto builtin = LookupBuiltinFunction(transaction, lookup_info)) {
    if (conn_ctx != nullptr) {
      RequireEntryAccess(transaction.GetContext(), conn_ctx->GetRoleId(),
                         *builtin);
    }
    return builtin;
  }

  if (name.GetIdentifierName() != StaticStrings::kPgCatalogSchema) {
    return nullptr;
  }

  // Pg-compat fallback for `pg_catalog.<x>` that redirects to the system
  // catalog.
  switch (lookup_info.GetCatalogType()) {
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
    case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
    case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
    case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY: {
      auto& sys = duckdb::Catalog::GetSystemCatalog(transaction.GetContext());
      auto main_schema = sys.GetSchema(transaction, DEFAULT_SCHEMA,
                                       duckdb::OnEntryNotFound::RETURN_NULL);
      if (main_schema) {
        return main_schema->LookupEntry(transaction, lookup_info);
      }
      break;
    }
    default:
      break;
  }
  return nullptr;
}

duckdb::SimilarCatalogEntry SereneDBSchemaEntry::GetSimilarEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  auto similar =
    duckdb::DuckSchemaEntry::GetSimilarEntry(transaction, lookup_info);
  auto* conn_ctx =
    transaction.HasContext()
      ? connector::GetSereneDBContextPtr(transaction.GetContext())
      : nullptr;
  if (conn_ctx != nullptr && conn_ctx->IsStorageConnection()) {
    conn_ctx = nullptr;
  }
  if (!similar.Found() || conn_ctx == nullptr ||
      !LookupIsPrivileged(lookup_info.GetCatalogType())) {
    return similar;
  }
  if (auto entry = GetCatalogSet(lookup_info.GetCatalogType())
                     .GetEntry(transaction, similar.name)) {
    const auto need = AccessNeededFor(entry->type);
    if (need != catalog::AclMode::NoRights &&
        !auth::ClosureFor(&transaction.GetContext(), conn_ctx->GetRoleId())
           ->Can(entry->type, entry->permissions, need)) {
      return {};
    }
  }
  return similar;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateTable(
  duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo& info) {
  auto& sdb_catalog = catalog.Cast<SereneDBCatalog>();
  if (sdb_catalog.IsReplaying()) {
    // A CREATE TABLE record out of this database's own data file: the
    // definition is already here from the catalog log, and what the record adds
    // are the rows in front of it.
    sdb_catalog.CreateTableStorage(transaction, info);
    return nullptr;
  }
  auto& create_info = info.Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();
  auto& context = transaction.GetContext();
  const auto table_name = table_info.GetTableName().GetIdentifierName();

  // The definition duckdb bound, taken over rather than copied across: ours is
  // that info with the per-column grants added, so what the binder produced is
  // what the catalog keeps. Only the identities and the serenedb-specific
  // rewrites below are added to it.
  auto built = catalog::Clone(table_info);
  built->SetSchema(name);

  // Consume the SereneDB-specific `storage` WITH option (selects the table
  // engine) + any Search maintenance-interval options before validating that no
  // unknown options remain.
  connector::ApplyStorageKind(context, *built, built->options);

  if (!built->options.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"",
                            built->options.begin()->first, "\""));
  }

  // PG-style constraint name generator with dedup against everything named so
  // far, which is what postgres does too.
  std::vector<std::string> constraint_names;
  auto choose_constraint_name = [&](std::string_view column,
                                    std::string_view label) -> std::string {
    const auto base_name =
      column.empty() ? absl::StrCat(table_name, "_", label)
                     : absl::StrCat(table_name, "_", column, "_", label);
    auto name_exists = [&](std::string_view candidate) {
      return absl::c_linear_search(constraint_names, candidate);
    };
    if (!name_exists(base_name)) {
      return base_name;
    }
    for (size_t counter = 1;; ++counter) {
      auto candidate = absl::StrCat(base_name, counter);
      if (!name_exists(candidate)) {
        return candidate;
      }
    }
  };
  // The order the rows behind the entry are verified in: NOT NULL, PRIMARY
  // KEY, UNIQUE, FOREIGN KEY, CHECK.
  duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>> not_nulls;
  duckdb::unique_ptr<duckdb::Constraint> primary_key;
  duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>> uniques;
  duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>> foreign_keys;
  duckdb::vector<duckdb::unique_ptr<duckdb::Constraint>> checks;
  const auto adopt = [&](auto& constraint, std::string name_in) {
    constraint->oid = catalog::NextId().id();
    constraint_names.push_back(name_in);
    constraint->constraint_name = std::move(name_in);
  };

  // Dedup against duplicate NOT NULL adds; grows on demand because the SERIAL
  // path calls append_not_null mid column loop.
  std::vector<bool> has_not_null;
  auto append_not_null = [&](duckdb::idx_t col_idx,
                             std::string explicit_name = {}) {
    if (col_idx >= built->columns.LogicalColumnCount()) {
      return;
    }
    if (col_idx >= has_not_null.size()) {
      has_not_null.resize(col_idx + 1, false);
    }
    if (has_not_null[col_idx]) {
      return;
    }
    has_not_null[col_idx] = true;
    const auto column_name =
      std::string{built->columns.GetColumn(duckdb::LogicalIndex{col_idx})
                    .Name()
                    .GetIdentifierName()};
    auto not_null = duckdb::make_uniq<duckdb::NotNullConstraint>(
      duckdb::LogicalIndex{col_idx});
    adopt(not_null, !explicit_name.empty()
                      ? std::move(explicit_name)
                      : choose_constraint_name(column_name, "not_null"));
    not_nulls.push_back(std::move(not_null));
  };

  // SERIAL expands to base int + nextval default + NOT NULL. The sequence name
  // and nextval default are resolved by the catalog under its mutex.
  std::vector<catalog::SerialSequence> sequences;
  // A generated column duckdb bound as VIRTUAL becomes STORED, which moves it
  // into the row layout -- so the list has to be laid out again afterwards.
  bool relayout = false;
  const auto column_count = built->columns.LogicalColumnCount();
  for (duckdb::idx_t index = 0; index < column_count; ++index) {
    auto& column = built->columns.GetColumnMutable(duckdb::LogicalIndex{index});
    const auto column_id = catalog::NextId();
    column.SetCatalogOid(column_id.id());

    const auto type_id = column.Type().id();
    const bool is_smallserial = pg::IsSmallserial(column.Type());
    const bool is_serial = pg::IsSerial(column.Type());
    const bool is_bigserial = pg::IsBigserial(column.Type());
    if (is_smallserial || is_serial || is_bigserial) {
      catalog::SequenceOptions seq_opts;
      if (is_smallserial) {
        seq_opts.max_value = std::numeric_limits<int16_t>::max();
      } else if (is_serial) {
        seq_opts.max_value = std::numeric_limits<int32_t>::max();
      } else {
        seq_opts.max_value = std::numeric_limits<int64_t>::max();
      }
      column.SetType(duckdb::LogicalType{type_id});
      // The nextval the sequence feeds is the column's default; anything the
      // statement wrote alongside SERIAL is not.
      if (!column.Generated() && column.HasDefaultValue()) {
        column.SetDefaultValue(nullptr);
      }
      sequences.push_back({column_id, seq_opts});
      append_not_null(index);
      continue;
    }
    if (column.Category() == duckdb::TableColumnType::GENERATED_VIRTUAL) {
      column.SetGeneratedExpression(column.GeneratedExpression().Copy(),
                                    duckdb::TableColumnType::GENERATED_STORED);
      relayout = true;
    }
  }
  if (relayout) {
    duckdb::ColumnList laid_out{built->columns.IsCaseSensitive()};
    for (const auto& column : built->columns.Logical()) {
      laid_out.AddColumn(column.Copy());
    }
    built->columns = std::move(laid_out);
  }

  const auto require_column = [&](const duckdb::Identifier& column_name) {
    const auto* column =
      catalog::ColumnByName(*built, column_name.GetIdentifierName());
    SDB_ASSERT(column);
    return column;
  };

  duckdb::vector<duckdb::Identifier> pk_columns;
  std::string pk_name;
  const auto append_pk = [&](const duckdb::Identifier& column_name) {
    const auto* column = require_column(column_name);
    append_not_null(column->Logical().index);  // PK implies NOT NULL
    pk_columns.push_back(column->Name());
  };

  // The bound constraints, taken off the adopted info: what goes back on is
  // the same set, named and ordered the way the rows are verified.
  auto bound_constraints = std::move(built->constraints);
  built->constraints.clear();
  for (auto& constraint : bound_constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::UNIQUE: {
        auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        duckdb::vector<duckdb::Identifier> keys;
        if (unique.HasIndex()) {
          keys.push_back(built->columns.GetColumn(unique.GetIndex()).Name());
        } else {
          for (const auto& key : unique.GetColumnNames()) {
            keys.push_back(require_column(key)->Name());
          }
        }
        if (unique.IsPrimaryKey()) {
          for (const auto& key : keys) {
            append_pk(key);
          }
          if (!unique.constraint_name.empty()) {
            pk_name = unique.constraint_name;
          }
          break;
        }
        auto built_unique =
          duckdb::make_uniq<duckdb::UniqueConstraint>(keys,
                                                      /*is_primary_key=*/false);
        built_unique->host_index_id = catalog::NextId().id();
        adopt(built_unique,
              unique.constraint_name.empty()
                ? choose_constraint_name(keys.empty()
                                           ? std::string_view{}
                                           : keys.front().GetIdentifierName(),
                                         "key")
                : unique.constraint_name);
        uniques.push_back(std::move(built_unique));
      } break;
      case duckdb::ConstraintType::NOT_NULL: {
        auto& nn = constraint->Cast<duckdb::NotNullConstraint>();
        append_not_null(nn.index.index, nn.constraint_name);
      } break;
      case duckdb::ConstraintType::CHECK: {
        auto& check = constraint->Cast<duckdb::CheckConstraint>();
        auto built_check =
          duckdb::make_uniq<duckdb::CheckConstraint>(check.expression->Copy());
        adopt(built_check,
              check.constraint_name.empty()
                ? choose_constraint_name(
                    FindConstraintColumn(*check.expression), "check")
                : check.constraint_name);
        checks.push_back(std::move(built_check));
      } break;
      case duckdb::ConstraintType::FOREIGN_KEY: {
        auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
        // FK_TYPE_PRIMARY_KEY_TABLE is the reciprocal entry on the referenced
        // table -- skip it (the FK is mirrored from the referencing side). A
        // self-referencing FK is FK_TYPE_SELF_REFERENCE_TABLE and must be kept,
        // else it is silently unenforced.
        if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE &&
            fk.info.type !=
              duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
          break;
        }
        duckdb::ForeignKeyInfo out_info;
        duckdb::vector<duckdb::Identifier> fk_columns;
        for (const auto& key : fk.fk_columns) {
          const auto* column = require_column(key);
          fk_columns.push_back(column->Name());
          out_info.fk_keys.emplace_back(column->Logical().index);
        }
        duckdb::vector<duckdb::Identifier> pk_names;
        std::vector<idx_t> host_pk_column_ids;
        ObjectId referenced_id;
        if (fk.info.type ==
            duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
          out_info.type = duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
          out_info.schema = name;
          out_info.table = built->GetTableName();
          for (const auto& key : fk.pk_columns) {
            const auto* column = require_column(key);
            pk_names.push_back(column->Name());
            host_pk_column_ids.push_back(column->CatalogOid());
            out_info.pk_keys.emplace_back(column->Logical().index);
          }
        } else {
          const auto& referenced_schema =
            fk.info.schema.empty() ? name : fk.info.schema;
          const auto referenced_schema_id = FindSchemaId(
            &context, GetDatabaseId(), referenced_schema.GetIdentifierName());
          const auto* referenced_entry =
            referenced_schema_id.isSet()
              ? Find<SereneDBTableEntry>(&context, referenced_schema_id,
                                         fk.info.table.GetIdentifierName())
              : nullptr;
          const auto referenced = referenced_entry != nullptr
                                    ? referenced_entry->Definition()
                                    : nullptr;
          if (!referenced) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_TABLE),
              ERR_MSG("referenced table \"", fk.info.table.GetIdentifierName(),
                      "\" does not exist"));
          }
          referenced_id = catalog::IdOf(*referenced);
          out_info.type = duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
          out_info.schema = duckdb::Identifier{referenced_schema};
          out_info.table =
            duckdb::Identifier{referenced->GetTableName().GetIdentifierName()};
          for (const auto& key : fk.pk_columns) {
            const auto* column =
              catalog::ColumnByName(*referenced, key.GetIdentifierName());
            if (column == nullptr) {
              THROW_SQL_ERROR(
                ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                ERR_MSG("column \"", key.GetIdentifierName(),
                        "\" named in foreign key does not exist"));
            }
            pk_names.push_back(column->Name());
            host_pk_column_ids.push_back(column->CatalogOid());
            out_info.pk_keys.emplace_back(column->Logical().index);
          }
        }
        auto built_fk = duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
          std::move(pk_names), fk_columns, std::move(out_info));
        built_fk->host_referenced_id = referenced_id.id();
        built_fk->host_pk_column_ids = std::move(host_pk_column_ids);
        adopt(built_fk,
              fk.constraint_name.empty()
                ? choose_constraint_name(
                    fk_columns.empty() ? std::string_view{}
                                       : fk_columns.front().GetIdentifierName(),
                    "fkey")
                : fk.constraint_name);
        foreign_keys.push_back(std::move(built_fk));
      } break;
      default:
        break;
    }
  }
  if (!pk_columns.empty()) {
    auto key = duckdb::make_uniq<duckdb::UniqueConstraint>(
      pk_columns, /*is_primary_key=*/true);
    key->host_index_id = catalog::NextId().id();
    adopt(key, pk_name.empty() ? absl::StrCat(table_name, "_pkey")
                               : std::move(pk_name));
    primary_key = std::move(key);
  }
  for (auto& constraint : not_nulls) {
    built->constraints.push_back(std::move(constraint));
  }
  if (primary_key) {
    built->constraints.push_back(std::move(primary_key));
  }
  for (auto& constraint : uniques) {
    built->constraints.push_back(std::move(constraint));
  }
  for (auto& constraint : foreign_keys) {
    built->constraints.push_back(std::move(constraint));
  }
  for (auto& constraint : checks) {
    built->constraints.push_back(std::move(constraint));
  }

  auto database_id = GetDatabaseId();

  const bool replace =
    create_info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;
  op_options.if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  // CREATE OR REPLACE TABLE (non-AS): drop the pre-existing table (cascade)
  // then create the new one, mirroring native duckdb's REPLACE_ON_CONFLICT.
  // Only a real table is dropped; a name held by a view or another relation
  // falls through to the duplicate-name path below.
  if (replace) {
    const auto schema_id =
      FindSchemaId(&context, database_id, name.GetIdentifierName());
    if (schema_id.isSet() &&
        Find<SereneDBTableEntry>(&context, schema_id, table_name)) {
      catalog::DropTable(catalog::ActingAs(context),
                         catalog.GetName().GetIdentifierName(),
                         name.GetIdentifierName(), table_name,
                         /*cascade=*/true, /*missing_ok=*/false);
    }
  }

  // Creator owns the table (and its generated serial/PK sequences) via the
  // access context.
  auto created = catalog::CreateTable(
    catalog::ActingAs(context), database_id, name.GetIdentifierName(),
    std::move(built), std::move(sequences), op_options);
  // Search tables maintain themselves in the background
  // (commit/consolidate/GC). Kick the maintenance chains now that the table
  // and its iresearch store exist; mirrors the inverted-index StartTasks in
  // the CREATE INDEX operator.
  if (created && created->IsSearchTable()) {
    created->GetSearchData()->StartTasks();
  }
  // CREATE TABLE AS appends into the entry this call made: duckdb's load
  // operator creates its own target and reads the answer back.
  return const_cast<SereneDBTableEntry*>(created);
}

namespace {

// The record of a plain ART, from what duckdb bound: every column the index
// reads is exactly what the build scanned -- for an expression over a struct
// field that is the struct column itself -- and a key slot names its column
// only when the key is one.
duckdb::unique_ptr<catalog::CreateIndexInfo> MakeIndexRecord(
  const SereneDBSchemaEntry& schema, const duckdb::CreateIndexInfo& info,
  const duckdb::TableCatalogEntry& table) {
  const auto& columns = table.GetColumns();
  std::vector<ColumnId> referenced;
  referenced.reserve(info.column_ids.size());
  for (const auto physical : info.column_ids) {
    const auto& column = columns.GetColumn(
      columns.PhysicalToLogical(duckdb::PhysicalIndex{physical}));
    const ColumnId id{column.CatalogOid()};
    if (id.isSet() && !absl::c_contains(referenced, id)) {
      referenced.push_back(id);
    }
  }
  std::vector<ColumnId> keys;
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> expressions;
  keys.reserve(info.parsed_expressions.size());
  expressions.reserve(info.parsed_expressions.size());
  for (const auto& key : info.parsed_expressions) {
    auto id = catalog::kInvalidColumnId;
    if (key->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      const auto& reference = key->Cast<duckdb::ColumnRefExpression>();
      if (auto column = columns.TryGetColumn(reference.ColumnNames().back())) {
        id = ColumnId{column->CatalogOid()};
      }
    }
    keys.push_back(id);
    expressions.push_back(key->Copy());
  }
  return duckdb::make_uniq<catalog::CreateIndexInfo>(
    catalog::IdOf(schema), ObjectId{0}, catalog::IdOf(table),
    info.GetIndexName().GetIdentifierName(),
    info.constraint_type == duckdb::IndexConstraintType::UNIQUE,
    std::move(keys), std::move(referenced), std::move(expressions));
}

}  // namespace

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  // duckdb has bound the keys and built the ART by the time it gets here; this
  // is where the index is handed to the catalog that owns it, and a null answer
  // attaches nothing.
  auto* context = transaction.context.get();
  const auto name = info.GetIndexName().GetIdentifierName();
  if (context == nullptr || connector::IsStorageStatement(*context)) {
    // The data half of a definition this catalog already holds: boot replay
    // and a reshape's rebuild re-run the build, and the record each fills is
    // the one already placed.
    const auto* existing =
      Find<SereneDBIndexEntry>(context, catalog::IdOf(*this), name);
    SDB_ASSERT(existing);
    return const_cast<SereneDBIndexEntry*>(existing);
  }
  const auto ax = catalog::ActingAs(*context);
  catalog::JoinStoreTransaction(context);
  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  catalog::RequireOwner(context, ax.role, table.permissions,
                        pg::ToPgObjectTypeName(table.type),
                        table.name.GetIdentifierName());
  if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT &&
      catalog::FindRelation(context, catalog::IdOf(*this), name)) {
    return nullptr;
  }
  auto record = MakeIndexRecord(*this, info, table);
  return catalog::CreateIndexImpl(
    context, *record,
    {.if_not_exists =
       info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT,
     .dependencies = info.dependencies});
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateFunction(
  duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo& info) {
  auto& context = transaction.GetContext();
  const ObjectId role{connector::GetSereneDBContext(context).GetRoleId()};
  const bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  const bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  const auto function_name = info.GetFunctionName().GetIdentifierName();

  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  const auto schema_id = RequireSchemaId(&context, role);
  const auto* existing = FindFunction(&context, schema_id, function_name);

  auto declared =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      info.Copy());
  catalog::Permissions perm{role};
  if (existing) {
    catalog::RequireOwner(&context, role, existing->permissions, "function",
                          existing->name.GetIdentifierName());
    // CREATE OR REPLACE preserves the original owner and grants (PG keeps the
    // existing catalog tuple's proowner and proacl).
    perm = existing->permissions;
    if (replace) {
      bool indexed = false;
      EdgeAttachments(context).ScanDependents(
        DependencyInfo(ObjectId{existing->oid}),
        [&](duckdb::optional_ptr<duckdb::CatalogEntry> dependent,
            duckdb::DependencyEntry&) {
          if (dependent &&
              dependent->type == duckdb::CatalogType::INDEX_ENTRY) {
            indexed = true;
          }
        });
      if (indexed) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                        ERR_MSG("cannot replace function \"", function_name,
                                "\" because indexes depend on it"));
      }
    }
    // PG semantics: several CREATE FUNCTIONs with the same name but different
    // parameter signatures are distinct overloads, and CREATE OR REPLACE
    // replaces only the matching one. The whole merged set is what a version
    // of the function is, so the write is always a replace.
    auto merged = existing->MergedWith(*declared, replace);
    if (!merged) {
      if (if_not_exists) {
        return nullptr;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_FUNCTION),
                      ERR_MSG("function \"", function_name,
                              "\" already exists with same argument types"));
    }
    declared = std::move(merged);
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  const auto id =
    existing != nullptr ? ObjectId{existing->oid} : catalog::NextId();
  catalog::SetIdentity(*declared, id, schema_id);
  PutEntry(&context,
           existing != nullptr ? std::string{existing->name.GetIdentifierName()}
                               : std::string{},
           std::move(declared), perm);
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  auto& context = transaction.GetContext();
  const ObjectId role{connector::GetSereneDBContext(context).GetRoleId()};
  const bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  const bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  const auto schema_id = RequireSchemaId(&context, role);
  const auto view_name = info.GetViewName().GetIdentifierName();
  const auto* existing =
    Find<duckdb::ViewCatalogEntry>(&context, schema_id, view_name);

  catalog::Permissions perm{role};
  if (replace && existing) {
    catalog::RequireOwner(&context, role, existing->permissions, "view",
                          existing->name.GetIdentifierName());
    // CREATE OR REPLACE preserves the original owner and grants (PG keeps the
    // existing catalog tuple's relowner and relacl).
    perm = existing->permissions;
  } else if (!replace && existing) {
    if (if_not_exists) {
      return nullptr;
    }
    catalog::ThrowDuplicateName(catalog::NameKind::Relation, view_name);
  }
  if (!existing && FindRelation(&context, schema_id, view_name)) {
    if (if_not_exists && !replace) {
      return nullptr;
    }
    // OR REPLACE says what the name is expected to be, so the kind mismatch is
    // what PG reports rather than a collision.
    if (replace) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", view_name, "\" is not a view"));
    }
    catalog::ThrowDuplicateName(catalog::NameKind::Relation, view_name);
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  const auto id =
    existing != nullptr ? ObjectId{existing->oid} : catalog::NextId();
  auto copied =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      info.Copy());
  catalog::SetIdentity(*copied, id, schema_id);
  PutEntry(&context,
           existing != nullptr ? std::string{existing->name.GetIdentifierName()}
                               : std::string{},
           std::move(copied), perm);
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateSequence(
  duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo& info) {
  if (info.increment <= 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("sequence INCREMENT must be positive (negative increments not "
              "yet supported)"));
  }
  if (info.min_value < 0 || info.max_value < 0 || info.start_value < 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("sequence MIN/MAX/START must be non-negative (negative "
              "sequences not yet supported)"));
  }
  if (info.start_value < info.min_value || info.start_value > info.max_value) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sequence START is out of range [MIN, MAX]"));
  }

  const ObjectId role{
    connector::GetSereneDBContext(transaction.GetContext()).GetRoleId()};
  catalog::SequenceOptions options;
  options.name = info.GetSequenceName().GetIdentifierName();
  options.start_value = static_cast<uint64_t>(info.start_value);
  options.increment = static_cast<uint64_t>(info.increment);
  options.min_value = static_cast<uint64_t>(info.min_value);
  options.max_value = static_cast<uint64_t>(info.max_value);
  options.cycle = info.cycle;
  if (!info.comment.IsNull()) {
    options.comment = info.comment.DefaultCastAs(duckdb::LogicalType::VARCHAR)
                        .GetValue<std::string>();
  }

  const bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  auto& context = transaction.GetContext();
  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  const auto schema_id = RequireSchemaId(&context, role);
  if (FindRelation(&context, schema_id, options.name)) {
    if (if_not_exists) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("relation \"", options.name, "\" already exists"));
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  const auto id = catalog::NextId();
  const auto seed = options.Seed();
  const catalog::Permissions perm{role};
  // One definition, handed to the record and to the entry: nothing is derived
  // at append time.
  auto definition =
    catalog::MakeSequenceInfo(id, schema_id, std::move(options));
  // The counter's seed rides the same frame as the definition, so a sequence
  // is never durable without the value it starts from.
  // Both under one scope: the record above is this write's own, and the entry
  // must not append a second one.
  catalog::GetCatalog().RecordSequenceSeed(&context, catalog::IdOf(*definition),
                                           seed);
  auto placed = PutEntry(&context, /*old_name=*/{}, definition->Copy(), perm);
  // The counter lives on the entry, seeded from START: this is a create, so
  // there was no predecessor for the build to inherit one from.
  if (const auto* seq =
        dynamic_cast<const SereneDBSequenceEntry*>(placed.get())) {
    seq->AdoptCounter(
      catalog::NewCounter(id, catalog::SequenceOptionsOf(*definition)));
  }
  return nullptr;
}

namespace {

// Stamps the type's own name and stable id into its extension info, so a
// LogicalType read back from a column, a parameter or a wire description still
// names the catalog row it came from.
duckdb::LogicalType StampUserType(const duckdb::LogicalType& type,
                                  std::string_view name, ObjectId id) {
  auto type_info = type.AuxInfo()
                     ? type.AuxInfo()->DeepCopy()
                     : duckdb::make_shared_ptr<duckdb::ExtraTypeInfo>(
                         duckdb::ExtraTypeInfoType::GENERIC_TYPE_INFO);
  type_info->alias = std::string{name};
  auto ext = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
  ext->properties[catalog::kPgSqlTypeOidProp] = duckdb::Value::UBIGINT(id.id());
  type_info->extension_info = std::move(ext);
  return {type.id(), std::move(type_info)};
}

}  // namespace

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateType(
  duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) {
  auto& context = transaction.GetContext();
  const ObjectId role{connector::GetSereneDBContext(context).GetRoleId()};
  const bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  catalog::Catalog::MutationScope mutation{catalog::GetCatalog()};
  const auto schema_id = RequireSchemaId(&context, role);
  const auto type_name = info.GetTypeName().GetIdentifierName();
  if (Find<duckdb::TypeCatalogEntry>(&context, schema_id, type_name)) {
    if (if_not_exists) {
      return nullptr;
    }
    catalog::ThrowDuplicateName(catalog::NameKind::Type, type_name);
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  auto copied =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
      info.Copy());
  // Two ids at once: the array type PG pairs with every scalar one sits
  // directly below it, so neither has to be written down twice.
  const ObjectId id{catalog::NextNIds(2).id() + 1};
  copied->type = StampUserType(copied->type, type_name, id);
  catalog::SetIdentity(*copied, id, schema_id);
  PutEntry(&context, /*old_name=*/{}, std::move(copied),
           catalog::Permissions{role});
  return nullptr;
}

void SereneDBSchemaEntry::DropEntry(duckdb::ClientContext& context,
                                    duckdb::DropInfo& info) {
  // A drop record out of this database's own data file. The catalog log decided
  // the drop and has already removed the definition, so the entry this would
  // name is gone and its rows went with it: nothing to do, and recording it a
  // second time would append to the catalog log during boot.
  if (catalog.Cast<SereneDBCatalog>().IsReplaying()) {
    return;
  }
  info.SetCatalog(catalog.GetName());
  info.SetSchema(name);
  DropObject(context, info);
}

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  // The tail of the data store's own index build: having built the ART over the
  // rows, duckdb records the constraint by altering the entry. That constraint
  // is already in the definition the mutator wrote -- the store op it emitted
  // is what brought us here -- and going round again would re-enter the catalog
  // mutex this call is already inside.
  if (transaction.HasContext() &&
      connector::IsStorageStatement(transaction.GetContext())) {
    return;
  }
  auto db = GetDatabaseId();
  const auto ax = catalog::ActingAs(transaction.GetContext());

  if (info.type == duckdb::AlterType::ALTER_SCALAR_FUNCTION) {
    auto& fn_info = info.Cast<duckdb::AlterScalarFunctionInfo>();
    if (fn_info.alter_scalar_function_type !=
        duckdb::AlterScalarFunctionType::RENAME_SCALAR_FUNCTION) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("only RENAME is supported for ALTER FUNCTION"));
    }
    auto& rename_info = fn_info.Cast<duckdb::RenameScalarFunctionInfo>();

    RenameFunctionObject(
      ax, db, name.GetIdentifierName(),
      info.GetQualifiedName().Name().GetIdentifierName(),
      rename_info.new_name.GetIdentifierName(),
      info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL, info);
    return;
  }

  if (info.type == duckdb::AlterType::ALTER_VIEW) {
    auto& view_info = info.Cast<duckdb::AlterViewInfo>();
    if (view_info.alter_view_type != duckdb::AlterViewType::RENAME_VIEW) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("only RENAME is supported for ALTER VIEW"));
    }
    auto& rename_info = view_info.Cast<duckdb::RenameViewInfo>();
    RenameViewObject(ax, db, name.GetIdentifierName(),
                     info.GetQualifiedName().Name().GetIdentifierName(),
                     rename_info.new_view_name.GetIdentifierName(), info);
    return;
  }

  // COMMENT ON TABLE/COLUMN are top-level AlterTypes (not inside ALTER_TABLE),
  // so intercept them before the ALTER_TABLE guard. NULL clears the comment
  // (empty string).
  if (info.type == duckdb::AlterType::SET_COMMENT) {
    auto& comment_info = info.Cast<duckdb::SetCommentInfo>();
    SetObjectComment(
      ax, db, name.GetIdentifierName(), comment_info.entry_catalog_type,
      info.GetQualifiedName().Name().GetIdentifierName(),
      CommentString(comment_info.comment_value),
      info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL, info);
    return;
  }

  // Everything below names something in the relation namespace: ALTER TABLE's
  // own actions and COMMENT ON COLUMN, whose target PG resolves the same way.
  if (info.type != duckdb::AlterType::ALTER_TABLE &&
      info.type != duckdb::AlterType::SET_COLUMN_COMMENT) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("this ALTER operation is not supported"));
  }

  // Resolve the target once: the relation namespace answers with whichever of
  // its kinds holds the name -- PG lets ALTER TABLE and ALTER INDEX both name
  // an index -- and every branch below acts on what came back.
  auto table_name = info.GetQualifiedName().Name().GetIdentifierName();
  const auto alter_schema_id =
    FindSchemaId(&transaction.GetContext(), db, name.GetIdentifierName());
  const auto target =
    FindRelationTarget(&transaction.GetContext(), alter_schema_id, table_name);
  const auto* relation_entry = EntryOf<SereneDBTableEntry>(target.entry);
  const auto relation =
    relation_entry != nullptr ? relation_entry->Definition() : nullptr;
  const auto* index_entry = EntryOf<SereneDBIndexEntry>(target.entry);
  const auto index =
    index_entry != nullptr
      ? duckdb::unique_ptr_cast<duckdb::CreateInfo, catalog::CreateIndexInfo>(
          index_entry->GetInfo())
      : nullptr;

  if (info.type == duckdb::AlterType::SET_COLUMN_COMMENT) {
    // A view's entry is the object, so COMMENT ON COLUMN of one is the view's
    // own rewrite rather than a table reshape; a table's rides duckdb's alter.
    if (relation) {
      catalog::ApplyTableAlter(ax, *relation, info);
      return;
    }
    if (target.kind == duckdb::CatalogType::VIEW_ENTRY) {
      catalog::ApplyEntryAlter(ax, duckdb::CatalogType::VIEW_ENTRY,
                               alter_schema_id, table_name, info);
      return;
    }
    if (info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
      return;
    }
    ThrowRelationMissing(table_name);
  }

  auto& table_info = info.Cast<duckdb::AlterTableInfo>();

  // ALTER INDEX <name> SET/RESET (option = ...): the maintenance/perf subset
  // of the inverted-index WITH options is alterable; SET writes the given
  // value, RESET restores the session-level default. Structural options
  // (row_group_size, store_pk, optimize_top_k, ...) shape the indexed data
  // and stay create-time only.
  if (table_info.alter_table_type ==
        duckdb::AlterTableType::SET_TABLE_OPTIONS ||
      table_info.alter_table_type ==
        duckdb::AlterTableType::RESET_TABLE_OPTIONS) {
    auto& context = transaction.GetContext();
    // ALTER TABLE <name> SET/RESET (...) (PG storage params) parses into the
    // same info as ALTER INDEX: only inverted indexes have alterable options.
    // A missing name is reported after the options are validated.
    if (relation) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("this ALTER TABLE operation is not supported"));
    }
    if (index && !index->IsInverted()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", table_name, "\" is not an inverted index"));
    }
    const auto require_alterable = [](std::string_view option) {
      if (!absl::c_contains(connector::kAlterableInvertedOptions, option)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("option \"", option,
                                "\" cannot be changed with ALTER INDEX"));
      }
    };
    std::vector<std::pair<std::string, uint64_t>> changes;
    if (table_info.alter_table_type ==
        duckdb::AlterTableType::SET_TABLE_OPTIONS) {
      for (auto& [option, expr] :
           table_info.Cast<duckdb::SetTableOptionsInfo>().table_options) {
        if (!expr ||
            expr->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("option \"", option, "\" requires a constant value"));
        }
        require_alterable(option);
        changes.emplace_back(
          option,
          connector::ValidateInvertedIndexOptionValue(
            option, expr->Cast<duckdb::ConstantExpression>().GetValue()));
      }
    } else {
      // RESET stores the session-resolved value, which goes through the same
      // validator as an explicit SET.
      for (const auto& option :
           table_info.Cast<duckdb::ResetTableOptionsInfo>().table_options) {
        const auto& option_name = option.GetIdentifierName();
        require_alterable(option_name);
        changes.emplace_back(
          option_name, connector::ValidateInvertedIndexOptionValue(
                         option_name, duckdb::Value::UBIGINT(
                                        connector::ResolveUbigintWithOption(
                                          context, option_name, nullptr))));
      }
    }
    if (!index) {
      if (info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("index \"", table_name, "\" does not exist"));
    }
    AlterInvertedIndexOptions(
      ax, *index,
      [changes = std::move(changes)](catalog::InvertedIndexOptions& options) {
        for (const auto& [option, value] : changes) {
          if (option == kRefreshIntervalSetting) {
            options.refresh_interval_ms = static_cast<uint32_t>(value);
          } else if (option == kReindexIntervalSetting) {
            options.reindex_interval_ms = static_cast<uint32_t>(value);
          } else if (option == kCompactionIntervalSetting) {
            options.compaction_interval_ms = static_cast<uint32_t>(value);
          } else if (option == kCleanupIntervalStepSetting) {
            options.cleanup_interval_step = static_cast<uint32_t>(value);
          } else if (option == kSegmentMemoryMaxSetting) {
            options.segment_memory_max = value;
          } else if (option == kSegmentDocsMaxSetting) {
            options.segment_docs_max = static_cast<uint32_t>(value);
          } else if (option == kCompactionMaxSegmentsSetting) {
            options.compaction_max_segments = static_cast<uint32_t>(value);
          } else if (option == kCompactionMaxSegmentsBytesSetting) {
            options.compaction_max_segments_bytes = value;
          } else {
            SDB_ASSERT(option == kCompactionFloorSegmentBytesSetting);
            options.compaction_floor_segment_bytes = value;
          }
        }
      });
    return;
  }

  if (!relation) {
    // An index takes RENAME and nothing else.
    if (index) {
      if (table_info.alter_table_type == duckdb::AlterTableType::RENAME_TABLE) {
        const auto new_name = table_info.Cast<duckdb::RenameTableInfo>()
                                .new_table_name.GetIdentifierName();
        RequireIndexOwner(ax, *index);
        if (FindRelation(ax.context, alter_schema_id, new_name)) {
          catalog::ThrowDuplicateName(catalog::NameKind::Relation, new_name);
        }
        catalog::RenameIndex(ax.context, *index, new_name);
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", table_name, "\" is not a table"));
    }
    if (target.kind == duckdb::CatalogType::SEQUENCE_ENTRY) {
      if (table_info.alter_table_type == duckdb::AlterTableType::RENAME_TABLE) {
        const auto new_name = table_info.Cast<duckdb::RenameTableInfo>()
                                .new_table_name.GetIdentifierName();
        if (FindRelation(ax.context, alter_schema_id, new_name)) {
          catalog::ThrowDuplicateName(catalog::NameKind::Relation, new_name);
        }
        catalog::ApplyEntryAlter(ax, duckdb::CatalogType::SEQUENCE_ENTRY,
                                 alter_schema_id, table_name, info);
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", table_name, "\" is not a table"));
    }
    // PG lets ALTER TABLE name a view, and RENAME is the action it can take.
    if (target.kind == duckdb::CatalogType::VIEW_ENTRY) {
      if (table_info.alter_table_type == duckdb::AlterTableType::RENAME_TABLE) {
        RenameViewObject(ax, db, name.GetIdentifierName(), table_name,
                         table_info.Cast<duckdb::RenameTableInfo>()
                           .new_table_name.GetIdentifierName(),
                         info);
        return;
      }
      // A view's columns are its query's, so renaming one is not a relkind
      // refusal but an unsupported operation, and it reads the same whichever
      // half of the namespace held the name.
      if (table_info.alter_table_type ==
          duckdb::AlterTableType::RENAME_COLUMN) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("cannot rename columns of a non-table relation"));
      }
      // A view carries no constraints, so the name is simply not there.
      if (table_info.alter_table_type ==
          duckdb::AlterTableType::RENAME_CONSTRAINT) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("constraint \"",
                  table_info.Cast<duckdb::RenameConstraintInfo>().old_name,
                  "\" for table \"", table_name, "\" does not exist"));
      }
      ThrowAlterNotSupportedOnView(table_info.alter_table_type, table_name);
    }
    ThrowRelationMissing(table_name);
  }

  // Search-backed tables have a fixed iresearch schema, so structural ALTERs
  // are rejected. Renames (table/column/constraint) are catalog-only metadata
  // -- iresearch fields and the scan are keyed by column id, not name -- so
  // they stay allowed.
  std::string_view unsupported_search_op;
  switch (table_info.alter_table_type) {
    case duckdb::AlterTableType::ADD_COLUMN:
      unsupported_search_op = "ALTER TABLE ADD COLUMN";
      break;
    case duckdb::AlterTableType::REMOVE_COLUMN:
      unsupported_search_op = "ALTER TABLE DROP COLUMN";
      break;
    case duckdb::AlterTableType::DROP_CONSTRAINT:
      unsupported_search_op = "ALTER TABLE DROP CONSTRAINT";
      break;
    case duckdb::AlterTableType::ALTER_COLUMN_TYPE:
      unsupported_search_op = "ALTER TABLE ALTER COLUMN TYPE";
      break;
    default:
      break;
  }
  if (!unsupported_search_op.empty()) {
    connector::RejectIfSearchTable(catalog::ReadTableEngineTag(relation->tags),
                                   unsupported_search_op);
  }

  switch (table_info.alter_table_type) {
    case duckdb::AlterTableType::RENAME_TABLE: {
      auto& rename_info = table_info.Cast<duckdb::RenameTableInfo>();
      const auto new_name = rename_info.new_table_name.GetIdentifierName();
      // Concurrency is duckdb's -- set.AlterEntry conflicts on the version
      // chain a concurrent rename or drop touched.
      if (FindRelation(ax.context, alter_schema_id, new_name)) {
        catalog::ThrowDuplicateName(catalog::NameKind::Relation, new_name);
      }
      catalog::ApplyTableAlter(ax, *relation, info);
      return;
    }

    // duckdb's own alter, run once and whole: what comes back is read as the
    // authored definition, which drops the foreign-key back-references the
    // builder derives -- those belong to the referencing table, not to this
    // one.
    case duckdb::AlterTableType::ADD_COLUMN: {
      auto& add_info = table_info.Cast<duckdb::AddColumnInfo>();
      if (add_info.new_column.Generated()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("adding a generated column is not supported"));
      }
      // The stable id, allocated here: duckdb's alter carries the column
      // through whole, and its identity must be decided before the definition
      // is built.
      add_info.new_column.SetCatalogOid(catalog::NextId().id());
      [[fallthrough]];
    }
    case duckdb::AlterTableType::RENAME_COLUMN:
    case duckdb::AlterTableType::SET_NOT_NULL:
    case duckdb::AlterTableType::DROP_NOT_NULL:
    case duckdb::AlterTableType::SET_DEFAULT:
    case duckdb::AlterTableType::DROP_CONSTRAINT:
    case duckdb::AlterTableType::RENAME_CONSTRAINT: {
      catalog::ApplyTableAlter(ax, *relation, info);
      return;
    }

    case duckdb::AlterTableType::ADD_CONSTRAINT: {
      auto& add_info = table_info.Cast<duckdb::AddConstraintInfo>();
      // duckdb's alter carries the constraint through whole, so its PG shape
      // is decided here, before the definition is built: the durable name,
      // the pre-allocated ids, and for a PRIMARY KEY the NOT NULLs it
      // implies, each a named constraint of its own.
      std::vector<std::string> chosen;
      const auto choose_name = [&](std::string_view base) {
        auto exists = [&](std::string_view candidate) {
          return absl::c_any_of(relation->constraints,
                                [&](const auto& constraint) {
                                  return constraint->constraint_name ==
                                         candidate;
                                }) ||
                 absl::c_linear_search(chosen, candidate);
        };
        std::string candidate{base};
        for (size_t counter = 1; exists(candidate); ++counter) {
          candidate = absl::StrCat(base, counter);
        }
        chosen.push_back(candidate);
        return candidate;
      };
      if (add_info.constraint->type == duckdb::ConstraintType::UNIQUE) {
        auto& unique = add_info.constraint->Cast<duckdb::UniqueConstraint>();
        const bool is_pk = unique.IsPrimaryKey();
        // Ahead of the alter because the ART build runs first and is not the
        // transaction's to roll back: a refusal thrown after it would leave
        // the index behind.
        if (is_pk && catalog::TablePrimaryKey(relation->constraints)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
                          ERR_MSG("multiple primary keys for table \"",
                                  table_name, "\" are not allowed"));
        }
        // Adding a primary key changes which columns provide row identity, so
        // every index already built over the table indexes the wrong thing
        // afterwards.
        if (is_pk && !catalog::RelationIndexRecords(ax.context, alter_schema_id,
                                                    catalog::IdOf(*relation))
                        .empty()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
            ERR_MSG("Cannot alter entry \"", table_name,
                    "\" because there are entries that depend on it."));
        }
        // The key columns in the current definition: the names and implied
        // NOT NULLs below are derived off them.
        std::vector<const duckdb::ColumnDefinition*> key_columns;
        if (unique.HasIndex()) {
          SDB_ASSERT(unique.GetIndex().index <
                     relation->columns.LogicalColumnCount());
          key_columns.push_back(
            &relation->columns.GetColumn(unique.GetIndex()));
        } else {
          for (const auto& key : unique.GetColumnNames()) {
            const auto* column =
              catalog::ColumnByName(*relation, key.GetIdentifierName());
            SDB_ASSERT(column);
            key_columns.push_back(column);
          }
        }
        for (const auto* column : key_columns) {
          // ART has no nested-type key, and a search table's key is enforced
          // by nothing at all -- letting either through would leave a key the
          // catalog reports and nothing checks.
          if (column->Type().IsNested()) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
              ERR_MSG(is_pk ? "primary key" : "unique constraint", " column \"",
                      column->Name().GetIdentifierName(),
                      "\" has unsupported nested type ",
                      column->Type().ToString()));
          }
        }
        if (unique.constraint_name.empty()) {
          unique.constraint_name = choose_name(
            is_pk ? absl::StrCat(table_name, "_pkey")
                  : absl::StrCat(
                      table_name, "_",
                      key_columns.front()->Name().GetIdentifierName(), "_key"));
        }
        unique.oid = catalog::NextId().id();
        unique.host_index_id = catalog::NextId().id();
        if (is_pk) {
          for (const auto* column : key_columns) {
            if (catalog::IsColumnNotNull(*relation,
                                         ObjectId{column->CatalogOid()})) {
              continue;
            }
            auto not_null =
              duckdb::make_uniq<duckdb::NotNullConstraint>(column->Logical());
            not_null->constraint_name = choose_name(
              absl::StrCat(table_name, "_", column->Name().GetIdentifierName(),
                           "_not_null"));
            not_null->oid = catalog::NextId().id();
            add_info.implied_not_nulls.push_back(std::move(not_null));
          }
        }
      } else if (add_info.constraint->type == duckdb::ConstraintType::CHECK) {
        auto& check = add_info.constraint->Cast<duckdb::CheckConstraint>();
        if (check.constraint_name.empty()) {
          // PostgreSQL-style auto name, matching the CREATE TABLE path.
          auto col = FindConstraintColumn(*check.expression);
          check.constraint_name = choose_name(
            col.empty() ? absl::StrCat(table_name, "_check")
                        : absl::StrCat(table_name, "_", col, "_check"));
        }
        check.oid = catalog::NextId().id();
      } else {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("ALTER TABLE ADD CONSTRAINT supports only CHECK, UNIQUE, and "
                  "PRIMARY KEY constraints"));
      }
      catalog::ApplyTableAlter(ax, *relation, info);
      return;
    }

    case duckdb::AlterTableType::REMOVE_COLUMN: {
      auto& remove_info = table_info.Cast<duckdb::RemoveColumnInfo>();
      catalog::DropTableColumn(ax, db, *relation,
                               remove_info.removed_column.GetIdentifierName(),
                               remove_info.if_column_exists);
      return;
    }

    case duckdb::AlterTableType::ADD_FIELD:
    case duckdb::AlterTableType::REMOVE_FIELD:
    case duckdb::AlterTableType::RENAME_FIELD: {
      // Nested-STRUCT field DDL, routed through ALTER COLUMN TYPE with
      // duckdb's exact by-name remap_struct(...) USING cast -- a plain or
      // positional cast silently mis-maps renamed/dropped fields.
      const duckdb::vector<duckdb::Identifier>* column_path = nullptr;
      if (table_info.alter_table_type == duckdb::AlterTableType::ADD_FIELD) {
        column_path = &table_info.Cast<duckdb::AddFieldInfo>().column_path;
      } else if (table_info.alter_table_type ==
                 duckdb::AlterTableType::REMOVE_FIELD) {
        column_path = &table_info.Cast<duckdb::RemoveFieldInfo>().column_path;
      } else {
        column_path = &table_info.Cast<duckdb::RenameFieldInfo>().column_path;
      }
      const std::string& root_column = (*column_path)[0].GetIdentifierName();
      const auto* root = catalog::ColumnByName(*relation, root_column);
      if (root == nullptr) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                        ERR_MSG("column \"", root_column, "\" of relation \"",
                                table_name, "\" does not exist"));
      }

      // The remap below ignores IF [NOT] EXISTS, so short-circuit the no-op
      // (Add of an existing field / Drop of a missing one) here.
      auto field_exists = [](const duckdb::LogicalType& root,
                             const duckdb::vector<duckdb::Identifier>& path,
                             size_t path_end, std::string_view leaf) -> bool {
        // Direct child of struct `type` named `name` (case-insensitive), or
        // null.
        auto child = [](const duckdb::LogicalType& type,
                        std::string_view name) -> const duckdb::LogicalType* {
          if (type.id() != duckdb::LogicalTypeId::STRUCT) {
            return nullptr;
          }
          const auto& children = duckdb::StructType::GetChildTypes(type);
          auto found = absl::c_find_if(children, [&](const auto& field) {
            return absl::EqualsIgnoreCase(field.first.GetIdentifierName(),
                                          name);
          });
          return found == children.end() ? nullptr : &found->second;
        };
        const duckdb::LogicalType* current = &root;
        for (size_t depth = 1; depth < path_end; ++depth) {
          current = child(*current, path[depth].GetIdentifierName());
          if (!current) {
            return false;
          }
        }
        return child(*current, leaf) != nullptr;
      };
      if (table_info.alter_table_type == duckdb::AlterTableType::ADD_FIELD) {
        const auto& add_field = table_info.Cast<duckdb::AddFieldInfo>();
        if (add_field.if_field_not_exists &&
            field_exists(root->Type(), *column_path, column_path->size(),
                         add_field.new_field.Name().GetIdentifierName())) {
          return;
        }
      } else if (table_info.alter_table_type ==
                 duckdb::AlterTableType::REMOVE_FIELD) {
        const auto& remove_field = table_info.Cast<duckdb::RemoveFieldInfo>();
        if (remove_field.if_column_exists &&
            !field_exists(root->Type(), *column_path, column_path->size() - 1,
                          column_path->back().GetIdentifierName())) {
          return;
        }
      }

      duckdb::StructFieldRemap remap;
      if (table_info.alter_table_type == duckdb::AlterTableType::ADD_FIELD) {
        auto& add_field = table_info.Cast<duckdb::AddFieldInfo>();
        remap = duckdb::BuildAddFieldRemap(
          root->Type(), duckdb::Identifier{root_column}, add_field.column_path,
          add_field.new_field);
      } else if (table_info.alter_table_type ==
                 duckdb::AlterTableType::REMOVE_FIELD) {
        remap = duckdb::BuildRemoveFieldRemap(root->Type(), *column_path);
      } else {
        remap = duckdb::BuildRenameFieldRemap(
          root->Type(), *column_path,
          table_info.Cast<duckdb::RenameFieldInfo>()
            .new_name.GetIdentifierName());
      }

      catalog.Cast<SereneDBCatalog>().ChangeColumnType(
        ax, *relation, root_column, std::move(remap.new_type),
        std::move(remap.remap_expression));
      return;
    }

    case duckdb::AlterTableType::ALTER_COLUMN_TYPE: {
      auto& type_info = table_info.Cast<duckdb::ChangeColumnTypeInfo>();
      catalog.Cast<SereneDBCatalog>().ChangeColumnType(
        ax, *relation, type_info.column_name.GetIdentifierName(),
        type_info.target_type,
        type_info.expression ? type_info.expression->Copy() : nullptr);
      return;
    }

    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("this ALTER TABLE operation is not supported"));
  }
}

}  // namespace sdb::catalog
