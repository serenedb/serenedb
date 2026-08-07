////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "catalog/catalog.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <duckdb/common/exception/parser_exception.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/query_context.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/storage/checkpoint/checkpoint_options.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <filesystem>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <ranges>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/async/when_all.hpp>

#include "app/app_server.h"
#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-compiler.h"
#include "catalog/column_expr.h"
#include "catalog/database.h"
#include "catalog/deferred_writes.h"
#include "catalog/drop_task.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/object_dependency.h"
#include "catalog/persistence/role.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/store/data_store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_dependency.h"
#include "connector/duckdb_global_catalog.h"
#include "connector/duckdb_index_entry.h"
#include "connector/duckdb_object_entry.h"
#include "connector/duckdb_object_index.h"
#include "connector/duckdb_storage_extension.h"
#include "connector/duckdb_table_entry.h"
#include "network/credentials.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

// A database file that is gone or will not open is not a state the server
// reaches on its own: somebody removed or corrupted it. Refusing is the default
// because silently recreating an empty database is the one outcome that turns a
// recoverable accident into data loss.
ABSL_FLAG(std::string, missing_database, "refuse",
          "What boot does with a database whose data file is missing or will "
          "not open: 'refuse' (default) stops the server, 'skip' leaves it "
          "unattached, 'drop' removes it from the catalog.");

namespace sdb::catalog {

AccessContext RequireAccess(duckdb::ClientContext& context, AclMode need) {
  return {connector::GetSereneDBContext(context).GetRoleId(), need, &context};
}

AccessContext ActingAs(duckdb::ClientContext& context) {
  return {connector::GetSereneDBContext(context).GetRoleId(), AclMode::NoRights,
          &context};
}

namespace {

// Roles and databases hang off the instance, not off a database, so their
// writes are attributed to the storage-less cluster-global attachment rather
// than to whichever database the statement happens to run in. Called before
// _mutex, for the same reason JoinStoreTransaction is: it opens the
// transaction there.
// The context-less callers -- boot, WAL replay, background drop tasks -- have
// no transaction to attribute anything to.
void JoinClusterGlobal(duckdb::ClientContext* context,
                       duckdb::DatabaseModificationType modification) {
  if (context != nullptr) {
    connector::ModifyGlobalDatabase(*context, modification);
  }
}

// One index record, and the resolution it carries. Every version's record has
// its own, taken where the version is built: a rename has moved the names it
// resolved by the time boot reads it back, so re-resolving there would drop the
// edge without a word. The relation the index covers rides on the info, so
// PutEntry's one parent is the schema its name lives in.
void PutIndex(CatalogStore::WriteContext& ctx, const IndexInfoRef& index,
              wal::PutMode mode) {
  ctx.catalog().PutEntry(index->GetSchemaId(), duckdb::CatalogType::INDEX_ENTRY,
                         index->GetId(), mode, index, Permissions{});
}

void RequireRoleAttribute(duckdb::ClientContext* context, ObjectId actor_id,
                          RoleOption attribute, std::string_view denied_action,
                          std::string_view detail = {});

void RequireAttributesGrantable(duckdb::ClientContext* context,
                                ObjectId actor_id, RoleOption granting,
                                bool creating);

[[noreturn]] void ThrowDuplicateDatabase(std::string_view name) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_DATABASE),
                  ERR_MSG("database \"", name, "\" already exists"));
}

std::optional<ObjectId> FindDatabaseId(duckdb::ClientContext* context,
                                       std::string_view name) {
  auto database = connector::FindDatabase(context, name);
  return database ? std::optional{database.Id()} : std::nullopt;
}

std::optional<ObjectId> FindSchemaId(duckdb::ClientContext* context,
                                     ObjectId database_id,
                                     std::string_view name) {
  const auto id = connector::FindSchemaId(context, database_id, name);
  return id.isSet() ? std::optional{id} : std::nullopt;
}

// The three namespaces a create can collide in, each with its own errcode and
// noun. A function shares the relation namespace, as it does in postgres.

[[noreturn]] void ThrowWrongObjectType(std::string_view name,
                                       std::string_view kind,
                                       duckdb::CatalogType actual) {
  const auto actual_kind = pg::ToPgObjectTypeName(actual);
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
    ERR_MSG("\"", name, "\" is not ", basics::string_utils::GetArticle(kind),
            " ", kind),
    ERR_HINT("Use DROP ", absl::AsciiStrToUpper(actual_kind), " to remove ",
             basics::string_utils::GetArticle(actual_kind), " ", actual_kind,
             "."));
}

// The record that opens a drop bracket: where the root sits, and everything
// under it whose artifacts the async sweep still has to reclaim. Carrying it is
// what lets a boot rebuild the task tree without reading back definitions the
// drop has already taken out of the catalog.
wal::DropPrepare MakeDropPrepare(const DropTask& task, ObjectId parent_id,
                                 duckdb::CatalogType type, ObjectId id,
                                 ObjectId database_id, ObjectId schema_id) {
  auto subtree = std::make_shared<std::vector<wal::DropNode>>();
  task.DescribeSubtree(parent_id, *subtree);
  return {.parent_id = parent_id,
          .type = type,
          .id = id,
          .database_id = database_id,
          .schema_id = schema_id,
          .subtree = std::move(subtree)};
}

void UpdatePendingDrops(PendingDrops& pending_drops, ObjectId parend_id,
                        ObjectId id, const std::shared_ptr<DropTask>& task,
                        bool is_root) {
  auto it = pending_drops.find(id);
  if (it != pending_drops.end()) {
    task->SetAttached(std::move(it->second));
    pending_drops.erase(it);
  }
  if (is_root) {
    pending_drops[parend_id].emplace_back(std::move(task));
  }
}

}  // namespace
namespace {

// The database and schema a qualified reference lands in, or unset when it
// names pg_catalog, information_schema, or something that is not there.
ObjectId ResolveSchemaOf(duckdb::ClientContext* context, ObjectId db_id,
                         ObjectId default_schema_id, std::string_view catalog,
                         std::string_view schema) {
  if (schema == StaticStrings::kPgCatalogSchema ||
      schema == StaticStrings::kInformationSchema) {
    return {};
  }
  if (!catalog.empty()) {
    auto r = connector::FindDatabase(context, catalog);
    if (!r) {
      return {};
    }
    db_id = r.Id();
  }
  if (schema.empty()) {
    return default_schema_id;
  }
  return connector::FindSchemaId(context, db_id, schema);
}

ObjectId ResolveTypeId(duckdb::ClientContext* context, ObjectId db_id,
                       ObjectId default_schema_id, std::string_view catalog,
                       std::string_view schema, std::string_view name) {
  const auto schema_id =
    ResolveSchemaOf(context, db_id, default_schema_id, catalog, schema);
  if (!schema_id.isSet()) {
    return {};
  }
  const auto* type = connector::FindType(context, schema_id, name);
  return type != nullptr ? ObjectId{type->oid} : ObjectId{};
}

ObjectId ResolveSequenceId(duckdb::ClientContext* context, ObjectId db_id,
                           ObjectId default_schema_id, std::string_view catalog,
                           std::string_view schema, std::string_view name) {
  const auto schema_id =
    ResolveSchemaOf(context, db_id, default_schema_id, catalog, schema);
  if (!schema_id.isSet()) {
    return {};
  }
  const auto* sequence = connector::FindSequence(context, schema_id, name);
  return sequence != nullptr ? ObjectId{sequence->oid} : ObjectId{};
}

ObjectId ResolveFunctionId(duckdb::ClientContext* context, ObjectId db_id,
                           ObjectId default_schema_id, std::string_view catalog,
                           std::string_view schema, std::string_view name) {
  const auto schema_id =
    ResolveSchemaOf(context, db_id, default_schema_id, catalog, schema);
  if (!schema_id.isSet()) {
    return {};
  }
  auto function = connector::FindFunction(context, schema_id, name);
  return function ? IdOf(*function) : ObjectId{};
}

// Both halves of the relation namespace answer here: a table and a view are
// separate kinds with one set between them.
ObjectId ResolveRelationId(duckdb::ClientContext* context, ObjectId db_id,
                           ObjectId default_schema_id, std::string_view catalog,
                           std::string_view schema, std::string_view name) {
  const auto schema_id =
    ResolveSchemaOf(context, db_id, default_schema_id, catalog, schema);
  if (!schema_id.isSet()) {
    return {};
  }
  if (const auto* table = connector::FindTable(context, schema_id, name)) {
    return ObjectId{table->oid};
  }
  auto view = connector::FindView(context, schema_id, name);
  return view ? IdOf(*view) : ObjectId{};
}

// The id one name in an expression body resolves to, by what the node states.
// An unset answer is a pg_catalog / information_schema reference or something
// that is not there, which is not an edge.
ObjectId ResolveExprRef(duckdb::ClientContext* context, ObjectId database_id,
                        ObjectId schema_id, RefKinds kind,
                        const QualifiedRef& ref) {
  switch (kind) {
    case RefKinds::Sequences:
      return ResolveSequenceId(context, database_id, schema_id, ref.catalog,
                               ref.schema, ref.name);
    case RefKinds::Functions:
      return ResolveFunctionId(context, database_id, schema_id, ref.catalog,
                               ref.schema, ref.name);
    default:
      return ResolveTypeId(context, database_id, schema_id, ref.catalog,
                           ref.schema, ref.name);
  }
}

// Stamps what every name in `expr` resolves to onto the node stating it. Taken
// where the version is written, because only the catalog the statement sees
// knows what the names point at -- and read back off the node afterwards,
// because a rename since has moved them.
void StampExprIds(duckdb::ClientContext* context, ObjectId database_id,
                  ObjectId schema_id, duckdb::ParsedExpression& expr) {
  auto refs = ExtractMutableRefs(
    expr, RefKinds::Sequences | RefKinds::Functions | RefKinds::Types);
  const auto stamp = [&](const std::vector<QualifiedRef>& list, RefKinds kind) {
    for (const auto& ref : list) {
      if (ref.node != nullptr) {
        ref.node->oid =
          ResolveExprRef(context, database_id, schema_id, kind, ref).id();
      }
    }
  };
  stamp(refs.sequences, RefKinds::Sequences);
  stamp(refs.functions, RefKinds::Functions);
  stamp(refs.unbound_types, RefKinds::Types);
}

void CollectRef(std::vector<ObjectId>& out, ObjectId target) {
  if (target.isSet()) {
    out.push_back(target);
  }
}

void CollectRefs(duckdb::ClientContext* context, ObjectId database_id,
                 ObjectId schema_id, const Refs& refs, ObjectId self,
                 std::vector<ObjectId>& out) {
  for (const auto& ref : refs.sequences) {
    CollectRef(out, ResolveSequenceId(context, database_id, schema_id,
                                      ref.catalog, ref.schema, ref.name));
  }
  for (const auto& ref : refs.relations) {
    CollectRef(out, ResolveRelationId(context, database_id, schema_id,
                                      ref.catalog, ref.schema, ref.name));
  }
  for (const auto& ref : refs.functions) {
    const auto target = ResolveFunctionId(context, database_id, schema_id,
                                          ref.catalog, ref.schema, ref.name);
    if (target != self) {  // a recursive function is not its own dependent
      CollectRef(out, target);
    }
  }
  for (const auto& ref : refs.unbound_types) {
    CollectRef(out, ResolveTypeId(context, database_id, schema_id, ref.catalog,
                                  ref.schema, ref.name));
  }
  for (const auto type_id : refs.types) {
    CollectRef(out, type_id);
  }
}

// The roles one object names -- as owner, as grantee, as grantor. Every one of
// them is what DROP ROLE is refused over.
void CollectRoleRefs(const Permissions& perm, std::vector<ObjectId>& out) {
  CollectRef(out, OwnerOf(perm));
  for (const auto& item : perm.acl) {
    CollectRef(out, GranteeOf(item));
    CollectRef(out, GrantorOf(item));
  }
}

duckdb::LogicalDependencyList ReferenceList(std::vector<ObjectId> ids) {
  std::ranges::sort(ids);
  ids.erase(std::ranges::unique(ids).begin(), ids.end());
  return connector::DependencyList(ids);
}

}  // namespace

std::vector<TableReference> TableReferences(
  const duckdb::CreateTableInfo& info) {
  std::vector<TableReference> out;
  const auto collect = [&](const duckdb::ParsedExpression& expr, ObjectId sub,
                           TableRefKind kind) {
    std::vector<ObjectId> ids;
    CollectExprIds(expr, ids);
    for (const auto id : ids) {
      out.push_back({id, sub, kind});
    }
  };
  for (const auto& column : info.columns.Logical()) {
    const ObjectId column_id{column.CatalogOid()};
    Refs type_refs;
    CollectTypeRefs(column.Type(), type_refs);
    for (const auto type_id : type_refs.types) {
      out.push_back({type_id, column_id, TableRefKind::ColumnType});
    }
    if (column.HasDefaultValue() && !column.Generated()) {
      collect(column.DefaultValue(), column_id, TableRefKind::ColumnDefault);
    } else if (column.Generated()) {
      collect(column.GeneratedExpression(), column_id,
              TableRefKind::ColumnDefault);
    }
  }
  for (const auto& constraint : info.constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      const auto& check = constraint->Cast<duckdb::CheckConstraint>();
      collect(*check.expression, ObjectId{check.oid}, TableRefKind::Check);
      continue;
    }
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    const ObjectId referenced{fk.host_referenced_id};
    if (referenced.isSet() && referenced != catalog::IdOf(info)) {
      out.push_back({referenced, ObjectId{fk.oid}, TableRefKind::ForeignKey});
    }
  }
  return out;
}

duckdb::LogicalDependencyList TableDependencies(
  const duckdb::CreateTableInfo& info, const Permissions& perm) {
  std::vector<ObjectId> ids;
  CollectRoleRefs(perm, ids);
  // A column carries grants of its own, which name roles the table's ACL does
  // not.
  for (const auto& [column_id, acl] : perm.column_acl) {
    for (const auto& item : acl) {
      CollectRef(ids, GranteeOf(item));
      CollectRef(ids, GrantorOf(item));
    }
  }
  for (const auto& reference : TableReferences(info)) {
    CollectRef(ids, reference.referenced);
  }
  return ReferenceList(std::move(ids));
}

TableInfoRef NextTableVersion(duckdb::ClientContext* context, ObjectId id,
                              ObjectId schema_id, TableInfoRef info) {
  auto next = catalog::Clone(*info);
  SetIdentity(*next, id, schema_id);
  const auto database_id = connector::SchemaDatabaseId(context, schema_id);
  // One member for both, as duckdb keeps them: a DEFAULT on a plain column, the
  // body on a generated one.
  for (idx_t i = 0; i < next->columns.LogicalColumnCount(); ++i) {
    auto& column = next->columns.GetColumnMutable(duckdb::LogicalIndex{i});
    if (auto* expression = column.ExpressionMutable()) {
      StampExprIds(context, database_id, schema_id, *expression);
    }
  }
  for (auto& constraint : next->constraints) {
    if (constraint->type == duckdb::ConstraintType::CHECK) {
      StampExprIds(context, database_id, schema_id,
                   *constraint->Cast<duckdb::CheckConstraint>().expression);
    }
  }
  return next;
}

std::shared_ptr<const duckdb::CreateViewInfo> NextViewVersion(
  duckdb::ClientContext* context,
  std::shared_ptr<const duckdb::CreateViewInfo> view) {
  auto next =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      view->Copy());
  const auto schema_id = ParentIdOf(*next);
  std::vector<ObjectId> ids;
  CollectRefs(context, connector::SchemaDatabaseId(context, schema_id),
              schema_id, ViewRefs(*next, RefKinds::All), IdOf(*next), ids);
  next->dependencies = ReferenceList(std::move(ids));
  return std::shared_ptr<const duckdb::CreateViewInfo>{next.release()};
}

std::shared_ptr<const duckdb::CreateMacroInfo> NextFunctionVersion(
  duckdb::ClientContext* context,
  std::shared_ptr<const duckdb::CreateMacroInfo> function) {
  auto next =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      function->Copy());
  const auto schema_id = ParentIdOf(*next);
  std::vector<ObjectId> ids;
  CollectRefs(context, connector::SchemaDatabaseId(context, schema_id),
              schema_id, MacroRefs(*next, RefKinds::All), IdOf(*next), ids);
  next->dependencies = ReferenceList(std::move(ids));
  return std::shared_ptr<const duckdb::CreateMacroInfo>{next.release()};
}

IndexInfoRef NextIndexVersion(duckdb::ClientContext* context,
                              const IndexInfoRef& index) {
  if (!index->IsInverted()) {
    return index;
  }
  auto next = duckdb::unique_ptr_cast<duckdb::CreateInfo, CreateIndexInfoBase>(
    index->Copy());
  const auto schema_id = next->GetSchemaId();
  const auto database_id = connector::SchemaDatabaseId(context, schema_id);
  // The dictionaries its entries name, by id, and the functions its expression
  // keys name, by resolution.
  std::vector<ObjectId> ids;
  for (const auto tokenizer_id : next->GetTokenizers()) {
    CollectRef(ids, tokenizer_id);
  }
  for (const auto& key : InvertedInfo(*next).ExpressionKeys()) {
    const auto& expr = key.data;
    SDB_ASSERT(!expr.pretty_printed.empty());
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
    try {
      parsed = duckdb::Parser::ParseExpressionList(expr.pretty_printed);
    } catch (const duckdb::ParserException&) {
      continue;
    }
    for (const auto& p : parsed) {
      SDB_ASSERT(p);
      CollectRefs(context, database_id, schema_id,
                  ExtractRefs(*p, RefKinds::Functions), ObjectId{}, ids);
    }
  }
  next->dependencies = ReferenceList(std::move(ids));
  return std::shared_ptr<const CreateIndexInfoBase>{next.release()};
}

void RequireAccess(duckdb::ClientContext* context, ObjectId role,
                   duckdb::CatalogType type, std::string_view name,
                   const Permissions& perm, AclMode need) {
  if (need == AclMode::NoRights ||
      auth::ClosureFor(context, role)->Can(type, perm, need)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied for ", pg::ToPgObjectTypeName(type), " ", name));
}

TableInfoRef GetTable(const AccessContext& ax, ObjectId database_id,
                      std::string_view schema, std::string_view name,
                      Permissions* perm) {
  const auto schema_id =
    connector::FindSchemaId(ax.context, database_id, schema);
  if (!schema_id.isSet()) {
    return nullptr;
  }
  const auto* table = connector::FindTable(ax.context, schema_id, name);
  if (table == nullptr) {
    return nullptr;
  }
  RequireAccess(ax.context, ax.role, duckdb::CatalogType::TABLE_ENTRY,
                table->name.GetIdentifierName(), table->permissions, ax.need);
  if (perm != nullptr) {
    *perm = table->permissions;
  }
  return table->Definition();
}

SequenceOwners CollectSequenceOwners(duckdb::ClientContext* context,
                                     ObjectId database_id) {
  SequenceOwners out;
  connector::VisitSequences(
    context, database_id,
    [&](const connector::SereneDBSequenceEntry& sequence) {
      const auto owner = sequence.GetOwnerTableId().isSet()
                           ? sequence.GetOwnerTableId()
                           : ObjectId{sequence.ParentSchema().oid};
      out.by_owner[owner].push_back(ObjectId{sequence.oid});
    });
  for (auto& [owner, ids] : out.by_owner) {
    std::ranges::sort(ids);
  }
  return out;
}

IndexOwners CollectIndexOwners(duckdb::ClientContext* context,
                               ObjectId database_id) {
  IndexOwners out;
  connector::VisitIndexes(context, database_id, [&](const IndexInfoRef& index) {
    out.by_relation[index->GetRelationId()].push_back(index);
  });
  for (auto& [relation, indexes] : out.by_relation) {
    std::ranges::sort(indexes, {},
                      [](const IndexInfoRef& index) { return index->GetId(); });
  }
  return out;
}

namespace {

// Every table of one database, with the reverse index a cascade needs: the
// tables of a schema. Read once per plan, because each answer is a walk of the
// sets.
struct DatabaseTables {
  containers::FlatHashMap<ObjectId, HeldTable> by_id;
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> by_schema;

  const HeldTable* Find(ObjectId id) const {
    const auto it = by_id.find(id);
    return it == by_id.end() ? nullptr : &it->second;
  }
};

DatabaseTables CollectDatabaseTables(duckdb::ClientContext* context,
                                     ObjectId database_id) {
  DatabaseTables out;
  if (!database_id.isSet()) {
    return out;
  }
  connector::VisitTables(
    context, database_id,
    [&](const TableInfoRef& table, const Permissions& perm) {
      const auto id = catalog::IdOf(*table);
      out.by_schema[catalog::ParentIdOf(*table)].push_back(id);
      out.by_id.emplace(id, HeldTable{catalog::Clone(*table), perm});
    });
  return out;
}

// The indexes of one relation the plan does not take with it.
std::vector<IndexInfoRef> SurvivingIndexes(std::vector<IndexInfoRef> indexes,
                                           const DropPlan& plan) {
  std::erase_if(indexes, [&](const IndexInfoRef& index) {
    return std::ranges::any_of(plan.index_drops,
                               [&](const IndexInfoRef& dropped) {
                                 return dropped->GetId() == index->GetId();
                               });
  });
  return indexes;
}

// The database everything a cascade from `seed` can reach lives in. Every
// object is an entry, so the seed's own entry names the schema it hangs off and
// the schema names the database; a database seed names itself.
ObjectId CascadeDatabase(duckdb::ClientContext* context, ObjectId seed) {
  if (context == nullptr) {
    return {};
  }
  if (const auto under_schema = connector::SchemaDatabaseId(context, seed);
      under_schema.isSet()) {
    return under_schema;
  }
  if (connector::FindDatabase(context, seed)) {
    return seed;
  }
  if (auto entry = connector::LookupEntryById(*context, seed)) {
    return connector::SchemaDatabaseId(context, ParentIdOf(*entry));
  }
  return {};
}

// The victim closure of one DROP, walked over the pre-mutation catalog.
//
// The fan-out is duckdb's: DependencyView is ScanDependents, and what each
// dependent is comes off its own entry. What duckdb cannot say is here and
// only here -- a table that names the victim is usually rewritten rather than
// dropped, and DependencyDependentFlags has two bits and CheckDropDependencies
// answers with a set of entries to drop, with no notion of a surviving one.
// The plan also has to reach containment no edge records, and come out in the
// order CommitDropPlan and the drop tasks consume it.
class DropCascade {
 public:
  DropCascade(duckdb::ClientContext* context, ObjectId seed)
    : _context{context}, _deps{context}, _auto_drops{seed}, _stack{seed} {
    // Read off the sets once: each of these is a walk of the schema sets, and a
    // cascade that cannot reach one leaves every DEFAULT naming it -- or every
    // index over it -- in place.
    const auto database_id = CascadeDatabase(context, seed);
    if (!database_id.isSet()) {
      return;
    }
    _sequences = CollectSequenceOwners(context, database_id);
    _indexes = CollectIndexOwners(context, database_id);
    _tables = CollectDatabaseTables(context, database_id);
    connector::VisitFunctions(
      context, database_id, [&](const duckdb::MacroCatalogEntry& function) {
        _functions[ObjectId{function.ParentSchema().oid}].push_back(
          ObjectId{function.oid});
      });
  }

  DropPlan Run() &&;

 private:
  // AUTO/INTERNAL dep. Tag closure, queue for walk (dedup via _visited).
  void EmitAutoDrop(ObjectId id) {
    _auto_drops.insert(id);
    if (_visited.insert(id).second) {
      _stack.push_back(id);
    }
  }

  // A view or a function the cascade takes whole. The parent and the name come
  // off the entry now, in the database the session is connected to -- which is
  // the one holding everything a cascade can reach.
  void EmitEntryDrop(const duckdb::CatalogEntry& entry) {
    const auto id = IdOf(entry);
    if (_auto_drops.contains(id) || !_visited.insert(id).second) {
      return;
    }
    _plan.entry_drops.push_back({ParentIdOf(entry), id,
                                 std::string{entry.name.GetIdentifierName()},
                                 connector::KindOf(entry.type)});
    _stack.push_back(id);
  }

  // Tokenizer->index, column->index etc. -- cross-tree index drop with
  // a recovery anchor written in CommitDropPlan.
  void EmitCascadeIndexDrop(IndexInfoRef index) {
    if (!index) {
      return;
    }
    const auto id = index->GetId();
    if (_auto_drops.contains(id) || !_visited.insert(id).second) {
      return;
    }
    _plan.index_drops.push_back(std::move(index));
  }

  // The index of a relation whose entry is the object. It is an AUTO drop --
  // it goes because its relation does -- but no drop task's subtree carries it,
  // so the plan has to name it.
  void EmitOwnedIndexDrop(const IndexInfoRef& index) {
    const auto id = index->GetId();
    if (std::ranges::none_of(
          _plan.owned_index_drops,
          [&](const IndexInfoRef& listed) { return listed->GetId() == id; })) {
      _plan.owned_index_drops.push_back(index);
    }
  }

  // The slot a surviving table's next definition is built in, or null when the
  // cascade takes the table itself.
  TableRewrite* RewriteSlot(ObjectId table_id) {
    if (!table_id.isSet() || _auto_drops.contains(table_id)) {
      return nullptr;
    }
    auto& slot = _plan.table_rewrites[table_id];
    if (slot.info) {
      return &slot;
    }
    const auto* held = _tables.Find(table_id);
    SDB_ASSERT(held != nullptr);
    if (held == nullptr) {
      return nullptr;
    }
    slot.schema_id = catalog::ParentIdOf(*held->first);
    slot.id = table_id;
    slot.before = held->first;
    slot.perm = held->second;
    slot.info = held->first;
    return &slot;
  }

  // Held back rather than applied here: a column drop is the one cascade effect
  // that reshapes the store table, so it lands in its own definition version
  // once the walk is done (see Run) and the shape-preserving half stays
  // separately durable.
  void EmitCascadeColumnDrop(ObjectId table_id, ObjectId col_id) {
    if (RewriteSlot(table_id) == nullptr) {
      return;
    }
    _column_drops[table_id].push_back(col_id);
    // PG's column->index cascade: any index that covers col_id goes too.
    const auto it = _indexes.by_relation.find(table_id);
    if (it == _indexes.by_relation.end()) {
      return;
    }
    for (const auto& index : it->second) {
      if (index->ReferencesColumn(col_id)) {
        EmitCascadeIndexDrop(index);
      }
    }
  }

  // What the drop of `self` does to a table that names it. The four rewrites
  // are read off the table's own definition -- which column, which constraint,
  // and therefore which one -- so nothing about the cascade is written down.
  void ApplyToTable(const duckdb::CreateTableInfo& table, ObjectId self) {
    const auto table_id = catalog::IdOf(table);
    for (const auto& reference : TableReferences(table)) {
      if (reference.referenced != self) {
        continue;
      }
      switch (reference.kind) {
        case TableRefKind::ForeignKey:
          if (auto* slot = RewriteSlot(table_id)) {
            slot->info = catalog::DropForeignKeysReferencing(*slot->info, self);
          }
          break;
        case TableRefKind::Check:
          if (auto* slot = RewriteSlot(table_id)) {
            slot->info = catalog::DropConstraint(*slot->info, reference.sub_id);
          }
          break;
        case TableRefKind::ColumnDefault:
          if (auto* slot = RewriteSlot(table_id)) {
            slot->info =
              catalog::DropColumnDefault(*slot->info, reference.sub_id);
          }
          break;
        case TableRefKind::ColumnType:
          EmitCascadeColumnDrop(table_id, reference.sub_id);
          break;
      }
    }
  }

  void Apply(const connector::Dependent& dependent, ObjectId self) {
    switch (dependent.type) {
      using enum duckdb::CatalogType;
      case VIEW_ENTRY:
      case MACRO_ENTRY:
      case TABLE_MACRO_ENTRY:
        EmitEntryDrop(*dependent.entry);
        return;
      case INDEX_ENTRY: {
        const auto* dropped = _context == nullptr ? nullptr
                                                  : connector::FindSessionIndex(
                                                      *_context, dependent.id);
        EmitCascadeIndexDrop(dropped != nullptr ? dropped->Definition()
                                                : nullptr);
      }
        return;
      case TABLE_ENTRY: {
        // The one dependent whose edges are not all the same thing: a column's
        // declared type, a column's DEFAULT, a CHECK and a foreign key each do
        // something different, and each names a part of the table rather than
        // the table. The index-name wrapper shares this slot and is nobody's
        // dependent.
        const auto* held = _tables.Find(dependent.id);
        if (held != nullptr) {
          ApplyToTable(*held->first, self);
        }
        return;
      }
      default:
        // A schema, a type, a sequence, a tokenizer, a database, a foreign
        // server: each references nothing but roles, and DROP ROLE is refused
        // over them rather than cascading.
        return;
    }
  }

  duckdb::ClientContext* const _context;
  SequenceOwners _sequences;
  IndexOwners _indexes;
  DatabaseTables _tables;
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> _functions;
  const connector::DependencyView _deps;
  DropPlan _plan;
  containers::FlatHashSet<ObjectId> _auto_drops;
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> _column_drops;
  std::vector<ObjectId> _stack;
  containers::FlatHashSet<ObjectId> _visited;  // push-dedup
};

DropPlan DropCascade::Run() && {
  while (!_stack.empty()) {
    const auto cur = _stack.back();
    _stack.pop_back();
    // Whether the removal of `cur` carries its own subtree: a relation with a
    // store table has a drop task describing everything under it, while every
    // other kind leaves a single record with nothing attached.
    const bool carries_subtree = _tables.Find(cur) != nullptr;
    // The structural children are the AUTO/INTERNAL half of the cascade:
    // everything under the object goes with it, and the seed's own drop task is
    // what removes it, so tagging the closure is all the plan needs.
    if (const auto it = _tables.by_schema.find(cur);
        it != _tables.by_schema.end()) {
      for (const auto child : it->second) {
        EmitAutoDrop(child);
      }
    }
    if (const auto it = _functions.find(cur); it != _functions.end()) {
      for (const auto child : it->second) {
        EmitAutoDrop(child);
      }
    }
    // And the children containment cannot hold. They are AUTO drops like the
    // ones above -- the parent's own removal takes them -- but their dependents
    // are reached from here and nowhere else, and an index under a relation
    // whose entry IS the object has to be named by the plan as well.
    if (const auto it = _sequences.by_owner.find(cur);
        it != _sequences.by_owner.end()) {
      for (const auto sequence_id : it->second) {
        EmitAutoDrop(sequence_id);
      }
    }
    if (const auto it = _indexes.by_relation.find(cur);
        it != _indexes.by_relation.end()) {
      for (const auto& index : it->second) {
        if (!carries_subtree) {
          EmitOwnedIndexDrop(index);
        }
        EmitAutoDrop(index->GetId());
      }
    }
    // Materialized rather than borrowed: the dependents come out of a
    // CatalogSet whose lock is held only for the scan, and Apply runs arbitrary
    // lookups.
    for (const auto& dependent : _deps.Dependents(cur)) {
      Apply(dependent, cur);
    }
  }

  // Auto drops run via the DropTask
  std::erase_if(_plan.entry_drops,
                [&](const EntryDrop& d) { return _auto_drops.contains(d.id); });
  std::erase_if(_plan.index_drops, [&](const IndexInfoRef& index) {
    return _auto_drops.contains(index->GetId());
  });
  absl::erase_if(_plan.table_rewrites, [&](const auto& kv) {
    return _auto_drops.contains(kv.first);
  });

  // The column drops go on last, on top of whatever the walk left, so the
  // shape-preserving definition stays available as its own version. Last is
  // also the safest order for the mutations themselves: a CHECK a column drop
  // takes with it can no longer be dropped twice.
  for (auto& [table_id, columns] : _column_drops) {
    auto it = _plan.table_rewrites.find(table_id);
    if (it == _plan.table_rewrites.end() || !it->second.info) {
      continue;
    }
    auto& slot = it->second;
    auto reshaped = slot.info;
    for (auto col_id : columns) {
      reshaped = catalog::DropColumn(*reshaped, col_id);
    }
    slot.reshaped = std::move(reshaped);
  }
  // The indexes a rewritten table keeps: a column removal is refused by the
  // store while one of them covers a column past the one going away, so the
  // recording pass drops and recreates them around it.
  for (auto& [table_id, rewrite] : _plan.table_rewrites) {
    rewrite.surviving_indexes = SurvivingIndexes(_indexes.Of(table_id), _plan);
  }
  return std::move(_plan);
}

}  // namespace

DropPlan ComputeDropPlan(duckdb::ClientContext* context, ObjectId seed) {
  return DropCascade{context, seed}.Run();
}

DropPlan ComputeDropPlanRestrict(duckdb::ClientContext* context, ObjectId seed,
                                 bool cascade, std::string_view kind,
                                 std::string_view name) {
  auto plan = ComputeDropPlan(context, seed);
  if (!cascade && plan.IsCascade()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop ", kind, " ", name,
              " because other objects depend on it"),
      ERR_DETAIL(plan.FormatDependentsDetail(kind, name)),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }
  return plan;
}

DropPlan ComputeColumnDropPlan(duckdb::ClientContext* context,
                               const TableInfoRef& table,
                               const Permissions& perm, ObjectId col_id) {
  DropPlan plan;
  const auto table_id = catalog::IdOf(*table);
  auto& rw = plan.table_rewrites[table_id];
  rw.schema_id = catalog::ParentIdOf(*table);
  rw.id = table_id;
  rw.before = table;
  rw.perm = perm;
  rw.info = table;
  // Nothing but the column changes, so there is no shape-preserving half to
  // make durable ahead of the data.
  rw.reshaped = catalog::DropColumn(*table, col_id);
  auto indexes =
    connector::RelationIndexes(context, catalog::ParentIdOf(*table), table_id);
  for (auto& index : indexes) {
    if (index->ReferencesColumn(col_id)) {
      plan.index_drops.push_back(index);
    }
  }
  rw.surviving_indexes = SurvivingIndexes(indexes, plan);
  return plan;
}

bool CheckSchemaEmptyDependency(duckdb::ClientContext* context,
                                ObjectId schema_id) {
  bool empty = true;
  const auto database_id = connector::SchemaDatabaseId(context, schema_id);
  if (!database_id.isSet()) {
    return empty;
  }
  connector::VisitTables(context, database_id,
                         [&](const TableInfoRef& table, const Permissions&) {
                           empty &= catalog::ParentIdOf(*table) != schema_id;
                         });
  connector::VisitFunctions(
    context, database_id, [&](const duckdb::MacroCatalogEntry& function) {
      empty &= ObjectId{function.ParentSchema().oid} != schema_id;
    });
  return empty;
}

void CommitDropPlan(duckdb::ClientContext* context,
                    CatalogStore::WriteContext& ctx, DropPlan& plan) {
  // Store-side index drops precede the column ALTERs below: a covering index
  // must be gone before its column can be dropped, and enforcement (UNIQUE)
  // must stop the moment the drop commits. The async task only sweeps index
  // storage and definitions.
  const auto drop_index = [&](const IndexInfoRef& index) {
    const auto db_id =
      connector::SchemaDatabaseId(context, index->GetParentId());
    ctx.catalog().DropPrepare({.parent_id = index->GetRelationId(),
                               .type = duckdb::CatalogType::INDEX_ENTRY,
                               .inverted = index->IsInverted(),
                               .id = index->GetId(),
                               .database_id = db_id,
                               .schema_id = index->GetParentId()});
    ctx.store().DropIndex(db_id, index->GetRelationId(), index->GetName());
  };
  for (const auto& index : plan.index_drops) {
    drop_index(index);
  }
  for (const auto& index : plan.owned_index_drops) {
    drop_index(index);
  }
  for (auto& [tid, rw] : plan.table_rewrites) {
    if (!rw.info) {
      continue;
    }
    // One record: the cascade's removals and the new shape of the table that
    // survives them are one decision, and the log position is what tells boot
    // whether the rows caught up with it.
    const auto final_table =
      NextTableVersion(context, tid, rw.schema_id, rw.Final());
    // A dropped column's grants go with it. Ids are never reissued, so a
    // leftover entry would name a column no reader can resolve while still
    // holding its grantee's role dependency open.
    std::erase_if(rw.perm.column_acl, [&](const auto& granted) {
      return catalog::ColumnById(*final_table, ObjectId{granted.catalog_oid}) ==
             nullptr;
    });
    ctx.catalog().PutTable(*final_table, wal::PutMode::Replace, rw.perm);
    rw.published = final_table;
    // Cascades can drop columns of surviving tables (e.g. a column whose
    // dependency lived in the dropped schema) -- the store table follows.
    if (catalog::TableEngineOf(*final_table) != TableEngine::Transactional ||
        !rw.before || !rw.reshaped) {
      continue;
    }
    const auto& old_info = *rw.before;
    const auto db_id = connector::SchemaDatabaseId(context, rw.schema_id);
    if (final_table->columns.LogicalColumnCount() == 0) {
      // Dropping the last column is refused by ALTER; rebuild the rows instead
      // (PG keeps the zero-column table).
      ctx.store().DropTable(db_id, tid);
      ctx.store().CreateTable(db_id, catalog::IdOf(*final_table));
      continue;
    }
    std::vector<std::pair<std::string, ObjectId>> dropped_columns;
    for (const auto& column : old_info.columns.Logical()) {
      const ObjectId column_id{column.CatalogOid()};
      if (catalog::ColumnById(*rw.reshaped, column_id) == nullptr) {
        dropped_columns.emplace_back(column.Name().GetIdentifierName(),
                                     column_id);
      }
    }
    if (dropped_columns.empty()) {
      continue;
    }
    // A CHECK the column drop takes with it goes from the store table first:
    // duckdb's own DROP COLUMN strips a CHECK that names only that column but
    // refuses one that names another as well, and postgres drops both.
    for (const auto& constraint : old_info.constraints) {
      if (constraint->type != duckdb::ConstraintType::CHECK) {
        continue;
      }
      const auto survives = absl::c_any_of(
        rw.reshaped->constraints,
        [&](const auto& kept) { return kept->oid == constraint->oid; });
      if (survives) {
        continue;
      }
      ctx.store().Alter(
        db_id, tid,
        duckdb::make_uniq<duckdb::DropConstraintInfo>(
          StoreTarget(duckdb::OnEntryNotFound::RETURN_NULL),
          constraint->Cast<duckdb::CheckConstraint>().expression->ToString(),
          /*if_constraint_not_found=*/true, /*cascade=*/false));
    }
    // Surviving store indexes block the ALTER whenever they cover a column
    // positioned after the dropped one; recreate them around the drop (data
    // lives in the rows / iresearch, so rebuilds are cheap and inverted
    // instances carry no state of their own).
    for (const auto& idx : rw.surviving_indexes) {
      ctx.store().DropIndex(db_id, idx->GetRelationId(), idx->GetName());
    }
    for (auto& [name, column_id] : dropped_columns) {
      ctx.store().Alter(db_id, tid,
                        duckdb::make_uniq<duckdb::RemoveColumnInfo>(
                          StoreTarget(), std::move(name),
                          /*if_column_exists=*/false, /*cascade=*/false));
    }
    for (const auto& idx : rw.surviving_indexes) {
      if (auto info = MakeStoreIndexInfo(*final_table, *idx)) {
        ctx.store().CreateIndex(db_id, std::move(info), final_table, idx);
      }
    }
  }
  for (const auto& drop : plan.entry_drops) {
    ctx.catalog().DropObject(drop.parent_id, drop.type, drop.id);
  }
}

void PublishDropPlan(duckdb::ClientContext* context, const DropPlan& plan) {
  // CommitDropPlan put every rewrite's record in the frame beside the removals.
  Catalog::RecordedScope recorded;
  for (const auto& [tid, rw] : plan.table_rewrites) {
    if (!rw.published) {
      continue;
    }
    connector::PutEntry(context, catalog::TableNameOf(*rw.published),
                        rw.published, rw.perm);
    // The keys the cascade stripped: the referenced half of one is derived from
    // the edges this table states, so every table the version before it pointed
    // at and that outlived the cascade has to be rebuilt.
    if (rw.before) {
      connector::RefreshForeignKeyTargets(context, *rw.before);
    }
  }
}

std::string DropPlan::FormatDependentsDetail(std::string_view seed_kind,
                                             std::string_view seed_name) const {
  std::vector<std::string> lines;
  const auto add = [&](std::string_view kind, std::string_view name) {
    lines.push_back(
      absl::StrCat(kind, " ", name, " depends on ", seed_kind, " ", seed_name));
  };
  for (const auto& [tid, rewrite] : table_rewrites) {
    if (rewrite.info) {
      add(pg::ToPgObjectTypeName(duckdb::CatalogType::TABLE_ENTRY),
          catalog::TableNameOf(*rewrite.info));
    }
  }
  for (const auto& drop : entry_drops) {
    add(pg::ToPgObjectTypeName(drop.type), drop.name);
  }
  for (const auto& index : index_drops) {
    add(pg::ToPgObjectTypeName(duckdb::CatalogType::INDEX_ENTRY),
        index->GetName());
  }
  return absl::StrJoin(lines, "\n");
}

void ThrowConcurrentlyDropped(duckdb::CatalogType type, std::string_view name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent delete of ",
            pg::ToPgObjectTypeName(type), " \"", name, "\""));
}

void ThrowConcurrentlyDropped(ObjectId /*id*/) {
  // No name to give: what is gone is an object this one hangs off, and the
  // catalog no longer holds it. The id is deliberately out of the message --
  // it is an internal oid, and it would make the error unassertable.
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
    ERR_MSG("could not serialize access due to concurrent delete of a "
            "referenced object"));
}

Catalog::Catalog() : _engine{&GetCatalogStore()} {}

namespace {

std::atomic<uint64_t> gCatalogVersion{1};

thread_local uint32_t gRecordedDepth = 0;

}  // namespace

Catalog::RecordedScope::RecordedScope() noexcept { ++gRecordedDepth; }

Catalog::RecordedScope::~RecordedScope() { --gRecordedDepth; }

bool Catalog::RecordedScope::Open() noexcept { return gRecordedDepth != 0; }

uint64_t CatalogVersion() noexcept {
  return gCatalogVersion.load(std::memory_order_relaxed);
}

void ThrowDuplicateName(NameKind kind, std::string_view name) {
  switch (kind) {
    case NameKind::Relation:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                      ERR_MSG("relation \"", name, "\" already exists"));
    case NameKind::Type:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("type \"", name, "\" already exists"));
    case NameKind::Role:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("role \"", name, "\" already exists"));
  }
  SDB_UNREACHABLE();
}

void Catalog::Apply(duckdb::ClientContext* context,
                    absl::FunctionRef<void(CatalogStore::WriteContext&)> fill) {
  // The records go to the log and the entries into the CatalogSets; nothing
  // else holds a copy of the catalog, so a batch is performed exactly where it
  // is written.
  _engine->Write(context, fill, [&](std::span<const wal::Entry> entries) {
    gCatalogVersion.fetch_add(1, std::memory_order_relaxed);
    if (context != nullptr) {
      RecordCatalogDelta(*context);
    }
  });
}

void Catalog::RecordEntry(duckdb::ClientContext* context, ObjectId parent_id,
                          duckdb::CatalogType type, ObjectId id,
                          wal::PutMode mode,
                          std::shared_ptr<const duckdb::CreateInfo> info,
                          Permissions perm) {
  Apply(context, [&](auto& ctx) {
    ctx.catalog().PutEntry(parent_id, type, id, mode, std::move(info),
                           std::move(perm));
  });
}

void Catalog::ReplaceFunction(
  duckdb::ClientContext& context, ObjectId database_id, std::string_view schema,
  std::string_view name, std::shared_ptr<const duckdb::CreateMacroInfo> info) {
  absl::MutexLock lock{&_mutex};
  const auto schema_id = FindSchemaId(&context, database_id, schema);
  if (!schema_id) {
    return;
  }
  const auto* existing = connector::FindFunction(&context, *schema_id, name);
  if (existing == nullptr) {
    return;
  }
  const auto& perm = existing->permissions;
  // PG: only the owner may drop an overload, and what is left is a rewrite of
  // the function the owner holds -- so the ACL and the owner carry over.
  const auto fn_name = existing->name.GetIdentifierName();
  RequireOwner(&context, ActingAs(context).role, perm, "function", fn_name);
  // The overloads that stay are the new info's; the identity is the one the
  // owner already holds.
  auto next =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      info->Copy());
  SetIdentity(*next, ObjectId{existing->oid},
              ObjectId{existing->ParentSchema().oid});
  connector::PutEntry(
    &context, fn_name,
    NextFunctionVersion(
      &context, std::shared_ptr<const duckdb::CreateMacroInfo>{next.release()}),
    perm);
}

void Catalog::RecordSequence(
  duckdb::ClientContext* context,
  std::shared_ptr<const duckdb::CreateSequenceInfo> sequence, Permissions perm,
  uint64_t seed) {
  const auto id = catalog::IdOf(*sequence);
  const auto schema_id = catalog::ParentIdOf(*sequence);
  Apply(context, [&](auto& ctx) {
    ctx.catalog().PutEntry(schema_id, duckdb::CatalogType::SEQUENCE_ENTRY, id,
                           wal::PutMode::Create, std::move(sequence),
                           std::move(perm));
    ctx.catalog().SetSequence(id, seed);
  });
}

void Catalog::RecordTable(duckdb::ClientContext* context,
                          const duckdb::CreateTableInfo& table,
                          wal::PutMode mode, Permissions perm) {
  Apply(context, [&](auto& ctx) {
    ctx.catalog().PutTable(table, mode, std::move(perm));
  });
}

uint64_t Catalog::CommitTransaction(
  duckdb::ClientContext* context,
  std::span<const std::vector<wal::Entry>> frames) {
  absl::MutexLock lock{&_mutex};
  return _engine->CommitFrames(context, frames, [] {});
}

// The lock is conditional and released by a guard, neither of which clang's
// analysis can follow.
ABSL_NO_THREAD_SAFETY_ANALYSIS bool Catalog::TryExcludingMutations(
  absl::FunctionRef<void()> fn) {
  // TryLock also fails when this very thread holds the mutex, which is the
  // common case: a mutation attempts a fold on its way out.
  if (!_mutex.TryLock()) {
    return false;
  }
  const absl::Cleanup unlock = [this]() ABSL_NO_THREAD_SAFETY_ANALYSIS {
    _mutex.Unlock();
  };
  fn();
  return true;
}

std::shared_ptr<IndexDrop> Catalog::CreateIndexDrop(
  ObjectId db_id, ObjectId schema_id, ObjectId table_id,
  const CreateIndexInfoBase& index, bool is_root) {
  // Capture the iresearch storage handle (weak) so the async IndexDrop can wait
  // for every holder (entry versions, txns, tasks) to release before removing
  // the on-disk directory. Secondary indexes have no storage -> empty weak.
  std::weak_ptr<search::InvertedIndexStorage> data;
  if (index.IsInverted()) {
    data = index.GetData();
  }
  auto task = std::make_shared<IndexDrop>(index, db_id, schema_id, table_id,
                                          std::move(data), is_root);
  UpdatePendingDrops(_pending_drops, table_id, index.GetId(), task, is_root);
  return task;
}

std::shared_ptr<TableDropBase> Catalog::CreateTableDrop(
  ObjectId db_id, ObjectId schema_id, const TableInfoRef& table,
  const SequenceOwners& sequences, const IndexOwners& indexes, bool is_root) {
  const auto table_id = catalog::IdOf(*table);
  auto owned_sequences = sequences.Of(table_id);

  std::shared_ptr<TableDropBase> task;
  if (catalog::TableEngineOf(*table) == TableEngine::Search) {
    const auto* entry = connector::FindTableEntryIn(nullptr, db_id, table_id);
    task = std::make_shared<SearchTableDrop>(
      table, entry != nullptr ? entry->GetSearchData() : nullptr, db_id,
      std::move(owned_sequences), schema_id, is_root);
  } else {
    auto index_drops =
      indexes.Of(table_id) |
      std::views::transform([&](const IndexInfoRef& index) {
        return CreateIndexDrop(db_id, schema_id, table_id, *index, false);
      }) |
      std::ranges::to<std::vector>();
    task = std::make_shared<TableDrop>(table, db_id, std::move(index_drops),
                                       std::move(owned_sequences), schema_id,
                                       is_root);
  }
  UpdatePendingDrops(_pending_drops, schema_id, table_id, task, is_root);
  return task;
}

std::shared_ptr<SchemaDrop> Catalog::CreateSchemaDrop(
  duckdb::ClientContext* context, ObjectId db_id, ObjectId schema_id,
  const SequenceOwners& sequences, const IndexOwners& indexes, bool is_root) {
  // Collected before anything is built: a table drop reads the index set of the
  // same schema, and the walk that found these is holding one of its sets.
  std::vector<TableInfoRef> tables;
  connector::VisitTables(context, db_id,
                         [&](const TableInfoRef& table, const Permissions&) {
                           if (catalog::ParentIdOf(*table) == schema_id) {
                             tables.push_back(table);
                           }
                         });
  std::vector<std::shared_ptr<TableDropBase>> tables_drop;
  tables_drop.reserve(tables.size());
  for (const auto& table : tables) {
    tables_drop.push_back(
      CreateTableDrop(db_id, schema_id, table, sequences, indexes, false));
  }

  auto task = std::make_shared<SchemaDrop>(
    schema_id, std::move(tables_drop), sequences.Of(schema_id), db_id, is_root);
  UpdatePendingDrops(_pending_drops, db_id, schema_id, task, is_root);
  return task;
}

std::shared_ptr<DatabaseDrop> Catalog::CreateDatabaseDrop(
  duckdb::ClientContext* context, ObjectId db_id,
  const SequenceOwners& sequences, const IndexOwners& indexes,
  duckdb::shared_ptr<void> keep_alive) {
  std::vector<ObjectId> schema_ids;
  connector::VisitSchemas(
    context, db_id,
    [&](const duckdb::CreateSchemaInfo& schema, const Permissions&) {
      schema_ids.push_back(IdOf(schema));
    });
  std::vector<std::shared_ptr<SchemaDrop>> schemas_drop;
  schemas_drop.reserve(schema_ids.size());
  for (const auto schema_id : schema_ids) {
    schemas_drop.push_back(
      CreateSchemaDrop(context, db_id, schema_id, sequences, indexes, false));
  }
  auto task = std::make_shared<DatabaseDrop>(db_id, std::move(schemas_drop),
                                             std::move(keep_alive));
  UpdatePendingDrops(_pending_drops, ObjectId{}, db_id, task, false);
  return task;
}

void Catalog::ScheduleDrop(duckdb::ClientContext* context,
                           std::shared_ptr<DropTask> task) {
  if (context != nullptr && QueueDropTask(*context, task)) {
    return;
  }
  DropTask::Schedule(std::move(task)).Detach();
}

void Catalog::ScheduleDropPlanIndexes(duckdb::ClientContext* context,
                                      ObjectId db_id, const DropPlan& plan) {
  // The entry-backed victims of the cascade. Their records ride the same batch
  // as everything else's; their entries are written outside it, as every
  // entry-backed mutator writes its own.
  for (const auto& drop : plan.entry_drops) {
    connector::DropEntryOfKind(context, drop.type, drop.parent_id, drop.name);
  }
  // The relation is not looked up: an index over a view has no table behind it,
  // and everything the task needs -- the schema it is named in and the relation
  // it covers -- the index itself carries. The entry goes here too: an index is
  // named in the schema's sets, which the cascade's records do not touch.
  const auto schedule = [&](const IndexInfoRef& index) {
    auto task = CreateIndexDrop(db_id, index->GetParentId(),
                                index->GetRelationId(), *index,
                                /*is_root=*/true);
    connector::DropIndexEntry(context, index->GetParentId(), index->GetName());
    ScheduleDrop(context, std::move(task));
  };
  for (const auto& index : plan.index_drops) {
    schedule(index);
  }
  for (const auto& index : plan.owned_index_drops) {
    schedule(index);
  }
}

HeldSchema Catalog::CreateDatabase(const AccessContext& ax,
                                   std::shared_ptr<CreateDatabaseInfo> database,
                                   ObjectId owner, bool if_not_exists) {
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  absl::MutexLock lock{&_mutex};
  RequireRoleAttribute(ax.context, ax.role, RoleOption::CreateDb,
                       "create database");
  if (connector::FindDatabase(ax.context, database->GetName())) {
    if (if_not_exists) {
      return {};
    }
    ThrowDuplicateDatabase(database->GetName());
  }
  if (!database->GetId().isSet()) {
    database->SetId(NextId());
  }
  const auto database_id = database->GetId();
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  Permissions perm{owner};
  auto schema =
    catalog::MakeSchemaInfo(NextId(), database_id, StaticStrings::kPublic);
  const auto schema_id = IdOf(*schema);
  // One frame, not two: a database and its public schema are one operation,
  // and a crash between two appends would leave a database that has no
  // schema to create anything in. Deferred to the transaction's commit like
  // any other create: the data file already exists at this point, so an
  // inline append would make a rolled-back CREATE DATABASE durable and the
  // file it names would never be reclaimed.
  RecordedScope recorded;
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().PutEntry(id::kInstance, duckdb::CatalogType::DATABASE_ENTRY,
                           database_id, wal::PutMode::Create, database, perm);
    ctx.catalog().PutEntry(database_id, duckdb::CatalogType::SCHEMA_ENTRY,
                           schema_id, wal::PutMode::Create, schema, perm);
  });
  connector::PutDatabase(ax.context, {}, std::move(database), perm);
  // The schema entry is the attach's to make: its set belongs to a catalog
  // that comes into being only once this call returns.
  return HeldSchema{std::move(schema), perm};
}

namespace {

// Defined below (with the other Create*/Drop* ownership helpers).
void RequireDatabaseAccess(duckdb::ClientContext* context, ObjectId role,
                           const connector::DatabaseRef& database,
                           AclMode need);
void RequireDatabaseOwner(duckdb::ClientContext* context, ObjectId role,
                          const connector::DatabaseRef& database);

}  // namespace

bool Catalog::CreateSchema(const AccessContext& ax, ObjectId database_id,
                           std::shared_ptr<duckdb::CreateSchemaInfo> schema,
                           Permissions perm, bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  // CREATE SCHEMA requires CREATE on the target database.
  RequireDatabaseAccess(ax.context, ax.role,
                        connector::FindDatabase(ax.context, database_id),
                        AclMode::Create);
  if (FindSchemaId(ax.context, database_id, SchemaNameOf(*schema))) {
    if (if_not_exists) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_SCHEMA),
      ERR_MSG("schema \"", SchemaNameOf(*schema), "\" already exists"));
  }
  SetIdentity(*schema, IdOf(*schema).isSet() ? IdOf(*schema) : NextId(),
              database_id);
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  connector::PutSchema(ax.context, {}, std::move(schema), std::move(perm));
  return true;
}

void Catalog::CreateRole(const AccessContext& ax,
                         std::shared_ptr<CreateRoleInfo> role) {
  SDB_DEBUG(GENERAL, "Creating role: ", role->GetName());
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  absl::MutexLock lock{&_mutex};
  RequireRoleAttribute(
    ax.context, ax.role, RoleOption::CreateRole, "create role",
    "Only roles with the CREATEROLE attribute may create roles.");
  RequireAttributesGrantable(ax.context, ax.role, role->Options(),
                             /*creating=*/true);
  if (connector::FindRole(ax.context, role->GetName())) {
    ThrowDuplicateName(NameKind::Role, role->GetName());
  }
  if (!role->GetId().isSet()) {
    role->SetId(NextId());
  }
  std::shared_ptr<CreateRoleInfo> updated;
  if (auto creator = connector::FindRole(ax.context, ax.role);
      creator && !creator->IsSuperuser()) {
    updated = creator->CloneRole();
    updated->AddMembership(Membership{
      .role = role->GetId(),
      .admin_option = true,
      .inherit_option = false,
      .set_option = false,
    });
  }
  // One frame: the new role and the creator's membership in it are the same
  // operation, so a crash must not be able to land only one of them.
  RecordedScope recorded;
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().PutEntry(id::kInstance, duckdb::CatalogType::ROLE_ENTRY,
                           role->GetId(), wal::PutMode::Create, role);
    if (updated) {
      ctx.catalog().PutEntry(id::kInstance, duckdb::CatalogType::ROLE_ENTRY,
                             updated->GetId(), wal::PutMode::Replace, updated);
    }
  });
  connector::PutRole(ax.context, {}, std::move(role));
  if (updated) {
    const auto name = updated->GetName();
    connector::PutRole(ax.context, name, std::move(updated));
  }
}

void Catalog::CreateIndexImpl(duckdb::ClientContext* context,
                              const IndexInfoRef& index,
                              CreateIndexOperationOptions operation_options) {
  const auto schema_id = index->GetParentId();
  // An index name is in the relation namespace, so every half of it answers.
  if (connector::FindTable(context, schema_id, index->GetName()) ||
      connector::FindSequence(context, schema_id, index->GetName()) ||
      connector::FindView(context, schema_id, index->GetName()) ||
      connector::FindIndex(context, schema_id, index->GetName())) {
    ThrowDuplicateName(NameKind::Relation, index->GetName());
  }
  // A key constraint's index is filed under the constraint's own name and goes
  // on the same list as this one, so the two share a namespace -- which is what
  // postgres says too, where a constraint's index is a relation.
  if (const auto* relation =
        connector::FindTable(context, schema_id, index->GetRelationId())) {
    for (const auto& constraint : relation->GetConstraints()) {
      if (constraint->type == duckdb::ConstraintType::UNIQUE &&
          constraint->constraint_name == index->GetName()) {
        ThrowDuplicateName(NameKind::Relation, index->GetName());
      }
    }
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  const auto db_id = connector::SchemaDatabaseId(context, schema_id);
  SDB_ASSERT(db_id.isSet());
  // The inverted index's iresearch storage hangs off the definition, so the
  // CREATE INDEX build (GetGlobalSinkState) reaches it via GetData(). Bind it
  // here, before the build runs.
  if (index->IsInverted()) {
    index->SetData(search::InvertedIndexStorage::Create(
      db_id, InvertedInfo(*index), /*is_new=*/true));
  }
  const auto stamped = NextIndexVersion(context, index);
  const auto* entry =
    connector::FindTable(context, schema_id, index->GetRelationId());
  const auto table = entry != nullptr ? entry->Definition() : nullptr;
  auto store_index = table ? MakeStoreIndexInfo(*table, *stamped) : nullptr;
  RecordedScope recorded;
  Apply(context, [&](auto& ctx) {
    PutIndex(ctx, stamped, wal::PutMode::Create);
    if (store_index) {
      ctx.store().CreateIndex(db_id, std::move(store_index), table, stamped);
    }
  });
  // After the store op, so a relation another transaction has already dropped
  // is refused by the rows rather than by the set -- the store names the
  // relation, and the set can only say that something clashed.
  connector::PutEntry(context, /*old_name=*/{}, stamped);
}

void Catalog::RenameIndex(duckdb::ClientContext* context,
                          const CreateIndexInfoBase& index,
                          std::string_view new_name) {
  const auto schema_id = index.GetParentId();
  const auto renamed = NextIndexVersion(context, RenamedIndex(index, new_name));
  const auto db_id = connector::SchemaDatabaseId(context, schema_id);
  const auto* entry =
    connector::FindTable(context, schema_id, index.GetRelationId());
  const auto table = entry != nullptr ? entry->Definition() : nullptr;
  RecordedScope recorded;
  Apply(context, [&](auto& ctx) {
    PutIndex(ctx, renamed, wal::PutMode::Replace);
    if (table && MakeStoreIndexInfo(*table, index)) {
      ctx.store().RenameIndex(db_id, index.GetRelationId(), index.GetName(),
                              new_name);
    }
  });
  connector::PutEntry(context, index.GetName(), renamed);
}

namespace {

// One column of the relation an index is being built on, as the key binder
// needs it: a table column carries its catalog id, a view column its position.
struct IndexableColumn {
  ObjectId id;
  std::string name;
  duckdb::LogicalType type;
};

struct ResolvedIndexRelation {
  ObjectId relation_id;
  std::vector<IndexableColumn> columns;
};

ResolvedIndexRelation ResolveIndexRelation(const IndexRelation& relation) {
  if (relation.table) {
    std::vector<IndexableColumn> columns;
    for (const auto& column : relation.table->columns.Logical()) {
      columns.push_back({ObjectId{column.CatalogOid()},
                         std::string{column.Name().GetIdentifierName()},
                         column.Type()});
    }
    return ResolvedIndexRelation{.relation_id = catalog::IdOf(*relation.table),
                                 .columns = std::move(columns)};
  }
  if (relation.view != nullptr) {
    const auto& view = *relation.view;
    std::vector<IndexableColumn> columns;
    if (const auto view_columns = view.GetColumnInfo()) {
      columns.reserve(view_columns->names.size());
      for (size_t i = 0; i != view_columns->names.size(); ++i) {
        columns.push_back(
          {ColumnId{i}, std::string{view_columns->names[i].GetIdentifierName()},
           view_columns->types[i]});
      }
    }
    return ResolvedIndexRelation{.relation_id = ObjectId{view.oid},
                                 .columns = std::move(columns)};
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("Only table or view indexes are supported"));
}

// CREATE INDEX requires ownership of the target relation; an index has no
// independent owner of its own.
void RequireRelationOwner(duckdb::ClientContext* context, ObjectId role,
                          const IndexRelation& relation) {
  if (auth::ClosureFor(context, role)->Owns(OwnerOf(relation.perm))) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("must be owner of ",
                          pg::ToPgObjectTypeName(
                            relation.table ? duckdb::CatalogType::TABLE_ENTRY
                                           : duckdb::CatalogType::VIEW_ENTRY),
                          " ", relation.GetName()));
}

// Points each key column at its catalog column in `relation_columns`, which
// must outlive the CreateIndexColumn vector. Expression keys carry their own
// bound payload (dependent columns + serialized expr) and have no base column
// to resolve by name; the store-side ART builds them from the rendered SQL.
void BindIndexColumns(const std::vector<IndexableColumn>& relation_columns,
                      std::vector<CreateIndexColumn>& columns) {
  for (auto& c : columns) {
    if (c.IsIndexedExpression()) {
      continue;
    }
    auto it = absl::c_find_if(
      relation_columns, [&](const auto& col) { return col.name == c.name; });
    if (it == relation_columns.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("column \"", c.name, "\" does not exist"));
    }
    c.column = IndexedColumnRef{it->id, it->type};
  }
}

void RequireDatabaseAccess(duckdb::ClientContext* context, ObjectId role,
                           const connector::DatabaseRef& database,
                           AclMode need) {
  if (!database ||
      auth::ClosureFor(context, role)
        ->Can(duckdb::CatalogType::DATABASE_ENTRY, database.perm, need)) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for database ", database.Name()));
}

void RequireDatabaseOwner(duckdb::ClientContext* context, ObjectId role,
                          const connector::DatabaseRef& database) {
  if (!database ||
      auth::ClosureFor(context, role)->Owns(catalog::OwnerOf(database.perm))) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("must be owner of database ", database.Name()));
}

}  // namespace

// CREATE inside `parent_id` requires CREATE on it (a schema for relations, a
// database for schemas). Throws 42501 "permission denied for <type> <name>"
// (type/name from the parent) on a non-creator; a missing parent is a no-op
// (the mutation's own resolution reports the real error).
void RequireCreateOn(duckdb::ClientContext* context, ObjectId role,
                     ObjectId parent_id) {
  Permissions schema_perm;
  auto schema = connector::FindSchema(context, parent_id, &schema_perm);
  if (!schema || auth::ClosureFor(context, role)
                   ->Can(duckdb::CatalogType::SCHEMA_ENTRY, schema_perm,
                         AclMode::Create)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied for schema ", SchemaNameOf(*schema)));
}

// PG's ownership test for an ALTER or a DROP: the actor must own the object,
// directly, through a role it is a member of, or as a superuser.
void RequireOwner(duckdb::ClientContext* context, ObjectId role,
                  const Permissions& perm, std::string_view noun,
                  std::string_view name) {
  if (auth::ClosureFor(context, role)->Owns(catalog::OwnerOf(perm))) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("must be owner of ", noun, " ", name));
}

void RequireOwnerTransfer(const AccessContext& ax, ObjectId schema_id,
                          const Permissions& perm, ObjectId new_owner,
                          std::string_view new_owner_name,
                          std::string_view noun, std::string_view name) {
  if (auth::ClosureFor(ax.context, ax.role)->is_superuser) {
    return;
  }
  RequireOwner(ax.context, ax.role, perm, noun, name);
  if (!auth::ComputeSetRoleClosure(*auth::RolesOf(ax.context), ax.role)
         .contains(new_owner)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("must be able to SET ROLE \"", new_owner_name, "\""));
  }
  // A schema has no schema above it, so an unset parent skips the last check
  // rather than resolving nothing.
  if (!schema_id.isSet() || new_owner == OwnerOf(perm)) {
    return;
  }
  Permissions schema_perm;
  auto schema = connector::FindSchema(ax.context, schema_id, &schema_perm);
  if (schema && !auth::ClosureFor(ax.context, new_owner)
                   ->Can(duckdb::CatalogType::SCHEMA_ENTRY, schema_perm,
                         AclMode::Create)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied for schema ", SchemaNameOf(*schema)));
  }
}

namespace {

void RequireRoleMembership(duckdb::ClientContext* context, ObjectId actor_id,
                           const CreateRoleInfo& target) {
  if (auth::ClosureFor(context, actor_id)->MemberOf(target.GetId())) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied"),
                  ERR_DETAIL("Must be a member of role \"", target.GetName(),
                             "\" to alter its default privileges."));
}

void RequireRoleAdmin(duckdb::ClientContext* context, ObjectId actor_id,
                      const CreateRoleInfo& target, std::string_view verb) {
  auto actor = connector::FindRole(context, actor_id);
  if (actor && actor->IsSuperuser()) {
    return;
  }
  if (target.IsSuperuser()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", verb, " role"),
                    ERR_DETAIL("Only roles with the SUPERUSER attribute may ",
                               verb, " roles with the SUPERUSER attribute."));
  }
  if (!actor || !actor->Has(RoleOption::CreateRole) ||
      !auth::HasAdminOption(*auth::RolesOf(context), actor_id,
                            target.GetId())) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role"),
      ERR_DETAIL("Only roles with the CREATEROLE attribute and the ADMIN "
                 "option on role \"",
                 target.GetName(), "\" may ", verb, " this role."));
  }
}

void RequireRoleAttribute(duckdb::ClientContext* context, ObjectId actor_id,
                          RoleOption attribute, std::string_view denied_action,
                          std::string_view detail) {
  if (actor_id == id::kRootUser) {
    return;
  }
  auto actor = connector::FindRole(context, actor_id);
  if (!actor || actor->IsSuperuser() || actor->Has(attribute)) {
    return;
  }
  if (detail.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied to ", denied_action));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied to ", denied_action),
                  ERR_DETAIL(detail));
}

// A role may only confer a privileged attribute it holds itself (superuser for
// SUPERUSER; the matching bit for CREATEDB/REPLICATION/BYPASSRLS). `granting`
// is the set of attributes being conferred; CREATEROLE/LOGIN/INHERIT are not
// gated (a CREATEROLE actor may set them, matching PostgreSQL).
void RequireAttributesGrantable(duckdb::ClientContext* context,
                                ObjectId actor_id, RoleOption granting,
                                bool creating) {
  if (actor_id == id::kRootUser) {
    return;
  }
  auto actor = connector::FindRole(context, actor_id);
  const bool actor_super = actor && actor->IsSuperuser();

  const auto require = [&](RoleOption attr, bool actor_has,
                           std::string_view attr_name) {
    if ((granting & attr) == RoleOption::None || actor_has) {
      return;
    }
    const auto detail =
      creating
        ? absl::StrCat("Only roles with the ", attr_name,
                       " attribute may create roles with the ", attr_name,
                       " attribute.")
        : absl::StrCat("Only roles with the ", attr_name,
                       " attribute may change the ", attr_name, " attribute.");
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", creating ? "create" : "alter", " role"),
      ERR_DETAIL(detail));
  };

  require(RoleOption::Superuser, actor_super, "SUPERUSER");
  require(RoleOption::CreateDb,
          actor_super || (actor && actor->Has(RoleOption::CreateDb)),
          "CREATEDB");
  require(RoleOption::Replication,
          actor_super || (actor && actor->Has(RoleOption::Replication)),
          "REPLICATION");
  require(RoleOption::BypassRls,
          actor_super || (actor && actor->Has(RoleOption::BypassRls)),
          "BYPASSRLS");
}

}  // namespace

IndexInfoRef Catalog::CreateSecondaryIndex(
  const AccessContext& ax, const IndexRelation& relation, std::string name,
  std::vector<CreateIndexColumn>&& columns, bool unique,
  CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Cannot create index without columns"));
  }
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};
  const auto& live = relation;
  const auto schema_id = live.GetParentId();
  RequireRelationOwner(ax.context, ax.role, live);
  if (operation_options.if_not_exists &&
      (connector::FindTable(ax.context, schema_id, name) ||
       connector::FindSequence(ax.context, schema_id, name) ||
       connector::FindIndex(ax.context, schema_id, name))) {
    return nullptr;
  }
  auto resolved = ResolveIndexRelation(live);
  BindIndexColumns(resolved.columns, columns);
  auto index = NewSecondaryIndex(schema_id, ObjectId{0}, resolved.relation_id,
                                 std::move(name), std::move(columns), unique);
  CreateIndexImpl(ax.context, index, operation_options);
  return index;
}

IndexInfoRef Catalog::CreateInvertedIndex(
  const AccessContext& ax, duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema, const IndexRelation& relation, std::string name,
  std::vector<CreateIndexColumn>&& columns, InvertedIndexOptions options,
  ExpressionData predicate, CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Cannot create index without columns"));
  }
  JoinStoreTransaction(&context);
  absl::MutexLock lock{&_mutex};
  const auto& live = relation;
  const auto schema_id = live.GetParentId();
  RequireRelationOwner(ax.context, ax.role, live);
  if (operation_options.if_not_exists &&
      (connector::FindTable(ax.context, schema_id, name) ||
       connector::FindSequence(ax.context, schema_id, name) ||
       connector::FindIndex(ax.context, schema_id, name))) {
    return nullptr;
  }
  auto resolved = ResolveIndexRelation(live);
  BindIndexColumns(resolved.columns, columns);
  auto index =
    NewInvertedIndex(context, database_id, schema, schema_id, ObjectId{0},
                     resolved.relation_id, std::move(name), std::move(columns),
                     std::move(options), std::move(predicate));
  CreateIndexImpl(&context, index, operation_options);
  return index;
}

TableInfoRef Catalog::CreateTable(
  const AccessContext& ax, ObjectId database_id, std::string_view schema,
  std::shared_ptr<duckdb::CreateTableInfo> info,
  std::vector<SerialSequence> sequence_specs,
  CreateTableOperationOptions operation_options) {
  const auto name = std::string{catalog::TableNameOf(*info)};
  // Uniqueness keys are enforced by the store table's DuckDB ART, which cannot
  // index nested types. Reject a nested-type key column up front with a clear
  // error instead of silently creating the table with the constraint dropped.
  for (const auto& constraint : info->constraints) {
    if (constraint->type != duckdb::ConstraintType::UNIQUE) {
      continue;
    }
    const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
    const std::string_view what =
      unique.IsPrimaryKey() ? "primary key" : "unique constraint";
    for (const auto& key : unique.GetColumnNames()) {
      const auto* column =
        catalog::ColumnByName(*info, key.GetIdentifierName());
      if (column != nullptr && column->Type().IsNested()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG(what, " column \"", column->Name().GetIdentifierName(),
                  "\" has unsupported nested type ",
                  column->Type().ToString()));
      }
    }
  }

  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};
  auto schema_id = FindSchemaId(ax.context, database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(ax.context, ax.role, *schema_id);
  if (operation_options.if_not_exists &&
      (connector::FindTable(ax.context, *schema_id, name) ||
       connector::FindView(ax.context, *schema_id, name) ||
       connector::FindIndex(ax.context, *schema_id, name) ||
       connector::FindSequence(ax.context, *schema_id, name))) {
    return nullptr;
  }

  // REFERENCES on the referenced table's key columns, which is what postgres
  // asks of the creator of a foreign key.
  for (const auto& constraint : info->constraints) {
    if (constraint->type != duckdb::ConstraintType::FOREIGN_KEY) {
      continue;
    }
    const auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
    const ObjectId referenced_id{fk.host_referenced_id};
    if (!referenced_id.isSet()) {
      continue;
    }
    const auto* ref =
      connector::FindTableIn(ax.context, database_id, referenced_id);
    if (ref == nullptr) {
      continue;
    }
    const auto& ref_perm = ref->permissions;
    std::vector<AclView> ref_acls;
    ref_acls.reserve(fk.host_pk_column_ids.size());
    for (const auto column_id : fk.host_pk_column_ids) {
      ref_acls.push_back(ref->GetColumnAcl(ObjectId{column_id}));
    }
    if (!auth::ClosureFor(ax.context, ax.role)
           ->CanColumns(ref_perm, AclMode::References, ref_acls)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for table ", ref->name.GetIdentifierName()));
    }
  }

  // PG mangles `<table>_<col>_seq` with a numeric suffix on collision. Done
  // under the mutex so concurrent CREATE TABLEs can't race on it. Every half of
  // the relation namespace is asked, because a sequence shares it.
  const auto pick_unique_name = [&](std::string_view base) {
    const auto taken = [&](std::string_view candidate) {
      return connector::FindTable(ax.context, *schema_id, candidate) ||
             connector::FindSequence(ax.context, *schema_id, candidate) ||
             connector::FindIndex(ax.context, *schema_id, candidate) ||
             connector::FindView(ax.context, *schema_id, candidate);
    };
    std::string candidate{base};
    for (size_t i = 1; taken(candidate); ++i) {
      candidate = absl::StrCat(base, i);
    }
    return candidate;
  };

  const auto make_nextval_default = [](std::string_view qualified) {
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> args;
    args.emplace_back(duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value{std::string{qualified}}));
    return duckdb::make_uniq<duckdb::FunctionExpression>("nextval",
                                                         std::move(args));
  };

  // A caller-provided id means CTAS mode: the data side makes the store table
  // itself, under its own transaction.
  const bool ctas_mode = operation_options.table_id.id() != 0;
  const auto table_id = ctas_mode ? operation_options.table_id : NextId();

  // Generated serial/PK sequences are owned by the table owner too (PG: ALTER
  // TABLE OWNER TO cascades to them, so they must start matching). The owner is
  // the acting role from the access context (root for internal/bootstrap
  // callers via NoAccessCheck).
  const ObjectId owner = ax.role;
  const Permissions perm{owner};
  const Permissions sequence_perm{owner};
  std::vector<std::shared_ptr<const duckdb::CreateSequenceInfo>> sequences;
  sequences.reserve(sequence_specs.size() + 1);
  const auto make_sequence = [&](SequenceOptions opts) {
    return catalog::MakeSequenceInfo(NextId(), *schema_id, std::move(opts));
  };
  for (const auto& spec : sequence_specs) {
    const auto* column = catalog::ColumnById(*info, spec.column_id);
    SDB_ASSERT(column != nullptr);
    auto resolved = pick_unique_name(
      absl::StrCat(name, "_", column->Name().GetIdentifierName(), "_seq"));
    info->columns.GetColumnMutable(column->Logical())
      .SetDefaultValue(
        make_nextval_default(absl::StrCat(schema, ".", resolved)));
    auto seq_opts = spec.options;
    seq_opts.name = resolved;
    seq_opts.owner_table_id = table_id.id();
    sequences.push_back(make_sequence(std::move(seq_opts)));
  }

  // Tables without an explicit PK get an auto-PK owned sequence, whose id the
  // definition holds directly so the insert path does not have to look for it.
  std::shared_ptr<const duckdb::CreateSequenceInfo> generated_pk_seq;
  ObjectId generated_pk_seq_id;
  if (TablePrimaryKey(*info) == nullptr) {
    SequenceOptions opts;
    opts.name = pick_unique_name(absl::StrCat(name, "_pk_seq"));
    opts.cache = 65536;
    opts.owner_table_id = table_id.id();
    generated_pk_seq = make_sequence(std::move(opts));
    generated_pk_seq_id = catalog::IdOf(*generated_pk_seq);
    sequences.push_back(generated_pk_seq);
  }
  catalog::SetTableTags(*info, catalog::TableEngineOf(*info),
                        catalog::SearchOptionsOf(*info), generated_pk_seq_id);

  auto table =
    NextTableVersion(ax.context, table_id, *schema_id, std::move(info));
  bool store_table =
    catalog::TableEngineOf(*table) == TableEngine::Transactional;
  // Runtime state, bound onto the entry once it is placed: the shard the rows
  // of a search table live in, and the counter the insert path reserves from.
  std::shared_ptr<search::SearchTable> search_data;
  if (catalog::TableEngineOf(*table) == TableEngine::Search) {
    search_data = search::SearchTable::Create(database_id, *schema_id, table_id,
                                              /*is_new=*/true,
                                              catalog::SearchOptionsOf(*table));
  }

  // The names the op registers, checked here in the order it registers them:
  // the store table is created before the publish, so a collision thrown from
  // inside the op would leave it behind. Two generated sequences can collide
  // with each other (pick_unique_name only sees the transaction's view), which
  // is why this tracks the batch's own names too. Sequences share the relation
  // namespace but not its errcode.
  {
    std::vector<std::string_view> registering;
    registering.reserve(sequences.size() + 1);
    const auto taken = [&](std::string_view candidate) {
      return connector::FindTable(ax.context, *schema_id, candidate) ||
             connector::FindView(ax.context, *schema_id, candidate) ||
             connector::FindIndex(ax.context, *schema_id, candidate) ||
             connector::FindSequence(ax.context, *schema_id, candidate) ||
             absl::c_linear_search(registering, candidate);
    };
    if (taken(name)) {
      ThrowDuplicateName(NameKind::Relation, name);
    }
    registering.push_back(name);
    for (const auto& seq : sequences) {
      if (taken(catalog::SequenceNameOf(*seq))) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                        ERR_MSG("relation \"", catalog::SequenceNameOf(*seq),
                                "\" already exists"));
      }
      registering.push_back(catalog::SequenceNameOf(*seq));
    }
  }

  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  // Ahead of the records: the table's SERIAL columns name these sequences in
  // their DEFAULT, and the resolution the table's record carries is taken while
  // the entry is written -- so the entries have to be there to be resolved to,
  // or the table lands with no edge to its own sequence and DROP SEQUENCE stops
  // being refused. Their records ride the table's own, one frame below.
  RecordedScope recorded;
  // The counter the auto-PK column reserves from has to be the one the entry
  // carries, not a second one over the same sequence.
  std::shared_ptr<SequenceCounter> generated_pk_counter;
  for (const auto& seq : sequences) {
    auto placed =
      connector::PutEntry(ax.context, /*old_name=*/{}, seq, sequence_perm);
    // Seeded from START: a create has no predecessor to inherit a counter
    // from, and the insert path reserves off the entry's.
    if (const auto* entry =
          dynamic_cast<const connector::SereneDBSequenceEntry*>(placed.get())) {
      auto counter =
        NewCounter(catalog::IdOf(*seq), catalog::SequenceOptionsOf(*seq));
      if (catalog::IdOf(*seq) == generated_pk_seq_id) {
        generated_pk_counter = counter;
      }
      entry->AdoptCounter(std::move(counter));
    }
  }
  // Re-resolved now that the owned sequences are written: the first pass
  // could not see them.
  table = NextTableVersion(ax.context, table_id, *schema_id, table);
  Apply(ax.context, [&](auto& ctx) {
    std::vector<wal::OwnedSequence> owned;
    owned.reserve(sequences.size());
    for (const auto& seq : sequences) {
      owned.push_back({.id = catalog::IdOf(*seq),
                       .info = seq,
                       .perm = sequence_perm,
                       .seed = catalog::SequenceOptionsOf(*seq).Seed()});
    }
    ctx.catalog().PutTable(*table, wal::PutMode::Create, perm,
                           std::move(owned));
    // In CTAS mode the store table is the caller's own work, made later in
    // the same transaction; the entry above waits for that transaction to
    // commit either way, so there is nothing further to hold back here.
    if (store_table && !ctas_mode) {
      ctx.store().CreateTable(database_id, table_id);
    }
  });
  const auto placed =
    connector::PutEntry(ax.context, /*old_name=*/{}, table, perm);
  if (const auto* entry =
        dynamic_cast<const connector::SereneDBTableEntry*>(placed.get())) {
    if (generated_pk_counter) {
      entry->Runtime()->SetGeneratedPkSequence(generated_pk_counter);
    }
    if (search_data) {
      entry->Runtime()->SetData(std::move(search_data));
    }
  }
  return table;
}

bool Catalog::CreateTokenizer(const AccessContext& ax, ObjectId database_id,
                              std::string_view schema,
                              std::shared_ptr<CreateTokenizerInfo> tokenizer,
                              bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  auto schema_id = FindSchemaId(ax.context, database_id, schema);
  if (!schema_id) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                    ERR_MSG("schema \"", schema, "\" does not exist"));
  }
  RequireCreateOn(ax.context, ax.role, *schema_id);
  const auto name = tokenizer->GetName();
  if (connector::FindTokenizer(ax.context, *schema_id, name)) {
    if (if_not_exists) {
      return false;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" already exists"));
  }
  if (!tokenizer->GetId().isSet()) {
    tokenizer->SetId(NextId());
  }
  tokenizer->SetSchemaId(*schema_id);
  Permissions perm{ax.role};
  std::shared_ptr<const CreateTokenizerInfo> info = std::move(tokenizer);
  connector::PutEntry(ax.context, /*old_name=*/{}, std::move(info),
                      std::move(perm));
  return true;
}

bool Catalog::CreateForeignServer(const AccessContext& ax, ObjectId database_id,
                                  std::shared_ptr<CreateForeignServerInfo> info,
                                  Permissions perm, bool if_not_exists) {
  absl::MutexLock lock{&_mutex};
  // Servers are database children, like PG (no schema). Gated on CREATE on
  // the database, same as CREATE SCHEMA -- PG gates on FDW USAGE instead, but
  // serenedb has no foreign-data-wrapper catalog object to hang an ACL on.
  RequireDatabaseAccess(ax.context, ax.role,
                        connector::FindDatabase(ax.context, database_id),
                        AclMode::Create);
  const auto name = info->GetName();
  if (!IsSupportedFdw(info->GetFdwName())) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("foreign-data wrapper \"", info->GetFdwName(),
                            "\" is not supported"),
                    ERR_HINT("Use clickhouse_fdw or postgres_fdw."));
  }
  if (connector::FindForeignServer(ax.context, database_id, name)) {
    if (if_not_exists) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("server \"", name, "\" already exists"));
  }
  // Catalog names are per-database, but the live attachment alias is
  // instance-global: a second same-named server would race the first for it
  // (nondeterministic boot winner; DROP DATABASE detaching the other
  // database's attachment). The attach cannot catch this -- it only collides
  // while the first server's attachment is live, not when its remote is down.
  std::vector<connector::DatabaseRef> databases;
  connector::VisitDatabases(ax.context, [&](const connector::DatabaseRef& db) {
    databases.push_back(db);
  });
  for (const auto& db : databases) {
    // A database shares the alias namespace with foreign servers, so a server
    // named after one would make DROP SERVER's detach tear the database down.
    if (db.Name() == name) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("database \"", db.Name(),
                "\" already exists, so a server cannot take that name"),
        ERR_HINT("Foreign server attachment names are instance-wide; "
                 "choose a name not used by any database."));
    }
    if (db.Id() != database_id &&
        connector::FindForeignServer(ax.context, db.Id(), name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("server \"", name, "\" already exists in database \"",
                db.Name(), "\""),
        ERR_HINT("Foreign server attachment names are instance-wide; "
                 "choose a name not used by any database."));
    }
  }
  info->SetDatabaseId(database_id);
  const auto id = info->GetId().isSet() ? info->GetId() : NextId();
  info->SetId(id);
  // One definition, handed to the record and to the entry: nothing is derived
  // at append time.
  connector::PutEntry(
    ax.context, /*old_name=*/{},
    std::shared_ptr<const CreateForeignServerInfo>{std::move(info)}, perm);
  return true;
}

void Catalog::ChangeRoleImpl(
  duckdb::ClientContext* context, ObjectId actor_id, std::string_view name,
  absl::FunctionRef<void(duckdb::ClientContext*, const CreateRoleInfo&)> check,
  ChangeCallback<CreateRoleInfo> callback) {
  JoinClusterGlobal(context, duckdb::DatabaseModificationType::ALTER_TABLE);
  absl::MutexLock lock{&_mutex};
  auto current = connector::FindRole(context, name);
  if (!current) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", name, "\" does not exist"));
  }
  connector::RequireRoleNotVanished(context, name);
  check(context, *current);  // caller's access check, on the live entry

  std::shared_ptr<CreateRoleInfo> updated;
  callback(*current, updated);
  if (!updated) {
    return;
  }
  // A change may only add privileged attributes the actor holds itself. A
  // rename onto a taken name is refused here rather than by the set, so the
  // error is PG's "role already exists" rather than a serialization failure.
  RequireAttributesGrantable(context, actor_id,
                             updated->Options() & ~current->Options(),
                             /*creating=*/false);
  const auto old_name = std::string{current->GetName()};
  if (updated->GetName() != old_name &&
      connector::FindRole(context, updated->GetName())) {
    ThrowDuplicateName(NameKind::Role, updated->GetName());
  }
  connector::PutRole(context, old_name, std::move(updated));
}

void Catalog::ChangeRole(const AccessContext& ax, std::string_view name,
                         std::string_view verb, bool allow_self,
                         ChangeCallback<CreateRoleInfo> callback) {
  ChangeRoleImpl(
    ax.context, ax.role, name,
    [&](duckdb::ClientContext* context, const CreateRoleInfo& role) {
      if (allow_self && ax.role == role.GetId()) {
        return;  // a role may change its own entry (e.g. SET config)
      }
      RequireRoleAdmin(context, ax.role, role, verb);
    },
    std::move(callback));
}

void Catalog::ChangeDefaultAcl(const AccessContext& ax,
                               std::string_view role_name, ObjectId schema,
                               char objtype, duckdb::CatalogType type,
                               absl::AnyInvocable<void(Acl&)> mutate) {
  ChangeRoleImpl(
    ax.context, ax.role, role_name,
    [&](duckdb::ClientContext* context, const CreateRoleInfo& role) {
      RequireRoleMembership(context, ax.role, role);
    },
    [schema, objtype, type, mutate = std::move(mutate)](
      const CreateRoleInfo& old_role,
      std::shared_ptr<CreateRoleInfo>& new_role) mutable {
      new_role = old_role.CloneRole();
      new_role->ChangeDefaultAcl(schema, objtype, type, mutate);
    });
}

void Catalog::ChangeMembership(const AccessContext& ax, ObjectId role,
                               std::string_view role_name, ObjectId member,
                               std::string_view member_name,
                               const Membership& edge, bool revoke,
                               bool admin_option_only) {
  JoinClusterGlobal(ax.context, duckdb::DatabaseModificationType::ALTER_TABLE);
  absl::MutexLock lock{&_mutex};
  auto roles = auth::RolesOf(ax.context);
  auto actor = connector::FindRole(ax.context, ax.role);
  if (!(actor && actor->IsSuperuser()) &&
      !auth::HasAdminOption(*roles, ax.role, role)) {
    const auto verb = revoke ? "revoke" : "grant";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to ", verb, " role \"", role_name, "\""),
      ERR_DETAIL("Only roles with the ADMIN option on role \"", role_name,
                 "\" may ", verb, " this role."));
  }
  if (!revoke) {
    if (!connector::FindRole(ax.context, role)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", role_name, "\" does not exist"));
    }
    connector::RequireRoleNotVanished(ax.context, role_name);
    if (auth::ComputeMembershipClosure(*roles, role).contains(member)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
                      ERR_MSG("role \"", role_name, "\" is a member of role \"",
                              member_name, "\""));
    }
  }

  const auto member_role = connector::FindRole(ax.context, member);
  if (!member_role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", member_name, "\" does not exist"));
  }
  connector::RequireRoleNotVanished(ax.context, member_name);

  auto new_role = member_role->CloneRole();
  if (revoke && admin_option_only) {
    auto edges = new_role->MemberOf();
    auto it = std::ranges::find(edges, role, &Membership::role);
    if (it != edges.end()) {
      Membership kept = *it;
      kept.admin_option = false;
      new_role->AddMembership(kept);
    }
  } else if (revoke) {
    new_role->RemoveMembership(role);
  } else {
    new_role->AddMembership(edge);
  }
  const auto name = std::string{new_role->GetName()};
  connector::PutRole(ax.context, name, std::move(new_role));
}

void Catalog::ChangeTableOwner(const AccessContext& ax,
                               const duckdb::CreateTableInfo& table,
                               duckdb::CatalogType type, ObjectId new_owner,
                               std::string_view new_owner_name) {
  absl::MutexLock lock{&_mutex};
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto* live = connector::FindTable(ax.context, schema_id, table_id);
  if (live == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             catalog::TableNameOf(table));
  }
  const auto& perm = live->permissions;
  const auto name = live->name.GetIdentifierName();
  // The definition the record carries: the owner moves, the table does not.
  const auto definition = live->Definition();
  // `type` is the type the statement named the table as, which is what the
  // refusal has to say -- a statement may name an index by ALTER TABLE.
  RequireOwnerTransfer(ax, schema_id, perm, new_owner, new_owner_name,
                       pg::ToPgObjectTypeName(type), name);

  auto updated_perm = auth::TransferredOwner(perm, new_owner);
  const auto database_id = connector::SchemaDatabaseId(ax.context, schema_id);
  // A table's SERIAL sequences follow its owner, and are rewritten as their own
  // definitions in the same frame.
  auto sequences = connector::DatabaseSequences(ax.context, database_id);
  std::erase_if(sequences, [&](const connector::SereneDBSequenceEntry* seq) {
    return seq->GetOwnerTableId() != table_id;
  });
  std::vector<
    std::pair<std::shared_ptr<const duckdb::CreateSequenceInfo>, Permissions>>
    rewritten;
  rewritten.reserve(sequences.size());
  for (const auto* sequence : sequences) {
    rewritten.emplace_back(
      sequence->Definition(),
      auth::TransferredOwner(sequence->permissions, new_owner));
  }
  // One frame for the table and everything that follows its owner: a table and
  // the sequences its SERIAL columns own must never end up owned by different
  // roles, which one append per object allows a crash to do.
  RecordedScope recorded;
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().PutTable(*definition, wal::PutMode::Replace, updated_perm);
    for (const auto& [sequence, sequence_perm] : rewritten) {
      ctx.catalog().PutEntry(catalog::ParentIdOf(*sequence),
                             duckdb::CatalogType::SEQUENCE_ENTRY,
                             catalog::IdOf(*sequence), wal::PutMode::Replace,
                             sequence, sequence_perm);
    }
  });
  connector::PutEntry(ax.context, name, definition, updated_perm);
  for (auto& [sequence, sequence_perm] : rewritten) {
    const auto sequence_name = std::string{catalog::SequenceNameOf(*sequence)};
    connector::PutEntry(ax.context, sequence_name, std::move(sequence),
                        std::move(sequence_perm));
  }
}

void Catalog::ChangeDatabaseAcl(const AccessContext& ax, ObjectId database_id,
                                AclMutator mutate) {
  JoinClusterGlobal(ax.context, duckdb::DatabaseModificationType::ALTER_TABLE);
  absl::MutexLock lock{&_mutex};
  auto database = connector::FindDatabase(ax.context, database_id);
  if (!database) [[unlikely]] {
    ThrowConcurrentlyDropped(database_id);
  }
  auto perm = auth::MutatedAcl(database.perm,
                               duckdb::CatalogType::DATABASE_ENTRY, mutate);
  auto updated = database.info->CloneDatabase();
  const auto name = std::string{updated->GetName()};
  connector::PutDatabase(ax.context, name, std::move(updated), std::move(perm));
}

void Catalog::ChangeTable(const AccessContext& ax,
                          const duckdb::CreateTableInfo& table,
                          TableChange change) {
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto* current = connector::FindTable(ax.context, schema_id, table_id);
  if (current == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             catalog::TableNameOf(table));
  }
  const auto& perm = current->permissions;
  RequireOwner(ax.context, ax.role, perm, "table",
               current->name.GetIdentifierName());

  const auto current_info = current->Definition();
  auto info = change(*current_info);
  if (!info) {
    return;
  }
  const auto& old_info = *current_info;
  // Adding a primary key changes which columns provide row identity, so every
  // index already built over the table indexes the wrong thing afterwards.
  if (TablePrimaryKey(old_info) == nullptr &&
      TablePrimaryKey(*info) != nullptr &&
      !connector::RelationIndexes(ax.context, schema_id, table_id).empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("Cannot alter entry \"", current->name.GetIdentifierName(),
              "\" because there are entries that depend on it."));
  }
  auto updated =
    NextTableVersion(ax.context, table_id, schema_id, std::move(info));
  const auto& new_info = *updated;
  const auto rerendered = RerenderedIndexes(
    connector::RelationIndexes(ax.context, schema_id, table_id), old_info,
    new_info);

  const bool reshape =
    catalog::TableEngineOf(*updated) == TableEngine::Transactional;
  const auto db_id = connector::SchemaDatabaseId(ax.context, schema_id);

  RecordedScope recorded;
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().PutTable(*updated, wal::PutMode::Replace, perm);
    for (const auto& new_idx : rerendered) {
      PutIndex(ctx, new_idx, wal::PutMode::Replace);
    }
    if (reshape) {
      ctx.store().ReshapeTable(db_id, table_id, old_info, new_info);
    }
  });
  // After the records and before the index wrappers: the entry the wrappers
  // project has to be the rewritten one.
  connector::PutEntry(ax.context, catalog::TableNameOf(*updated), updated,
                      perm);
  // What the new version states is refreshed by the write; a key it no longer
  // states leaves its referenced half behind on a table that still exists.
  connector::RefreshForeignKeyTargets(ax.context, *current_info);
  for (const auto& new_idx : rerendered) {
    connector::PutEntry(ax.context, new_idx->GetName(), new_idx);
  }
}

bool Catalog::DropRole(const AccessContext& ax, std::string_view role,
                       bool missing_ok) {
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  absl::MutexLock lock{&_mutex};
  RequireRoleAttribute(ax.context, ax.role, RoleOption::CreateRole, "drop role",
                       "Only roles with the CREATEROLE attribute and the ADMIN "
                       "option on the target roles may drop roles.");
  auto role_ptr = connector::FindRole(ax.context, role);
  if (!role_ptr) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", role, "\" does not exist"));
  }
  if (role_ptr->GetId() == ax.role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("current user cannot be dropped"));
  }
  if (role == StaticStrings::kDefaultUser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    ERR_MSG("cannot drop role ", role,
                            " because it is required by the database system"));
  }
  RequireRoleAdmin(ax.context, ax.role, *role_ptr, "drop");
  if (auto deps = connector::DependencyView{ax.context}.CountDependents(
        role_ptr->GetId());
      deps > 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("role \"", role,
              "\" cannot be dropped because some objects depend on it"),
      ERR_DETAIL(deps, " object(s) in database depend on role \"", role, "\""));
  }
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().DropObject(id::kInstance, duckdb::CatalogType::ROLE_ENTRY,
                             role_ptr->GetId());
  });
  connector::DropRoleEntry(ax.context, role);
  return true;
}

void Catalog::DropDatabase(const AccessContext& ax, std::string_view name,
                           duckdb::shared_ptr<void> keep_alive) {
  JoinStoreTransaction(ax.context);
  JoinClusterGlobal(ax.context,
                    duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  absl::MutexLock lock{&_mutex};
  auto database = connector::FindDatabase(ax.context, name);
  if (!database) {
    THROW_SQL_ERROR(ERR_MSG("database \"", name, "\" does not exist"));
  }
  const auto database_id = std::optional{database.Id()};
  RequireDatabaseOwner(ax.context, ax.role, database);

  auto plan = ComputeDropPlan(ax.context, *database_id);

  const auto owned_sequences = CollectSequenceOwners(ax.context, *database_id);
  const auto owned_indexes = CollectIndexOwners(ax.context, *database_id);
  auto task = CreateDatabaseDrop(ax.context, *database_id, owned_sequences,
                                 owned_indexes, std::move(keep_alive));
  // Against the pre-mutation view: a plan never names an index inside the
  // seed's own subtree, which the seed's structural task covers.
  ScheduleDropPlanIndexes(ax.context, *database_id, plan);
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().DropPrepare(
      MakeDropPrepare(*task, id::kInstance, duckdb::CatalogType::DATABASE_ENTRY,
                      *database_id, *database_id, ObjectId{}));
    CommitDropPlan(ax.context, ctx, plan);
    task->EmitStoreDrops(ctx);
  });
  PublishDropPlan(ax.context, plan);
  // Check that SereneDB won't open this database after reboot
  bool crash_on_drop = false;
  SDB_IF_FAILURE("crash_on_drop") { crash_on_drop = true; }
  if (!crash_on_drop) {
    ScheduleDrop(ax.context, std::move(task));
  }
  connector::DropDatabaseEntry(ax.context, name);
}

bool Catalog::DropSchema(const AccessContext& ax, std::string_view database,
                         std::string_view name, bool cascade, bool missing_ok) {
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};

  const auto database_id = FindDatabaseId(ax.context, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("schema \"", name, "\" does not exist"));
  }
  Permissions schema_perm;
  const auto schema =
    connector::FindSchema(ax.context, *database_id, name, &schema_perm);
  if (!schema) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("schema \"", name, "\" does not exist"));
  }
  const std::optional schema_id{IdOf(*schema)};
  RequireOwner(ax.context, ax.role, schema_perm, "schema",
               SchemaNameOf(*schema));

  // The containment index does not hold a tokenizer or a type -- their entry is
  // the object -- so what it contains is asked of the sets that do.
  bool has_entry_child = false;
  connector::VisitTokenizers(
    ax.context, *database_id,
    [&](const CreateTokenizerInfo& tokenizer, const Permissions&) {
      has_entry_child |= tokenizer.GetParentId() == *schema_id;
    });
  connector::VisitTypes(
    ax.context, *database_id, [&](const duckdb::TypeCatalogEntry& type) {
      has_entry_child |= ObjectId{type.ParentSchema().oid} == *schema_id;
    });
  connector::VisitFunctions(
    ax.context, *database_id, [&](const duckdb::MacroCatalogEntry& function) {
      has_entry_child |= ObjectId{function.ParentSchema().oid} == *schema_id;
    });
  connector::VisitViews(
    ax.context, *database_id, [&](const duckdb::ViewCatalogEntry& view) {
      has_entry_child |= ObjectId{view.ParentSchema().oid} == *schema_id;
    });
  connector::VisitSequences(
    ax.context, *database_id,
    [&](const connector::SereneDBSequenceEntry& sequence) {
      has_entry_child |= ObjectId{sequence.ParentSchema().oid} == *schema_id;
    });
  if (!cascade && (has_entry_child ||
                   !CheckSchemaEmptyDependency(ax.context, *schema_id))) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
      ERR_MSG("cannot drop schema ", name,
              " because other objects depend on it"),
      ERR_HINT("Use DROP ... CASCADE to drop the dependent objects too."));
  }

  auto plan = ComputeDropPlan(ax.context, *schema_id);

  const auto owned_sequences = CollectSequenceOwners(ax.context, *database_id);
  const auto owned_indexes = CollectIndexOwners(ax.context, *database_id);
  auto task = CreateSchemaDrop(ax.context, *database_id, *schema_id,
                               owned_sequences, owned_indexes, true);
  ScheduleDropPlanIndexes(ax.context, *database_id, plan);
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().DropPrepare(
      MakeDropPrepare(*task, *database_id, duckdb::CatalogType::SCHEMA_ENTRY,
                      *schema_id, *database_id, *schema_id));
    CommitDropPlan(ax.context, ctx, plan);
    task->EmitStoreDrops(ctx);
  });
  PublishDropPlan(ax.context, plan);
  connector::DropSchemaEntry(ax.context, *database_id, name);
  // Check that SereneDB won't open this schema after reboot
  bool crash_on_drop = false;
  SDB_IF_FAILURE("crash_on_drop") { crash_on_drop = true; }
  if (!crash_on_drop) {
    ScheduleDrop(ax.context, std::move(task));
  }
  return true;
}

bool Catalog::DropTable(const AccessContext& ax, std::string_view database,
                        std::string_view schema, std::string_view name,
                        bool cascade, bool missing_ok) {
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};

  const auto database_id = FindDatabaseId(ax.context, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto schema_id = FindSchemaId(ax.context, *database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto* table = connector::FindTable(ax.context, *schema_id, name);
  if (table == nullptr) {
    // A view, a sequence and an index hold a relation name too, so the kind
    // mismatch has to be asked of the sets that hold them.
    if (connector::FindSequence(ax.context, *schema_id, name)) {
      ThrowWrongObjectType(name, "table", duckdb::CatalogType::SEQUENCE_ENTRY);
    }
    if (connector::FindView(ax.context, *schema_id, name)) {
      ThrowWrongObjectType(name, "table", duckdb::CatalogType::VIEW_ENTRY);
    }
    if (connector::FindIndex(ax.context, *schema_id, name) != nullptr) {
      ThrowWrongObjectType(name, "table", duckdb::CatalogType::INDEX_ENTRY);
    }
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("table \"", name, "\" does not exist"));
  }
  const auto table_id = ObjectId{table->oid};
  RequireOwner(ax.context, ax.role, table->permissions, "table",
               table->name.GetIdentifierName());
  const auto definition = table->Definition();

  auto plan =
    ComputeDropPlanRestrict(ax.context, table_id, cascade, "table", name);

  // The sequences and indexes this table owns, read off the sets before
  // anything is removed: nothing else names them, and the drop task's subtree
  // is what makes their ids and counters part of this one operation.
  const auto owned_sequences = CollectSequenceOwners(ax.context, *database_id);
  const auto owned_indexes = CollectIndexOwners(ax.context, *database_id);
  std::vector<std::string> owned_sequence_names;
  connector::VisitSequences(
    ax.context, *database_id,
    [&](const connector::SereneDBSequenceEntry& sequence) {
      if (sequence.GetOwnerTableId() == table_id) {
        owned_sequence_names.emplace_back(sequence.name.GetIdentifierName());
      }
    });
  auto task = CreateTableDrop(*database_id, *schema_id, definition,
                              owned_sequences, owned_indexes, true);
  ScheduleDropPlanIndexes(ax.context, *database_id, plan);
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().DropPrepare(
      MakeDropPrepare(*task, *schema_id, duckdb::CatalogType::TABLE_ENTRY,
                      table_id, *database_id, *schema_id));
    CommitDropPlan(ax.context, ctx, plan);
    task->EmitStoreDrops(ctx);
  });
  PublishDropPlan(ax.context, plan);
  // The schema's sets outlive the table, so the owned sequences' and the
  // indexes' entries need a drop of their own -- the table's subtree covers the
  // records, not the entries.
  for (const auto& sequence_name : owned_sequence_names) {
    connector::DropEntryOfKind(ax.context, duckdb::CatalogType::SEQUENCE_ENTRY,
                               *schema_id, sequence_name);
  }
  for (const auto& index : owned_indexes.Of(table_id)) {
    connector::DropIndexEntry(ax.context, *schema_id, index->GetName());
  }
  connector::DropEntryOfKind(ax.context, duckdb::CatalogType::TABLE_ENTRY,
                             *schema_id, name);
  bool crash_on_drop = false;
  SDB_IF_FAILURE("crash_on_drop") { crash_on_drop = true; }
  if (!crash_on_drop) {
    ScheduleDrop(ax.context, std::move(task));
  }
  return true;
}

void Catalog::DropTableColumn(const AccessContext& ax, ObjectId database_id,
                              const duckdb::CreateTableInfo& table,
                              std::string_view column, bool if_exists) {
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};
  const auto table_id = catalog::IdOf(table);
  const auto* entry =
    connector::FindTable(ax.context, catalog::ParentIdOf(table), table_id);
  if (entry == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             catalog::TableNameOf(table));
  }
  const auto& perm = entry->permissions;
  const auto live = entry->Definition();
  RequireOwner(ax.context, ax.role, perm, "table",
               entry->name.GetIdentifierName());
  const auto* col = catalog::ColumnByName(*live, column);
  if (col == nullptr) {
    if (if_exists) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", column, "\" of relation \"",
                            catalog::TableNameOf(*live), "\" does not exist"));
  }
  const ObjectId col_id{col->CatalogOid()};

  auto plan = ComputeColumnDropPlan(ax.context, live, perm, col_id);

  ScheduleDropPlanIndexes(ax.context, database_id, plan);
  Apply(ax.context, [&](auto& ctx) { CommitDropPlan(ax.context, ctx, plan); });
  PublishDropPlan(ax.context, plan);
}

void Catalog::ChangeColumnType(
  const AccessContext& ax, const duckdb::CreateTableInfo& table,
  std::string_view column, duckdb::LogicalType new_type,
  duckdb::unique_ptr<duckdb::ParsedExpression> using_expr) {
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};
  const auto table_id = catalog::IdOf(table);
  const auto schema_id = catalog::ParentIdOf(table);
  const auto* entry = connector::FindTable(ax.context, schema_id, table_id);
  if (entry == nullptr) {
    ThrowConcurrentlyDropped(duckdb::CatalogType::TABLE_ENTRY,
                             catalog::TableNameOf(table));
  }
  const auto& perm = entry->permissions;
  const auto live = entry->Definition();
  RequireOwner(ax.context, ax.role, perm, "table",
               entry->name.GetIdentifierName());
  const auto* col = catalog::ColumnByName(*live, column);
  if (col == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", column, "\" of relation \"",
                            catalog::TableNameOf(*live), "\" does not exist"));
  }
  const ObjectId col_id{col->CatalogOid()};

  const auto table_indexes =
    connector::RelationIndexes(ax.context, schema_id, table_id);
  // The index stores values of the column's old type; a type change would
  // leave them inconsistent. Reject and let the user drop the index first.
  for (const auto& idx : table_indexes) {
    if (idx->ReferencesColumn(col_id)) {
      THROW_SQL_ERROR(ERR_MSG("cannot alter type of column \"", column,
                              "\" because index \"", idx->GetName(),
                              "\" depends on it; drop the index first"));
    }
  }

  auto updated =
    NextTableVersion(ax.context, table_id, schema_id,
                     catalog::ChangeColumnType(*live, column, new_type));

  const bool reshape =
    catalog::TableEngineOf(*updated) == TableEngine::Transactional;
  duckdb::Identifier store_column;
  if (reshape) {
    if (const auto* moved = catalog::ColumnById(*updated, col_id)) {
      store_column = moved->Name();
    }
  }
  const auto db_id = connector::SchemaDatabaseId(ax.context, schema_id);

  RecordedScope recorded;
  Apply(ax.context, [&](auto& ctx) {
    ctx.catalog().PutTable(*updated, wal::PutMode::Replace, perm);
    if (!reshape) {
      return;
    }
    // The store blocks ALTER COLUMN TYPE while any index depends on the
    // table; drop the mirrored store indexes, change the type, then
    // recreate them (the data lives in the rows / iresearch, so the
    // rebuild carries no state of its own).
    std::vector<
      std::pair<duckdb::unique_ptr<duckdb::CreateIndexInfo>, IndexInfoRef>>
      recreate;
    for (const auto& idx : table_indexes) {
      if (auto info = MakeStoreIndexInfo(*updated, *idx)) {
        ctx.store().DropIndex(db_id, idx->GetRelationId(), idx->GetName());
        recreate.emplace_back(std::move(info), idx);
      }
    }
    ctx.store().Alter(db_id, table_id,
                      duckdb::make_uniq<duckdb::ChangeColumnTypeInfo>(
                        StoreTarget(), store_column, new_type,
                        using_expr ? using_expr->Copy() : nullptr));
    for (auto& [info, idx] : recreate) {
      ctx.store().CreateIndex(db_id, std::move(info), updated, std::move(idx));
    }
  });
  connector::PutEntry(ax.context, catalog::TableNameOf(*updated), updated,
                      perm);
}

bool Catalog::DropIndex(const AccessContext& ax, std::string_view database,
                        std::string_view schema, std::string_view name,
                        bool cascade, bool missing_ok) {
  JoinStoreTransaction(ax.context);
  absl::MutexLock lock{&_mutex};

  const auto database_id = FindDatabaseId(ax.context, database);
  if (!database_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  const auto schema_id = FindSchemaId(ax.context, *database_id, schema);
  if (!schema_id) {
    if (missing_ok) {
      return false;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  const auto* index_entry = connector::FindIndex(ax.context, *schema_id, name);
  const auto index =
    index_entry != nullptr ? index_entry->Definition() : nullptr;
  if (!index) {
    if (missing_ok) {
      return false;
    }
    if (connector::FindTable(ax.context, *schema_id, name) ||
        connector::FindView(ax.context, *schema_id, name) ||
        connector::FindSequence(ax.context, *schema_id, name)) {
      ThrowWrongObjectType(name, "index", duckdb::CatalogType::TABLE_ENTRY);
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  // An index has no independent owner (PG: its relowner is derived from the
  // table). Drop authority therefore belongs to the underlying relation's
  // owner, so check that, not the index's own (unset) owner.
  connector::RequireIndexOwner(ax, *index);
  DropIndexLocked(ax.context, *database_id, index, cascade);
  return true;
}

void Catalog::DropIndexById(duckdb::ClientContext* context,
                            ObjectId database_id, ObjectId index_id,
                            bool cascade) {
  JoinStoreTransaction(context);
  absl::MutexLock lock{&_mutex};
  const auto* session_index =
    context != nullptr ? connector::FindSessionIndex(*context, index_id)
                       : nullptr;
  IndexInfoRef index =
    session_index != nullptr ? session_index->Definition() : nullptr;
  if (!index) {
    // The compensating drop of a build that failed can arrive with no statement
    // to read through, so the committed sets of the index's own database are
    // all there is.
    if (auto database = TryStoreDatabase(database_id)) {
      if (const auto* found =
            connector::FindIndexIn(nullptr, database->GetCatalog(), index_id)) {
        index = found->Definition();
      }
    }
  }
  if (!index) {
    THROW_SQL_ERROR(
      ERR_MSG("index with id ", index_id.id(), " does not exist"));
  }
  DropIndexLocked(context, database_id, index, cascade);
}

void Catalog::DropIndexLocked(duckdb::ClientContext* context,
                              ObjectId database_id, const IndexInfoRef& index,
                              bool cascade) {
  const auto schema_id = index->GetParentId();
  const auto index_id = index->GetId();
  // Store-side index drop is synchronous: UNIQUE enforcement
  // must stop when DROP INDEX commits, not when the async sweep
  // runs.
  auto task = CreateIndexDrop(database_id, schema_id, index->GetRelationId(),
                              *index, true);
  // Before the records: the relation's own entry advertises a virtual column
  // per indexed column, and Apply is what rebuilds it.
  connector::DropIndexEntry(context, schema_id, index->GetName());
  Apply(context, [&](auto& ctx) {
    ctx.catalog().DropPrepare({.parent_id = index->GetRelationId(),
                               .type = duckdb::CatalogType::INDEX_ENTRY,
                               .inverted = index->IsInverted(),
                               .id = index_id,
                               .database_id = database_id,
                               .schema_id = schema_id});
    ctx.store().DropIndex(database_id, index->GetRelationId(),
                          index->GetName());
  });

  // Check that SereneDB won't open this index after reboot
  SDB_IF_FAILURE("crash_on_drop") { return; }

  ScheduleDrop(context, std::move(task));
}

void Catalog::DropUncommittedIndex(duckdb::ClientContext& context,
                                   ObjectId database_id, ObjectId index_id) {
  absl::MutexLock lock{&_mutex};
  const auto* index_entry = connector::FindSessionIndex(context, index_id);
  if (index_entry == nullptr) {
    return;
  }
  const auto index = index_entry->Definition();
  const auto relation_id = index->GetRelationId();
  // No catalog entries: passing the transaction keeps Write off `_mutex`, and
  // the store op runs inline either way.
  _engine->Write(&context, [&](auto& ctx) {
    ctx.store().DropIndex(database_id, relation_id, index->GetName());
  });
  auto task = CreateIndexDrop(database_id, index->GetParentId(), relation_id,
                              *index, true);
  DropTask::Schedule(std::move(task)).Detach();
}

void Catalog::DropResolved(duckdb::ClientContext* context, ObjectId database_id,
                           ObjectId parent_id, duckdb::CatalogType type,
                           ObjectId id, std::string_view name, DropPlan& plan) {
  ScheduleDropPlanIndexes(context, database_id, plan);
  Apply(context, [&](auto& ctx) {
    ctx.catalog().DropObject(parent_id, type, id);
    if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
      // The counter row goes in the same atomic batch.
      ctx.catalog().DropSequence(id);
    }
    CommitDropPlan(context, ctx, plan);
  });
  PublishDropPlan(context, plan);
  connector::DropEntryOfKind(context, type, parent_id, name);
}

void Catalog::ReplayRecords(std::span<const wal::Entry> entries) {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_loading);
  // These are the records being read: an entry write must not append them
  // again.
  RecordedScope recorded;
  // A record goes straight into the CatalogSet that holds its kind. The global
  // attachment is up before the log is read, which is what lets this run here
  // rather than in a second pass.
  for (const auto& entry : entries) {
    std::visit(
      [&](const auto& e) {
        using T = std::decay_t<decltype(e)>;
        if constexpr (std::is_same_v<T, wal::PutTable>) {
          connector::ReplayTableRecord(e);
        } else if constexpr (std::is_same_v<T, wal::PutEntry>) {
          connector::ReplayEntryRecord(e);
        } else if constexpr (std::is_same_v<T, wal::DropObject> ||
                             std::is_same_v<T, wal::DropPrepare>) {
          // An index's own removal is a DropPrepare naming the relation as its
          // parent, so the schema its name lives in is the record's schema_id
          // and the subtree pass below is what takes it.
          if (e.type != duckdb::CatalogType::INDEX_ENTRY) {
            if (auto name = connector::EntryNameOfKind(nullptr, e.type,
                                                       e.parent_id, e.id);
                !name.empty()) {
              connector::DropEntryOfKind(nullptr, e.type, e.parent_id, name);
              if (e.type == duckdb::CatalogType::DATABASE_ENTRY) {
                // The attachment the database's own record made a moment ago.
                // The record has removed the database itself, so this only
                // takes the attachment back out.
                connector::DiscardDatabaseAttachment(name);
              }
            }
          }
          // A table's owned sequences and its indexes live in the schema's set,
          // which the table's own removal does not touch, and no record of
          // their own names them -- so the subtree it carries is what takes
          // their entries. A schema or database drop needs none of this: the
          // sets go with the schema entries.
          if constexpr (std::is_same_v<T, wal::DropPrepare>) {
            const auto drop = [&](duckdb::CatalogType type, ObjectId id) {
              if (auto name =
                    connector::EntryNameOfKind(nullptr, type, e.schema_id, id);
                  !name.empty()) {
                connector::DropEntryOfKind(nullptr, type, e.schema_id, name);
              }
            };
            if (e.type == duckdb::CatalogType::INDEX_ENTRY) {
              drop(duckdb::CatalogType::INDEX_ENTRY, e.id);
            }
            if (e.type == duckdb::CatalogType::TABLE_ENTRY && e.subtree) {
              for (const auto& node : *e.subtree) {
                if (node.type == duckdb::CatalogType::SEQUENCE_ENTRY ||
                    node.type == duckdb::CatalogType::INDEX_ENTRY) {
                  drop(node.type, node.id);
                }
              }
            }
          }
        }
      },
      entry);
  }
}

namespace {

// The counter of a sequence a table owns, bound onto the table so an insert
// finds it without a catalog lookup. Only the version that survived the log
// matters, which is the one the entry holds.
void BindOwnedSequence(duckdb::ClientContext* context, ObjectId database_id,
                       const connector::SereneDBSequenceEntry& sequence) {
  const auto owner_id = sequence.GetOwnerTableId();
  if (!owner_id.isSet()) {
    return;
  }
  const auto* owner =
    connector::FindTableEntryIn(context, database_id, owner_id);
  if (owner != nullptr && catalog::GeneratedPkSeqIdOf(*owner->Definition()) ==
                            ObjectId{sequence.oid}) {
    owner->Runtime()->SetGeneratedPkSequence(sequence.Counter());
  }
}

}  // namespace

void Catalog::OpenBootStorage() {
  std::vector<ObjectId> databases;
  connector::VisitDatabases(nullptr, [&](const connector::DatabaseRef& ref) {
    databases.push_back(ref.Id());
  });
  for (const auto database_id : databases) {
    // The shard a search table's rows live in. Off the sets, like everything
    // else here: what survived the log is the version the entries hold.
    std::vector<const connector::SereneDBTableEntry*> tables;
    connector::VisitTableEntriesOf(
      nullptr, database_id, [&](const connector::SereneDBTableEntry& table) {
        if (table.IsSearchTable()) {
          tables.push_back(&table);
        }
      });
    for (const auto* table : tables) {
      table->Runtime()->SetData(search::SearchTable::Create(
        database_id, ObjectId{table->ParentSchema().oid}, ObjectId{table->oid},
        /*is_new=*/false, table->SearchOptions()));
    }
    // The sequences: replay read each definition mid-log, where the counter
    // records after it had not been folded in yet, and the table a counter is
    // bound onto has to be the version that survived the log.
    for (const auto* sequence :
         connector::DatabaseSequences(nullptr, database_id)) {
      sequence->Counter()->ReloadDurable();
      BindOwnedSequence(nullptr, database_id, *sequence);
    }
    // Before the databases attach: their data WAL replays into the index
    // through GetData(), so the segments have to be open by then.
    std::vector<IndexInfoRef> indexes;
    connector::VisitIndexes(nullptr, database_id,
                            [&](const IndexInfoRef& index) {
                              if (index->IsInverted()) {
                                indexes.push_back(index);
                              }
                            });
    for (const auto& index : indexes) {
      index->SetData(search::InvertedIndexStorage::Create(
        database_id, InvertedInfo(*index), /*is_new=*/false));
    }
  }
}

void Catalog::FinalizeLoad() {
  absl::MutexLock lock{&_mutex};
  // Boot's closing pass, over a catalog nobody else can reach yet: the state
  // that hangs off a definition rather than living in it cannot be bound
  // mid-log, because a later record may drop the object it belongs to.
  SDB_ASSERT(_loading);
  OpenBootStorage();
  _loading = false;
}

namespace {

// A drop still open at boot, rebuilt from the record that opened it. The
// subtree the record carries is flat and keyed by parent, so each level picks
// up its own children by id. Nothing here reads a definition: the reclamation
// travels with the record, exactly as the store operations do.
class DropRecovery {
 public:
  explicit DropRecovery(const wal::DropPrepare& drop) : _drop{drop} {
    if (!drop.subtree) {
      return;
    }
    for (const auto& node : *drop.subtree) {
      _children[node.parent_id].push_back(&node);
    }
  }

  std::shared_ptr<DropTask> Build() const {
    switch (_drop.type) {
      case duckdb::CatalogType::DATABASE_ENTRY:
        return BuildDatabase(_drop.id);
      case duckdb::CatalogType::SCHEMA_ENTRY:
        return BuildSchema(_drop.database_id, _drop.id, true);
      case duckdb::CatalogType::TABLE_ENTRY:
        return BuildTable(_drop.database_id, _drop.schema_id, _drop.id,
                          RootEngine(), true);
      case duckdb::CatalogType::INDEX_ENTRY:
        return std::make_shared<IndexDrop>(_drop.id, _drop.inverted,
                                           _drop.database_id, _drop.schema_id,
                                           _drop.parent_id, true);
      default:
        SDB_FATAL(STARTUP, "catalog: open drop of ",
                  duckdb::CatalogTypeToString(_drop.type),
                  " cannot be reclaimed");
    }
  }

 private:
  std::span<const wal::DropNode* const> ChildrenOf(ObjectId parent) const {
    const auto it = _children.find(parent);
    if (it == _children.end()) {
      return {};
    }
    return it->second;
  }

  TableEngine RootEngine() const {
    for (const auto* node : ChildrenOf(_drop.parent_id)) {
      if (node->id == _drop.id) {
        return node->engine;
      }
    }
    return TableEngine::Transactional;
  }

  std::shared_ptr<DatabaseDrop> BuildDatabase(ObjectId db_id) const {
    std::vector<std::shared_ptr<SchemaDrop>> schemas;
    for (const auto* node : ChildrenOf(db_id)) {
      if (node->type == duckdb::CatalogType::SCHEMA_ENTRY) {
        schemas.push_back(BuildSchema(db_id, node->id, false));
      }
    }
    return std::make_shared<DatabaseDrop>(db_id, std::move(schemas));
  }

  std::shared_ptr<SchemaDrop> BuildSchema(ObjectId db_id, ObjectId schema_id,
                                          bool is_root) const {
    std::vector<std::shared_ptr<TableDropBase>> tables;
    std::vector<ObjectId> sequences;
    for (const auto* node : ChildrenOf(schema_id)) {
      if (node->type == duckdb::CatalogType::TABLE_ENTRY) {
        tables.push_back(
          BuildTable(db_id, schema_id, node->id, node->engine, false));
      } else if (node->type == duckdb::CatalogType::SEQUENCE_ENTRY) {
        sequences.push_back(node->id);
      }
    }
    return std::make_shared<SchemaDrop>(schema_id, std::move(tables),
                                        std::move(sequences), db_id, is_root);
  }

  std::shared_ptr<TableDropBase> BuildTable(ObjectId db_id, ObjectId schema_id,
                                            ObjectId table_id,
                                            TableEngine engine,
                                            bool is_root) const {
    std::vector<ObjectId> owned;
    std::vector<std::shared_ptr<IndexDrop>> indexes;
    for (const auto* node : ChildrenOf(table_id)) {
      if (node->type == duckdb::CatalogType::SEQUENCE_ENTRY) {
        owned.push_back(node->id);
      } else if (node->type == duckdb::CatalogType::INDEX_ENTRY) {
        indexes.push_back(std::make_shared<IndexDrop>(
          node->id, node->inverted, db_id, schema_id, table_id, false));
      }
    }
    // Search tables reject CREATE INDEX, so they carry no child index drops.
    if (engine == TableEngine::Search) {
      return std::make_shared<SearchTableDrop>(
        table_id, db_id, std::move(owned), schema_id, is_root);
    }
    return std::make_shared<TableDrop>(table_id, db_id, std::move(indexes),
                                       std::move(owned), schema_id, is_root);
  }

  const wal::DropPrepare& _drop;
  containers::FlatHashMap<ObjectId, std::vector<const wal::DropNode*>>
    _children;
};

// Driven by the records rather than by the definitions they removed: the
// record carries the whole subtree the drop has to sweep.
void ScheduleOpenDrops() {
  auto& store = GetCatalogStore();
  for (const auto id : store.AllOpenDrops()) {
    const auto drop = store.OpenDrop(id);
    SDB_ASSERT(drop);
    if (!drop) {
      continue;
    }
    DropTask::Schedule(DropRecovery{*drop}.Build()).Detach();
  }
}

// The first boot of a data directory: the log is empty, so the database every
// connection defaults to has to be written before anything can be created in
// it. Not through Catalog::CreateDatabase -- there is no role to check a
// privilege against yet, and the id is fixed rather than allocated.
void EnsureSystemDatabase(Catalog& catalog) {
  if (connector::FindDatabase(nullptr, id::kSystemDB)) {
    SDB_TRACE(STARTUP, "Found system database");
    return;
  }
  // One frame, as in Catalog::CreateDatabase: a first boot that crashed between
  // two appends would come back with a database and no public schema.
  std::vector<wal::Entry> entries;
  entries.emplace_back(
    wal::PutEntry{.parent_id = id::kInstance,
                  .type = duckdb::CatalogType::DATABASE_ENTRY,
                  .id = id::kSystemDB,
                  .mode = wal::PutMode::Create,
                  .info = std::make_shared<CreateDatabaseInfo>(
                    id::kSystemDB, StaticStrings::kDefaultDatabase),
                  .perm = Permissions{id::kRootUser}});
  const auto schema_id = NextId();
  entries.emplace_back(wal::PutEntry{
    .parent_id = id::kSystemDB,
    .type = duckdb::CatalogType::SCHEMA_ENTRY,
    .id = schema_id,
    .mode = wal::PutMode::Create,
    .info =
      catalog::MakeSchemaInfo(schema_id, id::kSystemDB, StaticStrings::kPublic),
    .perm = Permissions{id::kRootUser}});
  // The records are the intent, so they go to the log and through the
  // applier -- not built twice, once for each.
  GetCatalogStore().WriteFrame(entries);
  catalog.ReplayRecords(entries);
}

}  // namespace
namespace {

std::shared_ptr<Catalog> gCatalog;

// What boot does with a database whose data file is gone or will not open. Not
// a state that occurs on its own: somebody removed or corrupted a file.
enum class MissingDatabase : uint8_t {
  Refuse,
  Skip,
  Drop,
};

MissingDatabase ParseMissingDatabasePolicy() {
  const auto value = absl::GetFlag(FLAGS_missing_database);
  if (value == "refuse") {
    return MissingDatabase::Refuse;
  }
  if (value == "skip") {
    return MissingDatabase::Skip;
  }
  if (value == "drop") {
    return MissingDatabase::Drop;
  }
  SDB_FATAL(STARTUP, "--missing_database must be refuse, skip or drop, not '",
            value, "'");
}

void ReportUnusableDatabase(const connector::DatabaseRef& db,
                            std::string_view reason, MissingDatabase policy) {
  switch (policy) {
    case MissingDatabase::Refuse:
      SDB_FATAL(STARTUP, "database '", db.Name(), "' (id ", db.Id().id(),
                ") cannot be opened: ", reason,
                ". Pass --missing_database=skip to leave it unattached or "
                "--missing_database=drop to remove it from the catalog.");
    case MissingDatabase::Skip:
      SDB_WARN(STARTUP, "database '", db.Name(), "' is not attached: ", reason);
      return;
    case MissingDatabase::Drop:
      SDB_WARN(STARTUP, "dropping database '", db.Name(),
               "' from the catalog: ", reason);
      return;
  }
}

// A missing file is a loss only when the catalog says there was something in
// it. An empty database whose file has not been created yet is the ordinary
// first-boot shape, and recreating it loses nothing.
bool DatabaseFileUsable(const connector::DatabaseRef& db,
                        MissingDatabase policy) {
  const auto path = CatalogStore::DatabaseFilePath(db.Id());
  std::error_code ec;
  if (std::filesystem::exists(path, ec)) {
    return true;
  }
  bool has_content = false;
  connector::VisitTables(
    nullptr, db.Id(),
    [&](const TableInfoRef&, const Permissions&) { has_content = true; });
  connector::VisitViews(nullptr, db.Id(), [&](const duckdb::ViewCatalogEntry&) {
    has_content = true;
  });
  if (!has_content) {
    return true;
  }
  ReportUnusableDatabase(db, absl::StrCat("'", path, "' does not exist"),
                         policy);
  return false;
}

// Brings every attached database up to the catalog log tail. The catalog
// commits first, so a database's committed position is behind exactly when a
// crash landed between the two halves; the work is the frames in between and
// nothing else -- one number per database in the clean case, which is why this
// costs nothing on a boot that has no gap.
void ReplayCatalogGaps() {
  const auto begin = std::chrono::steady_clock::now();
  auto& store = GetCatalogStore();
  std::vector<connector::DatabaseRef> databases;
  connector::VisitDatabases(nullptr, [&](const connector::DatabaseRef& db) {
    databases.push_back(db);
  });
  std::vector<ObjectId> live;
  live.reserve(databases.size());
  for (const auto& db : databases) {
    live.push_back(db.Id());
  }
  store.ForgetUnackedExcept(live);
  for (const auto& db : databases) {
    auto attachment = TryStoreDatabase(db.Id());
    if (!attachment || !attachment->HasStorageManager()) {
      store.AckDatabasePosition(db.Id(), UINT64_MAX);
      continue;
    }
    const auto committed = attachment->GetStorageManager().GetCatalogPosition();
    auto pending = store.PendingFor(db.Id(), committed);
    if (pending.empty()) {
      store.AckDatabasePosition(db.Id(), committed);
      GetDataStore().RebuildMissingIndexes(db.Id());
      continue;
    }
    SDB_INFO(STARTUP, "database '", db.Name(), "' is at catalog position ",
             committed, ", replaying ", pending.size(),
             " batch(es) up to position ", pending.back().position);
    for (const auto& batch : pending) {
      if (auto r =
            GetDataStore().ApplyStoreOps(nullptr, *batch.ops, batch.position);
          !r.ok()) {
        SDB_FATAL(STARTUP, "database '", db.Name(),
                  "': replaying catalog position ", batch.position,
                  " failed: ", r.message());
      }
      store.AckDatabasePosition(db.Id(), batch.position);
    }
    // What the replay built is in memory only: it ran without a transaction, so
    // nothing named it in the data WAL, and the fold below drops the very ops
    // that would rebuild it. A checkpoint puts it in the file, and carries the
    // position it came from into the header the next boot compares against.
    GetDataStore().RebuildMissingIndexes(db.Id());
    auto& storage = attachment->GetStorageManager();
    storage.SetCatalogPosition(pending.back().position);
    duckdb::CheckpointOptions options;
    options.action = duckdb::CheckpointAction::ALWAYS_CHECKPOINT;
    storage.CreateCheckpoint(duckdb::QueryContext{}, options);
  }
  SDB_INFO(STARTUP, "catalog position checked for ", live.size(),
           " database(s) against log tail ", store.LogPosition(), " in ",
           absl::FormatDuration(
             absl::FromChrono(std::chrono::steady_clock::now() - begin)));
  // The gap is closed, so a log the outstanding work was holding open can fold.
  store.TryCompact();
}

}  // namespace

void InitCatalog() {
  gCatalog = std::make_shared<Catalog>();

  // Before the roles are read and long before any database is attached: a
  // cluster-global write must never run without the attachment it belongs to.
  connector::AttachGlobalDatabase();

  // The catalog log, replayed straight into the CatalogSets that hold it. A
  // database record attaches its database on the spot -- catalog only, no file
  // -- so every record after it lands in a real set. No data file is open yet
  // and no data WAL has been replayed: that is the last boot step.
  try {
    GetCatalogStore().Replay([](std::span<const wal::Entry> entries) {
      GetCatalog().ReplayRecords(entries);
    });
  } catch (const SqlException& e) {
    SDB_FATAL(GENERAL, "Failed to read the catalog log, ", e.message());
  }
  EnsureSystemDatabase(GetCatalog());

  bool has_roles = false;
  connector::VisitRoles(nullptr,
                        [&](const CreateRoleInfo&) { has_roles = true; });
  if (!has_roles) {
    std::string initial_verifier;
    if (const char* pw = std::getenv("POSTGRES_PASSWORD");
        pw != nullptr && *pw != '\0') {
      auto verifier = network::BuildScramVerifierString(pw);
      if (!verifier) {
        SDB_FATAL(GENERAL,
                  "could not derive a password verifier from "
                  "POSTGRES_PASSWORD");
      }
      initial_verifier = std::move(*verifier);
      SDB_INFO(GENERAL, "bootstrap: initial password set for role '",
               StaticStrings::kDefaultUser, "' from POSTGRES_PASSWORD");
    }
    auto root = std::make_shared<CreateRoleInfo>(
      id::kRootUser, persistence::RoleData{
                       .name = std::string{StaticStrings::kDefaultUser},
                       .options = static_cast<uint32_t>(RoleOption::All),
                       .conn_limit = CreateRoleInfo::kNoConnLimit,
                       .valid_until = CreateRoleInfo::kNoValidUntil,
                       .password_verifier = {std::move(initial_verifier)},
                     });
    GetCatalog().CreateRole(NoAccessCheck(), std::move(root));
  }

  ScheduleOpenDrops();

  GetCatalog().FinalizeLoad();

  if (!catalog::GetDatabaseId(StaticStrings::kDefaultDatabase).isSet()) {
    SDB_FATAL(GENERAL, "No ", StaticStrings::kDefaultDatabase,
              " database found in database directory");
  }

  // A data file whose id no committed catalog record names is garbage: the
  // create crashed between the file operation and the catalog append, or the
  // drop crashed after it. Ids are never reissued, so the file can only ever
  // be unreachable. Reclaim before attaching, so nothing opens one.
  {
    for (const auto id : CatalogStore::DatabaseFileIds()) {
      if (connector::FindDatabase(nullptr, id)) {
        continue;
      }
      const auto path = CatalogStore::DatabaseFilePath(id);
      SDB_INFO(STARTUP, "reclaiming orphaned database file '", path, "'");
      for (const auto& name : {path, path + ".wal"}) {
        std::error_code ec;
        std::filesystem::remove(name, ec);
        if (ec) {
          SDB_WARN(STARTUP, "could not remove '", name, "': ", ec.message());
        }
      }
    }
  }

  // The data half of every attachment: the catalog log already made each one
  // and filled its sets, so this opens the file and replays the data WAL into
  // inverted indexes OpenBootStorage has already injected.
  {
    const auto attach_begin = std::chrono::steady_clock::now();
    const auto missing_policy = ParseMissingDatabasePolicy();
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    std::vector<connector::DatabaseRef> databases;
    connector::VisitDatabases(nullptr, [&](const connector::DatabaseRef& db) {
      databases.push_back(db);
    });
    std::vector<connector::DatabaseRef> unusable;
    for (const auto& db : databases) {
      if (!DatabaseFileUsable(db, missing_policy)) {
        unusable.push_back(db);
        continue;
      }
      try {
        connector::LoadDatabaseStorage(db.Name());
      } catch (const std::exception& e) {
        ReportUnusableDatabase(db, e.what(), missing_policy);
        unusable.push_back(db);
      }
    }
    for (const auto& db : unusable) {
      // The attachment goes whatever the policy is: nothing may reach a
      // database whose rows could not be opened.
      connector::DiscardDatabaseAttachment(db.Name());
      if (missing_policy == MissingDatabase::Drop) {
        GetCatalog().DropDatabase(NoAccessCheck(), std::string{db.Name()},
                                  nullptr);
      }
    }
    // DuckDB always has a main database, so an unused in-memory "memory" one
    // exists until something is attached; the default database supersedes it.
    // The default has to move first -- DETACH refuses the default database,
    // and detaching does not repoint it, so a connection with no search path
    // would resolve a name that is gone.
    duckdb::DatabaseManager::Get(*conn->context)
      .SetDefaultDatabase(*conn->context,
                          std::string{StaticStrings::kDefaultDatabase});
    if (auto result = conn->Query("DETACH \"memory\""); result->HasError()) {
      SDB_WARN(STARTUP, "could not detach the initial in-memory database: ",
               result->GetError());
    }
    SDB_INFO(STARTUP, "database storage loaded in ",
             absl::FormatDuration(absl::FromChrono(
               std::chrono::steady_clock::now() - attach_begin)));
  }
  GetDataStore().MarkReady();

  // Now that every database is attached and its entries are in their sets, the
  // tables a foreign key points at can be rebuilt to carry the referenced half
  // of that key -- the half a DELETE checks children with, and one a parent
  // placed ahead of its child could not see.
  {
    std::vector<ObjectId> databases;
    connector::VisitDatabases(nullptr, [&](const connector::DatabaseRef& ref) {
      databases.push_back(ref.Id());
    });
    for (const auto database_id : databases) {
      // Contextless, so the rebuilt entries are committed outright: boot has
      // no statement of its own and nobody else can reach the catalog yet.
      connector::RefreshForeignKeyReferents(nullptr, database_id);
    }
  }

  // After MarkReady: a replayed CREATE INDEX builds its store-side index
  // through the bind contexts, which are gated on the store being up.
  ReplayCatalogGaps();

  // Re-attach persisted foreign servers (external DBs: clickhouse/postgres) so
  // they survive restart, the same way the databases above do. Unlike a local
  // database, a remote being unreachable must NOT abort startup -- warn and
  // continue; the server stays defined and a later access will surface it.
  {
    // Collected before anything is attached: the ATTACH runs a whole statement
    // on a fresh connection, and the walk that found these is holding the set
    // it came out of.
    std::vector<HeldForeignServer> servers;
    std::vector<ObjectId> databases;
    connector::VisitDatabases(nullptr, [&](const connector::DatabaseRef& db) {
      databases.push_back(db.Id());
    });
    for (const auto database_id : databases) {
      auto of_database =
        connector::DatabaseForeignServers(nullptr, database_id);
      servers.insert(servers.end(),
                     std::make_move_iterator(of_database.begin()),
                     std::make_move_iterator(of_database.end()));
    }
    auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
    for (const auto& [server, perm] : servers) {
      auto res = RunForeignServerAttach(*conn, *server);
      if (res.status == ForeignServerAttachResult::Status::Failed) {
        SDB_WARN(GENERAL, "Failed to re-attach foreign server ",
                 server->GetName(), ": ", res.error);
      }
    }
  }
}

void ShutdownCatalog() { gCatalog.reset(); }

ObjectId GetDatabaseId(std::string_view name) {
  return connector::FindDatabase(nullptr, name).Id();
}

Catalog& GetCatalog() {
  SDB_ASSERT(gCatalog, "Catalog is not initialized");
  return *gCatalog;
}

Catalog* TryGetCatalog() { return gCatalog.get(); }

}  // namespace sdb::catalog
