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

#include "connector/optimizer/rbac.h"

#include <absl/functional/function_ref.h>
#include <cstdint>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/catalog_type.hpp>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/expression/bound_case_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <duckdb/planner/expression_binder/where_binder.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/foreign_server.h"
#include "catalog/object.h"
#include "catalog/policy.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_system_table_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_view_entry.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::optimizer {
namespace {

using PolicyCommand = catalog::persistence::PolicyCommand;

bool PolicyGoverns(PolicyCommand cmd, PolicyCommand verb) {
  return cmd == PolicyCommand::All || cmd == verb;
}

bool PolicyAppliesTo(const catalog::Policy& policy,
                     const auth::RoleClosure& closure) {
  if (policy.AppliesToPublic()) {
    return true;
  }
  for (auto role_id : policy.Roles()) {
    if (closure.MemberOf(role_id)) {
      return true;
    }
  }
  return false;
}

ObjectId EffectiveRlsRole(const duckdb::CatalogEntry* who, ObjectId caller) {
  if (const auto* view = dynamic_cast<const connector::SereneDBViewEntry*>(who)) {
    return view->GetSereneDBView()->GetOwner();
  }
  return caller;
}

bool BypassesRls(const auth::RoleClosure& closure, const catalog::Table& table,
                 bool forced) {
  // is_superuser is tested explicitly, not left to Owns() folding it in: FORCE
  // ROW LEVEL SECURITY subjects the owner to policies, but never a superuser.
  return closure.IsSuperuser() || closure.Has(catalog::RoleOption::BypassRls) ||
         (!forced && closure.Owns(table));
}

duckdb::unique_ptr<duckdb::Expression> BoolConst(bool value) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BOOLEAN(value));
}

duckdb::unique_ptr<duckdb::Expression> UnpushableMarker() {
  static const duckdb::ScalarFunction kFn = [] {
    duckdb::ScalarFunction fn(
      "sdb_rls_barrier", {}, duckdb::LogicalType::BOOLEAN,
      [](duckdb::DataChunk&, duckdb::ExpressionState&, duckdb::Vector& result) {
        result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
        duckdb::ConstantVector::GetData<bool>(result)[0] = true;
      });
    fn.SetStability(duckdb::FunctionStability::VOLATILE);
    return fn;
  }();
  return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction{kFn},
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>{}, nullptr);
}

using ExprList = duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>;

duckdb::unique_ptr<duckdb::Expression> Combine(ExprList parts,
                                               duckdb::ExpressionType op) {
  if (parts.empty()) {
    return nullptr;
  }
  if (parts.size() == 1) {
    return std::move(parts[0]);
  }
  auto conjunction =
    duckdb::make_uniq<duckdb::BoundConjunctionExpression>(op);
  conjunction->GetChildrenMutable() = std::move(parts);
  return conjunction;
}

struct RlsSession {
  const catalog::Snapshot& snapshot;
  ObjectId caller;
};

struct RlsContext {
  const catalog::Table* table = nullptr;
  const auth::RoleClosure* closure = nullptr;
};

std::optional<RlsContext> ResolveRls(const RlsSession& session,
                                     duckdb::Binder& binder,
                                     const duckdb::TableCatalogEntry& table_entry,
                                     const duckdb::CatalogEntry* who) {
  const auto* facade = dynamic_cast<const connector::SereneDBTableEntry*>(&table_entry);
  if (!facade) {
    return std::nullopt;
  }
  const auto& table = *facade->GetSereneDBTable();
  const auto rls = session.snapshot.GetRowSecurity(table.GetId());
  if (!rls.enabled) {
    return std::nullopt;
  }
  binder.SetAlwaysRequireRebind();

  const auto role = EffectiveRlsRole(who, session.caller);
  const auto& closure = session.snapshot.ClosureFor(role);
  if (BypassesRls(closure, table, rls.forced)) {
    return std::nullopt;
  }
  return RlsContext{&table, &closure};
}

duckdb::unique_ptr<duckdb::Expression> CombinePolicies(
  const RlsSession& session, const RlsContext& rls, PolicyCommand verb,
  absl::FunctionRef<duckdb::unique_ptr<duckdb::Expression>(
    const catalog::Policy&)>
    bind_one) {
  ExprList permissive;
  ExprList restrictive;

  for (auto policy_id : session.snapshot.PolicyIds(rls.table->GetId())) {
    auto policy = session.snapshot.GetObject<catalog::Policy>(policy_id);
    if (!PolicyGoverns(policy->Command(), verb) ||
        !PolicyAppliesTo(*policy, *rls.closure)) {
      continue;
    }
    auto expr = bind_one(*policy);
    if (policy->Permissive()) {
      permissive.push_back(expr ? std::move(expr) : BoolConst(true));
    } else if (expr) {
      restrictive.push_back(std::move(expr));
    }
  }
  if (permissive.empty()) {
    return BoolConst(false);
  }
  restrictive.insert(restrictive.begin(),
                     Combine(std::move(permissive),
                             duckdb::ExpressionType::CONJUNCTION_OR));
  return Combine(std::move(restrictive),
                 duckdb::ExpressionType::CONJUNCTION_AND);
}

duckdb::optional_ptr<duckdb::Binding> TargetBinding(
  duckdb::Binder& binder, const duckdb::TableCatalogEntry& table_entry) {
  duckdb::optional_ptr<duckdb::Binding> found;
  for (const auto& binding : binder.bind_context.GetBindingsList()) {
    if (binding->GetStandardEntry().get() != &table_entry) {
      continue;
    }
    if (found) {
      return nullptr;
    }
    found = binding.get();
  }
  return found;
}

void QualifyColumns(duckdb::ParsedExpression& expr, duckdb::Binding& target) {
  const auto& alias = target.GetBindingAlias();
  if (!alias.IsSet()) {
    return;
  }
  duckdb::ParsedExpressionIterator::VisitExpressionMutable<
    duckdb::ColumnRefExpression>(
    expr, [&](duckdb::ColumnRefExpression& colref) {
      if (colref.IsQualified() ||
          !target.HasMatchingBinding(colref.GetColumnName())) {
        return;
      }
      duckdb::vector<duckdb::Identifier> qualified;
      if (!alias.GetCatalog().empty()) {
        qualified.emplace_back(alias.GetCatalog());
      }
      if (!alias.GetSchema().empty()) {
        qualified.emplace_back(alias.GetSchema());
      }
      qualified.emplace_back(alias.GetAlias());
      qualified.emplace_back(colref.GetColumnName());
      colref.ColumnNamesMutable() = std::move(qualified);
    });
}

bool IsSpecialRegister(const std::string& name) {
  return name == "current_user" || name == "session_user" ||
         name == "current_role" || name == "user" ||
         name == "current_catalog" || name == "current_database";
}

void RewriteSpecialRegisters(duckdb::unique_ptr<duckdb::ParsedExpression>& expr) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    auto& colref = expr->Cast<duckdb::ColumnRefExpression>();
    if (!colref.IsQualified() &&
        IsSpecialRegister(colref.GetColumnName().GetIdentifierName())) {
      duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> args;
      expr = duckdb::make_uniq<duckdb::FunctionExpression>(
        colref.GetColumnName(), std::move(args));
      return;
    }
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    *expr, [](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
      RewriteSpecialRegisters(child);
    });
}

duckdb::unique_ptr<duckdb::Expression> BindVisibilityExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  const duckdb::TableCatalogEntry& table_entry, const ColumnExpr& predicate) {
  auto parsed = predicate.GetExpr().Copy();
  if (auto target = TargetBinding(binder, table_entry)) {
    QualifyColumns(*parsed, *target);
  }
  duckdb::WhereBinder where_binder(binder, context);
  return where_binder.Bind(parsed);
}

duckdb::unique_ptr<duckdb::Expression> BindPostImageExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  duckdb::TableCatalogEntry& table, const ColumnExpr& predicate,
  duckdb::physical_index_set_t& bound_columns) {
  auto parsed = predicate.GetExpr().Copy();
  RewriteSpecialRegisters(parsed);
  duckdb::CheckBinder check_binder(binder, context, table.name,
                                   table.GetColumns(), bound_columns);
  return duckdb::BoundCastExpression::AddDefaultCastToType(
    check_binder.Bind(parsed), duckdb::LogicalType::BOOLEAN);
}

duckdb::unique_ptr<duckdb::Expression> ReadFilter(
  const RlsSession& session, duckdb::ClientContext& context,
  duckdb::Binder& binder, const duckdb::TableCatalogEntry& table_entry,
  const duckdb::CatalogEntry* who) {
  auto rls = ResolveRls(session, binder, table_entry, who);
  if (!rls) {
    return nullptr;
  }
  return CombinePolicies(
    session, *rls, PolicyCommand::Select,
    [&](const catalog::Policy& policy) -> duckdb::unique_ptr<duckdb::Expression> {
      if (!policy.HasUsing()) {
        return nullptr;
      }
      return BindVisibilityExpr(binder, context, table_entry, policy.Using());
    });
}

void AppendWriteCheck(
  const RlsSession& session, duckdb::ClientContext& context,
  duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
  PolicyCommand verb, const duckdb::CatalogEntry* who,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>&
    bound_constraints) {
  auto rls = ResolveRls(session, binder, table_entry, who);
  if (!rls) {
    return;
  }
  duckdb::physical_index_set_t bound_columns;
  auto check = CombinePolicies(
    session, *rls, verb,
    [&](const catalog::Policy& policy) -> duckdb::unique_ptr<duckdb::Expression> {
      const bool has_check = policy.HasCheck();
      if (!has_check && !policy.HasUsing()) {
        return nullptr;
      }
      const auto& predicate = has_check ? policy.Check() : policy.Using();
      return BindPostImageExpr(binder, context, table_entry, predicate,
                               bound_columns);
    });

  check = duckdb::make_uniq<duckdb::BoundCaseExpression>(
    std::move(check),
    duckdb::make_uniq<duckdb::BoundConstantExpression>(
      duckdb::Value::INTEGER(1)),
    duckdb::make_uniq<duckdb::BoundConstantExpression>(
      duckdb::Value::INTEGER(0)));

  auto constraint = duckdb::make_uniq<duckdb::BoundCheckConstraint>();
  constraint->expression = std::move(check);
  constraint->bound_columns = std::move(bound_columns);
  constraint->is_rls = true;
  bound_constraints.push_back(std::move(constraint));
}

const duckdb::AccessRequirement* RequirementFor(duckdb::Binder& binder,
                                                duckdb::idx_t table_index) {
  for (const auto& req : binder.GetStatementProperties().access_requirements) {
    if (req.table_index == table_index) {
      return &req;
    }
  }
  return nullptr;
}

const duckdb::CatalogEntry* DefinerFor(duckdb::Binder& binder,
                                       duckdb::idx_t table_index) {
  const auto* req = RequirementFor(binder, table_index);
  return req ? req->who : nullptr;
}

void RlsOptimize(duckdb::unique_ptr<duckdb::LogicalOperator>& op,
                 const RlsSession& session, duckdb::ClientContext& context,
                 duckdb::Binder& binder) {
  for (auto& child : op->children) {
    RlsOptimize(child, session, context, binder);
  }

  switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_GET: {
      auto& get = op->Cast<duckdb::LogicalGet>();
      const auto* req = RequirementFor(binder, get.table_index.index);
      const auto* table =
        req ? dynamic_cast<const duckdb::TableCatalogEntry*>(req->table)
            : nullptr;
      if (!table) {
        return;
      }
      auto filter = ReadFilter(session, context, binder, *table, req->who);
      if (!filter) {
        return;
      }
      ExprList guarded;
      guarded.push_back(std::move(filter));
      guarded.push_back(UnpushableMarker());
      filter = Combine(std::move(guarded),
                       duckdb::ExpressionType::CONJUNCTION_AND);
      auto logical_filter =
        duckdb::make_uniq<duckdb::LogicalFilter>(std::move(filter));
      logical_filter->AddChild(std::move(op));
      op = std::move(logical_filter);
      return;
    }
    case duckdb::LogicalOperatorType::LOGICAL_INSERT: {
      auto& insert = op->Cast<duckdb::LogicalInsert>();
      AppendWriteCheck(session, context, binder, insert.table,
                       PolicyCommand::Insert,
                       DefinerFor(binder, insert.table_index.index),
                       insert.bound_constraints);
      return;
    }
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE: {
      auto& update = op->Cast<duckdb::LogicalUpdate>();
      AppendWriteCheck(session, context, binder, update.table,
                       PolicyCommand::Update,
                       DefinerFor(binder, update.table_index.index),
                       update.bound_constraints);
      return;
    }
    case duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO: {
      auto& merge = op->Cast<duckdb::LogicalMergeInto>();
      const duckdb::CatalogEntry* who = nullptr;
      bool has_insert = false;
      bool has_update = false;
      for (auto& [condition, actions] : merge.actions) {
        for (auto& action : actions) {
          has_insert |=
            action->action_type == duckdb::MergeActionType::MERGE_INSERT;
          has_update |=
            action->action_type == duckdb::MergeActionType::MERGE_UPDATE;
        }
      }
      if (has_insert) {
        AppendWriteCheck(session, context, binder, merge.table,
                         PolicyCommand::Insert, who,
                         merge.bound_constraints);
      }
      if (has_update) {
        AppendWriteCheck(session, context, binder, merge.table,
                         PolicyCommand::Update, who,
                         merge.bound_constraints);
      }
      return;
    }
    default:
      return;
  }
}


void EnforceRls(duckdb::ClientContext& context, duckdb::Binder& binder,
                duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (!plan) {
    return;
  }
  auto* conn = connector::GetSereneDBContextPtr(context);
  if (!conn) {
    return;
  }
  const auto snapshot = conn->CatalogSnapshot();
  if (!snapshot) {
    return;
  }
  const RlsSession session{*snapshot, conn->GetRoleId()};
  RlsOptimize(plan, session, context, binder);
}

void RlsGuardTruncate(const catalog::Snapshot& snapshot,
                      const catalog::Table& table, ObjectId role) {
  const auto rls = snapshot.GetRowSecurity(table.GetId());
  if (!rls.enabled) {
    return;
  }
  const auto& closure = snapshot.ClosureFor(role);
  if (BypassesRls(closure, table, rls.forced)) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied to truncate table ", table.GetName()),
    ERR_DETAIL("row-level security is enabled and TRUNCATE cannot be filtered "
               "by policy; delete the rows instead"));
}

bool Has(duckdb::AccessVerb verb, duckdb::AccessVerb bit) {
  return (static_cast<uint8_t>(verb) & static_cast<uint8_t>(bit)) != 0;
}

catalog::AclMode AsAclMode(duckdb::AccessVerb verb) {
  static constexpr std::pair<duckdb::AccessVerb, catalog::AclMode> kVerbAcl[]{
    {duckdb::AccessVerb::INSERT, catalog::AclMode::Insert},
    {duckdb::AccessVerb::SELECT, catalog::AclMode::Select},
    {duckdb::AccessVerb::UPDATE, catalog::AclMode::Update},
    {duckdb::AccessVerb::DELETE, catalog::AclMode::Delete},
    {duckdb::AccessVerb::TRUNCATE, catalog::AclMode::Truncate},
  };
  catalog::AclMode mode = catalog::AclMode::NoRights;
  for (const auto& [bit, acl] : kVerbAcl) {
    if (Has(verb, bit)) {
      mode |= acl;
    }
  }
  return mode;
}

bool IsSystemSchema(const duckdb::CatalogEntry& entry) {
  const auto schema = entry.ParentSchema().name.GetIdentifierName();
  return schema == StaticStrings::kPgCatalogSchema ||
         schema == StaticStrings::kInformationSchema;
}

const catalog::Object* SereneDBRelation(const duckdb::CatalogEntry* entry) {
  if (const auto* facade =
        dynamic_cast<const connector::SereneDBTableEntry*>(entry)) {
    return facade->GetSereneDBTable().get();
  }
  if (const auto* view =
        dynamic_cast<const connector::SereneDBViewEntry*>(entry)) {
    return view->GetSereneDBView().get();
  }
  if (const auto* index =
        dynamic_cast<const connector::SereneDBIndexScanEntry*>(entry)) {
    return index->GetIndexedRelation();
  }
  if (const auto* system =
        dynamic_cast<const connector::SystemTableEntry*>(entry)) {
    return &system->GetSystemObject();
  }
  return nullptr;
}

bool IsStoreEntry(const duckdb::CatalogEntry& entry) {
  return entry.ParentCatalog().GetName().GetIdentifierName() ==
         catalog::kStoreDatabaseName;
}

void RequireColumns(const auth::RoleClosure& closure,
                    const catalog::Table& table, catalog::AclMode need,
                    const duckdb::unordered_set<uint64_t>& logical) {
  const bool ok = logical.empty()
                    ? closure.CanAnyColumn(table, need)
                    : closure.CanColumns(
                        table, need, [&](uint64_t i, const catalog::Column&) {
                          return logical.contains(i);
                        });
  if (ok) {
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("permission denied for table ", table.GetName()));
}

ObjectId EffectiveRole(ObjectId caller, const duckdb::CatalogEntry* who) {
  if (const auto* view = SereneDBRelation(who)) {
    return view->GetOwner();
  }
  return caller;
}

using AccessRequirements = duckdb::vector<duckdb::AccessRequirement>;

// A foreign catalog is attached under its foreign server's name, so relations
// behind it need ownership or USAGE on that server. Local catalogs are rejected
// by IS_REMOTE before any lookup; a remote one with no server object is a raw
// ATTACH and ungoverned. The resolve is instance-wide on purpose -- the
// attachment is too, so a session in another database must not slip past -- and
// it walks every database, hence the dedup per alias.
void RequireForeignServerUsage(const catalog::Snapshot& snapshot,
                               ObjectId caller,
                               const AccessRequirements& reqs) {
  containers::FlatHashSet<std::string_view> checked;
  for (const auto& req : reqs) {
    if (!req.table) {
      continue;
    }
    const auto& parent = req.table->ParentCatalog();
    if (!parent.Supports(duckdb::RemoteCapability::IS_REMOTE)) {
      continue;
    }
    const std::string_view catalog = parent.GetName().GetIdentifierName();
    if (!checked.insert(catalog).second) {
      continue;
    }
    const auto server = snapshot.GetForeignServer(catalog);
    if (!server) {
      continue;
    }
    const auto& closure = snapshot.ClosureFor(caller);
    if (!closure.Owns(*server) &&
        !closure.Can(*server, catalog::AclMode::Usage)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for foreign server ", server->GetName()));
    }
  }
}

// The store-side name of a table facade, composed exactly as the store composes
// it. Never the reverse: a store name is not split back into its parts.
std::string StoreNameOf(const duckdb::CatalogEntry& facade) {
  return catalog::StoreTableName(
    facade.ParentCatalog().GetName().GetIdentifierName(),
    facade.ParentSchema().name.GetIdentifierName(),
    facade.name.GetIdentifierName());
}

// The catalog object each requirement grants against, indexed like `reqs`.
//
// A DML plan scans the store-side table, whose entry is a plain DuckTableEntry
// that SereneDBRelation cannot resolve -- so it lands here with no object and
// its privileges would go unchecked. Every such orphan is matched to the table
// facade bound alongside it, whose store name it carries verbatim.
std::vector<const catalog::Object*> CollectRelations(
  const AccessRequirements& reqs) {
  std::vector<const catalog::Object*> objects(reqs.size(), nullptr);
  std::vector<size_t> facades;
  std::vector<size_t> orphans;
  for (size_t i = 0; i < reqs.size(); ++i) {
    const auto* entry = reqs[i].table;
    if (!entry) {
      continue;
    }
    objects[i] = SereneDBRelation(entry);
    if (!objects[i]) {
      if (IsStoreEntry(*entry)) {
        orphans.push_back(i);
      }
    } else if (dynamic_cast<const connector::SereneDBTableEntry*>(entry)) {
      facades.push_back(i);
    }
  }
  // Statements that touch no store table -- everything but DML -- stop here,
  // having composed no names.
  if (orphans.empty()) {
    return objects;
  }
  containers::FlatHashMap<std::string, const catalog::Object*> by_store_name;
  by_store_name.reserve(facades.size());
  for (const size_t i : facades) {
    by_store_name.emplace(StoreNameOf(*reqs[i].table), objects[i]);
  }
  for (const size_t i : orphans) {
    const auto it = by_store_name.find(reqs[i].table->name.GetIdentifierName());
    if (it != by_store_name.end()) {
      objects[i] = it->second;
    }
  }
  return objects;
}

containers::FlatHashSet<uint64_t> CollectWriteTargets(
  const AccessRequirements& reqs,
  const std::vector<const catalog::Object*>& objects) {
  containers::FlatHashSet<uint64_t> targets;
  for (size_t i = 0; i < reqs.size(); ++i) {
    const catalog::Object* obj = objects[i];
    if (obj && obj->GetType() == catalog::ObjectType::Table &&
        Has(reqs[i].verb,
            duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
              duckdb::AccessVerb::DELETE | duckdb::AccessVerb::TRUNCATE)) {
      targets.insert(obj->GetId().id());
    }
  }
  return targets;
}

void CollectAndEnforce(duckdb::ClientContext& context, duckdb::Binder& binder,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  auto state = context.registered_state->Get<connector::SereneDBClientState>(
    connector::kSereneDBClientStateKey);
  if (!state) {
    return;
  }
  auto& ctx = state->GetConnectionContext();
  const auto snapshot = ctx.CatalogSnapshot();
  const auto caller = ctx.GetRoleId();

  const auto& properties = binder.GetStatementProperties();
  const auto& reqs = properties.access_requirements;

  RequireForeignServerUsage(*snapshot, caller, reqs);

  const auto objects = CollectRelations(reqs);
  const auto write_targets = CollectWriteTargets(reqs, objects);

  for (size_t i = 0; i < reqs.size(); ++i) {
    const auto& req = reqs[i];
    const catalog::Object* obj = objects[i];

    if (!obj) {
      continue;
    }

    const ObjectId role = EffectiveRole(caller, req.who);
    const auto& closure = snapshot->ClosureFor(role);

    // System relations (and views) are read-only from the caller's side: a
    // single SELECT check on the object's ACL, no columns or DML.
    if (req.table && IsSystemSchema(*req.table)) {
      if (!closure.Can(*obj, catalog::AclMode::Select)) {
        const char* kind =
          req.table->type == duckdb::CatalogType::VIEW_ENTRY ? "view" : "table";
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for ", kind, " ",
                                req.table->name.GetIdentifierName()));
      }
      continue;
    }

    if (obj->GetType() == catalog::ObjectType::View) {
      if (!closure.Can(*obj, catalog::AclMode::Select)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("permission denied for view ", obj->GetName()));
      }
      continue;
    }

    const auto& t = basics::downCast<catalog::Table>(*obj);

    const auto del = AsAclMode(req.verb) &
                     (catalog::AclMode::Delete | catalog::AclMode::Truncate);
    if (del != catalog::AclMode::NoRights && !closure.Can(t, del)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied for table ", t.GetName()));
    }
    // Row-level security cannot filter a TRUNCATE, so it is refused outright for
    // a role the policies apply to. Checked here, on the statement's own access
    // inventory, so no write path can reach the table without being considered.
    if (Has(req.verb, duckdb::AccessVerb::TRUNCATE)) {
      RlsGuardTruncate(*snapshot, t, role);
    }
    if (Has(req.verb, duckdb::AccessVerb::SELECT)) {
      // A DML's own-target scan reads no column, so needs no SELECT (PG);
      // count(*) also has an empty read set but is not a write target.
      const bool bare_dml_scan =
        req.read.empty() && write_targets.contains(t.GetId().id());
      if (!bare_dml_scan) {
        RequireColumns(closure, t, catalog::AclMode::Select, req.read);
      }
    }
    if (Has(req.verb, duckdb::AccessVerb::UPDATE)) {
      RequireColumns(closure, t, catalog::AclMode::Update, req.write);
    }
    if (Has(req.verb, duckdb::AccessVerb::INSERT)) {
      RequireColumns(closure, t, catalog::AclMode::Insert, req.write);
    }
  }

  // Row-level security rewrites the plan once privileges are settled. It runs
  // here rather than as an OptimizerExtension because this seam is mandatory:
  // SET enable_optimizer=false skips the optimizer entirely, and an
  // always_require_rebind plan reports RequireOptimizer()==false, either of
  // which would silently disable enforcement.
  EnforceRls(context, binder, plan);
}

}  // namespace

void RegisterRbacAccessCheck(duckdb::DatabaseInstance& db) {
  db.config.access_check_function = &CollectAndEnforce;
}

}  // namespace sdb::optimizer
