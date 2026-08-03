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
#include <ranges>
#include <algorithm>
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
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
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
  if (logical.empty() ? closure.CanAnyColumn(table, need)
                      : closure.CanColumns(
                          table, need, [&](uint64_t i, const catalog::Column&) {
                            return logical.contains(i);
                          })) {
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
  const auto& closure = snapshot.ClosureFor(caller);
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
  for (const auto& [req, obj] : std::views::zip(reqs, objects)) {
    if (obj && obj->GetType() == catalog::ObjectType::Table &&
        Has(req.verb, duckdb::AccessVerb::INSERT | duckdb::AccessVerb::UPDATE |
                        duckdb::AccessVerb::DELETE |
                        duckdb::AccessVerb::TRUNCATE)) {
      targets.insert(obj->GetId().id());
    }
  }
  return targets;
}

// Row-level security. Everything below turns the policies on a relation into
// the plan rewrite that enforces them; CollectAndEnforce calls Enforce() once
// privileges are settled, and GuardTruncate() from its TRUNCATE branch.
namespace rls {


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

duckdb::unique_ptr<duckdb::Expression> BoolConst(bool value) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BOOLEAN(value));
}

duckdb::unique_ptr<duckdb::Expression> UnpushableMarker(
  duckdb::unique_ptr<duckdb::Expression> anchor) {
  static const duckdb::ScalarFunction kFn = [] {
    duckdb::ScalarFunction fn(
      "sdb_rls_barrier", {duckdb::LogicalType::ANY},
      duckdb::LogicalType::BOOLEAN,
      [](duckdb::DataChunk&, duckdb::ExpressionState&, duckdb::Vector& result) {
        result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
        duckdb::ConstantVector::GetData<bool>(result)[0] = true;
      });
    fn.SetStability(duckdb::FunctionStability::VOLATILE);
    return fn;
  }();
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> args;
  args.push_back(std::move(anchor));
  return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction{kFn}, std::move(args), nullptr);
}

// A column the policy predicate already reads. The marker takes it as an
// argument so the expression is bound to this scan: a nullary marker is a
// free-floating constant predicate, and a join will adopt it as its join
// condition, which lifts it out of the scan's filter list -- the one place it
// has to stay.
duckdb::unique_ptr<duckdb::Expression> AnchorColumn(
  const duckdb::Expression& policy) {
  duckdb::unique_ptr<duckdb::Expression> anchor;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    policy, [&](const duckdb::BoundColumnRefExpression& col) {
      if (!anchor) {
        anchor = col.Copy();
      }
    });
  return anchor;
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

struct Session {
  const catalog::Snapshot& snapshot;
  ObjectId caller;
};

struct Context {
  const catalog::Table* table = nullptr;
  const auth::RoleClosure* closure = nullptr;
};

std::optional<Context> Resolve(const Session& session,
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

  const auto role = EffectiveRole(session.caller, who);
  const auto& closure = session.snapshot.ClosureFor(role);
  if (closure.Has(catalog::RoleOption::Superuser |
                  catalog::RoleOption::BypassRls) ||
      (!rls.forced && closure.Owns(table))) {
    return std::nullopt;
  }
  return Context{&table, &closure};
}

duckdb::unique_ptr<duckdb::Expression> CombinePolicies(
  const Session& session, const Context& rls, PolicyCommand verb,
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

// CheckBinder exists for CHECK constraints on a table definition, where a bare
// identifier is always a column -- so it routes every column ref to
// BindCheckColumn, and a policy naming current_user dies there. Bare names that
// are not columns go to the generic binder instead, which reaches duckdb's own
// niladic-function table. The target type is BOOLEAN rather than CheckBinder's
// INTEGER, so the result needs no cast back.
class PolicyCheckBinder : public duckdb::CheckBinder {
 public:
  PolicyCheckBinder(duckdb::Binder& binder, duckdb::ClientContext& context,
                    duckdb::Identifier table, const duckdb::ColumnList& columns,
                    duckdb::physical_index_set_t& bound_columns)
    : CheckBinder(binder, context, std::move(table), columns, bound_columns) {
    target_type = duckdb::LogicalType::BOOLEAN;
  }

 protected:
  duckdb::BindResult BindExpression(
    duckdb::unique_ptr<duckdb::ParsedExpression>& expr_ptr, duckdb::idx_t depth,
    bool root_expression) override {
    const auto& expr = *expr_ptr;
    if (expr.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
      const auto& colref = expr.Cast<duckdb::ColumnRefExpression>();
      if (!colref.IsQualified() &&
          !columns.ColumnExists(colref.GetColumnName())) {
        return duckdb::ExpressionBinder::BindExpression(expr_ptr, depth,
                                                        root_expression);
      }
    }
    return duckdb::CheckBinder::BindExpression(expr_ptr, depth,
                                               root_expression);
  }
};

duckdb::unique_ptr<duckdb::Expression> BindPostImageExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  duckdb::TableCatalogEntry& table, const ColumnExpr& predicate,
  duckdb::physical_index_set_t& bound_columns) {
  auto parsed = predicate.GetExpr().Copy();
  PolicyCheckBinder check_binder(binder, context, table.name,
                                 table.GetColumns(), bound_columns);
  return check_binder.Bind(parsed);
}

duckdb::unique_ptr<duckdb::Expression> ReadFilter(
  const Session& session, duckdb::ClientContext& context,
  duckdb::Binder& binder, const duckdb::TableCatalogEntry& table_entry,
  const duckdb::CatalogEntry* who) {
  auto rls = Resolve(session, binder, table_entry, who);
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
  const Session& session, duckdb::ClientContext& context,
  duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
  PolicyCommand verb, const duckdb::CatalogEntry* who,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>&
    bound_constraints) {
  auto rls = Resolve(session, binder, table_entry, who);
  if (!rls) {
    return;
  }
  duckdb::physical_index_set_t bound_columns;
  auto check = CombinePolicies(
    session, *rls, verb,
    [&](const catalog::Policy& policy) -> duckdb::unique_ptr<duckdb::Expression> {
      if (!policy.HasCheck() && !policy.HasUsing()) {
        return nullptr;
      }
      const auto& predicate =
        policy.HasCheck() ? policy.Check() : policy.Using();
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
  const auto& reqs = binder.GetStatementProperties().access_requirements;
  const auto it = std::ranges::find(reqs, table_index,
                                    &duckdb::AccessRequirement::table_index);
  return it == reqs.end() ? nullptr : &*it;
}

const duckdb::CatalogEntry* DefinerFor(duckdb::Binder& binder,
                                       duckdb::idx_t table_index) {
  const auto* req = RequirementFor(binder, table_index);
  return req ? req->who : nullptr;
}

void Rewrite(duckdb::unique_ptr<duckdb::LogicalOperator>& op,
                 const Session& session, duckdb::ClientContext& context,
                 duckdb::Binder& binder) {
  for (auto& child : op->children) {
    Rewrite(child, session, context, binder);
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
      if (auto anchor = AnchorColumn(*filter)) {
        ExprList guarded;
        guarded.push_back(std::move(filter));
        guarded.push_back(UnpushableMarker(std::move(anchor)));
        filter = Combine(std::move(guarded),
                         duckdb::ExpressionType::CONJUNCTION_AND);
      }
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
      const auto performs = [&](duckdb::MergeActionType type) {
        return std::ranges::any_of(merge.actions, [&](const auto& entry) {
          return std::ranges::any_of(entry.second, [&](const auto& action) {
            return action->action_type == type;
          });
        });
      };
      for (const auto& [type, verb] :
           {std::pair{duckdb::MergeActionType::MERGE_INSERT,
                      PolicyCommand::Insert},
            std::pair{duckdb::MergeActionType::MERGE_UPDATE,
                      PolicyCommand::Update}}) {
        if (performs(type)) {
          AppendWriteCheck(session, context, binder, merge.table, verb,
                           /*who=*/nullptr, merge.bound_constraints);
        }
      }
      return;
    }
    default:
      return;
  }
}


void Enforce(duckdb::ClientContext& context, duckdb::Binder& binder,
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
  const Session session{*snapshot, conn->GetRoleId()};
  Rewrite(plan, session, context, binder);
}

void GuardTruncate(const catalog::Snapshot& snapshot,
                      const catalog::Table& table, ObjectId role) {
  const auto rls = snapshot.GetRowSecurity(table.GetId());
  if (!rls.enabled) {
    return;
  }
  const auto& closure = snapshot.ClosureFor(role);
  if (closure.Has(catalog::RoleOption::Superuser |
                  catalog::RoleOption::BypassRls) ||
      (!rls.forced && closure.Owns(table))) {
    return;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied to truncate table ", table.GetName()),
    ERR_DETAIL("row-level security is enabled and TRUNCATE cannot be filtered "
               "by policy; delete the rows instead"));
}

}  // namespace rls

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
      rls::GuardTruncate(*snapshot, t, role);
    }
    if (Has(req.verb, duckdb::AccessVerb::SELECT)) {
      // A DML's own-target scan reads no column, so needs no SELECT (PG);
      // count(*) also has an empty read set but is not a write target.
      if (!req.read.empty() || !write_targets.contains(t.GetId().id())) {
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
  rls::Enforce(context, binder, plan);
}
}  // namespace

void RegisterRbacAccessCheck(duckdb::DatabaseInstance& db) {
  db.config.access_check_function = &CollectAndEnforce;
}

}  // namespace sdb::optimizer
