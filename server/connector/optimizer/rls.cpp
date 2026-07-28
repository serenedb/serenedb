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

#include "connector/optimizer/rls.h"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/expression/bound_case_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <duckdb/planner/expression_binder/where_binder.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/policy.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_view_entry.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

// The role RLS policies are evaluated as. A definer-rights view shifts the
// effective principal to the view's owner (PG: a plain view accesses base
// relations, and thus applies their RLS, as the view owner); an invoker view
// (security_invoker=true) or a direct query keeps the connection's caller.
// Mirrors the RBAC EffectiveRole fold, driven by Binder::EffectiveDefiner().
ObjectId EffectiveRlsRole(const duckdb::CatalogEntry* who, ObjectId caller) {
  if (const auto* view = dynamic_cast<const SereneDBViewEntry*>(who)) {
    return view->GetSereneDBView()->GetOwner();
  }
  return caller;
}

ObjectId EffectiveRlsRole(duckdb::Binder& binder, ObjectId caller) {
  return EffectiveRlsRole(binder.EffectiveDefiner().get(), caller);
}

// A policy applies to the acting role when it targets PUBLIC (empty role list)
// or when the acting role is a member of one of the named roles.
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

// SELECT reads are gated by ALL and SELECT policies (PG: a SELECT sees SELECT +
// ALL policies; the read-portion of UPDATE/DELETE also uses SELECT/ALL).
bool PolicyGovernsRead(catalog::persistence::PolicyCommand cmd) {
  return cmd == catalog::persistence::PolicyCommand::All ||
         cmd == catalog::persistence::PolicyCommand::Select;
}

// WITH CHECK on a write is gated by ALL policies plus the matching write verb.
bool PolicyGovernsWrite(catalog::persistence::PolicyCommand cmd, bool is_update) {
  using PC = catalog::persistence::PolicyCommand;
  if (cmd == PC::All) {
    return true;
  }
  return is_update ? cmd == PC::Update : cmd == PC::Insert;
}

// Whether `role` bypasses RLS on `table`: superuser and BYPASSRLS always do, and
// the owner does unless the table is FORCE ROW LEVEL SECURITY.
bool BypassesRls(const catalog::Snapshot& snapshot,
                 const auth::RoleClosure& closure, ObjectId role,
                 const catalog::Table& table, bool forced) {
  if (closure.is_superuser) {
    return true;
  }
  if (auto acting = snapshot.GetObject<catalog::Role>(role);
      acting && acting->Has(catalog::RoleOption::BypassRls)) {
    return true;
  }
  return !forced && closure.Owns(table);
}

// The binding of the target table in the binder's context, or null when it is
// absent or ambiguous (the same table joined to itself).
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

// Qualify each unqualified reference to a target-table column with the target
// table's alias. A policy predicate is scoped to its own table, so it must not
// bind to a same-named column of a table joined alongside it (UPDATE ... FROM,
// DELETE ... USING). A bare name that is not one of the table's columns -- e.g.
// current_user -- is left alone.
void QualifyColumns(duckdb::ParsedExpression& expr, duckdb::Binding& target) {
  const auto& alias = target.GetBindingAlias();
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    auto& colref = expr.Cast<duckdb::ColumnRefExpression>();
    if (!colref.IsQualified() && alias.IsSet() &&
        target.HasMatchingBinding(colref.GetColumnName())) {
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
    }
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr,
    [&](duckdb::ParsedExpression& child) { QualifyColumns(child, target); });
}

// Parse and bind a single stored USING predicate against the target table's
// columns. Returns null on parse/bind failure (the caller then falls back to
// deny-all).
duckdb::unique_ptr<duckdb::Expression> BindPolicyExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  const duckdb::TableCatalogEntry& table_entry, const std::string& text) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
  try {
    parsed = duckdb::Parser::ParseExpressionList(text);
  } catch (const std::exception&) {
    return nullptr;
  }
  if (parsed.size() != 1) {
    return nullptr;
  }
  if (auto target = TargetBinding(binder, table_entry)) {
    QualifyColumns(*parsed[0], *target);
  }
  try {
    duckdb::WhereBinder where_binder(binder, context);
    return where_binder.Bind(parsed[0]);
  } catch (const std::exception&) {
    return nullptr;
  }
}

duckdb::unique_ptr<duckdb::Expression> BoolConst(bool value) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BOOLEAN(value));
}

duckdb::unique_ptr<duckdb::Expression> Conjoin(
  duckdb::unique_ptr<duckdb::Expression> a,
  duckdb::unique_ptr<duckdb::Expression> b, duckdb::ExpressionType op) {
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  return duckdb::make_uniq<duckdb::BoundConjunctionExpression>(op, std::move(a),
                                                               std::move(b));
}

}  // namespace

// The row-visibility predicate for `table_entry` as seen by the role reading it,
// or null when RLS does not apply (not a serened table, not enabled, or the role
// bypasses). `who` is the definer view the scan sits inside, if any.
duckdb::unique_ptr<duckdb::Expression> RlsReadFilter(
  duckdb::ClientContext& context, duckdb::Binder& binder,
  const duckdb::TableCatalogEntry& table_entry,
  const duckdb::CatalogEntry* who) {
  auto* facade = dynamic_cast<const SereneDBTableEntry*>(&table_entry);
  if (!facade) {
    return nullptr;  // system / store / non-serened table: no RLS.
  }
  auto state = context.registered_state->Get<SereneDBClientState>(
    kSereneDBClientStateKey);
  if (!state) {
    return nullptr;
  }
  auto& ctx = state->GetConnectionContext();
  const auto snapshot = ctx.CatalogSnapshot();
  if (!snapshot) {
    return nullptr;
  }
  const auto& serene_table = *facade->GetSereneDBTable();
  const auto table_id = serene_table.GetId();

  const auto rls = snapshot->GetRowSecurity(table_id);
  if (!rls.enabled) {
    return nullptr;  // RLS not enabled on this table.
  }

  // Whether a filter is emitted, and which policies it combines, depend on the
  // role this statement was planned for. That makes the plan role-specific, so it
  // must never be reused after SET ROLE -- a bypassing role's plan carries no
  // filter at all. Marked before the bypass check below, precisely because that
  // is the return that emits nothing.
  binder.SetAlwaysRequireRebind();

  const auto role = EffectiveRlsRole(who, ctx.GetRoleId());
  const auto& closure = snapshot->ClosureFor(role);
  if (BypassesRls(*snapshot, closure, role, serene_table, rls.forced)) {
    return nullptr;
  }

  // Combine the applicable read policies: PERMISSIVE OR'd, RESTRICTIVE AND'd.
  // PG semantics: (perm1 OR perm2 OR ...) AND restr1 AND restr2 ...
  duckdb::unique_ptr<duckdb::Expression> permissive;
  duckdb::unique_ptr<duckdb::Expression> restrictive;
  bool any_permissive = false;

  for (auto policy_id : snapshot->PolicyIds(table_id)) {
    auto policy = snapshot->GetObject<catalog::Policy>(policy_id);
    if (!policy || !PolicyGovernsRead(policy->Command()) ||
        !PolicyAppliesTo(*policy, closure)) {
      continue;
    }
    // A policy without USING contributes no visibility restriction (true).
    if (!policy->HasUsing()) {
      if (policy->Permissive()) {
        any_permissive = true;
        permissive =
          Conjoin(std::move(permissive), BoolConst(true),
                  duckdb::ExpressionType::CONJUNCTION_OR);
      }
      continue;
    }
    auto expr =
      BindPolicyExpr(binder, context, table_entry, policy->UsingText());
    if (!expr) {
      // A policy we cannot bind must not silently open the table: treat as
      // "false" so it neither adds visibility (permissive) nor is skipped.
      expr = BoolConst(false);
    }
    if (policy->Permissive()) {
      any_permissive = true;
      permissive = Conjoin(std::move(permissive), std::move(expr),
                           duckdb::ExpressionType::CONJUNCTION_OR);
    } else {
      restrictive = Conjoin(std::move(restrictive), std::move(expr),
                            duckdb::ExpressionType::CONJUNCTION_AND);
    }
  }

  // PG default-deny: RLS enabled but no permissive policy grants visibility ->
  // no rows are visible.
  duckdb::unique_ptr<duckdb::Expression> filter;
  if (!any_permissive) {
    filter = BoolConst(false);
  } else {
    filter = Conjoin(std::move(permissive), std::move(restrictive),
                     duckdb::ExpressionType::CONJUNCTION_AND);
  }

  return filter;
}

// Every base-relation read is tagged by the binder with the catalog entry it
// resolves to and the definer view enclosing it, keyed by the scan's table index.
// That lets a plan operator be traced back to its policy owner without resolving
// any name again -- notably, a store-delegated scan reports the storage table, not
// the facade the policies hang off.
const duckdb::AccessRequirement* RequirementFor(duckdb::Binder& binder,
                                                duckdb::idx_t table_index) {
  for (const auto& req : binder.GetStatementProperties().access_requirements) {
    if (req.table_index == table_index) {
      return &req;
    }
  }
  return nullptr;
}

// Wrap every RLS-governed scan in its visibility filter, and attach the WITH
// CHECK constraints of every write. Runs as a pre-optimizer pass so the filters it
// emits are still seen by filter pushdown (and so reach the scan as pushed-down
// table filters, keeping row-group pruning).
void RlsOptimize(duckdb::unique_ptr<duckdb::LogicalOperator>& op,
                 duckdb::ClientContext& context, duckdb::Binder& binder) {
  for (auto& child : op->children) {
    RlsOptimize(child, context, binder);
  }

  switch (op->type) {
    case duckdb::LogicalOperatorType::LOGICAL_GET: {
      auto& get = op->Cast<duckdb::LogicalGet>();
      const auto* req = RequirementFor(binder, get.table_index.index);
      if (!req || !req->table) {
        return;
      }
      const auto* as_table =
        dynamic_cast<const duckdb::TableCatalogEntry*>(req->table);
      if (!as_table) {
        return;
      }
      auto filter = RlsReadFilter(context, binder, *as_table, req->who);
      if (!filter) {
        return;
      }
      auto logical_filter =
        duckdb::make_uniq<duckdb::LogicalFilter>(std::move(filter));
      logical_filter->AddChild(std::move(op));
      op = std::move(logical_filter);
      return;
    }
    case duckdb::LogicalOperatorType::LOGICAL_INSERT: {
      auto& insert = op->Cast<duckdb::LogicalInsert>();
      RlsAppendCheckConstraints(context, binder, insert.table,
                                /*is_update=*/false, insert.bound_constraints);
      return;
    }
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE: {
      auto& update = op->Cast<duckdb::LogicalUpdate>();
      RlsAppendCheckConstraints(context, binder, update.table,
                                /*is_update=*/true, update.bound_constraints);
      return;
    }
    case duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO: {
      auto& merge = op->Cast<duckdb::LogicalMergeInto>();
      // A MERGE produces target rows through its INSERT and UPDATE actions; each
      // verb present needs the check for its own policy set.
      bool has_insert = false;
      bool has_update = false;
      for (auto& [condition, actions] : merge.actions) {
        for (auto& action : actions) {
          if (action->action_type == duckdb::MergeActionType::MERGE_INSERT) {
            has_insert = true;
          } else if (action->action_type ==
                     duckdb::MergeActionType::MERGE_UPDATE) {
            has_update = true;
          }
        }
      }
      if (has_insert) {
        RlsAppendCheckConstraints(context, binder, merge.table,
                                  /*is_update=*/false, merge.bound_constraints);
      }
      if (has_update) {
        RlsAppendCheckConstraints(context, binder, merge.table,
                                  /*is_update=*/true, merge.bound_constraints);
      }
      return;
    }
    default:
      return;
  }
}

namespace {

// The parser emits SQL special registers (current_user, session_user, ...) as
// bare column refs. In a WHERE binder they resolve as functions, but CheckBinder
// treats every unqualified identifier as a column and errors. Rewrite these
// known nullary registers to function calls before check-binding.
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

// Bind a stored WITH CHECK / USING predicate against the table's columns for a
// post-image check (CheckBinder yields storage-offset references, matching how
// CHECK constraints validate the new row). Null on parse/bind failure.
duckdb::unique_ptr<duckdb::Expression> BindCheckExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  duckdb::TableCatalogEntry& table, const std::string& text,
  duckdb::physical_index_set_t& bound_columns) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
  try {
    parsed = duckdb::Parser::ParseExpressionList(text);
  } catch (const std::exception&) {
    return nullptr;
  }
  if (parsed.size() != 1) {
    return nullptr;
  }
  RewriteSpecialRegisters(parsed[0]);
  try {
    duckdb::CheckBinder check_binder(binder, context, table.name,
                                     table.GetColumns(), bound_columns);
    return check_binder.Bind(parsed[0]);
  } catch (const std::exception&) {
    return nullptr;
  }
}

}  // namespace

void RlsAppendCheckConstraints(
  duckdb::ClientContext& context, duckdb::Binder& binder,
  duckdb::TableCatalogEntry& table_entry, bool is_update,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>& bound_constraints) {
  auto* facade = dynamic_cast<const SereneDBTableEntry*>(&table_entry);
  if (!facade) {
    return;
  }
  auto state = context.registered_state->Get<SereneDBClientState>(
    kSereneDBClientStateKey);
  if (!state) {
    return;
  }
  auto& ctx = state->GetConnectionContext();
  const auto snapshot = ctx.CatalogSnapshot();
  if (!snapshot) {
    return;
  }
  const auto& serene_table = *facade->GetSereneDBTable();
  const auto table_id = serene_table.GetId();

  const auto rls = snapshot->GetRowSecurity(table_id);
  if (!rls.enabled) {
    return;
  }
  // Which WITH CHECK constraints are appended -- if any -- depends on the role
  // binding this statement, so the plan must not outlive it. See RlsWrapScan.
  binder.SetAlwaysRequireRebind();

  const auto role = EffectiveRlsRole(binder, ctx.GetRoleId());
  const auto& closure = snapshot->ClosureFor(role);
  if (BypassesRls(*snapshot, closure, role, serene_table, rls.forced)) {
    return;
  }

  duckdb::unique_ptr<duckdb::Expression> permissive;
  duckdb::unique_ptr<duckdb::Expression> restrictive;
  bool any_permissive = false;
  duckdb::physical_index_set_t bound_columns;

  for (auto policy_id : snapshot->PolicyIds(table_id)) {
    auto policy = snapshot->GetObject<catalog::Policy>(policy_id);
    if (!policy || !PolicyGovernsWrite(policy->Command(), is_update) ||
        !PolicyAppliesTo(*policy, closure)) {
      continue;
    }
    // WITH CHECK falls back to USING when no explicit WITH CHECK (PG semantics).
    const bool has_check = policy->HasCheck();
    const bool has_using = policy->HasUsing();
    if (!has_check && !has_using) {
      // No expression -> unconditionally allowed for this policy.
      if (policy->Permissive()) {
        any_permissive = true;
        permissive = Conjoin(std::move(permissive), BoolConst(true),
                             duckdb::ExpressionType::CONJUNCTION_OR);
      }
      continue;
    }
    const std::string& text =
      has_check ? policy->CheckText() : policy->UsingText();
    auto expr = BindCheckExpr(binder, context, table_entry, text, bound_columns);
    if (!expr) {
      expr = BoolConst(false);
    }
    // CheckBinder targets INTEGER (the CHECK-constraint convention); normalize to
    // BOOLEAN so several policies compose under AND/OR without a vector type
    // mismatch at evaluation.
    expr = duckdb::BoundCastExpression::AddDefaultCastToType(
      std::move(expr), duckdb::LogicalType::BOOLEAN);
    if (policy->Permissive()) {
      any_permissive = true;
      permissive = Conjoin(std::move(permissive), std::move(expr),
                           duckdb::ExpressionType::CONJUNCTION_OR);
    } else {
      restrictive = Conjoin(std::move(restrictive), std::move(expr),
                            duckdb::ExpressionType::CONJUNCTION_AND);
    }
  }

  // PG write default-deny: with RLS enabled and the role not bypassing, a write
  // must be authorized by some PERMISSIVE policy applicable to the verb. None at
  // all, only SELECT policies, or only RESTRICTIVE ones => the row is rejected.
  duckdb::unique_ptr<duckdb::Expression> check;
  if (!any_permissive) {
    check = BoolConst(false);
  } else {
    check = Conjoin(std::move(permissive), std::move(restrictive),
                    duckdb::ExpressionType::CONJUNCTION_AND);
  }

  // A WITH CHECK evaluating to NULL must reject the row -- PG evaluates it as a
  // qual, where NULL means not-satisfied. The CHECK-constraint verifier this
  // rides on instead treats a NULL result as satisfied, so route NULL to 0
  // explicitly: a NULL condition takes the CASE's ELSE branch.
  check = duckdb::BoundCastExpression::AddDefaultCastToType(
    std::move(check), duckdb::LogicalType::BOOLEAN);
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

void RlsGuardTruncate(const catalog::Snapshot& snapshot,
                      const catalog::Table& table, ObjectId role) {
  const auto rls = snapshot.GetRowSecurity(table.GetId());
  if (!rls.enabled) {
    return;
  }
  const auto& closure = snapshot.ClosureFor(role);
  if (BypassesRls(snapshot, closure, role, table, rls.forced)) {
    return;
  }
  // TRUNCATE removes rows wholesale; it cannot be row-filtered, so there is no
  // way to honour the table's policies. Refuse rather than silently ignoring
  // them (PG restricts TRUNCATE on an RLS table to bypassing roles).
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied to truncate table ", table.GetName()),
    ERR_DETAIL("row-level security is enabled and TRUNCATE cannot be filtered "
               "by policy; delete the rows instead"));
}

namespace {

void RlsOptimizePass(duckdb::OptimizerExtensionInput& input,
                     duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan) {
    RlsOptimize(plan, input.context, input.optimizer.binder);
  }
}

}  // namespace

void RegisterRlsEnforcement(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension rls;
  rls.pre_optimize_function = RlsOptimizePass;
  duckdb::OptimizerExtension::Register(db.config, std::move(rls));
}

}  // namespace sdb::connector
