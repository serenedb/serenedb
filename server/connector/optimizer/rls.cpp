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
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <duckdb/planner/expression_binder/where_binder.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

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
ObjectId EffectiveRlsRole(duckdb::Binder& binder, ObjectId caller) {
  if (auto definer = binder.EffectiveDefiner()) {
    if (const auto* view =
          dynamic_cast<const SereneDBViewEntry*>(definer.get())) {
      return view->GetSereneDBView()->GetOwner();
    }
  }
  return caller;
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

// Parse and bind a single stored USING predicate against the scan's columns.
// Returns null on parse/bind failure (the caller then falls back to deny-all).
duckdb::unique_ptr<duckdb::Expression> BindPolicyExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  const std::string& text) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parsed;
  try {
    parsed = duckdb::Parser::ParseExpressionList(text);
  } catch (const std::exception&) {
    return nullptr;
  }
  if (parsed.size() != 1) {
    return nullptr;
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

duckdb::unique_ptr<duckdb::LogicalOperator> RlsWrapScan(
  duckdb::ClientContext& context, duckdb::Binder& binder, duckdb::LogicalGet&,
  duckdb::TableCatalogEntry& table_entry,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  auto* facade = dynamic_cast<const SereneDBTableEntry*>(&table_entry);
  if (!facade) {
    return plan;  // system / store / non-serened table: no RLS.
  }
  auto state = context.registered_state->Get<SereneDBClientState>(
    kSereneDBClientStateKey);
  if (!state) {
    return plan;
  }
  auto& ctx = state->GetConnectionContext();
  const auto snapshot = ctx.CatalogSnapshot();
  if (!snapshot) {
    return plan;
  }
  const auto& serene_table = *facade->GetSereneDBTable();
  const auto table_id = serene_table.GetId();

  const auto rls = snapshot->GetRowSecurity(table_id);
  if (!rls.enabled) {
    return plan;  // RLS not enabled on this table.
  }

  const auto role = EffectiveRlsRole(binder, ctx.GetRoleId());
  const auto& closure = snapshot->ClosureFor(role);

  // Bypass: superuser and BYPASSRLS roles always bypass; the owner bypasses
  // unless the table is FORCE ROW LEVEL SECURITY.
  if (closure.is_superuser) {
    return plan;
  }
  if (auto acting = snapshot->GetObject<catalog::Role>(role);
      acting && acting->Has(catalog::RoleOption::BypassRls)) {
    return plan;
  }
  if (!rls.forced && closure.Owns(serene_table)) {
    return plan;
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
    auto expr = BindPolicyExpr(binder, context, policy->UsingText());
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

  auto logical_filter = duckdb::make_uniq<duckdb::LogicalFilter>(std::move(filter));
  logical_filter->AddChild(std::move(plan));
  return logical_filter;
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
  const auto role = EffectiveRlsRole(binder, ctx.GetRoleId());
  const auto& closure = snapshot->ClosureFor(role);
  if (closure.is_superuser) {
    return;
  }
  if (auto acting = snapshot->GetObject<catalog::Role>(role);
      acting && acting->Has(catalog::RoleOption::BypassRls)) {
    return;
  }
  if (!rls.forced && closure.Owns(serene_table)) {
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
    if (policy->Permissive()) {
      any_permissive = true;
      permissive = Conjoin(std::move(permissive), std::move(expr),
                           duckdb::ExpressionType::CONJUNCTION_OR);
    } else {
      restrictive = Conjoin(std::move(restrictive), std::move(expr),
                            duckdb::ExpressionType::CONJUNCTION_AND);
    }
  }

  // No applicable write policy -> nothing to enforce here (a row still has to
  // pass any read policy on the way back, but the write itself is unconstrained
  // by WITH CHECK, matching PG when only SELECT policies exist).
  if (!any_permissive && !restrictive) {
    return;
  }
  duckdb::unique_ptr<duckdb::Expression> check;
  if (!any_permissive) {
    // Only restrictive policies: they all must hold.
    check = std::move(restrictive);
  } else {
    check = Conjoin(std::move(permissive), std::move(restrictive),
                    duckdb::ExpressionType::CONJUNCTION_AND);
  }

  auto constraint = duckdb::make_uniq<duckdb::BoundCheckConstraint>();
  constraint->expression = std::move(check);
  constraint->bound_columns = std::move(bound_columns);
  constraint->is_rls = true;
  bound_constraints.push_back(std::move(constraint));
}

void RegisterRlsEnforcement(duckdb::DatabaseInstance& db) {
  db.config.rls_wrap_scan = &RlsWrapScan;
  db.config.rls_append_check_constraints = &RlsAppendCheckConstraints;
}

}  // namespace sdb::connector
