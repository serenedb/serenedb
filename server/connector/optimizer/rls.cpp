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

#include <absl/functional/function_ref.h>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/constraints/bound_check_constraint.hpp>
#include <duckdb/planner/expression/bound_case_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <duckdb/planner/expression_binder/where_binder.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <optional>

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

using PolicyCommand = catalog::persistence::PolicyCommand;

// PG: a policy governs a verb when it is FOR ALL, or FOR that exact verb.
bool PolicyGoverns(PolicyCommand cmd, PolicyCommand verb) {
  return cmd == PolicyCommand::All || cmd == verb;
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

// The role policies are evaluated as. A definer-rights view shifts the effective
// principal to the view's owner (PG: a plain view reads base relations, and so
// applies their RLS, as the view owner); an invoker view (security_invoker=true)
// or a direct query keeps the caller. Mirrors the RBAC EffectiveRole fold.
ObjectId EffectiveRlsRole(const duckdb::CatalogEntry* who, ObjectId caller) {
  if (const auto* view = dynamic_cast<const SereneDBViewEntry*>(who)) {
    return view->GetSereneDBView()->GetOwner();
  }
  return caller;
}

// Superuser and BYPASSRLS always bypass; the owner bypasses unless the table is
// FORCE ROW LEVEL SECURITY.
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

duckdb::unique_ptr<duckdb::Expression> BoolConst(bool value) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::BOOLEAN(value));
}

// A volatile no-op that evaluates to true, conjoined onto every visibility
// predicate. duckdb never pushes a volatile expression into a scan
// (pushdown_get.cpp), so this term stays in the filter list and is never erased
// from it.
//
// That is the whole point. The guard keeping a throwing user qual out of the
// scan is `expr.CanThrow() && filters.size() > 1`. A simple policy predicate is
// pushed *fully* and therefore erased from the set, dropping the count to 1 and
// disarming the guard -- the user qual is then admitted into the scan beside the
// policy, where AdaptiveFilter(const TableFilterSet&) orders the two with no
// security check at all, and can evaluate it against rows the policy hides. That
// is an exploitable read; see rls_problems.md P1 and
// sdb/pg/rls/predicate_ordering_leak.test.
//
// This term keeps the count above 1 so the guard stays armed. The policy itself
// is still pushed into the scan, so row-group pruning is unaffected.
//
// This is a mitigation, not a guarantee: it restores the arrangement that tests
// clean rather than asserting an ordering the optimizer must honour. The real
// fix is a barrier flag duckdb respects when ordering scan filters.
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

////////////////////////////////////////////////////////////////////////////////
// Everything a policy decision needs, resolved once per governed relation.
////////////////////////////////////////////////////////////////////////////////

// The per-statement pieces, resolved once by RlsOptimizePass. The session owns
// the snapshot for the statement's duration, so this borrows rather than shares.
struct RlsSession {
  const catalog::Snapshot& snapshot;
  ObjectId caller;
};

struct RlsContext {
  const catalog::Table* table = nullptr;
  const auth::RoleClosure* closure = nullptr;
};

// Resolves the policy context for `table_entry`, or nullopt when RLS does not
// apply: not a serened table, not enabled, or the acting role bypasses.
//
// Also marks the plan as role-specific. Which policies apply -- and whether any
// filter is emitted at all -- depend on the role this statement was planned for,
// so the plan must not be reused after SET ROLE. Marked before the bypass check
// below, precisely because that is the exit that emits nothing.
std::optional<RlsContext> ResolveRls(const RlsSession& session,
                                     duckdb::Binder& binder,
                                     const duckdb::TableCatalogEntry& table_entry,
                                     const duckdb::CatalogEntry* who) {
  const auto* facade = dynamic_cast<const SereneDBTableEntry*>(&table_entry);
  if (!facade) {
    return std::nullopt;  // system / store / non-serened table: no RLS.
  }
  const auto& table = *facade->GetSereneDBTable();
  const auto rls = session.snapshot.GetRowSecurity(table.GetId());
  if (!rls.enabled) {
    return std::nullopt;
  }
  binder.SetAlwaysRequireRebind();

  const auto role = EffectiveRlsRole(who, session.caller);
  const auto& closure = session.snapshot.ClosureFor(role);
  if (BypassesRls(session.snapshot, closure, role, table, rls.forced)) {
    return std::nullopt;
  }
  return RlsContext{&table, &closure};
}

// The predicate governing `verb`: (perm1 OR perm2 ...) AND restr1 AND restr2 ...
// `bind_one` yields a policy's bound predicate, or null when the policy carries no
// expression and is therefore unconditionally satisfied.
//
// Never returns null: with no applicable PERMISSIVE policy the result is a literal
// false, which is PG's default-deny.
duckdb::unique_ptr<duckdb::Expression> CombinePolicies(
  const RlsSession& session, const RlsContext& rls, PolicyCommand verb,
  absl::FunctionRef<duckdb::unique_ptr<duckdb::Expression>(
    const catalog::Policy&)>
    bind_one) {
  duckdb::unique_ptr<duckdb::Expression> permissive;
  duckdb::unique_ptr<duckdb::Expression> restrictive;
  bool any_permissive = false;

  for (auto policy_id : session.snapshot.PolicyIds(rls.table->GetId())) {
    // A table's policy ids are inserted and erased with the objects themselves,
    // so every id here resolves; there is deliberately no null branch, since
    // skipping an unresolvable policy would drop a RESTRICTIVE one and widen
    // visibility.
    auto policy = session.snapshot.GetObject<catalog::Policy>(policy_id);
    if (!PolicyGoverns(policy->Command(), verb) ||
        !PolicyAppliesTo(*policy, *rls.closure)) {
      continue;
    }
    auto expr = bind_one(*policy);
    if (policy->Permissive()) {
      // A permissive policy with no expression grants unconditionally.
      any_permissive = true;
      permissive = Conjoin(std::move(permissive),
                           expr ? std::move(expr) : BoolConst(true),
                           duckdb::ExpressionType::CONJUNCTION_OR);
    } else if (expr) {
      // A restrictive policy with no expression constrains nothing.
      restrictive = Conjoin(std::move(restrictive), std::move(expr),
                            duckdb::ExpressionType::CONJUNCTION_AND);
    }
  }
  if (!any_permissive) {
    return BoolConst(false);
  }
  return Conjoin(std::move(permissive), std::move(restrictive),
                 duckdb::ExpressionType::CONJUNCTION_AND);
}

////////////////////////////////////////////////////////////////////////////////
// Binding stored policy text.
////////////////////////////////////////////////////////////////////////////////

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

// The parser emits SQL special registers (current_user, session_user, ...) as
// bare column refs. A WHERE binder resolves them as functions, but CheckBinder
// treats every unqualified identifier as a column and errors. Rewrite the known
// nullary registers to function calls before check-binding.
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

duckdb::unique_ptr<duckdb::ParsedExpression> ParsePredicate(
  const std::string& text) {
  auto parsed = duckdb::Parser::ParseExpressionList(text);
  if (parsed.size() != 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("row-level security policy predicate is not a "
                            "single expression: ",
                            text));
  }
  return std::move(parsed[0]);
}

// A USING predicate bound against a scan's columns, for row visibility. Null on
// parse/bind failure, which the caller turns into a deny -- a policy we cannot
// bind must never silently open the relation.
duckdb::unique_ptr<duckdb::Expression> BindVisibilityExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  const duckdb::TableCatalogEntry& table_entry, const std::string& text) {
  auto parsed = ParsePredicate(text);
  if (auto target = TargetBinding(binder, table_entry)) {
    QualifyColumns(*parsed, *target);
  }
  duckdb::WhereBinder where_binder(binder, context);
  return where_binder.Bind(parsed);
}

// A WITH CHECK predicate bound against the table's columns, for a write's
// post-image. CheckBinder yields storage-offset references, matching how ordinary
// CHECK constraints validate the new row, and targets INTEGER -- so the result is
// normalized to BOOLEAN for composition.
duckdb::unique_ptr<duckdb::Expression> BindPostImageExpr(
  duckdb::Binder& binder, duckdb::ClientContext& context,
  duckdb::TableCatalogEntry& table, const std::string& text,
  duckdb::physical_index_set_t& bound_columns) {
  auto parsed = ParsePredicate(text);
  RewriteSpecialRegisters(parsed);
  duckdb::CheckBinder check_binder(binder, context, table.name,
                                   table.GetColumns(), bound_columns);
  return duckdb::BoundCastExpression::AddDefaultCastToType(
    check_binder.Bind(parsed), duckdb::LogicalType::BOOLEAN);
}

////////////////////////////////////////////////////////////////////////////////
// Enforcement.
////////////////////////////////////////////////////////////////////////////////

// The row-visibility predicate for a scan of `table_entry`, or null when RLS does
// not apply.
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
        return nullptr;  // no restriction on visibility
      }
      return BindVisibilityExpr(binder, context, table_entry,
                                policy.UsingText());
    });
}

// Appends the WITH CHECK constraint validating a write's post-image for `verb`.
// `who` is the definer view enclosing the write, if any.
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
      // WITH CHECK falls back to USING when the policy has no explicit WITH
      // CHECK (PG semantics).
      const bool has_check = policy.HasCheck();
      if (!has_check && !policy.HasUsing()) {
        return nullptr;  // unconditionally allowed
      }
      const std::string& text =
        has_check ? policy.CheckText() : policy.UsingText();
      return BindPostImageExpr(binder, context, table_entry, text,
                               bound_columns);
    });

  // A WITH CHECK evaluating to NULL must reject the row -- PG evaluates it as a
  // qual, where NULL means not-satisfied. The CHECK-constraint verifier this
  // rides on instead treats a NULL result as satisfied, so route NULL to 0
  // explicitly: a NULL condition takes the CASE's ELSE branch.
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

////////////////////////////////////////////////////////////////////////////////
// The plan pass.
////////////////////////////////////////////////////////////////////////////////

// Every base-relation read is tagged by the binder with the catalog entry it
// resolved to and the definer view enclosing it, keyed by the scan's table index.
// That lets a plan operator be traced back to the object its policies hang off
// without resolving any name again -- notably, a store-delegated scan reports the
// storage table, not the facade.
const duckdb::AccessRequirement* RequirementFor(duckdb::Binder& binder,
                                                duckdb::idx_t table_index) {
  for (const auto& req : binder.GetStatementProperties().access_requirements) {
    if (req.table_index == table_index) {
      return &req;
    }
  }
  return nullptr;
}

// The definer view enclosing the relation bound at `table_index`, or null when it
// was reached directly.
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
      // Policy first so it short-circuits; the marker only has to be present.
      filter = Conjoin(std::move(filter), UnpushableMarker(),
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
      // MERGE records its access under the target *scan's* table index rather
      // than its own (bind_merge_into.cpp), so the definer is not reachable from
      // merge.table_index. Writes through a definer view are unreachable anyway
      // while views are not updatable.
      const duckdb::CatalogEntry* who = nullptr;
      // A MERGE produces target rows through its INSERT and UPDATE actions; each
      // verb present needs the check for its own policy set.
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

void RlsOptimizePass(duckdb::OptimizerExtensionInput& input,
                     duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (!plan) {
    return;
  }
  auto* conn = GetSereneDBContextPtr(input.context);
  if (!conn) {
    return;
  }
  const auto snapshot = conn->CatalogSnapshot();
  if (!snapshot) {
    return;
  }
  const RlsSession session{*snapshot, conn->GetRoleId()};
  RlsOptimize(plan, session, input.context, input.optimizer.binder);
}

}  // namespace

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
  // them.
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("permission denied to truncate table ", table.GetName()),
    ERR_DETAIL("row-level security is enabled and TRUNCATE cannot be filtered "
               "by policy; delete the rows instead"));
}

void RegisterRlsEnforcement(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension rls;
  rls.pre_optimize_function = RlsOptimizePass;
  duckdb::OptimizerExtension::Register(db.config, std::move(rls));
}

}  // namespace sdb::connector
