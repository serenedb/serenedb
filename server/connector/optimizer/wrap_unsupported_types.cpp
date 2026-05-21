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

#include "connector/optimizer/wrap_unsupported_types.h"

#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/bound_statement.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/planner_extension.hpp>

#include "basics/assert.h"

namespace sdb::optimizer {

namespace {

bool NeedsClientCast(const duckdb::LogicalType& type) {
  return type.id() == duckdb::LogicalTypeId::VARIANT;
}

duckdb::LogicalType ClientCastTarget(const duckdb::LogicalType& type) {
  SDB_ASSERT(type.id() == duckdb::LogicalTypeId::VARIANT);
  return duckdb::LogicalType::JSON();
}

void WrapVariantOutputs(duckdb::PlannerExtensionInput& input,
                        duckdb::BoundStatement& statement) {
  if (!statement.plan) {
    return;
  }
  const auto& types = statement.types;
  bool any = false;
  for (const auto& t : types) {
    if (NeedsClientCast(t)) {
      any = true;
      break;
    }
  }
  if (!any) {
    return;
  }

  statement.plan->ResolveOperatorTypes();
  const auto plan_bindings = statement.plan->GetColumnBindings();
  const auto& plan_types = statement.plan->types;
  SDB_ASSERT(plan_bindings.size() == plan_types.size());
  SDB_ASSERT(plan_bindings.size() == types.size());

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
  exprs.reserve(plan_bindings.size());
  duckdb::vector<duckdb::LogicalType> new_types;
  new_types.reserve(plan_bindings.size());

  for (size_t i = 0; i < plan_bindings.size(); ++i) {
    auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      statement.names[i], plan_types[i], plan_bindings[i]);
    if (NeedsClientCast(plan_types[i])) {
      auto target = ClientCastTarget(plan_types[i]);
      auto cast = duckdb::BoundCastExpression::AddCastToType(
        input.context, std::move(ref), target);
      cast->SetAlias(statement.names[i]);
      new_types.push_back(target);
      exprs.push_back(std::move(cast));
    } else {
      new_types.push_back(plan_types[i]);
      exprs.push_back(std::move(ref));
    }
  }

  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    input.binder.GenerateTableIndex(), std::move(exprs));
  proj->children.push_back(std::move(statement.plan));
  statement.plan = std::move(proj);
  statement.types = std::move(new_types);
}

}  // namespace

void RegisterWrapUnsupportedTypesExtension(duckdb::DatabaseInstance& db) {
  duckdb::PlannerExtension::Register(
    duckdb::DBConfig::GetConfig(db),
    duckdb::PlannerExtension{
      .post_bind_function = &WrapVariantOutputs,
    });
}

}  // namespace sdb::optimizer
