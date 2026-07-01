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
#include <duckdb/planner/operator/logical_explain.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/planner_extension.hpp>

#include "basics/assert.h"

namespace sdb::optimizer {
namespace {

bool NeedsClientCast(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::VARIANT:
      return true;
    case duckdb::LogicalTypeId::LIST:
      return NeedsClientCast(duckdb::ListType::GetChildType(type));
    case duckdb::LogicalTypeId::ARRAY:
      return NeedsClientCast(duckdb::ArrayType::GetChildType(type));
    case duckdb::LogicalTypeId::STRUCT:
      for (const auto& [_, field] : duckdb::StructType::GetChildTypes(type)) {
        if (NeedsClientCast(field)) {
          return true;
        }
      }
      return false;
    case duckdb::LogicalTypeId::MAP:
      return NeedsClientCast(duckdb::MapType::KeyType(type)) ||
             NeedsClientCast(duckdb::MapType::ValueType(type));
    default:
      return false;
  }
}

duckdb::LogicalType ClientCastTarget(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::VARIANT:
      return duckdb::LogicalType::JSON();
    case duckdb::LogicalTypeId::LIST:
      return duckdb::LogicalType::LIST(
        ClientCastTarget(duckdb::ListType::GetChildType(type)));
    case duckdb::LogicalTypeId::ARRAY:
      return duckdb::LogicalType::ARRAY(
        ClientCastTarget(duckdb::ArrayType::GetChildType(type)),
        duckdb::ArrayType::GetSize(type));
    case duckdb::LogicalTypeId::STRUCT: {
      duckdb::child_list_t<duckdb::LogicalType> children;
      for (const auto& [name, field] :
           duckdb::StructType::GetChildTypes(type)) {
        children.emplace_back(name, ClientCastTarget(field));
      }
      return duckdb::LogicalType::STRUCT(std::move(children));
    }
    case duckdb::LogicalTypeId::MAP:
      return duckdb::LogicalType::MAP(
        ClientCastTarget(duckdb::MapType::KeyType(type)),
        ClientCastTarget(duckdb::MapType::ValueType(type)));
    default:
      return type;
  }
}

bool AnyNeedsClientCast(const duckdb::vector<duckdb::LogicalType>& types) {
  for (const auto& t : types) {
    if (NeedsClientCast(t)) {
      return true;
    }
  }
  return false;
}

duckdb::unique_ptr<duckdb::LogicalOperator> WrapPlan(
  duckdb::ClientContext& context, duckdb::Binder& binder,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  plan->ResolveOperatorTypes();
  if (!AnyNeedsClientCast(plan->types)) {
    return plan;
  }
  const auto& plan_types = plan->types;
  const auto plan_bindings = plan->GetColumnBindings();
  SDB_ASSERT(plan_bindings.size() == plan_types.size());

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
  exprs.reserve(plan_bindings.size());
  for (size_t i = 0; i < plan_bindings.size(); ++i) {
    auto ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      plan_types[i], plan_bindings[i]);
    if (NeedsClientCast(plan_types[i])) {
      auto cast = duckdb::BoundCastExpression::AddCastToType(
        context, std::move(ref), ClientCastTarget(plan_types[i]));
      exprs.emplace_back(std::move(cast));
    } else {
      exprs.emplace_back(std::move(ref));
    }
  }
  auto proj = duckdb::make_uniq<duckdb::LogicalProjection>(
    binder.GenerateTableIndex(), std::move(exprs));
  proj->children.emplace_back(std::move(plan));
  return proj;
}

void WrapVariantOutputs(duckdb::PlannerExtensionInput& input,
                        duckdb::BoundStatement& statement) {
  if (!statement.plan) {
    return;
  }
  if (statement.plan->type == duckdb::LogicalOperatorType::LOGICAL_EXPLAIN) {
    auto& explain = statement.plan->Cast<duckdb::LogicalExplain>();
    SDB_ASSERT(!explain.children.empty());
    explain.children[0]->ResolveOperatorTypes();
    if (!AnyNeedsClientCast(explain.children[0]->types)) {
      return;
    }
    explain.children[0] =
      WrapPlan(input.context, input.binder, std::move(explain.children[0]));
    explain.logical_plan_unopt =
      explain.children[0]->ToString(input.context, explain.format);
    return;
  }
  if (!AnyNeedsClientCast(statement.types)) {
    return;
  }
  auto wrapped =
    WrapPlan(input.context, input.binder, std::move(statement.plan));
  duckdb::vector<duckdb::LogicalType> new_types;
  new_types.reserve(statement.types.size());
  for (const auto& t : statement.types) {
    new_types.emplace_back(NeedsClientCast(t) ? ClientCastTarget(t) : t);
  }
  statement.plan = std::move(wrapped);
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
