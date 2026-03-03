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

#pragma once

#include <velox/core/ExpressionEvaluator.h>
#include <velox/core/Expressions.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "basics/fwd.h"
#include "basics/result.h"
#include "iresearch/search/boolean_filter.hpp"
#include "magic_enum/magic_enum.hpp"
#include "velox/core/ITypedExpr.h"

namespace sdb::connector {

// TODO improve performance

// How to handle
// If something like
// t=(a int PK, b int PK)
// select * from t where (a = 1 and b = 2 ) or (a = 10 and b = 20)?

enum class FilterKind : uint8_t {
  Eq,
  // TODO
  //   Less,
  //   Leq,
  //   Greater,
  //   Geq,
  And,
};

struct Filter {
  Filter(FilterKind kind) : kind{kind} {}
  FilterKind kind;
  virtual ~Filter() = default;
};

struct EqFilter : Filter {
  EqFilter(std::string column_name, velox::core::ConstantTypedExprPtr value)
    : Filter{FilterKind::Eq},
      column_name{std::move(column_name)},
      value{std::move(value)} {}
  std::string column_name;
  velox::core::ConstantTypedExprPtr value;
};

struct AndFilter : Filter {
  AndFilter(std::vector<std::unique_ptr<Filter>> filters)
    : Filter{FilterKind::And}, filters{std::move(filters)} {}
  std::vector<std::unique_ptr<Filter>> filters;
};

inline std::unique_ptr<Filter> ParseFilter(velox::core::TypedExprPtr& filter) {
  if (filter->isCallKind()) {
    auto* as_call = filter->asUnchecked<velox::core::CallTypedExpr>();
    SDB_PRINT("  name=", as_call->name());
    SDB_PRINT("  inputs=");
    for (const auto& input : as_call->inputs()) {
      SDB_PRINT("    input=", input->toString(),
                "; kind=", magic_enum::enum_name(input->kind()));
    }
    if (as_call->name() == "presto_eq") {
      SDB_ASSERT(as_call->inputs().size() == 2);
      SDB_ASSERT(as_call->inputs()[0]->kind() ==
                 velox::core::ExprKind::kFieldAccess);
      auto as_access = basics::downCast<velox::core::FieldAccessTypedExpr>(
        as_call->inputs()[0]);
      SDB_ASSERT(as_call->inputs()[1]->kind() ==
                 velox::core::ExprKind::kConstant);
      auto as_const =
        basics::downCast<velox::core::ConstantTypedExpr>(as_call->inputs()[1]);
      return std::make_unique<EqFilter>(as_access->name(), as_const);
    }
  }
  return {};
}

// TODO find out which interface will be better
// But some shit like
// f(col) = ... is not ok
inline std::unique_ptr<Filter> ParseFilters(
  std::vector<velox::core::TypedExprPtr>& filters) {
  // TODO better rejected filters logics
  std::vector<std::unique_ptr<Filter>> resp;
  std::vector<velox::core::TypedExprPtr> rejected;
  for (auto& filter : filters) {
    auto cur_parsed = ParseFilter(filter);
    if (!cur_parsed) {
      rejected.emplace_back(filter);
    } else {
      resp.emplace_back(std::move(cur_parsed));
    }
  }
  if (resp.size() == 1) {
    // filters = std::move(rejected);
    return std::move(resp[0]);
  }
  if (resp.size() > 1) {
    // filters = std::move(rejected);
    return std::make_unique<AndFilter>(std::move(resp));
  }
  return {};
}

// cannot create row vec I suppose
// Somehow need to pass kind of row {a: 42, b: "lol"}  into data source
inline std::shared_ptr<velox::RowVector> TryGetPoint(
  const Filter& filter, std::span<const std::string> pk_names,
  velox::TypePtr pk_type, velox::memory::MemoryPool* pool) {
  SDB_ASSERT(pk_names.size() == pk_type->size());
  SDB_ASSERT(!pk_names.empty());
  if (pk_names.size() == 1) {
    if (filter.kind != FilterKind::Eq) {
      return {};
    }
    auto as_eq = basics::downCast<EqFilter>(&filter);
    if (as_eq->column_name == pk_names[0]) {
      std::vector<velox::VectorPtr> vectors{
        as_eq->value->toConstantVector(pool)};

      return std::make_shared<velox::RowVector>(
        pool, pk_type, velox::BufferPtr{}, 1, std::move(vectors));
    }
  } else {
    if (filter.kind != FilterKind::And) {
      return {};
    }
    auto* as_and = basics::downCast<AndFilter>(&filter);
    absl::flat_hash_map<std::string, EqFilter*> cname2filter;
    for (const auto& sub_filter : as_and->filters) {
      auto* sf = sub_filter.get();
      if (sf->kind != FilterKind::Eq)
        continue;
      auto* as_eq = basics::downCast<EqFilter>(sf);
      auto res = cname2filter.emplace(as_eq->column_name, as_eq);
      SDB_ASSERT(res.second);
    }
    // for each column name extract expr
    std::vector<velox::VectorPtr> vectors;
    for (std::string_view pk_col_name : pk_names) {
      if (auto it = cname2filter.find(pk_col_name); it != cname2filter.end()) {
        vectors.emplace_back(it->second->value->toConstantVector(pool));
      } else {
        return {};
      }
    }
    return std::make_shared<velox::RowVector>(pool, pk_type, velox::BufferPtr{},
                                              1, std::move(vectors));
  }
  return {};
}

}  // namespace sdb::connector
