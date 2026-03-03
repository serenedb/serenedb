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

inline std::unique_ptr<Filter> ParseFilter(
  const velox::core::TypedExprPtr& filter) {
  if (!filter->isCallKind()) {
    return {};
  }
  auto* func_call = filter->asUnchecked<velox::core::CallTypedExpr>();
  if (func_call->name() == "presto_eq") {
    SDB_ASSERT(func_call->inputs().size() == 2);
    if (func_call->inputs()[0]->kind() != velox::core::ExprKind::kFieldAccess ||
        func_call->inputs()[1]->kind() != velox::core::ExprKind::kConstant) {
      return {};
    }
    auto field_access = basics::downCast<velox::core::FieldAccessTypedExpr>(
      func_call->inputs()[0]);
    auto const_val =
      basics::downCast<velox::core::ConstantTypedExpr>(func_call->inputs()[1]);
    return std::make_unique<EqFilter>(field_access->name(), const_val);
  }
  return {};
}

inline std::unique_ptr<Filter> ParseFilters(
  const std::vector<velox::core::TypedExprPtr>& filters) {
  std::vector<std::unique_ptr<Filter>> resp;
  for (const auto& filter : filters) {
    if (auto parsed = ParseFilter(filter)) {
      resp.emplace_back(std::move(parsed));
    }
  }

  return std::make_unique<AndFilter>(std::move(resp));
}

// Somehow need to pass kind of row {a: 42, b: "lol"}  into data source
inline std::shared_ptr<velox::RowVector> TryGetPoint(
  const Filter& filter, velox::RowTypePtr pk_type,
  velox::memory::MemoryPool* pool) {
  SDB_ASSERT(pk_type->size() != 0);
  SDB_ASSERT(filter.kind == FilterKind::And);
  auto& and_filter = basics::downCast<AndFilter>(filter);
  absl::flat_hash_map<std::string, EqFilter*> cname2filter;
  for (const auto& sub_filter : and_filter.filters) {
    if (sub_filter->kind != FilterKind::Eq)
      continue;
    auto* as_eq = basics::downCast<EqFilter>(sub_filter.get());
    auto res = cname2filter.emplace(as_eq->column_name, as_eq);
    SDB_ASSERT(res.second);
  }

  std::vector<velox::VectorPtr> children;
  for (std::string_view pk_col_name : pk_type->names()) {
    if (auto it = cname2filter.find(pk_col_name); it != cname2filter.end()) {
      children.emplace_back(it->second->value->toConstantVector(pool));
    } else {
      return {};
    }
  }
  return std::make_shared<velox::RowVector>(pool, pk_type, velox::BufferPtr{},
                                            1, std::move(children));
}

}  // namespace sdb::connector
