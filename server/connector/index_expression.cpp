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

#include "connector/index_expression.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/type_visitor.hpp>
#include <duckdb/execution/column_binding_resolver.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/lambda_functions.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/optimizer/expression_rewriter.hpp>
#include <duckdb/optimizer/rule/concat_flattening.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/lambda_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_lambda_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "connector/column_id.h"
#include "connector/pg_logical_types.h"

namespace sdb::connector {
namespace {

// TODO(mkornaukhov): replace with the regular constant-folding optimizer.
duckdb::unique_ptr<duckdb::Expression> FoldConstantCasts(
  duckdb::unique_ptr<duckdb::Expression> expr, duckdb::ClientContext& context) {
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = FoldConstantCasts(std::move(child), context);
    });
  if (duckdb::BoundCastExpression::IsCast(*expr)) {
    if (duckdb::BoundCastExpression::Child(
          expr->Cast<duckdb::BoundFunctionExpression>())
          .GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
      duckdb::Value folded;
      if (duckdb::ExpressionExecutor::TryEvaluateScalar(context, *expr,
                                                        folded)) {
        return duckdb::make_uniq<duckdb::BoundConstantExpression>(
          std::move(folded));
      }
    }
  }
  return expr;
}

class ChunkBindingResolver final : public duckdb::ColumnBindingResolver {
 public:
  ChunkBindingResolver(duckdb::vector<duckdb::ColumnBinding> b,
                       duckdb::vector<duckdb::LogicalType> t)
    : duckdb::ColumnBindingResolver(std::move(b), std::move(t)) {}
  void Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) {
    VisitExpression(&expr);
  }
};

std::string SerializeBoundExpression(const duckdb::Expression& expr) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(expr, stream);
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

duckdb::unique_ptr<duckdb::Expression> FlattenConcats(
  duckdb::unique_ptr<duckdb::Expression> expr, duckdb::ClientContext& context) {
  duckdb::ExpressionRewriter rewriter{context};
  rewriter.rules.push_back(
    duckdb::make_uniq<duckdb::ConcatFlatteningRule>(rewriter));
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> select_list;
  select_list.push_back(std::move(expr));
  duckdb::LogicalProjection projection{duckdb::TableIndex{0},
                                       std::move(select_list)};
  rewriter.VisitOperator(projection);
  return std::move(projection.expressions[0]);
}

void ClearSource(duckdb::Expression& expr);

duckdb::shared_ptr<duckdb::Expression> WithoutSource(
  const duckdb::Expression& body) {
  auto copy = body.Copy();
  duckdb::ExpressionIterator::EnumerateExpression(copy, ClearSource);
  return duckdb::shared_ptr<duckdb::Expression>{std::move(copy)};
}

void ClearSource(duckdb::Expression& expr) {
  expr.SetAlias("");
  expr.SetQueryLocation(duckdb::optional_idx{});
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_LAMBDA) {
    auto& lambda = expr.Cast<duckdb::BoundLambdaExpression>();
    lambda.SetLambdaExpr(WithoutSource(*lambda.LambdaExpr()));
  } else if (expr.GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& function = expr.Cast<duckdb::BoundFunctionExpression>();
    function.IsOperatorMutable() = false;
    if (function.Function().HasBindLambdaCallback() && function.BindInfo()) {
      auto& data =
        function.BindInfoMutable()->Cast<duckdb::LambdaFunctionData>();
      if (const auto body = data.GetLambdaExpression()) {
        data.SetLambdaExpression(WithoutSource(*body));
      }
    }
  }
}

duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, duckdb::idx_t table_id,
  std::span<const ColumnId> col_index_to_id, duckdb::ClientContext& context) {
  auto copy = FlattenConcats(FoldConstantCasts(expr.Copy(), context), context);
  duckdb::ExpressionIterator::EnumerateExpression(
    copy, [&](duckdb::Expression& e) {
      ClearSource(e);
      if (e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        auto& cref = e.Cast<duckdb::BoundColumnRefExpression>();
        const auto idx = cref.Binding().column_index.GetIndex();
        SDB_ASSERT(idx < col_index_to_id.size());
        const auto col_id = col_index_to_id[idx];
        cref.BindingMutable() = duckdb::ColumnBinding(
          duckdb::TableIndex(table_id),
          duckdb::ProjectionIndex(static_cast<duckdb::idx_t>(col_id)));
      }
    });
  return copy;
}

}  // namespace

duckdb::unique_ptr<duckdb::Expression> DeserializeBoundExpression(
  std::string_view bytes, duckdb::ClientContext& context) {
  duckdb::MemoryStream stream(
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(bytes.data())),
    bytes.size());
  duckdb::bound_parameter_map_t params;
  return duckdb::BinaryDeserializer::Deserialize<duckdb::Expression>(
    stream, context, params);
}

duckdb::Vector EvaluateExprOverChunk(const duckdb::Expression& bound_expr,
                                     duckdb::DataChunk& chunk,
                                     duckdb::idx_t table_id,
                                     std::span<const ColumnId> slot_to_col_id,
                                     duckdb::ClientContext& context) {
  SDB_ASSERT(chunk.ColumnCount() >= slot_to_col_id.size());
  duckdb::vector<duckdb::ColumnBinding> bindings;
  duckdb::vector<duckdb::LogicalType> types;
  bindings.reserve(slot_to_col_id.size());
  types.reserve(slot_to_col_id.size());
  for (duckdb::idx_t slot = 0; slot < slot_to_col_id.size(); ++slot) {
    bindings.emplace_back(duckdb::TableIndex(table_id),
                          duckdb::ProjectionIndex(slot_to_col_id[slot]));
    types.emplace_back(chunk.data[slot].GetType());
  }
  ChunkBindingResolver resolver(std::move(bindings), std::move(types));
  auto resolved = bound_expr.Copy();
  resolver.Resolve(resolved);
  const auto num_rows = chunk.size();
  duckdb::Vector result(resolved->GetReturnType(), num_rows);
  duckdb::ExpressionExecutor executor(context, *resolved);
  executor.ExecuteExpression(chunk, result);
  return result;
}

namespace {

void ExpandStoredExpressions(duckdb::unique_ptr<duckdb::Expression>& expr,
                             const StoredExpressionOf& stored_expression,
                             duckdb::ClientContext& context) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto stored = stored_expression(
      static_cast<ColumnId>(expr->Cast<duckdb::BoundColumnRefExpression>()
                              .Binding()
                              .column_index.GetIndex()));
    if (!stored.empty()) {
      expr = DeserializeBoundExpression(stored, context);
    }
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      ExpandStoredExpressions(child, stored_expression, context);
    });
}

}  // namespace

std::string NormalizedExpression(const duckdb::Expression& expr,
                                 duckdb::idx_t table_id,
                                 std::span<const ColumnId> col_index_to_id,
                                 duckdb::ClientContext& context,
                                 const StoredExpressionOf& stored_expression) {
  auto normalized =
    NormalizeBoundExpression(expr, table_id, col_index_to_id, context);
  if (stored_expression) {
    ExpandStoredExpressions(normalized, stored_expression, context);
  }
  return SerializeBoundExpression(*normalized);
}

namespace {

bool DependsOnSession(const duckdb::LogicalType& type) {
  return duckdb::TypeVisitor::Contains(type, [](const duckdb::LogicalType& t) {
    return t.id() == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
           t.id() == duckdb::LogicalTypeId::TIMESTAMP_TZ_NS ||
           t.id() == duckdb::LogicalTypeId::TIME_TZ ||
           t.id() == duckdb::LogicalTypeId::VARIANT || pg::IsRegLike(t);
  });
}

bool PinsTimeZone(const duckdb::BoundFunctionExpression& func) {
  return func.GetChildren().size() == 2 &&
         func.Function().GetName().GetIdentifierName() == "timezone";
}

}  // namespace

bool ComputedFromRowAlone(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
    if (DependsOnSession(func.GetReturnType()) ||
        (!PinsTimeZone(func) &&
         absl::c_any_of(func.GetChildren(), [](const auto& arg) {
           return DependsOnSession(arg->GetReturnType());
         }))) {
      return false;
    }
    if (duckdb::BoundCastExpression::IsCast(func) &&
        duckdb::TypeVisitor::Contains(
          duckdb::BoundCastExpression::SourceType(func),
          duckdb::LogicalTypeId::BLOB) &&
        duckdb::TypeVisitor::Contains(func.GetReturnType(),
                                      duckdb::LogicalTypeId::VARCHAR)) {
      return false;
    }
  }
  bool alone = true;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      alone = alone && ComputedFromRowAlone(child);
    });
  return alone;
}

}  // namespace sdb::connector
