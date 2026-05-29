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

#include "catalog/column_expr.h"

#include <absl/algorithm/container.h>
#include <vpack/vpack_helper.h>

#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/expression/type_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/query_node.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>

#include "connector/functions/sequence.h"

namespace sdb {
namespace {

bool IsSequenceFunctionName(std::string_view fn_name) {
  return absl::c_contains(connector::kSequenceFunctionNames, fn_name);
}

void WalkSelect(const duckdb::SelectStatement& stmt, RefKinds kinds, Refs& out);

std::optional<QualifiedRef> ExtractUnboundTypeName(
  const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::UNBOUND) {
    return std::nullopt;
  }
  auto info = type.AuxInfo();
  if (!info) {
    return std::nullopt;
  }
  const auto& unbound = info->Cast<duckdb::UnboundTypeInfo>();
  if (!unbound.expr ||
      unbound.expr->GetExpressionType() != duckdb::ExpressionType::TYPE) {
    return std::nullopt;
  }
  const auto& te = unbound.expr->Cast<duckdb::TypeExpression>();
  return QualifiedRef{te.GetCatalog(), te.GetSchema(), te.GetTypeName()};
}

void CollectTypeRefs(const duckdb::LogicalType& type, Refs& out) {
  if (auto qr = ExtractUnboundTypeName(type)) {
    out.unbound_types.push_back(std::move(*qr));
  }
}

void WalkExpr(const duckdb::ParsedExpression& expr, RefKinds kinds, Refs& out) {
  if (RefKinds::None != (kinds & RefKinds::Types) &&
      expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_CAST) {
    CollectTypeRefs(expr.Cast<duckdb::CastExpression>().cast_type, out);
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::FUNCTION) {
    const auto& fn = expr.Cast<duckdb::FunctionExpression>();
    if (IsSequenceFunctionName(fn.function_name)) {
      if (RefKinds::None != (kinds & RefKinds::Sequences) &&
          !fn.children.empty()) {
        const auto& arg = *fn.children[0];
        if (arg.GetExpressionType() == duckdb::ExpressionType::VALUE_CONSTANT) {
          const auto& konst = arg.Cast<duckdb::ConstantExpression>();
          if (konst.GetValue().type().id() == duckdb::LogicalTypeId::VARCHAR &&
              !konst.GetValue().IsNull()) {
            // nextval('[schema.]name') -- split here so callers see a
            // uniform QualifiedRef like relations/functions.
            auto qualified = konst.GetValue().GetValue<std::string>();
            auto dot = qualified.find('.');
            if (dot == std::string::npos) {
              out.sequences.emplace_back("", "", std::move(qualified));
            } else {
              out.sequences.emplace_back("", qualified.substr(0, dot),
                                         qualified.substr(dot + 1));
            }
          }
        }
      }
    } else if (RefKinds::None != (kinds & RefKinds::Functions)) {
      out.functions.emplace_back(fn.catalog, fn.schema, fn.function_name);
    }
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::SUBQUERY) {
    const auto& sub = expr.Cast<duckdb::SubqueryExpression>();
    if (sub.subquery) {
      WalkSelect(*sub.subquery, kinds, out);
    }
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::ParsedExpression& child) {
      WalkExpr(child, kinds, out);
    });
}

void WalkQueryNode(const duckdb::QueryNode& node, RefKinds kinds, Refs& out) {
  for (const auto& kv : node.cte_map.map) {
    if (kv.second && kv.second->query_node) {
      WalkQueryNode(*kv.second->query_node, kinds, out);
    }
  }
  auto expr_cb = [&](duckdb::unique_ptr<duckdb::ParsedExpression>& expr) {
    if (expr) {
      WalkExpr(*expr, kinds, out);
    }
  };
  auto ref_cb = [&](duckdb::TableRef& ref) {
    if (ref.type == duckdb::TableReferenceType::BASE_TABLE &&
        RefKinds::None != (kinds & RefKinds::Relations)) {
      const auto& base = ref.Cast<duckdb::BaseTableRef>();
      out.relations.emplace_back(base.catalog_name, base.schema_name,
                                 base.table_name);
    }
    // DuckDB's EnumerateTableRefChildren already recursed into the
    // SubqueryRef's QueryNode for expressions and FROM, but it doesn't
    // touch cte_map on the way down. Pick those up here.
    if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
      const auto& sub = ref.Cast<duckdb::SubqueryRef>();
      if (sub.subquery && sub.subquery->node) {
        for (const auto& kv : sub.subquery->node->cte_map.map) {
          if (kv.second && kv.second->query_node) {
            WalkQueryNode(*kv.second->query_node, kinds, out);
          }
        }
      }
    }
  };
  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
    const_cast<duckdb::QueryNode&>(node), expr_cb, ref_cb);
}

void WalkSelect(const duckdb::SelectStatement& stmt, RefKinds kinds,
                Refs& out) {
  if (stmt.node) {
    WalkQueryNode(*stmt.node, kinds, out);
  }
}

}  // namespace

Refs ExtractRefs(const duckdb::SelectStatement& stmt, RefKinds kinds) {
  Refs out;
  WalkSelect(stmt, kinds, out);
  return out;
}

Refs ColumnExpr::ExtractRefs(RefKinds kinds) const {
  Refs out;
  if (HasExpr()) {
    WalkExpr(GetExpr(), kinds, out);
  }
  return out;
}

Refs ExtractRefs(const duckdb::ParsedExpression& expr, RefKinds kinds) {
  Refs out;
  WalkExpr(expr, kinds, out);
  return out;
}

ColumnExpr::ColumnExpr(duckdb::unique_ptr<duckdb::ParsedExpression> expr)
  : _expr(std::move(expr)) {}

Result ColumnExpr::FromVPack(vpack::Slice slice, ColumnExpr& column_expr) {
  auto blob = basics::VPackHelper::getString(slice, "duckdb_expr", {});
  if (blob.empty()) {
    return {ERROR_BAD_PARAMETER, "column expression must contain duckdb_expr"};
  }
  duckdb::MemoryStream stream(
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(blob.data())),
    blob.size());
  duckdb::BinaryDeserializer deserializer(stream);
  deserializer.Begin();
  column_expr._expr = duckdb::ParsedExpression::Deserialize(deserializer);
  deserializer.End();
  return {};
}

void ColumnExpr::ToVPack(vpack::Builder& builder) const {
  SDB_ASSERT(_expr);
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer(stream);
  serializer.Begin();
  _expr->Serialize(serializer);
  serializer.End();
  builder.openObject();
  builder.add("duckdb_expr",
              std::string_view(reinterpret_cast<const char*>(stream.GetData()),
                               stream.GetPosition()));
  builder.close();
}

}  // namespace sdb
