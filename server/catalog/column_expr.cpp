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

#include <duckdb/common/extra_type_info.hpp>
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
#include <type_traits>

#include "catalog/user_type.h"
#include "connector/functions/sequence.h"

namespace sdb {
namespace {

bool IsSequenceFunctionName(std::string_view fn_name) {
  return absl::c_contains(connector::kSequenceFunctionNames, fn_name);
}

void WalkSelect(const duckdb::SelectStatement& stmt, RefKinds kinds, Refs& out);

// The node naming one unbound type, or null when the type is not one. Not
// const: it is where the resolution of that name is stamped, and a LogicalType
// hands its aux info back read-only whichever side asked.
duckdb::ParsedExpression* UnboundTypeExpr(const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::UNBOUND) {
    return nullptr;
  }
  auto info = type.AuxInfo();
  if (!info) {
    return nullptr;
  }
  const auto& unbound = info->Cast<duckdb::UnboundTypeInfo>();
  if (!unbound.expr ||
      unbound.expr->GetExpressionType() != duckdb::ExpressionType::TYPE) {
    return nullptr;
  }
  return const_cast<duckdb::ParsedExpression*>(unbound.expr.get());
}

std::optional<QualifiedRef> ExtractUnboundTypeName(
  const duckdb::LogicalType& type) {
  auto* node = UnboundTypeExpr(type);
  if (node == nullptr) {
    return std::nullopt;
  }
  const auto& te = node->Cast<duckdb::TypeExpression>();
  return QualifiedRef{te.GetCatalog().GetIdentifierName(),
                      te.GetSchema().GetIdentifierName(),
                      te.GetTypeName().GetIdentifierName(), node};
}

}  // namespace

void CollectTypeRefs(const duckdb::LogicalType& type, Refs& out) {
  if (auto qr = ExtractUnboundTypeName(type)) {
    out.unbound_types.push_back(std::move(*qr));
    return;
  }
  if (auto ext = type.GetExtensionInfo()) {
    if (auto it = ext->properties.find(catalog::kPgSqlTypeOidProp);
        it != ext->properties.end()) {
      out.types.push_back(ObjectId{it->second.GetValue<uint64_t>()});
      return;
    }
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
      CollectTypeRefs(duckdb::ListType::GetChildType(type), out);
      break;
    case duckdb::LogicalTypeId::ARRAY:
      CollectTypeRefs(duckdb::ArrayType::GetChildType(type), out);
      break;
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::VARIANT:
      for (const auto& child : duckdb::StructType::GetChildTypes(type)) {
        CollectTypeRefs(child.second, out);
      }
      break;
    case duckdb::LogicalTypeId::MAP:
      CollectTypeRefs(duckdb::MapType::KeyType(type), out);
      CollectTypeRefs(duckdb::MapType::ValueType(type), out);
      break;
    case duckdb::LogicalTypeId::UNION:
      for (idx_t i = 0; i < duckdb::UnionType::GetMemberCount(type); ++i) {
        CollectTypeRefs(duckdb::UnionType::GetMemberType(type, i), out);
      }
      break;
    default:
      break;
  }
}

namespace {

// Null on the read-only walk, which has no node to hand back.
constexpr duckdb::ParsedExpression* NodeOf(
  const duckdb::ParsedExpression&) noexcept {
  return nullptr;
}
constexpr duckdb::ParsedExpression* NodeOf(
  duckdb::ParsedExpression& expr) noexcept {
  return &expr;
}

template<typename Expr>
void WalkExpr(Expr& expr, RefKinds kinds, Refs& out) {
  if (RefKinds::None != (kinds & RefKinds::Types) &&
      expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_CAST) {
    CollectTypeRefs(expr.template Cast<duckdb::CastExpression>().TargetType(),
                    out);
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::FUNCTION) {
    const auto& fn = expr.template Cast<duckdb::FunctionExpression>();
    if (IsSequenceFunctionName(fn.FunctionName().GetIdentifierName())) {
      if (RefKinds::None != (kinds & RefKinds::Sequences) &&
          !fn.GetArguments().empty()) {
        const auto& arg = fn.GetArguments()[0].GetExpression();
        if (arg.GetExpressionType() == duckdb::ExpressionType::VALUE_CONSTANT) {
          const auto& konst = arg.template Cast<duckdb::ConstantExpression>();
          if (konst.GetValue().type().id() == duckdb::LogicalTypeId::VARCHAR &&
              !konst.GetValue().IsNull()) {
            // nextval('[schema.]name') -- split here so callers see a
            // uniform QualifiedRef like relations/functions.
            auto qualified = konst.GetValue().template GetValue<std::string>();
            auto dot = qualified.find('.');
            if (dot == std::string::npos) {
              out.sequences.emplace_back("", "", std::move(qualified),
                                         NodeOf(expr));
            } else {
              out.sequences.emplace_back("", qualified.substr(0, dot),
                                         qualified.substr(dot + 1),
                                         NodeOf(expr));
            }
          }
        }
      }
    } else if (RefKinds::None != (kinds & RefKinds::Functions)) {
      out.functions.emplace_back(
        fn.GetQualifiedName().Catalog().GetIdentifierName(),
        fn.GetQualifiedName().Schema().GetIdentifierName(),
        fn.FunctionName().GetIdentifierName(), NodeOf(expr));
    }
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::SUBQUERY) {
    const auto& sub = expr.template Cast<duckdb::SubqueryExpression>();
    if (sub.Subquery()) {
      WalkSelect(*sub.Subquery(), kinds, out);
    }
  }
  if constexpr (std::is_const_v<Expr>) {
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](const duckdb::ParsedExpression& child) {
        WalkExpr(child, kinds, out);
      });
  } else {
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
        WalkExpr(*child, kinds, out);
      });
  }
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
      out.relations.emplace_back(
        base.GetQualifiedName().Catalog().GetIdentifierName(),
        base.GetQualifiedName().Schema().GetIdentifierName(),
        base.GetQualifiedName().Name().GetIdentifierName());
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

Refs ColumnExpr::GetRefs(RefKinds kinds) const {
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

Refs ExtractMutableRefs(duckdb::ParsedExpression& expr, RefKinds kinds) {
  Refs out;
  WalkExpr(expr, kinds, out);
  return out;
}

void CollectExprIds(const duckdb::ParsedExpression& expr,
                    std::vector<ObjectId>& out) {
  const auto take = [&](idx_t oid) {
    if (oid != 0) {
      out.push_back(ObjectId{oid});
    }
  };
  take(expr.oid);
  if (expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_CAST) {
    Refs types;
    CollectTypeRefs(expr.Cast<duckdb::CastExpression>().TargetType(), types);
    // A bound type is already an id; an unbound one took its resolution on the
    // node naming it, which is one node per name however deeply nested.
    for (const auto id : types.types) {
      out.push_back(id);
    }
    for (const auto& ref : types.unbound_types) {
      if (ref.node != nullptr) {
        take(ref.node->oid);
      }
    }
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr,
    [&](const duckdb::ParsedExpression& child) { CollectExprIds(child, out); });
}

Refs ExtractRefs(const duckdb::QueryNode& node, RefKinds kinds) {
  Refs out;
  WalkQueryNode(node, kinds, out);
  return out;
}

ColumnExpr::ColumnExpr(duckdb::unique_ptr<duckdb::ParsedExpression> expr)
  : _expr(std::move(expr)) {}

}  // namespace sdb
