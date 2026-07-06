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
#include <simdjson.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/execution/column_binding_resolver.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/lambda_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/bound_parameter_map.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <utility>
#include <vector>

#include "basics/serialization.h"
#include "connector/common.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

bool IsJsonExtractString(std::string_view name) noexcept {
  return name == "->>" || name == "#>>" ||     //
         name == "json_extract_field_text" ||  //
         name == "json_extract_index_text" ||  //
         name == "json_extract_path_text" ||   //
         name == "json_extract_string" ||      //
         name == "pg_json_extract_path_text";
}

bool IsJsonExtract(std::string_view name) noexcept {
  if (IsJsonExtractString(name)) {
    return true;
  }
  return name == "->" || name == "#>" ||  //
         name == "json_extract" ||        //
         name == "json_extract_field" ||  //
         name == "json_extract_index" ||  //
         name == "json_extract_path" ||   //
         name == "pg_json_extract_path";
}

class ChunkBindingResolver final : public duckdb::ColumnBindingResolver {
 public:
  ChunkBindingResolver(duckdb::vector<duckdb::ColumnBinding> b,
                       duckdb::vector<duckdb::LogicalType> t) {
    bindings = std::move(b);
    types = std::move(t);
  }
  void Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) {
    VisitExpression(&expr);
  }
};

bool IsValidKey(const duckdb::Value& v) {
  switch (v.type().id()) {
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
      return true;
    default:
      return false;
  }
}

bool IsValidPathList(const duckdb::Value& v) {
  if (v.type().id() != duckdb::LogicalTypeId::LIST) {
    return false;
  }
  const auto& children = duckdb::ListValue::GetChildren(v);
  if (children.empty()) {
    return false;
  }
  for (const auto& child : children) {
    if (child.IsNull() || !IsValidKey(child)) {
      return false;
    }
  }
  return true;
}

// TODO(mkornaukhov): replace with the regular constant-folding optimizer.
duckdb::unique_ptr<duckdb::Expression> FoldConstantCasts(
  duckdb::unique_ptr<duckdb::Expression> expr, duckdb::ClientContext& context) {
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = FoldConstantCasts(std::move(child), context);
    });
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    auto& cast = expr->Cast<duckdb::BoundCastExpression>();
    if (cast.Child().GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      duckdb::Value folded;
      SDB_ENSURE(
        duckdb::ExpressionExecutor::TryEvaluateScalar(context, *expr, folded),
        ERROR_INTERNAL, "Failed to fold constant cast for inverted index");
      return duckdb::make_uniq<duckdb::BoundConstantExpression>(
        std::move(folded));
    }
  }
  return expr;
}

}  // namespace

std::string SerializeBoundExpression(const duckdb::Expression& expr) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(expr, stream,
                                      duckdb::VersionStorageOptions());
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

duckdb::unique_ptr<duckdb::Expression> DeserializeBoundExpression(
  std::string_view bytes, duckdb::ClientContext& context) {
  duckdb::MemoryStream stream(
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(bytes.data())),
    bytes.size());
  duckdb::bound_parameter_map_t params;
  return duckdb::BinaryDeserializer::Deserialize<duckdb::Expression>(
    stream, context, params);
}

duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, ObjectId table_id,
  std::span<const catalog::Column::Id> col_index_to_id,
  duckdb::ClientContext& context) {
  auto copy = FoldConstantCasts(expr.Copy(), context);
  auto visit = [&](auto& self, duckdb::Expression& e) -> void {
    e.SetAlias("");
    e.SetQueryLocation(duckdb::optional_idx{});
    if (e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      auto& cref = e.Cast<duckdb::BoundColumnRefExpression>();
      const auto idx = cref.Binding().column_index.GetIndex();
      SDB_ASSERT(idx < col_index_to_id.size());
      const auto col_id = col_index_to_id[idx];
      cref.BindingMutable() = duckdb::ColumnBinding(
        duckdb::TableIndex(table_id.id()),
        duckdb::ProjectionIndex(static_cast<duckdb::idx_t>(col_id)));
    } else if (e.GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      e.Cast<duckdb::BoundFunctionExpression>().IsOperatorMutable() = false;
    }
    duckdb::ExpressionIterator::EnumerateChildren(
      e, [&](duckdb::Expression& child) { self(self, child); });
  };
  visit(visit, *copy);
  return copy;
}

const duckdb::BoundColumnRefExpression* TryGetJsonLeafColumnRef(
  const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION ||
      !IsJsonExtractString(expr.Cast<duckdb::BoundFunctionExpression>()
                             .Function()
                             .GetName()
                             .GetIdentifierName())) {
    return nullptr;
  }
  const duckdb::Expression* cur = &expr;
  while (cur->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& f = cur->Cast<duckdb::BoundFunctionExpression>();
    if (!IsJsonExtract(f.Function().GetName().GetIdentifierName()) ||
        f.GetChildren().size() < 2) {
      return nullptr;
    }
    for (size_t i = 1; i < f.GetChildren().size(); ++i) {
      const auto* key_expr = f.GetChildren()[i].get();
      if (key_expr->GetExpressionClass() !=
          duckdb::ExpressionClass::BOUND_CONSTANT) {
        return nullptr;
      }
      const auto& key_const = key_expr->Cast<duckdb::BoundConstantExpression>();
      if (key_const.GetValue().IsNull()) {
        return nullptr;
      }
      if (!IsValidKey(key_const.GetValue()) &&
          !IsValidPathList(key_const.GetValue())) {
        return nullptr;
      }
    }
    cur = f.GetChildren()[0].get();
  }
  if (cur->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &cur->Cast<duckdb::BoundColumnRefExpression>();
}

std::vector<catalog::Column::Id> CollectDependentColumns(
  const duckdb::Expression& expr) {
  constexpr size_t kReserved = 8;
  std::vector<catalog::Column::Id> out;
  out.reserve(kReserved);
  auto visit = [&](auto& self, const duckdb::Expression& node) -> void {
    if (node.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      out.push_back(static_cast<catalog::Column::Id>(
        node.Cast<duckdb::BoundColumnRefExpression>()
          .Binding()
          .column_index.GetIndex()));
    }
    duckdb::ExpressionIterator::EnumerateChildren(
      node, [&](const duckdb::Expression& child) { self(self, child); });
  };
  visit(visit, expr);
  std::ranges::sort(out);
  out.erase(std::ranges::unique(out).begin(), out.end());
  return out;
}

void RejectUserDefinedFunctions(const duckdb::Expression& expr,
                                duckdb::ClientContext& context) {
  auto visit = [&](auto& self, const duckdb::Expression& node) -> void {
    if (node.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
      const auto& f = node.Cast<duckdb::BoundFunctionExpression>();
      const auto& schema = f.Function().GetSchemaName();
      const auto& cat = f.Function().GetCatalogName();
      bool is_user_defined = !cat.empty() && cat != SYSTEM_CATALOG;
      if (!is_user_defined) {
        auto& sys = duckdb::Catalog::GetSystemCatalog(context);
        auto entry = sys.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
          context,
          schema.empty() ? duckdb::Identifier::DefaultSchema() : schema,
          f.Function().GetName(), duckdb::OnEntryNotFound::RETURN_NULL);
        if (!entry) {
          is_user_defined = true;
        } else if (!entry->internal) {
          is_user_defined = true;
        }
      }
      if (is_user_defined) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("user-defined functions in indexed expressions are not "
                  "supported yet (function: ",
                  f.Function().GetName().GetIdentifierName(), ")"));
      }
    }
    duckdb::ExpressionIterator::EnumerateChildren(
      node, [&](const duckdb::Expression& child) { self(self, child); });
  };
  visit(visit, expr);
}

void RejectUserDefinedFunctions(const duckdb::ParsedExpression& expr,
                                duckdb::ClientContext& context) {
  auto visit = [&](auto& self, const duckdb::ParsedExpression& node) -> void {
    if (node.GetExpressionClass() == duckdb::ExpressionClass::FUNCTION) {
      const auto& f = node.Cast<duckdb::FunctionExpression>();
      auto fail = [&] {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("user-defined functions in indexed "
                                "expressions are not supported yet"));
      };
      const auto& parsed_schema = f.GetQualifiedName().Schema();
      const auto schema = parsed_schema.empty()
                            ? duckdb::Identifier::DefaultSchema()
                            : parsed_schema;
      auto check_catalog = [&](duckdb::Catalog& cat) {
        for (auto type : {duckdb::CatalogType::MACRO_ENTRY,
                          duckdb::CatalogType::SCALAR_FUNCTION_ENTRY}) {
          auto entry = cat.GetEntry(context, type, schema, f.FunctionName(),
                                    duckdb::OnEntryNotFound::RETURN_NULL);
          if (!entry) {
            continue;
          }
          if (entry->type == duckdb::CatalogType::MACRO_ENTRY) {
            fail();
          }
          if (entry->type == duckdb::CatalogType::SCALAR_FUNCTION_ENTRY &&
              !entry->internal) {
            fail();
          }
        }
      };
      if (!f.GetQualifiedName().Catalog().empty()) {
        auto cat = duckdb::Catalog::GetCatalogEntry(
          context, f.GetQualifiedName().Catalog());
        if (cat) {
          check_catalog(*cat);
        }
      } else {
        for (auto& cat_ref :
             duckdb::DatabaseManager::Get(context).GetDatabases(context)) {
          check_catalog(cat_ref.get()->GetCatalog());
        }
      }
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      node, [&](const duckdb::ParsedExpression& child) { self(self, child); });
  };
  visit(visit, expr);
}

void RejectJsonObjectArrayLeaves(const duckdb::Vector& result,
                                 duckdb::idx_t num_rows) {
  if (!result.GetType().IsJSONType()) {
    return;
  }
  duckdb::UnifiedVectorFormat fmt;
  result.ToUnifiedFormat(num_rows, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  // ondemand is lazy; DOM rejects malformed input up front.
  simdjson::dom::parser dom_parser;
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      continue;
    }
    const auto view = AsView(data[idx]);
    const auto first = view.find_first_not_of(" \t\n\r");
    if (first == std::string_view::npos) {
      continue;
    }
    if (view[first] != '{' && view[first] != '[') {
      continue;
    }

    simdjson::dom::element doc;
    if (dom_parser.parse(view.data(), view.size()).get(doc) !=
        simdjson::SUCCESS) {
      continue;
    }
    if (doc.is_object() || doc.is_array()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("JSON expression indexed by an inverted index must point to "
                "a primitive (string/number/boolean/null) leaf; got an "
                "object or array"));
    }
  }
}

duckdb::Vector EvaluateExprOverChunk(
  const duckdb::Expression& bound_expr, duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& context, bool is_geojson) {
  auto resolved =
    ResolveBoundColumnRefsForChunk(bound_expr, chunk, table_id, slot_to_col_id);
  const auto num_rows = chunk.size();
  duckdb::Vector result(resolved->GetReturnType(), num_rows);
  duckdb::ExpressionExecutor executor(context, *resolved);
  executor.ExecuteExpression(chunk, result);
  if (!is_geojson) {
    RejectJsonObjectArrayLeaves(result, num_rows);
  }
  return result;
}

duckdb::unique_ptr<duckdb::Expression> ResolveBoundColumnRefsForChunk(
  const duckdb::Expression& expr, const duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id) {
  duckdb::vector<duckdb::ColumnBinding> bindings;
  duckdb::vector<duckdb::LogicalType> types;
  SDB_ASSERT(chunk.ColumnCount() >= slot_to_col_id.size());
  const auto count = slot_to_col_id.size();
  bindings.reserve(count);
  types.reserve(count);
  for (duckdb::idx_t slot = 0; slot < count; ++slot) {
    bindings.emplace_back(duckdb::TableIndex(table_id.id()),
                          duckdb::ProjectionIndex(
                            static_cast<duckdb::idx_t>(slot_to_col_id[slot])));
    types.push_back(chunk.data[slot].GetType());
  }
  ChunkBindingResolver resolver(std::move(bindings), std::move(types));
  auto copy = expr.Copy();
  resolver.Resolve(copy);
  return copy;
}

}  // namespace sdb::connector
