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

#include "connector/json_expression_canonicalizer.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/execution/column_binding_resolver.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/lambda_expression.hpp>
#include <duckdb/planner/bound_parameter_map.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <utility>
#include <vector>

#include "connector/json_extract_names.hpp"

namespace sdb::connector {

std::string SerializeBoundExpression(const duckdb::Expression& expr) {
  // Caller is responsible for `NormalizeBoundExpression`-ing the input when
  // bytes will be compared cross-context (CREATE INDEX vs SELECT). The
  // helper itself is now context-free.
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(expr, stream);
  return std::string(reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition());
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

namespace {

// ColumnBindingResolver that uses caller-supplied bindings/types instead of
// building them from a LogicalOperator tree.
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

// Primitive key types accepted in a `->` / `->>` / `json_extract_*` chain.
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

// `#>` / `#>>` pass a single text[] argument (LIST of VARCHAR). Each child
// element must be a non-null primitive key for the chain to be indexable.
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

// Fold `BoundCast(BoundConstant)` -> `BoundConstant(cast(value))`. The
// IndexBinder leaves these in place where the query binder folds them;
// matching the bytes requires us to fold here too.
// TODO(mkornaukhov): run the regular constant-folding optimizer on the
// IndexBinder output so this hand-rolled fold can go away.
duckdb::unique_ptr<duckdb::Expression> FoldConstantCasts(
  duckdb::unique_ptr<duckdb::Expression> expr, duckdb::ClientContext& context) {
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = FoldConstantCasts(std::move(child), context);
    });
  if (expr->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    auto& cast = expr->Cast<duckdb::BoundCastExpression>();
    if (cast.child && cast.child->expression_class ==
                        duckdb::ExpressionClass::BOUND_CONSTANT) {
      try {
        auto folded =
          duckdb::ExpressionExecutor::EvaluateScalar(context, *expr);
        return duckdb::make_uniq<duckdb::BoundConstantExpression>(
          std::move(folded));
      } catch (const std::exception& e) {
        // Falling back to the un-folded cast would diverge from the
        // SELECT-side bytes and silently break index matching.
        SDB_THROW(
          ERROR_INTERNAL,
          "Failed to fold constant cast for inverted index: ", e.what());
      }
    }
  }
  return expr;
}

}  // namespace

duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, ObjectId table_id,
  std::span<const catalog::Column::Id> col_index_to_id,
  duckdb::ClientContext& context) {
  auto copy = FoldConstantCasts(expr.Copy(), context);
  auto visit = [&](auto& self, duckdb::Expression& e) -> void {
    e.SetAlias("");
    e.SetQueryLocation(duckdb::optional_idx());
    if (e.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      auto& cref = e.Cast<duckdb::BoundColumnRefExpression>();
      const auto idx = cref.binding.column_index.GetIndex();
      SDB_ASSERT(idx < col_index_to_id.size());
      const auto col_id = col_index_to_id[idx];
      // Stuff stable catalog ids into the binding fields the binary
      // serialiser already writes -- no alias gymnastics, no extra bytes.
      // Two `(table_id, col_id)` agree across binders by construction.
      cref.binding = duckdb::ColumnBinding(
        duckdb::TableIndex(table_id.id()),
        duckdb::ProjectionIndex(static_cast<duckdb::idx_t>(col_id)));
    } else if (e.GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      e.Cast<duckdb::BoundFunctionExpression>().is_operator = false;
    }
    duckdb::ExpressionIterator::EnumerateChildren(
      e, [&](duckdb::Expression& child) { self(self, child); });
  };
  visit(visit, *copy);
  return copy;
}

const duckdb::BoundColumnRefExpression* TryGetJsonLeafColumnRef(
  const duckdb::Expression& expr) {
  const duckdb::Expression* cur = &expr;
  while (cur->expression_class == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& f = cur->Cast<duckdb::BoundFunctionExpression>();
    // Accept any json-extract shape: `->` / `->>` (2 children: json, key),
    // variadic `json_extract_path[_text]` (2+ children: json, key, key...),
    // and `#>` / `#>>` (2 children: json, text[] list of keys).
    if (!IsJsonExtract(f.function.name) || f.children.size() < 2) {
      return nullptr;
    }
    for (size_t i = 1; i < f.children.size(); ++i) {
      const auto* key_expr = f.children[i].get();
      if (key_expr->expression_class !=
          duckdb::ExpressionClass::BOUND_CONSTANT) {
        return nullptr;
      }
      const auto& key_const = key_expr->Cast<duckdb::BoundConstantExpression>();
      if (key_const.value.IsNull()) {
        return nullptr;
      }
      if (!IsValidKey(key_const.value) && !IsValidPathList(key_const.value)) {
        return nullptr;
      }
    }
    cur = f.children[0].get();
  }
  if (cur->expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &cur->Cast<duckdb::BoundColumnRefExpression>();
}

// TODO(mkornaukhov) get rid of such a function, #597
void RejectJsonObjectArrayLeaves(const duckdb::Vector& result,
                                 duckdb::idx_t num_rows) {
  if (result.GetType().id() != duckdb::LogicalTypeId::VARCHAR) {
    return;
  }
  duckdb::UnifiedVectorFormat fmt;
  result.ToUnifiedFormat(num_rows, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  // DOM (eager): ondemand::iterate() is lazy and would mis-classify
  // `{not json}` as an object on the first character alone.
  simdjson::dom::parser dom_parser;
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      continue;
    }
    const auto& s = data[idx];
    const std::string_view view{s.GetData(), s.GetSize()};
    const auto first = view.find_first_not_of(" \t\n\r");
    if (first == std::string_view::npos) {
      continue;
    }
    if (view[first] != '{' && view[first] != '[') {
      continue;
    }
    // A `{` / `[` prefix means either a real object/array leaf (reject)
    // or a string leaf that happens to look like JSON (accept). Parse to
    // tell them apart. A string that *is* valid JSON (e.g. `"{}"`) still
    // gets mis-rejected -- the post-hoc check can't disambiguate (#597).
    simdjson::dom::element doc;
    if (dom_parser.parse(view.data(), view.size()).get(doc) !=
        simdjson::SUCCESS) {
      continue;
    }
    if (doc.is_object() || doc.is_array()) {
      throw duckdb::InvalidInputException(
        "JSON path indexed by an inverted index must point to a primitive "
        "(string/number/boolean/null) leaf; got an object or array");
    }
  }
}

duckdb::Vector EvaluateJsonPathOverChunk(
  const duckdb::Expression& bound_expr, duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id,
  duckdb::ClientContext& context) {
  auto resolved =
    ResolveBoundColumnRefsForChunk(bound_expr, chunk, table_id, slot_to_col_id);
  const auto num_rows = chunk.size();
  duckdb::Vector result(resolved->return_type, num_rows);
  duckdb::ExpressionExecutor executor(context, *resolved);
  executor.ExecuteExpression(chunk, result);
  RejectJsonObjectArrayLeaves(result, num_rows);
  return result;
}

duckdb::unique_ptr<duckdb::Expression> ResolveBoundColumnRefsForChunk(
  const duckdb::Expression& expr, const duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id) {
  // Build bindings that match the leaves' normalized
  // (TableIndex(table_id.id()), ProjectionIndex(Column::Id)) shape, paired
  // 1:1 with the chunk's column types so the resolver can emit
  // BoundReferenceExpression(slot, chunk_types[slot]).
  duckdb::vector<duckdb::ColumnBinding> bindings;
  duckdb::vector<duckdb::LogicalType> types;
  const auto count =
    std::min<duckdb::idx_t>(chunk.ColumnCount(), slot_to_col_id.size());
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
