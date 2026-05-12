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

// It looks like a bad idea to serialize parsed expr,
// but I suppose it's ok for now

std::string SerializeParsedExpression(const duckdb::ParsedExpression& expr) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer::Serialize(expr, stream);
  auto resp = std::string(reinterpret_cast<const char*>(stream.GetData()),
                          stream.GetPosition());
  return resp;
}

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

// Wraps DuckDB's ColumnBindingResolver so it can resolve a single standalone
// expression against a fixed chunk layout. Bindings are synthesised as
// (TableIndex(0), 0..N) so a normalised BoundColumnRefExpression with
// `column_index = c` is rewritten to BoundReferenceExpression(slot=c).
class StandaloneBindingResolver final : public duckdb::ColumnBindingResolver {
 public:
  explicit StandaloneBindingResolver(
    duckdb::vector<duckdb::LogicalType> column_types) {
    bindings = duckdb::LogicalOperator::GenerateColumnBindings(
      duckdb::TableIndex(0), column_types.size());
    types = std::move(column_types);
  }

  void Resolve(duckdb::unique_ptr<duckdb::Expression>& expr) {
    VisitExpression(&expr);
  }
};

}  // namespace
namespace {

// Stringify a single JSON-path key (a constant). Mirrors AppendJsonPathKey
// but writes into a single concatenated string instead of a vector.
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

}  // namespace

duckdb::unique_ptr<duckdb::Expression> NormalizeBoundExpression(
  const duckdb::Expression& expr, ObjectId table_id,
  std::span<const catalog::Column::Id> col_index_to_id) {
  auto copy = expr.Copy();
  // ExpressionIterator::EnumerateChildren only walks one level; recurse
  // explicitly so every node in the tree is normalised.
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

// Walk through any chain of BoundCastExpression wrappers. The binder
// inserts implicit casts between JSON-extract steps (e.g. when a `->>`
// returns VARCHAR but the caller wants JSON, or when an integer key
// constant is widened) and they show up in the bound tree as BOUND_CAST
// nodes that we want to look through.
const duckdb::Expression* PeelBoundCasts(const duckdb::Expression* expr) {
  while (expr->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& c = expr->Cast<duckdb::BoundCastExpression>();
    if (!c.child) {
      break;
    }
    expr = c.child.get();
  }
  return expr;
}

bool IsValidJsonExpr(const duckdb::Expression& expr) {
  const duckdb::Expression* cur = PeelBoundCasts(&expr);
  while (cur->expression_class == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& f = cur->Cast<duckdb::BoundFunctionExpression>();
    if (!IsJsonExtract(f.function.name) || f.children.size() != 2) {
      return false;
    }
    const auto* key_expr = PeelBoundCasts(f.children[1].get());
    if (key_expr->expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
      return false;
    }
    const auto& key_const = key_expr->Cast<duckdb::BoundConstantExpression>();
    if (key_const.value.IsNull()) {
      return false;
    }
    std::string key_str;
    if (!IsValidKey(key_const.value)) {
      return false;
    }
    cur = PeelBoundCasts(f.children[0].get());
  }
  return cur->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF;
}

void RejectJsonObjectArrayLeaves(const duckdb::Vector& result,
                                 duckdb::idx_t num_rows) {
  if (result.GetType().id() != duckdb::LogicalTypeId::VARCHAR) {
    return;
  }
  duckdb::UnifiedVectorFormat fmt;
  result.ToUnifiedFormat(num_rows, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      continue;
    }
    const auto& s = data[idx];
    if (s.GetSize() == 0) {
      continue;
    }
    const char first = *s.GetData();
    if (first == '{' || first == '[') {
      throw duckdb::InvalidInputException(
        "JSON path indexed by an inverted index must point to a primitive "
        "(string/number/boolean/null) leaf; got an object or array");
    }
  }
}

namespace {

// Subclass of ColumnBindingResolver that takes pre-built (binding, type)
// arrays, so callers can wire any (table_index, column_index) scheme they
// like instead of being constrained to the default
// `LogicalCreateIndex` (TableIndex(0), 0..N) shape.
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

}  // namespace

duckdb::unique_ptr<duckdb::Expression> ResolveBoundColumnRefsForChunk(
  const duckdb::Expression& expr, const duckdb::DataChunk& chunk,
  ObjectId table_id, std::span<const catalog::Column::Id> slot_to_col_id) {
  // Build bindings that match the leaves' normalised
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

duckdb::idx_t ExtractJsonSourceColId(const duckdb::Expression& expr) {
  const duckdb::Expression* cur = PeelBoundCasts(&expr);
  while (cur->expression_class == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& f = cur->Cast<duckdb::BoundFunctionExpression>();
    if (!IsJsonExtract(f.function.name) || f.children.size() != 2) {
      return static_cast<duckdb::idx_t>(-1);
    }
    cur = PeelBoundCasts(f.children[0].get());
  }
  if (cur->expression_class == duckdb::ExpressionClass::BOUND_REF) {
    return cur->Cast<duckdb::BoundReferenceExpression>().index;
  }
  if (cur->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return cur->Cast<duckdb::BoundColumnRefExpression>()
      .binding.column_index.GetIndex();
  }
  return static_cast<duckdb::idx_t>(-1);
}

void ComputeJsonMissingMask(const duckdb::Vector& source_json,
                            duckdb::idx_t num_rows,
                            std::span<const std::string> path_keys,
                            std::vector<bool>& out_mask) {
  out_mask.assign(num_rows, false);
  if (path_keys.empty()) {
    return;
  }
  duckdb::UnifiedVectorFormat fmt;
  source_json.ToUnifiedFormat(num_rows, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  // Use simdjson::dom -- non-lazy and re-entrant. ondemand's value-state
  // machine is finicky enough that a navigation into a null-valued field
  // can leave the iterator in a state where subsequent rows misparse;
  // DOM avoids that whole class of issues.
  simdjson::dom::parser parser;
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(idx)) {
      out_mask[i] = true;
      continue;
    }
    const auto& s = data[idx];
    simdjson::dom::element root;
    if (parser.parse(s.GetData(), s.GetSize()).get(root)) {
      out_mask[i] = true;
      continue;
    }
    simdjson::dom::element cur = root;
    bool missing = false;
    for (const auto& key : path_keys) {
      if (cur.is_object()) {
        simdjson::dom::object obj;
        if (cur.get(obj)) {
          missing = true;
          break;
        }
        if (obj.at_key(key).get(cur)) {
          missing = true;
          break;
        }
      } else if (cur.is_array()) {
        int64_t index;
        if (!absl::SimpleAtoi(key, &index)) {
          missing = true;
          break;
        }
        simdjson::dom::array arr;
        if (cur.get(arr)) {
          missing = true;
          break;
        }
        const auto size = arr.size();
        size_t resolved;
        if (index < 0) {
          if (static_cast<size_t>(-index) > size) {
            missing = true;
            break;
          }
          resolved = size + index;
        } else {
          resolved = static_cast<size_t>(index);
        }
        if (arr.at(resolved).get(cur)) {
          missing = true;
          break;
        }
      } else {
        missing = true;
        break;
      }
    }
    out_mask[i] = missing;
  }
}

}  // namespace sdb::connector
