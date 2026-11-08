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

#include "connector/constraint_keys.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <iresearch/utils/assert.hpp>

#include "connector/index_expression.hpp"

namespace sdb::connector {
namespace {

const duckdb::OrderModifiers kKeyOrder{duckdb::OrderType::ASCENDING,
                                       duckdb::OrderByNullType::NULLS_LAST};

void CollectReads(const duckdb::Expression& expr, std::vector<KeyRead>& reads) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = expr.Cast<duckdb::BoundColumnRefExpression>();
    const auto column =
      static_cast<ColumnId>(ref.Binding().column_index.GetIndex());
    if (absl::c_none_of(
          reads, [&](const KeyRead& read) { return read.column == column; })) {
      reads.push_back({.column = column, .type = ref.GetReturnType()});
    }
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) { CollectReads(child, reads); });
}

duckdb::vector<duckdb::Value> DecodeKey(
  std::string_view key, std::span<const duckdb::LogicalType> types) {
  duckdb::DataChunk chunk;
  chunk.Initialize(
    duckdb::Allocator::DefaultAllocator(),
    duckdb::vector<duckdb::LogicalType>{types.begin(), types.end()}, 1);
  duckdb::vector<duckdb::OrderModifiers> modifiers(types.size(), kKeyOrder);
  duckdb::CreateSortKeyHelpers::DecodeSortKey(
    duckdb::string_t{key.data(), static_cast<uint32_t>(key.size())}, chunk, 0,
    modifiers);
  duckdb::vector<duckdb::Value> values;
  values.reserve(types.size());
  for (auto& column : chunk.data) {
    values.push_back(column.GetValue(0));
  }
  return values;
}

}  // namespace

ConstraintKeyEncoder::ConstraintKeyEncoder(duckdb::ClientContext& context,
                                           duckdb::idx_t table_id,
                                           const catalog::ConstraintKey& key)
  : _context{&context}, _table_id{table_id}, _key{key} {
  _expressions.resize(key.expressions.size());
  for (size_t i = 0; i != key.expressions.size(); ++i) {
    if (!key.expressions[i].empty()) {
      _expressions[i] = DeserializeBoundExpression(key.expressions[i], context);
    }
  }
}

std::vector<KeyRead> ConstraintKeyEncoder::ExpressionReads() const {
  std::vector<KeyRead> reads;
  for (const auto& expr : _expressions) {
    if (expr) {
      CollectReads(*expr, reads);
    }
  }
  return reads;
}

void ConstraintKeyEncoder::Encode(duckdb::DataChunk& chunk,
                                  std::span<const ColumnId> column_ids,
                                  duckdb::Vector& keys) const {
  const auto count = chunk.size();
  const auto parts = _key.columns.size();
  std::vector<duckdb::Vector> evaluated;
  evaluated.reserve(_expressions.size());
  std::vector<duckdb::Vector*> values;
  values.reserve(parts);
  duckdb::vector<duckdb::LogicalType> types;
  types.reserve(parts);
  for (size_t i = 0; i != parts; ++i) {
    if (i < _expressions.size() && _expressions[i]) {
      values.push_back(&evaluated.emplace_back(EvaluateExprOverChunk(
        *_expressions[i], chunk, _table_id, column_ids, *_context)));
    } else {
      const auto slot =
        absl::c_find(column_ids, _key.columns[i]) - column_ids.begin();
      SDB_ASSERT(static_cast<size_t>(slot) < column_ids.size());
      values.push_back(&chunk.data[slot]);
    }
    types.push_back(values.back()->GetType());
  }
  const duckdb::vector<duckdb::OrderModifiers> modifiers(parts, kKeyOrder);
  duckdb::DataChunk input;
  if (_key.types.empty()) {
    input.InitializeEmpty(types);
    for (size_t i = 0; i != parts; ++i) {
      input.data[i].Reference(*values[i]);
    }
  } else {
    input.Initialize(
      duckdb::Allocator::DefaultAllocator(),
      duckdb::vector<duckdb::LogicalType>{_key.types.begin(), _key.types.end()},
      count);
    for (size_t i = 0; i != parts; ++i) {
      if (types[i] == _key.types[i]) {
        input.data[i].Reference(*values[i]);
      } else {
        duckdb::VectorOperations::DefaultCast(*values[i], input.data[i], count);
      }
    }
  }
  input.SetCardinality(count);
  duckdb::CreateSortKeyHelpers::CreateSortKey(input, modifiers, keys);
  duckdb::UnifiedVectorFormat format;
  for (duckdb::idx_t i = 0; i != input.ColumnCount(); ++i) {
    input.data[i].ToUnifiedFormat(count, format);
    if (format.validity.CheckAllValid(count)) {
      continue;
    }
    auto& validity = duckdb::FlatVector::ValidityMutable(keys);
    for (duckdb::idx_t row = 0; row != count; ++row) {
      if (!format.validity.RowIsValid(format.sel->get_index(row))) {
        validity.SetInvalid(row);
      }
    }
  }
}

std::string ConstraintKeyText(duckdb::ClientContext& context,
                              std::string_view key,
                              std::span<const std::string> names,
                              std::span<const duckdb::LogicalType> types) {
  const auto values = DecodeKey(key, types);
  std::string out = absl::StrCat("(", absl::StrJoin(names, ", "), ")=(");
  for (size_t i = 0; i != values.size(); ++i) {
    if (i != 0) {
      out += ", ";
    }
    const auto& value = values[i];
    out += value.IsNull() ? "null"
                          : value.CastAs(context, duckdb::LogicalType::VARCHAR)
                              .GetValue<std::string>();
  }
  out += ")";
  return out;
}

std::string EncodeKeyValue(const duckdb::Value& value) {
  const duckdb::Vector input{value, duckdb::count_t{1}};
  duckdb::Vector keys{duckdb::LogicalType::BLOB, 1};
  duckdb::CreateSortKeyHelpers::CreateSortKey(input, 1, kKeyOrder, keys);
  const auto key = keys.Values<duckdb::string_t>()[0].GetValue();
  return std::string{key.GetData(), key.GetSize()};
}

std::string KeyValueText(std::string_view key,
                         const duckdb::LogicalType& type) {
  return DecodeKey(key, {&type, 1}).front().ToString();
}

}  // namespace sdb::connector
