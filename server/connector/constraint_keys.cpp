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
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <iresearch/utils/assert.hpp>

#include "connector/index_expression.hpp"

namespace sdb::connector {
namespace {

const duckdb::OrderModifiers kKeyOrder{duckdb::OrderType::ASCENDING,
                                       duckdb::OrderByNullType::NULLS_LAST};

duckdb::vector<duckdb::Value> DecodeKey(
  std::string_view key, const duckdb::vector<duckdb::LogicalType>& types) {
  duckdb::DataChunk chunk;
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types, 1);
  duckdb::vector<duckdb::OrderModifiers> modifiers(types.size(), kKeyOrder);
  duckdb::CreateSortKeyHelpers::DecodeSortKey(duckdb::string_t::Unsafe(key),
                                              chunk, 0, modifiers);
  duckdb::vector<duckdb::Value> values;
  values.reserve(types.size());
  for (auto& column : chunk.data) {
    values.push_back(column.GetValue(0));
  }
  return values;
}

duckdb::unique_ptr<duckdb::Expression> CollationOf(
  duckdb::ClientContext& context, const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::VARCHAR ||
      duckdb::StringType::GetCollation(type).empty()) {
    return nullptr;
  }
  duckdb::unique_ptr<duckdb::Expression> collation =
    duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, 0);
  if (!duckdb::ExpressionBinder::PushCollation(
        context, collation, type, duckdb::CollationType::EQUALITY_COLLATIONS)) {
    return nullptr;
  }
  return collation;
}

}  // namespace

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

void ConstraintKeyEncoder::Prepare(const duckdb::DataChunk& chunk,
                                   std::span<const ColumnId> column_ids) {
  _prepared = std::make_unique<Prepared>(*_context);
  auto& prepared = *_prepared;
  const auto parts = _key.columns.size();
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::LogicalType> intervals;
  for (size_t i = 0; i != parts; ++i) {
    duckdb::unique_ptr<duckdb::Expression> part;
    if (i < _expressions.size() && _expressions[i]) {
      part = ResolveOverChunk(*_expressions[i], chunk, _table_id, column_ids);
    } else {
      const auto slot =
        absl::c_find(column_ids, _key.columns[i]) - column_ids.begin();
      SDB_ASSERT(static_cast<size_t>(slot) < column_ids.size());
      part = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        chunk.data[slot].GetType(), static_cast<duckdb::idx_t>(slot));
    }
    if (!_key.types.empty() && part->GetReturnType() != _key.types[i]) {
      part = duckdb::BoundCastExpression::AddDefaultCastToType(std::move(part),
                                                               _key.types[i]);
    }
    const auto type = part->GetReturnType();
    if (type.id() == duckdb::LogicalTypeId::INTERVAL) {
      prepared.intervals.push_back(i);
      intervals.push_back(type);
    } else if (type.id() == duckdb::LogicalTypeId::VARCHAR &&
               !duckdb::StringType::GetCollation(type).empty()) {
      duckdb::ExpressionBinder::PushCollation(
        *_context, part, type, duckdb::CollationType::EQUALITY_COLLATIONS);
    }
    types.push_back(part->GetReturnType());
    prepared.executor.AddExpression(*part);
    prepared.parts.push_back(std::move(part));
  }
  auto& allocator = duckdb::Allocator::Get(*_context);
  prepared.input.Initialize(allocator, types);
  if (!intervals.empty()) {
    prepared.normalized.Initialize(allocator, intervals);
  }
  prepared.modifiers.assign(parts, kKeyOrder);
  prepared.keys.Initialize(allocator, {duckdb::LogicalType::BLOB});
}

const duckdb::Vector& ConstraintKeyEncoder::Encode(
  duckdb::DataChunk& chunk, std::span<const ColumnId> column_ids) {
  if (!_prepared) {
    Prepare(chunk, column_ids);
  }
  auto& prepared = *_prepared;
  const auto count = chunk.size();
  auto& input = prepared.input;
  input.Reset();
  prepared.keys.Reset();
  auto& keys = prepared.keys.data[0];
  prepared.executor.Execute(&chunk, input);
  if (!prepared.intervals.empty()) {
    prepared.normalized.Reset();
    for (size_t i = 0; i != prepared.intervals.size(); ++i) {
      auto& column = input.data[prepared.intervals[i]];
      auto& normalized = prepared.normalized.data[i];
      duckdb::UnaryExecutor::Execute<duckdb::interval_t, duckdb::interval_t>(
        column, normalized, count,
        [](duckdb::interval_t value) { return value.Normalize(); });
      column.Reference(normalized);
    }
  }
  duckdb::CreateSortKeyHelpers::CreateSortKey(input, prepared.modifiers, keys);
  auto& format = prepared.format;
  for (auto& column : input.data) {
    column.ToUnifiedFormat(count, format);
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
  return keys;
}

std::string ConstraintKeyText(duckdb::ClientContext& context,
                              std::string_view key,
                              std::span<const std::string> names,
                              std::span<const duckdb::LogicalType> types) {
  duckdb::vector<duckdb::LogicalType> encoded;
  encoded.reserve(types.size());
  for (const auto& type : types) {
    const auto collation = CollationOf(context, type);
    encoded.push_back(collation ? collation->GetReturnType() : type);
  }
  const auto values = DecodeKey(key, encoded);
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
  return DecodeKey(key, duckdb::vector<duckdb::LogicalType>{type})
    .front()
    .ToString();
}

}  // namespace sdb::connector
