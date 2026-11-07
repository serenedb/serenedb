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
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <iresearch/utils/assert.hpp>

namespace sdb::connector {
namespace {

const duckdb::OrderModifiers kKeyOrder{duckdb::OrderType::ASCENDING,
                                       duckdb::OrderByNullType::NULLS_LAST};

}  // namespace

void EncodeConstraintKeys(duckdb::DataChunk& chunk,
                          std::span<const ColumnId> column_ids,
                          const catalog::ConstraintKey& key,
                          duckdb::Vector& keys) {
  const auto count = chunk.size();
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
  std::vector<duckdb::idx_t> slots;
  types.reserve(key.columns.size());
  slots.reserve(key.columns.size());
  for (const auto column : key.columns) {
    const auto slot = absl::c_find(column_ids, column) - column_ids.begin();
    SDB_ASSERT(static_cast<size_t>(slot) < column_ids.size());
    slots.push_back(static_cast<duckdb::idx_t>(slot));
    types.push_back(chunk.data[slot].GetType());
    modifiers.push_back(kKeyOrder);
  }
  duckdb::DataChunk input;
  if (key.types.empty()) {
    input.InitializeEmpty(types);
    for (size_t i = 0; i != slots.size(); ++i) {
      input.data[i].Reference(chunk.data[slots[i]]);
    }
  } else {
    input.Initialize(duckdb::Allocator::DefaultAllocator(),
                     duckdb::vector<duckdb::LogicalType>{key.types.begin(),
                                                         key.types.end()},
                     count);
    for (size_t i = 0; i != slots.size(); ++i) {
      if (types[i] == key.types[i]) {
        input.data[i].Reference(chunk.data[slots[i]]);
      } else {
        duckdb::VectorOperations::DefaultCast(chunk.data[slots[i]],
                                              input.data[i], count);
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

std::string ConstraintKeyText(std::string_view key,
                              std::span<const std::string> names,
                              std::span<const duckdb::LogicalType> types) {
  duckdb::DataChunk values;
  values.Initialize(
    duckdb::Allocator::DefaultAllocator(),
    duckdb::vector<duckdb::LogicalType>{types.begin(), types.end()}, 1);
  duckdb::vector<duckdb::OrderModifiers> modifiers(types.size(), kKeyOrder);
  duckdb::CreateSortKeyHelpers::DecodeSortKey(
    duckdb::string_t{key.data(), static_cast<uint32_t>(key.size())}, values, 0,
    modifiers);
  std::string out = absl::StrCat("(", absl::StrJoin(names, ", "), ")=(");
  for (size_t i = 0; i != types.size(); ++i) {
    if (i != 0) {
      out += ", ";
    }
    out += values.data[i].GetValue(0).ToString();
  }
  out += ")";
  return out;
}

}  // namespace sdb::connector
