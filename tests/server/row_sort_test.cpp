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

#include <absl/algorithm/container.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/sorting/row_sort.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace {

using Row = std::vector<duckdb::Value>;

const duckdb::OrderModifiers kAscNullsLast{duckdb::OrderType::ASCENDING,
                                           duckdb::OrderByNullType::NULLS_LAST};
const duckdb::OrderModifiers kAscNullsFirst{
  duckdb::OrderType::ASCENDING, duckdb::OrderByNullType::NULLS_FIRST};
const duckdb::OrderModifiers kDescNullsLast{
  duckdb::OrderType::DESCENDING, duckdb::OrderByNullType::NULLS_LAST};
const duckdb::OrderModifiers kDescNullsFirst{
  duckdb::OrderType::DESCENDING, duckdb::OrderByNullType::NULLS_FIRST};

struct Shape {
  std::string_view name;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
};

duckdb::Value RandomValue(const duckdb::LogicalType& type,
                          std::mt19937_64& rng) {
  if (rng() % 8 == 0) {
    return duckdb::Value{type};
  }
  const auto v = static_cast<int64_t>(rng() % 50) - 25;
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return duckdb::Value::BOOLEAN(v > 0);
    case duckdb::LogicalTypeId::SMALLINT:
      return duckdb::Value::SMALLINT(static_cast<int16_t>(v));
    case duckdb::LogicalTypeId::INTEGER:
      return duckdb::Value::INTEGER(static_cast<int32_t>(v));
    case duckdb::LogicalTypeId::BIGINT:
      return duckdb::Value::BIGINT(v * 1'000'000'007);
    case duckdb::LogicalTypeId::HUGEINT:
      return duckdb::Value::HUGEINT(duckdb::hugeint_t{v});
    case duckdb::LogicalTypeId::DOUBLE:
      return duckdb::Value::DOUBLE(static_cast<double>(v) * 0.5);
    default: {
      std::string value(rng() % 24, 'a');
      for (auto& c : value) {
        c = static_cast<char>('a' + rng() % 2);
      }
      return duckdb::Value{value};
    }
  }
}

int Compare(const Row& lhs, const Row& rhs, const Shape& shape) {
  for (size_t c = 0; c != lhs.size(); ++c) {
    const auto& l = lhs[c];
    const auto& r = rhs[c];
    const auto& modifiers = shape.modifiers[c];
    if (l.IsNull() || r.IsNull()) {
      if (l.IsNull() == r.IsNull()) {
        continue;
      }
      const bool nulls_first =
        modifiers.null_type == duckdb::OrderByNullType::NULLS_FIRST;
      return l.IsNull() == nulls_first ? -1 : 1;
    }
    const int cmp = l < r ? -1 : (r < l ? 1 : 0);
    if (cmp != 0) {
      return modifiers.order_type == duckdb::OrderType::DESCENDING ? -cmp : cmp;
    }
  }
  return 0;
}

TEST(RowSortTest, OrdersRowsByEveryKeyLayout) {
  const std::vector<Shape> shapes{
    {"integer", {duckdb::LogicalType::INTEGER}, {kAscNullsLast}},
    {"boolean_smallint",
     {duckdb::LogicalType::BOOLEAN, duckdb::LogicalType::SMALLINT},
     {kDescNullsFirst, kAscNullsLast}},
    {"bigint", {duckdb::LogicalType::BIGINT}, {kDescNullsLast}},
    {"double_integer",
     {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::INTEGER},
     {kAscNullsFirst, kDescNullsLast}},
    {"bigint_bigint",
     {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
     {kAscNullsLast, kDescNullsFirst}},
    {"hugeint", {duckdb::LogicalType::HUGEINT}, {kDescNullsFirst}},
    {"varchar", {duckdb::LogicalType::VARCHAR}, {kAscNullsLast}},
    {"varchar_bigint",
     {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
     {kDescNullsFirst, kAscNullsFirst}},
  };
  constexpr size_t kRows = 2 * STANDARD_VECTOR_SIZE + 904;
  std::mt19937_64 rng{42};
  for (const auto& shape : shapes) {
    SCOPED_TRACE(shape.name);
    std::vector<Row> rows(kRows);
    for (auto& row : rows) {
      for (const auto& type : shape.types) {
        row.push_back(RandomValue(type, rng));
      }
    }
    duckdb::RowSort sort{shape.types, shape.modifiers, kRows};
    duckdb::DataChunk chunk;
    chunk.Initialize(duckdb::Allocator::DefaultAllocator(), shape.types);
    for (size_t first = 0; first < kRows; first += STANDARD_VECTOR_SIZE) {
      const auto n = std::min<size_t>(STANDARD_VECTOR_SIZE, kRows - first);
      chunk.Reset();
      for (size_t c = 0; c != shape.types.size(); ++c) {
        for (size_t r = 0; r != n; ++r) {
          chunk.data[c].Append(rows[first + r][c]);
        }
      }
      chunk.CheckCardinality(n);
      sort.Append(chunk);
    }
    std::vector<duckdb::sel_t> order(kRows);
    sort.Sort(order);
    auto seen = order;
    absl::c_sort(seen);
    for (size_t i = 0; i != kRows; ++i) {
      ASSERT_EQ(seen[i], i);
    }
    for (size_t i = 1; i != kRows; ++i) {
      ASSERT_LE(Compare(rows[order[i - 1]], rows[order[i]], shape), 0)
        << "at " << i;
    }
  }
}

}  // namespace
