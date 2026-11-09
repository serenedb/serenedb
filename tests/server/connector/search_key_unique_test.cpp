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

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <iresearch/analysis/numeric_terms.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "connector/search_key_unique.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

irs::bstring Bytes(std::string_view text) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(text)};
}

primary_key::KeySlot Slot(duckdb::idx_t column, std::string name,
                          duckdb::LogicalType type, irs::DictLayout layout) {
  return {.input_col_idx = column,
          .name = std::move(name),
          .type = std::move(type),
          .column = ColumnId{column + 1},
          .layout = layout};
}

// Fills `chunk` with columns of `types`, row by row from `rows`.
void MakeChunk(duckdb::DataChunk& chunk,
               const duckdb::vector<duckdb::LogicalType>& types,
               const std::vector<std::vector<duckdb::Value>>& rows) {
  chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);
  for (duckdb::idx_t row = 0; row != rows.size(); ++row) {
    for (duckdb::idx_t column = 0; column != types.size(); ++column) {
      chunk.SetValue(column, row, rows[row][column]);
    }
  }
  chunk.SetCardinality(rows.size());
}

// A boolean is indexed as a dictionary member with an empty term, so a key
// built from terms alone makes every boolean row equal. The key carries the
// member, so the two values are told apart -- as PostgreSQL tells them apart
// in a composite primary key.
TEST(SearchKeyUnique, BooleanKeyColumnsDifferByMember) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "tenant_id", duckdb::LogicalType::INTEGER,
         irs::DictLayout::Numeric),
    Slot(1, "is_active", duckdb::LogicalType::BOOLEAN, irs::DictLayout::Bool),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER, duckdb::LogicalType::BOOLEAN},
            {{duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(true)},
             {duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(false)},
             {duckdb::Value::INTEGER(1), duckdb::Value::BOOLEAN(true)}});

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);
  ASSERT_EQ(3, keys.size());

  EXPECT_NE(keys[0], keys[1]);
  EXPECT_EQ(keys[0], keys[2]);

  // The two keys differ in nothing but the member: the mechanism, not just
  // the outcome.
  EXPECT_EQ(keys[0][0], keys[1][0]);
  EXPECT_TRUE(keys[0][1].term.empty());
  EXPECT_TRUE(keys[1][1].term.empty());
  EXPECT_EQ((irs::DictRef{.layout = irs::DictLayout::Bool,
                          .member = irs::DictMember::True}),
            keys[0][1].dict);
  EXPECT_EQ((irs::DictRef{.layout = irs::DictLayout::Bool,
                          .member = irs::DictMember::False}),
            keys[1][1].dict);
}

// Every key column is keyed under the member its values were written to, so a
// number and a string of the same bytes can never collide.
TEST(SearchKeyUnique, ComponentsCarryTheirMember) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "name", duckdb::LogicalType::VARCHAR, irs::DictLayout::String),
    Slot(1, "amount", duckdb::LogicalType::BIGINT, irs::DictLayout::Numeric),
  };
  // Longer than the twelve bytes string_t inlines, so the raw-bytes path is
  // read off the heap rather than out of the vector's own storage.
  constexpr std::string_view kName = "a name far past the inline limit";
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT},
            {{duckdb::Value(std::string{kName}), duckdb::Value::BIGINT(-42)}});

  std::vector<KeyTerms> keys;
  EncodeChunkKeys(chunk, slots, keys);
  ASSERT_EQ(1, keys.size());
  ASSERT_EQ(2, keys[0].size());

  EXPECT_EQ((irs::DictRef{.layout = irs::DictLayout::String,
                          .member = irs::DictMember::Strings}),
            keys[0][0].dict);
  EXPECT_EQ(Bytes(kName), keys[0][0].term);
  EXPECT_EQ((irs::DictRef{.layout = irs::DictLayout::Numeric,
                          .member = irs::DictMember::Numbers}),
            keys[0][1].dict);
  // The bytes the sink would have written for the same value, from the sink's
  // own encoder: a key that drifts from it looks nothing up.
  constexpr int64_t kAmount = -42;
  duckdb::string_t expected[1];
  irs::AppendNumericTermsBlock<int64_t>(expected,
                                        std::span<const int64_t>{&kAmount, 1});
  EXPECT_EQ(Bytes({expected[0].GetData(), expected[0].GetSize()}),
            keys[0][1].term);
}

// GEOMETRY is indexed, but as a cloud of cells rather than as one value, so no
// key component matches a row the way a lookup would. The column is refused by
// name rather than keyed by something the index does not hold.
TEST(SearchKeyUnique, RefusesAColumnWithNoSingleTerm) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "shape", duckdb::LogicalType::GEOMETRY(), irs::DictLayout::String),
  };
  duckdb::DataChunk chunk;
  chunk.Initialize(
    duckdb::Allocator::DefaultAllocator(),
    duckdb::vector<duckdb::LogicalType>{duckdb::LogicalType::GEOMETRY()});
  chunk.SetCardinality(1);

  std::vector<KeyTerms> keys;
  EXPECT_THROW(EncodeChunkKeys(chunk, slots, keys), irs::SqlException);
}

// The keys of one buffer are encoded once: a caller that already holds the
// keys of the rows it has seen extends them with what arrived since.
TEST(SearchKeyUnique, ResumesAtFirstRow) {
  const std::vector<primary_key::KeySlot> slots{
    Slot(0, "id", duckdb::LogicalType::INTEGER, irs::DictLayout::Numeric),
  };
  duckdb::DataChunk chunk;
  MakeChunk(chunk, {duckdb::LogicalType::INTEGER},
            {{duckdb::Value::INTEGER(7)},
             {duckdb::Value::INTEGER(8)},
             {duckdb::Value::INTEGER(9)}});

  std::vector<KeyTerms> whole;
  EncodeChunkKeys(chunk, slots, whole);
  ASSERT_EQ(3, whole.size());

  std::vector<KeyTerms> resumed;
  EncodeChunkKeys(chunk, slots, resumed, 1);
  ASSERT_EQ(2, resumed.size());
  EXPECT_EQ(whole[1], resumed[0]);
  EXPECT_EQ(whole[2], resumed[1]);

  // Past the end appends nothing rather than reading off the chunk.
  EncodeChunkKeys(chunk, slots, resumed, chunk.size());
  EXPECT_EQ(2, resumed.size());
}

}  // namespace
}  // namespace sdb::connector
