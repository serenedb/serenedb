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

#include <duckdb.hpp>
#include <limits>
#include <vector>

#include "connector/row_position.h"
#include "gtest/gtest.h"

namespace {

using namespace sdb;
using namespace sdb::connector;

TEST(RowPositionTest, PositionRoundTrips) {
  for (const uint32_t index : {uint32_t{0}, uint32_t{1}, uint32_t{77},
                               std::numeric_limits<uint32_t>::max()}) {
    for (const irs::doc_id_t doc :
         {irs::doc_limits::min(), irs::doc_id_t{2}, irs::doc_id_t{1 << 20},
          std::numeric_limits<irs::doc_id_t>::max() - 1}) {
      const auto position = MakeRowPosition(index, doc);
      EXPECT_EQ(index, RowPositionIndex(position));
      EXPECT_EQ(doc, RowPositionDoc(position));
    }
  }
}

TEST(RowPositionTest, PositionsOrderBySegmentThenDoc) {
  EXPECT_LT(MakeRowPosition(1, 5), MakeRowPosition(1, 6));
  EXPECT_LT(MakeRowPosition(1, std::numeric_limits<irs::doc_id_t>::max() - 1),
            MakeRowPosition(2, irs::doc_limits::min()));
}

TEST(RowPositionTest, FillRowPositions) {
  const std::vector<irs::doc_id_t> docs{10, 12, 14, 16};
  duckdb::Vector from_docs{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(7, docs, from_docs);
  duckdb::SelectionVector sel{4};
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    sel.set_index(i, 2 * i);
  }
  duckdb::Vector from_sel{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(7, 10, &sel, 4, from_sel);
  duckdb::Vector from_run{duckdb::LogicalType::ROW_TYPE, 4};
  FillRowPositions(7, 10, nullptr, 4, from_run);
  EXPECT_EQ(duckdb::VectorType::SEQUENCE_VECTOR, from_run.GetVectorType());
  for (const auto* out : {&from_docs, &from_sel, &from_run}) {
    EXPECT_EQ(4, out->size());
  }
  for (duckdb::idx_t i = 0; i < 4; ++i) {
    EXPECT_EQ(MakeRowPosition(7, docs[i]),
              static_cast<uint64_t>(from_docs.GetValue(i).GetValue<int64_t>()));
    EXPECT_EQ(MakeRowPosition(7, docs[i]),
              static_cast<uint64_t>(from_sel.GetValue(i).GetValue<int64_t>()));
    EXPECT_EQ(MakeRowPosition(7, static_cast<irs::doc_id_t>(10 + i)),
              static_cast<uint64_t>(from_run.GetValue(i).GetValue<int64_t>()));
  }
}

TEST(RowPositionTest, FillRowPositionsOfHighIndexes) {
  constexpr auto kLast = std::numeric_limits<irs::doc_id_t>::max();
  const std::vector<irs::doc_id_t> docs{kLast - 2, kLast - 1, kLast};
  for (const uint32_t index :
       {uint32_t{0}, uint32_t{1} << 31, uint32_t{3000000000},
        std::numeric_limits<uint32_t>::max()}) {
    duckdb::Vector from_docs{duckdb::LogicalType::ROW_TYPE, 3};
    FillRowPositions(index, docs, from_docs);
    duckdb::Vector from_run{duckdb::LogicalType::ROW_TYPE, 3};
    FillRowPositions(index, docs[0], nullptr, 3, from_run);
    for (const auto* out : {&from_docs, &from_run}) {
      for (duckdb::idx_t i = 0; i < 3; ++i) {
        const auto position =
          static_cast<uint64_t>(out->GetValue(i).GetValue<int64_t>());
        EXPECT_EQ(index, RowPositionIndex(position)) << index << " " << i;
        EXPECT_EQ(docs[i], RowPositionDoc(position)) << index << " " << i;
      }
    }
  }
}

}  // namespace
