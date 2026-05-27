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

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/search_table_marker.h"
#include "rocksdb_engine_catalog/wal_log_data_magics.h"

namespace sdb::connector::search_table_marker {
namespace {

constexpr ObjectId kTableId{0xABCDEF0123456789ULL};

duckdb::vector<duckdb::LogicalType> TwoColTypes() {
  return {duckdb::LogicalType::INTEGER, duckdb::LogicalType::VARCHAR};
}

// Build a DataChunk holding `num_rows` rows of (INTEGER, VARCHAR) with the
// int column = row index and the varchar column = "row_<i>".
void FillTwoColChunk(duckdb::DataChunk& chunk, duckdb::Allocator& alloc,
                     duckdb::idx_t num_rows) {
  auto types = TwoColTypes();
  chunk.Initialize(alloc, types);
  auto& int_vec = chunk.data[0];
  auto& str_vec = chunk.data[1];
  for (duckdb::idx_t i = 0; i < num_rows; ++i) {
    int_vec.SetValue(i, duckdb::Value::INTEGER(static_cast<int32_t>(i)));
    str_vec.SetValue(i, duckdb::Value{"row_" + std::to_string(i)});
  }
  chunk.SetCardinality(num_rows);
}

std::unique_ptr<duckdb::ColumnDataCollection> MakeTwoColCdc(
  duckdb::Allocator& alloc, duckdb::idx_t num_rows) {
  auto cdc =
    std::make_unique<duckdb::ColumnDataCollection>(alloc, TwoColTypes());
  duckdb::DataChunk chunk;
  FillTwoColChunk(chunk, alloc, num_rows);
  cdc->Append(chunk);
  return cdc;
}

// Pull (magic, table_id, column_count, column_ids[]) out of an Insert
// marker and return the remaining bytes (the payload blob).
std::string_view ParseInsertPrefix(std::string_view in, uint8_t& magic_out,
                                   ObjectId& table_id_out,
                                   std::vector<uint64_t>& column_ids_out) {
  EXPECT_GE(in.size(), 1u + 8u + 4u);
  magic_out = static_cast<uint8_t>(in.front());
  in.remove_prefix(1);
  table_id_out = ObjectId{absl::big_endian::Load64(in.data())};
  in.remove_prefix(8);
  auto col_count = absl::big_endian::Load32(in.data());
  in.remove_prefix(4);
  EXPECT_GE(in.size(), col_count * 8u);
  column_ids_out.clear();
  column_ids_out.reserve(col_count);
  for (uint32_t i = 0; i < col_count; ++i) {
    column_ids_out.push_back(absl::big_endian::Load64(in.data()));
    in.remove_prefix(8);
  }
  return in;
}

TEST(SearchTableMarkerTest, EncodeInsertCdcHeader) {
  auto& alloc = duckdb::Allocator::DefaultAllocator();
  auto cdc = MakeTwoColCdc(alloc, 3);
  std::vector<catalog::Column::Id> cols{catalog::Column::Id{42},
                                        catalog::Column::Id{99}};

  auto blob = EncodeInsertCdc(kTableId, cols, *cdc);

  uint8_t magic = 0;
  ObjectId table_id;
  std::vector<uint64_t> column_ids;
  auto payload = ParseInsertPrefix(blob, magic, table_id, column_ids);

  EXPECT_EQ(magic, wal_log_data::kSearchTableInsertCdc);
  EXPECT_EQ(table_id, kTableId);
  ASSERT_EQ(column_ids.size(), 2u);
  EXPECT_EQ(column_ids[0], 42u);
  EXPECT_EQ(column_ids[1], 99u);
  EXPECT_FALSE(payload.empty()) << "CDC payload must follow the header";
}

TEST(SearchTableMarkerTest, EncodeInsertCdcRoundTrips) {
  auto& alloc = duckdb::Allocator::DefaultAllocator();
  auto cdc = MakeTwoColCdc(alloc, 5);
  std::vector<catalog::Column::Id> cols{catalog::Column::Id{1},
                                        catalog::Column::Id{2}};

  auto blob = EncodeInsertCdc(kTableId, cols, *cdc);

  uint8_t magic = 0;
  ObjectId table_id;
  std::vector<uint64_t> column_ids;
  auto payload = ParseInsertPrefix(blob, magic, table_id, column_ids);

  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(payload.data())),
    payload.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  auto decoded = duckdb::ColumnDataCollection::Deserialize(deserializer);
  deserializer.End();

  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->Count(), cdc->Count());
  ASSERT_EQ(decoded->Types().size(), 2u);
  EXPECT_EQ(decoded->Types()[0], duckdb::LogicalType::INTEGER);
  EXPECT_EQ(decoded->Types()[1], duckdb::LogicalType::VARCHAR);
}

TEST(SearchTableMarkerTest, EncodeInsertChunkHeader) {
  auto& alloc = duckdb::Allocator::DefaultAllocator();
  // Real chunks ridden by the operator come from CDC.Chunks() iteration; use
  // the same source so the test sees the same vector shape (flat, with the
  // CDC's allocator-owned blocks) the encoder will see in production.
  auto cdc = MakeTwoColCdc(alloc, 4);
  duckdb::DataChunk chunk;
  chunk.Initialize(alloc, TwoColTypes());
  cdc->FetchChunk(0, chunk);
  std::vector<catalog::Column::Id> cols{catalog::Column::Id{7}};

  auto blob = EncodeInsertChunk(kTableId, cols, chunk);

  uint8_t magic = 0;
  ObjectId table_id;
  std::vector<uint64_t> column_ids;
  auto payload = ParseInsertPrefix(blob, magic, table_id, column_ids);

  EXPECT_EQ(magic, wal_log_data::kSearchTableInsertChunk);
  EXPECT_EQ(table_id, kTableId);
  ASSERT_EQ(column_ids.size(), 1u);
  EXPECT_EQ(column_ids[0], 7u);
  EXPECT_FALSE(payload.empty()) << "DataChunk payload must follow the header";
}

TEST(SearchTableMarkerTest, EncodeInsertChunkRoundTrips) {
  auto& alloc = duckdb::Allocator::DefaultAllocator();
  auto cdc = MakeTwoColCdc(alloc, 6);
  duckdb::DataChunk chunk;
  chunk.Initialize(alloc, TwoColTypes());
  cdc->FetchChunk(0, chunk);
  std::vector<catalog::Column::Id> cols{catalog::Column::Id{1},
                                        catalog::Column::Id{2}};

  auto blob = EncodeInsertChunk(kTableId, cols, chunk);

  uint8_t magic = 0;
  ObjectId table_id;
  std::vector<uint64_t> column_ids;
  auto payload = ParseInsertPrefix(blob, magic, table_id, column_ids);

  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(const_cast<char*>(payload.data())),
    payload.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  duckdb::DataChunk decoded;
  decoded.Deserialize(deserializer);
  deserializer.End();

  EXPECT_EQ(decoded.size(), chunk.size());
  ASSERT_EQ(decoded.ColumnCount(), 2u);
  EXPECT_EQ(decoded.data[0].GetType(), duckdb::LogicalType::INTEGER);
  EXPECT_EQ(decoded.data[1].GetType(), duckdb::LogicalType::VARCHAR);
}

TEST(SearchTableMarkerTest, EncodeDeleteIteratesUntilEnd) {
  std::vector<std::string> pk_storage{"pk_one", "pk_two_longer", "pk3"};
  std::vector<std::string_view> pks{pk_storage[0], pk_storage[1],
                                    pk_storage[2]};

  auto blob = EncodeDelete(kTableId, pks);

  std::string_view in{blob};
  ASSERT_FALSE(in.empty());
  EXPECT_EQ(static_cast<uint8_t>(in.front()), wal_log_data::kSearchTableDelete);
  in.remove_prefix(1);
  ASSERT_GE(in.size(), 8u);
  EXPECT_EQ(ObjectId{absl::big_endian::Load64(in.data())}, kTableId);
  in.remove_prefix(8);

  std::vector<std::string_view> parsed;
  while (!in.empty()) {
    ASSERT_GE(in.size(), 4u);
    auto pk_len = absl::big_endian::Load32(in.data());
    in.remove_prefix(4);
    ASSERT_GE(in.size(), pk_len);
    parsed.emplace_back(in.data(), pk_len);
    in.remove_prefix(pk_len);
  }
  ASSERT_EQ(parsed.size(), pks.size());
  for (size_t i = 0; i < pks.size(); ++i) {
    EXPECT_EQ(parsed[i], pks[i]);
  }
}

TEST(SearchTableMarkerTest, EncodeDeleteEmptyPkListProducesHeaderOnly) {
  auto blob = EncodeDelete(kTableId, {});
  ASSERT_EQ(blob.size(), 1u + 8u);
  EXPECT_EQ(static_cast<uint8_t>(blob[0]), wal_log_data::kSearchTableDelete);
  EXPECT_EQ(absl::big_endian::Load64(blob.data() + 1), kTableId.id());
}

}  // namespace
}  // namespace sdb::connector::search_table_marker
