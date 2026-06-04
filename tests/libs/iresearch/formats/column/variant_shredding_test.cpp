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

#include <gtest/gtest.h>

#include <algorithm>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <span>
#include <string>
#include <vector>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/merge.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/columnstore/scan.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

using irs::columnstore::ColumnReader;
using irs::columnstore::ReadContext;
using irs::columnstore::Reader;
using irs::columnstore::Writer;

class IRSVariantShreddingTest : public ::testing::Test {
 protected:
  IRSVariantShreddingTest() : _db{nullptr} {}

  duckdb::DatabaseInstance& Db() { return *_db.instance; }

  void SetShreddingSize(int64_t value) {
    duckdb::Connection con{Db()};
    auto r = con.Query("SET variant_minimum_shredding_size = " +
                       std::to_string(value));
    ASSERT_FALSE(r->HasError()) << r->GetError();
  }

 private:
  duckdb::DuckDB _db;
};

// Write `sql` (a single VARIANT column) into a cs column, capturing each row's
// expected Value. The shredding gate must already be configured.
void WriteVariantColumn(duckdb::DatabaseInstance& db, irs::Directory& dir,
                        std::string_view segment, irs::field_id id,
                        const std::string& sql, uint32_t row_group_size,
                        std::vector<duckdb::Value>& expected) {
  Writer w{dir, segment, db};
  auto& cw = w.OpenColumn(id, duckdb::LogicalType::VARIANT(),
                          /*skip_validity=*/false, row_group_size,
                          duckdb::CompressionType::COMPRESSION_AUTO);
  duckdb::Connection con{db};
  auto result = con.Query(sql);
  ASSERT_FALSE(result->HasError()) << result->GetError();
  uint64_t produced = 0;
  while (auto chunk = result->Fetch()) {
    if (chunk->size() == 0) {
      continue;
    }
    ASSERT_EQ(chunk->data[0].GetType().id(), duckdb::LogicalTypeId::VARIANT);
    cw.Append(produced, chunk->data[0], chunk->size());
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      expected.push_back(chunk->GetValue(0, k));
    }
    produced += chunk->size();
  }
  const auto filename = w.Commit(produced);
  ASSERT_FALSE(filename.empty());
}

// Materialize the whole VARIANT column back into Values via the scan path.
std::vector<duckdb::Value> ReadVariantColumn(Reader& r,
                                             const ColumnReader& col) {
  ReadContext ctx{r};
  auto state = irs::columnstore::MakeMaterializeState(col, ctx);
  std::vector<duckdb::Value> out;
  const auto total = col.RowCount();
  out.reserve(total);
  uint64_t pos = 0;
  while (pos < total) {
    const auto take =
      std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
    // Fresh batch each iteration: VARIANT reconstruction copies into the
    // output's internal lists, which would otherwise accumulate across calls.
    duckdb::Vector batch{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
    irs::columnstore::MaterializeNode(
      col, *state, irs::columnstore::IotaRange{pos, take}, batch, 0);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      out.push_back(batch.GetValue(k));
    }
    pos += take;
  }
  return out;
}

void ExpectValuesEqual(const std::vector<duckdb::Value>& expected,
                       const std::vector<duckdb::Value>& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i].IsNull(), actual[i].IsNull()) << "row=" << i;
    if (!expected[i].IsNull()) {
      EXPECT_EQ(expected[i].ToString(), actual[i].ToString()) << "row=" << i;
    }
  }
}

size_t ShreddedRgCount(const ColumnReader& col) {
  size_t n = 0;
  for (size_t rg = 0; rg < col.VariantRgCount(); ++rg) {
    n += col.VariantRg(rg).shredded ? 1 : 0;
  }
  return n;
}

// Run a single-column query and collect its Values (one per row).
std::vector<duckdb::Value> QueryScalarColumn(duckdb::DatabaseInstance& db,
                                             const std::string& sql) {
  duckdb::Connection con{db};
  auto result = con.Query(sql);
  EXPECT_FALSE(result->HasError()) << result->GetError();
  std::vector<duckdb::Value> out;
  while (auto chunk = result->Fetch()) {
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      out.push_back(chunk->GetValue(0, k));
    }
  }
  return out;
}

// Read object path `path` of a VARIANT column directly via the field-extract
// pushdown path (MaterializeExtractNode), one batch at a time, into Values.
std::vector<duckdb::Value> ReadVariantExtract(
  Reader& r, const ColumnReader& col, duckdb::ClientContext& context,
  std::span<const std::string> path, const duckdb::LogicalType& scan_type) {
  ReadContext ctx{r};
  auto state = irs::columnstore::MakeMaterializeState(col, ctx);
  std::vector<duckdb::Value> out;
  const auto total = col.RowCount();
  out.reserve(total);
  uint64_t pos = 0;
  while (pos < total) {
    const auto take =
      std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector batch{scan_type, STANDARD_VECTOR_SIZE};
    irs::columnstore::MaterializeExtractNode(
      col, *state, irs::columnstore::IotaRange{pos, take}, path, scan_type,
      batch, 0, context);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      out.push_back(batch.GetValue(k));
    }
    pos += take;
  }
  return out;
}

// Shredding disabled (-1): every row group stays unshredded, values round-trip.
TEST_F(IRSVariantShreddingTest, RoundTripNoShredding) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "no_shred";
  constexpr uint32_t kRowGroupSize = 64;
  SetShreddingSize(-1);

  std::vector<duckdb::Value> expected;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/1,
                     "SELECT i::VARIANT FROM range(200) t(i)", kRowGroupSize,
                     expected);
  ASSERT_EQ(expected.size(), 200u);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Type().id(), duckdb::LogicalTypeId::VARIANT);
  EXPECT_EQ(col->RowCount(), 200u);
  ASSERT_GT(col->VariantRgCount(), 1u);
  EXPECT_EQ(ShreddedRgCount(*col), 0u);

  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// Shredding forced on (0) with a single row group larger than a DuckDB vector
// (5000 > STANDARD_VECTOR_SIZE): exercises a single-call ShredVariantData over
// a full row group and the unshred-on-read reconstruction.
TEST_F(IRSVariantShreddingTest, RoundTripForcedShreddingLargeRowGroup) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "forced_shred";
  constexpr uint32_t kRowGroupSize = 5000;
  SetShreddingSize(0);

  std::vector<duckdb::Value> expected;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/2,
                     "SELECT i::VARIANT FROM range(5000) t(i)", kRowGroupSize,
                     expected);
  ASSERT_EQ(expected.size(), 5000u);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(2);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), 5000u);
  ASSERT_EQ(col->VariantRgCount(), 1u);
  EXPECT_TRUE(col->VariantRg(0).shredded);

  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// A threshold between the full row-group size and the short trailing row group
// yields a mix of shredded and unshredded row groups in one column.
TEST_F(IRSVariantShreddingTest, MixedShreddedRowGroups) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "mixed_shred";
  constexpr uint32_t kRowGroupSize = 64;
  // Full RGs (64 rows) shred; the trailing partial RG (< 64) does not.
  SetShreddingSize(64);

  std::vector<duckdb::Value> expected;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/3,
                     "SELECT i::VARIANT FROM range(200) t(i)", kRowGroupSize,
                     expected);
  ASSERT_EQ(expected.size(), 200u);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(3);
  ASSERT_NE(col, nullptr);
  // 200 / 64 -> 4 row groups: 3 full (shredded) + 1 partial (unshredded).
  ASSERT_EQ(col->VariantRgCount(), 4u);
  EXPECT_EQ(ShreddedRgCount(*col), 3u);
  EXPECT_FALSE(col->VariantRg(3).shredded);

  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// NULL variant rows interleaved with shredding on: variant-level validity must
// survive reconstruction.
TEST_F(IRSVariantShreddingTest, NullHandlingWithShredding) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "null_shred";
  constexpr uint32_t kRowGroupSize = 64;
  SetShreddingSize(0);

  std::vector<duckdb::Value> expected;
  WriteVariantColumn(
    Db(), dir, kSegment, /*id=*/4,
    "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i::VARIANT END "
    "FROM range(200) t(i)",
    kRowGroupSize, expected);
  ASSERT_EQ(expected.size(), 200u);
  // Sanity: the source really has NULLs.
  ASSERT_TRUE(expected[0].IsNull());
  ASSERT_FALSE(expected[1].IsNull());

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(4);
  ASSERT_NE(col, nullptr);
  EXPECT_GT(ShreddedRgCount(*col), 0u);

  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// Mixed underlying types: the dominant type shreds, the rest land in the
// unshredded leftover column. Round-trips both.
TEST_F(IRSVariantShreddingTest, MixedTypesLeftover) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "leftover_shred";
  constexpr uint32_t kRowGroupSize = 256;
  SetShreddingSize(0);

  std::vector<duckdb::Value> expected;
  WriteVariantColumn(
    Db(), dir, kSegment, /*id=*/5,
    "SELECT CASE WHEN i % 7 = 0 THEN ('str_' || i)::VARIANT "
    "ELSE i::VARIANT END FROM range(500) t(i)",
    kRowGroupSize, expected);
  ASSERT_EQ(expected.size(), 500u);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(5);
  ASSERT_NE(col, nullptr);
  EXPECT_GT(ShreddedRgCount(*col), 0u);

  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// Nested objects: shredding recurses into object fields. Round-trips.
TEST_F(IRSVariantShreddingTest, NestedObjectShredding) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "object_shred";
  constexpr uint32_t kRowGroupSize = 256;
  SetShreddingSize(0);

  std::vector<duckdb::Value> expected;
  WriteVariantColumn(
    Db(), dir, kSegment, /*id=*/6,
    "SELECT {'a': i, 'b': ('v' || i)}::VARIANT FROM range(500) t(i)",
    kRowGroupSize, expected);
  ASSERT_EQ(expected.size(), 500u);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(6);
  ASSERT_NE(col, nullptr);
  EXPECT_GT(ShreddedRgCount(*col), 0u);

  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// Merge two segments (one shredded, one unshredded) into one and round-trip.
// The merge reads each source via MaterializeNode and re-appends, so the
// output re-decides shredding per row group.
TEST_F(IRSVariantShreddingTest, MergeReshreds) {
  irs::MemoryDirectory dir{};
  constexpr uint32_t kRowGroupSize = 128;

  std::vector<duckdb::Value> expected_a;
  std::vector<duckdb::Value> expected_b;

  SetShreddingSize(0);  // segment A shreds
  WriteVariantColumn(Db(), dir, "seg_a", /*id=*/7,
                     "SELECT i::VARIANT FROM range(300) t(i)", kRowGroupSize,
                     expected_a);
  SetShreddingSize(-1);  // segment B does not shred
  WriteVariantColumn(Db(), dir, "seg_b", /*id=*/7,
                     "SELECT (i + 1000)::VARIANT FROM range(300) t(i)",
                     kRowGroupSize, expected_b);

  // Merge into a single segment with shredding on.
  SetShreddingSize(0);
  Reader ra{dir, "seg_a", Db()};
  Reader rb{dir, "seg_b", Db()};
  {
    Writer w{dir, "merged", Db()};
    irs::columnstore::MergeSource sources[2] = {
      {.reader = nullptr, .cs_reader = &ra, .mask = nullptr,
       .alive_count = 300},
      {.reader = nullptr, .cs_reader = &rb, .mask = nullptr,
       .alive_count = 300},
    };
    irs::columnstore::MergeInto(sources, w, /*column_options=*/nullptr);
    const auto filename = w.Commit(600);
    ASSERT_FALSE(filename.empty());
  }

  Reader r{dir, "merged", Db()};
  const auto* col = r.Column(7);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), 600u);

  std::vector<duckdb::Value> expected;
  expected.insert(expected.end(), expected_a.begin(), expected_a.end());
  expected.insert(expected.end(), expected_b.begin(), expected_b.end());
  ExpectValuesEqual(expected, ReadVariantColumn(r, *col));
}

// Field-extract fast path: a fully-shredded object column, read field `a`
// directly as DOUBLE. Matches a CAST(variant.a AS DOUBLE) over the source.
TEST_F(IRSVariantShreddingTest, ExtractFastPath) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "ex_fast";
  constexpr uint32_t kRowGroupSize = 256;
  SetShreddingSize(0);

  const std::string obj =
    "{'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/10,
                     "SELECT " + obj + " FROM range(500) t(i)", kRowGroupSize,
                     ignored);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(10);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->VariantRgCount(), 1u);
  for (size_t rg = 0; rg < col->VariantRgCount(); ++rg) {
    EXPECT_TRUE(col->VariantRg(rg).shredded) << "rg=" << rg;
    EXPECT_TRUE(col->VariantRg(rg).fully_shredded) << "rg=" << rg;
  }

  duckdb::Connection con{Db()};
  const auto expected = QueryScalarColumn(
    Db(), "SELECT (" + obj + ").a::DOUBLE FROM range(500) t(i)");
  const std::vector<std::string> path{"a"};
  ExpectValuesEqual(expected, ReadVariantExtract(r, *col, *con.context, path,
                                                 duckdb::LogicalType::DOUBLE));
}

// Field-extract with a cast: shredded leaf is INTEGER, requested as BIGINT.
TEST_F(IRSVariantShreddingTest, ExtractWithCast) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "ex_cast";
  SetShreddingSize(0);

  const std::string obj = "{'a': (i % 1000)::INTEGER}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/11,
                     "SELECT " + obj + " FROM range(400) t(i)", 256, ignored);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(11);
  ASSERT_NE(col, nullptr);
  EXPECT_GT(ShreddedRgCount(*col), 0u);

  duckdb::Connection con{Db()};
  const auto expected = QueryScalarColumn(
    Db(), "SELECT (" + obj + ").a::BIGINT FROM range(400) t(i)");
  const std::vector<std::string> path{"a"};
  ExpectValuesEqual(expected, ReadVariantExtract(r, *col, *con.context, path,
                                                 duckdb::LogicalType::BIGINT));
}

// Nested object path a.b.
TEST_F(IRSVariantShreddingTest, ExtractNestedPath) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "ex_nested";
  SetShreddingSize(0);

  const std::string obj =
    "{'a': {'b': (i * 2)::DOUBLE}, 'c': 'x'}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/12,
                     "SELECT " + obj + " FROM range(400) t(i)", 256, ignored);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(12);
  ASSERT_NE(col, nullptr);

  duckdb::Connection con{Db()};
  const auto expected = QueryScalarColumn(
    Db(), "SELECT (" + obj + ").a.b::DOUBLE FROM range(400) t(i)");
  const std::vector<std::string> path{"a", "b"};
  ExpectValuesEqual(expected, ReadVariantExtract(r, *col, *con.context, path,
                                                 duckdb::LogicalType::DOUBLE));
}

// NULL variant rows: the extracted field must be NULL there.
TEST_F(IRSVariantShreddingTest, ExtractNullRows) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "ex_null";
  SetShreddingSize(0);

  const std::string obj =
    "CASE WHEN i % 3 = 0 THEN NULL "
    "ELSE {'a': (i * 0.5)::DOUBLE}::VARIANT END";
  std::vector<duckdb::Value> ignored;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/13,
                     "SELECT " + obj + " FROM range(400) t(i)", 256, ignored);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(13);
  ASSERT_NE(col, nullptr);

  duckdb::Connection con{Db()};
  const auto expected = QueryScalarColumn(
    Db(), "SELECT (" + obj + ").a::DOUBLE FROM range(400) t(i)");
  const std::vector<std::string> path{"a"};
  const auto actual = ReadVariantExtract(r, *col, *con.context, path,
                                         duckdb::LogicalType::DOUBLE);
  ExpectValuesEqual(expected, actual);
}

// Fallback path: rows carry a field absent from the shredded type, so the row
// group is not fully shredded and the read must reconstruct + extract. The
// extracted field still matches.
TEST_F(IRSVariantShreddingTest, ExtractFallbackNotFullyShredded) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegment = "ex_fallback";
  SetShreddingSize(0);

  // Field 'a' is DOUBLE for most rows but BIGINT every 5th row. Shredding
  // picks the dominant DOUBLE typed leaf; the BIGINT rows land in the
  // unshredded leftover, so the row group is not fully shredded and the read
  // must reconstruct + extract for it. The extracted value still matches
  // (BIGINT a=i extracts and casts to i.0).
  const std::string obj =
    "CASE WHEN i % 5 = 0 THEN {'a': i}::VARIANT "
    "ELSE {'a': (i * 0.5)::DOUBLE}::VARIANT END";
  std::vector<duckdb::Value> ignored;
  WriteVariantColumn(Db(), dir, kSegment, /*id=*/14,
                     "SELECT " + obj + " FROM range(400) t(i)", 256, ignored);

  Reader r{dir, std::string{kSegment}, Db()};
  const auto* col = r.Column(14);
  ASSERT_NE(col, nullptr);
  // At least one row group should have leftovers (not fully shredded).
  bool any_not_fully = false;
  for (size_t rg = 0; rg < col->VariantRgCount(); ++rg) {
    any_not_fully |= col->VariantRg(rg).shredded &&
                     !col->VariantRg(rg).fully_shredded;
  }
  EXPECT_TRUE(any_not_fully);

  duckdb::Connection con{Db()};
  const auto expected = QueryScalarColumn(
    Db(), "SELECT (" + obj + ").a::DOUBLE FROM range(400) t(i)");
  const std::vector<std::string> path{"a"};
  ExpectValuesEqual(expected, ReadVariantExtract(r, *col, *con.context, path,
                                                 duckdb::LogicalType::DOUBLE));
}

}  // namespace
