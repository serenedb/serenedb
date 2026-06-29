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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <random>
#include <set>

#include "basics/resource_manager.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/norm_column_reader.hpp"
#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/column/scan.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory_cleaner.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

class IRSColumnstoreTest : public ::testing::Test {
 protected:
  IRSColumnstoreTest() : _db{nullptr} {}

  duckdb::DatabaseInstance& Db() { return *_db.instance; }

 private:
  duckdb::DuckDB _db;
};

// Count data-row-groups in a typed ColumnReader by walking via Locate.
// The public ColumnReader no longer exposes RowGroupCount; tests that
// only need the count derive it from the per-row Locate walk.
size_t CountRowGroups(const irs::ColumnReader& col) {
  if (col.RowCount() == 0) {
    return 0;
  }
  size_t n = 0;
  irs::RgWindow w{};
  for (uint64_t row = 0; row < col.RowCount();) {
    w = col.Locate(row, w);
    ++n;
    row = w.end;
  }
  return n;
}

using TypedPointCursor = irs::ColumnReader::PointReader;

TEST_F(IRSColumnstoreTest, RoundTripInt64Dense) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "test_segment";
  constexpr uint64_t kRowCount = 5000;

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);

    // Build a chunk's worth of values, append in 2048-row batches.
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        data[k] = static_cast<int64_t>((produced + k) * 7 + 3);
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }

  // Read
  {
    irs::SegmentMeta meta;
    meta.name = std::string{kSegmentName};
    irs::ColReader r{dir, meta.name, Db()};

    ASSERT_TRUE(r.HasColumn(1));
    const auto* col = r.Column(1);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    EXPECT_EQ(CountRowGroups(*col), 1u);

    irs::ReadContext ctx{r};
    auto seg = col->OpenSegment(0, ctx);
    duckdb::ColumnScanState state{nullptr};
    seg->InitializeScan(state);
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};

    duckdb::idx_t scanned = 0;
    while (scanned < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - scanned, STANDARD_VECTOR_SIZE);
      seg->Scan(state, take, out, 0, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      state.offset_in_column += take;
      auto* data = duckdb::FlatVector::GetData<int64_t>(out);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        EXPECT_EQ(data[k], static_cast<int64_t>((scanned + k) * 7 + 3))
          << "scanned=" << scanned << " k=" << k;
      }
      scanned += take;
    }
  }
}

TEST_F(IRSColumnstoreTest, RoundTripInt64WithNulls) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "test_nulls";
  constexpr uint64_t kRowCount = 100;

  // Write all rows contiguously: even rows hold a value, odd rows are
  // null. One Append() per pair of rows.
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);

    duckdb::Vector batch{duckdb::LogicalType::BIGINT, 2};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    auto& valid = duckdb::FlatVector::ValidityMutable(batch);
    for (uint64_t i = 0; i < kRowCount; i += 2) {
      data[0] = static_cast<int64_t>(i);
      valid.SetValid(0);
      valid.SetInvalid(1);
      cw.Append(i, batch, 2);
    }
    w.Commit(kRowCount);
  }

  {
    irs::SegmentMeta meta;
    meta.name = std::string{kSegmentName};
    irs::ColReader r{dir, meta.name, Db()};
    const auto* col = r.Column(1);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);

    TypedPointCursor cursor{r, *col};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
    auto* outd = duckdb::FlatVector::GetDataMutable<int64_t>(out);

    for (uint64_t i = 0; i < kRowCount; ++i) {
      duckdb::FlatVector::ValidityMutable(out).Reset();
      cursor.FetchRow(i, out, 0);
      const auto& validity = duckdb::FlatVector::Validity(out);
      if (i % 2 == 0) {
        EXPECT_TRUE(validity.RowIsValid(0)) << "i=" << i;
        EXPECT_EQ(outd[0], static_cast<int64_t>(i));
      } else {
        EXPECT_FALSE(validity.RowIsValid(0)) << "i=" << i;
      }
    }
  }
}

// Norm columns share the .col file but bypass the duckdb codec pipeline:
// per-row-group raw 1/2/4-byte payload picked from the row group's max,
// plus per-row-group max/sum/non_zero stats serialised into the footer.
// The test exercises a multi-row-group write where each row group lands
// at a different byte_size (forces the per-RG encoding choice path).
TEST_F(IRSColumnstoreTest, NormColumnRoundTripPerRowGroupEncoding) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "norm_segment";
  // Three row groups of 100 each. Group 0 stays in 1-byte range,
  // group 1 escalates to 2-byte, group 2 to 4-byte. Row-group size is
  // configured small so each block flushes inside Append().
  constexpr uint64_t kPerGroup = 100;
  constexpr uint64_t kRowCount = 3 * kPerGroup;

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& nw = w.OpenNormColumn(/*id=*/42, /*row_group_size=*/kPerGroup);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      uint32_t v;
      if (i < kPerGroup) {
        v = static_cast<uint32_t>(i % 200);  // 1-byte range
      } else if (i < 2 * kPerGroup) {
        v = 300 + static_cast<uint32_t>(i);  // 2-byte range
      } else {
        v = 100000 + static_cast<uint32_t>(i);  // 4-byte range
      }
      nw.Append(i, v);
    }
    w.Commit(kRowCount);
  }

  // Read + verify each row group's encoding + per-row values.
  {
    irs::ColReader r{dir, kSegmentName, Db()};
    ASSERT_TRUE(r.HasNormColumn(42));
    const auto* col = r.NormColumn(42);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    ASSERT_EQ(col->RowGroupCount(), 3u);

    EXPECT_EQ(col->ByteSize(0), 1u);
    EXPECT_EQ(col->ByteSize(1), 2u);
    EXPECT_EQ(col->ByteSize(2), 4u);

    EXPECT_EQ(col->RowGroupRowCount(0), kPerGroup);
    EXPECT_EQ(col->RowGroupRowCount(1), kPerGroup);
    EXPECT_EQ(col->RowGroupRowCount(2), kPerGroup);

    // Locate -> RowGroupBytes -> stride-indexed read mirrors the BM25
    // hot-loop pattern.
    for (uint64_t i = 0; i < kRowCount; ++i) {
      auto [rg, in_rg] = col->Locate(i);
      const auto byte_size = col->ByteSize(rg);
      auto bytes = col->RowGroupBytes(rg);
      const auto v =
        irs::ReadNormValue(bytes.data() + in_rg * byte_size, byte_size);

      uint32_t expected;
      if (i < kPerGroup) {
        expected = static_cast<uint32_t>(i % 200);
      } else if (i < 2 * kPerGroup) {
        expected = 300 + static_cast<uint32_t>(i);
      } else {
        expected = 100000 + static_cast<uint32_t>(i);
      }
      EXPECT_EQ(v, expected) << "i=" << i << " rg=" << rg << " in_rg=" << in_rg;
      // Get() convenience path matches the stride-indexed read.
      EXPECT_EQ(col->Get(i), expected) << "Get i=" << i;
    }

    // Aggregate stats: BM25 GetAvg = sum / non_zero_count summed across
    // row groups -- verify the reader's totals match a manual rollup.
    uint64_t expected_sum = 0;
    uint64_t expected_non_zero = 0;
    for (uint64_t i = 0; i < kRowCount; ++i) {
      uint32_t v;
      if (i < kPerGroup) {
        v = static_cast<uint32_t>(i % 200);
      } else if (i < 2 * kPerGroup) {
        v = 300 + static_cast<uint32_t>(i);
      } else {
        v = 100000 + static_cast<uint32_t>(i);
      }
      expected_sum += v;
      expected_non_zero += static_cast<uint64_t>(v != 0);
    }
    EXPECT_EQ(col->Sum(), expected_sum);
    EXPECT_EQ(col->NonZeroCount(), expected_non_zero);
  }
}

// Single-row-group config: matches the old format's byte-pointer-fast
// access -- one big block, one open-time read, RowGroupBytes() spans the
// entire segment.
TEST_F(IRSColumnstoreTest, NormColumnSingleRowGroupOldFormatShape) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "norm_segment_single";
  constexpr uint64_t kRowCount = 4096;

  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& nw = w.OpenNormColumn(/*id=*/7, /*row_group_size=*/kRowCount + 1);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      // All values fit in 1 byte -- this exercises the byte_size==1 path
      // and the stride-1 BM25 fast path the user described.
      nw.Append(i, static_cast<uint32_t>(i % 250));
    }
    w.Commit(kRowCount);
  }

  {
    irs::ColReader r{dir, kSegmentName, Db()};
    const auto* col = r.NormColumn(7);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowGroupCount(), 1u);
    EXPECT_EQ(col->ByteSize(0), 1u);
    auto bytes = col->RowGroupBytes(0);
    ASSERT_EQ(bytes.size(), kRowCount);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      EXPECT_EQ(static_cast<uint32_t>(bytes[i]),
                static_cast<uint32_t>(i % 250));
    }
  }
}

// The SegmentWriter flush integration historically read per-doc norms from
// the NormColumnWriter during the same flush cycle the values were being
// appended. The current NormColumnWriter doesn't expose in-flight reads;
// the SegmentWriter side accumulates per-row-group stats during Append
// and serialises them at Commit. Verifies the on-disk view only.
TEST_F(IRSColumnstoreTest, NormColumnRoundTripWithStats) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "norm_in_flight";
  constexpr uint64_t kRowCount = 250;

  irs::ColWriter w{dir, kSegmentName, Db()};
  auto& nw = w.OpenNormColumn(/*id=*/3, /*row_group_size=*/100);

  uint64_t expected_sum = 0;
  uint64_t expected_non_zero = 0;
  for (uint64_t i = 0; i < kRowCount; ++i) {
    const uint32_t v = (i % 7 == 0) ? 0 : static_cast<uint32_t>(i + 1);
    nw.Append(i, v);
    expected_sum += v;
    expected_non_zero += static_cast<uint64_t>(v != 0);
  }

  w.Commit(kRowCount);

  irs::ColReader r{dir, kSegmentName, Db()};
  const auto* col = r.NormColumn(3);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), kRowCount);
  EXPECT_EQ(col->Sum(), expected_sum);
  EXPECT_EQ(col->NonZeroCount(), expected_non_zero);
  // 100 + 100 + 50 -> 3 row groups under the row_group_size=100 setting.
  EXPECT_EQ(col->RowGroupCount(), 3u);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    const uint32_t expected = (i % 7 == 0) ? 0 : static_cast<uint32_t>(i + 1);
    EXPECT_EQ(col->Get(i), expected) << "post-commit i=" << i;
  }
}

// Forward-compat: opening a `.col` file that has no norm_columns list
// (older writes) returns no norm readers and HasNormColumn=false.
TEST_F(IRSColumnstoreTest, NormColumnAbsentOnTypedOnlySegment) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "typed_only";
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(1, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, 8};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (int i = 0; i < 8; ++i) {
      data[i] = i;
    }
    cw.Append(0, batch, 8);
    w.Commit(/*target_row=*/8);
  }
  irs::ColReader r{dir, kSegmentName, Db()};
  EXPECT_FALSE(r.HasNormColumn(1));
  EXPECT_EQ(r.NormColumn(1), nullptr);
  EXPECT_TRUE(r.HasColumn(1));
}

// ARRAY(FLOAT, dim) round-trip. Mirrors how IVF vectors will be stored:
// fixed-size float arrays per doc with array-level validity. Verifies the
// recursive walk (writer FlushNode -> reader ColumnReader::Child()) lands
// the same float bytes back.
TEST_F(IRSColumnstoreTest, RoundTripArrayFloatDense) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "array_dense";
  constexpr uint64_t kRowCount = 1000;
  constexpr uint64_t kDim = 8;

  const auto array_type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/42, array_type);

    duckdb::Vector batch{array_type, STANDARD_VECTOR_SIZE};
    auto& child = duckdb::ArrayVector::GetChildMutable(batch);
    auto* child_data = duckdb::FlatVector::GetDataMutable<float>(child);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        for (uint64_t d = 0; d < kDim; ++d) {
          child_data[k * kDim + d] =
            static_cast<float>((produced + k) * 100 + d);
        }
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }

  // Read: parent ARRAY column exposes child ColumnReader; element data is
  // a flat FLOAT primitive segment with kRowCount * kDim entries.
  {
    irs::ColReader r{dir, kSegmentName, Db()};
    ASSERT_TRUE(r.HasColumn(42));
    const auto* col = r.Column(42);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->Type().id(), duckdb::LogicalTypeId::ARRAY);
    EXPECT_EQ(col->ArraySize(), kDim);
    EXPECT_EQ(col->RowCount(), kRowCount);

    const auto* element = col->Child();
    ASSERT_NE(element, nullptr);
    EXPECT_EQ(element->Type().id(), duckdb::LogicalTypeId::FLOAT);
    EXPECT_EQ(element->RowCount(), kRowCount * kDim);

    // Scan the element child as a flat FLOAT column and reassemble per-doc
    // vectors -- mirrors how IvfVectorReader will pull per-doc bytes.
    irs::ReadContext ctx{r};
    auto seg = element->OpenSegment(0, ctx);
    duckdb::ColumnScanState state{nullptr};
    seg->InitializeScan(state);
    duckdb::Vector out{duckdb::LogicalType::FLOAT, STANDARD_VECTOR_SIZE};
    const uint64_t total_floats = kRowCount * kDim;
    duckdb::idx_t scanned = 0;
    while (scanned < total_floats) {
      const auto take =
        std::min<duckdb::idx_t>(total_floats - scanned, STANDARD_VECTOR_SIZE);
      seg->Scan(state, take, out, 0, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
      state.offset_in_column += take;
      auto* data = duckdb::FlatVector::GetData<float>(out);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto global = scanned + k;
        const auto row = global / kDim;
        const auto d = global % kDim;
        EXPECT_FLOAT_EQ(data[k], static_cast<float>(row * 100 + d))
          << "row=" << row << " d=" << d;
      }
      scanned += take;
    }
  }
}

// VARCHAR round-trip exercising the tight-packed overflow string path.
// Strings span three regimes:
//   - short (< STRING_INLINE_LENGTH = 12 bytes): inlined into string_t,
//     never reach the overflow path.
//   - medium (fits inline in segment dictionary block): stored in the
//     segment block bytes, never reach the overflow path.
//   - long (overflow > segment block remaining space): routed through
//     IndexOutputOverflowWriter on write, IndexInputOverflowReader on
//     read. Verifies our (block_id = file_offset, length-prefixed bytes)
//     layout round-trips correctly and that strings exceeding 256KB
//     round-trip in a single ReadData call (no DuckDB-style block chain).
TEST_F(IRSColumnstoreTest, RoundTripVarcharOverflow) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "varchar_overflow";
  constexpr uint64_t kRowCount = 200;

  // Build inputs: a mix of sizes hitting all three regimes.
  std::vector<std::string> inputs(kRowCount);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    if (i < 50) {
      inputs[i] = "s" + std::to_string(i);  // short, inlined string_t
    } else if (i < 150) {
      // ~512 bytes -- lives in segment block, no overflow.
      inputs[i] = std::string(500, char('a' + (i % 26))) + std::to_string(i);
    } else {
      // 64KB+ -- forces overflow. Last few span > 256KB to exercise what
      // DuckDB would split across multiple chained blocks; for us it's a
      // single contiguous ReadData lookup.
      const auto sz = (i == kRowCount - 1) ? (300u * 1024u) : (70u * 1024u);
      inputs[i] = std::string(sz, char('A' + (i % 26)));
    }
  }

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/7, duckdb::LogicalType::VARCHAR);

    duckdb::Vector batch{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto& s = inputs[produced + k];
        slots[k] =
          duckdb::StringVector::AddStringOrBlob(batch, s.data(), s.size());
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }

  // Read
  {
    irs::ColReader r{dir, std::string{kSegmentName}, Db()};
    ASSERT_TRUE(r.HasColumn(7));
    const auto* col = r.Column(7);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->Type().id(), duckdb::LogicalTypeId::VARCHAR);
    EXPECT_EQ(col->RowCount(), kRowCount);

    // Some codecs split the row group into multiple data segments. Walk
    // every data row group, scan all rows, and verify byte-identity.
    irs::ReadContext ctx{r};
    duckdb::idx_t verified = 0;
    irs::RgWindow window{};
    for (uint64_t row_pos = 0; row_pos < col->RowCount();) {
      window = col->Locate(row_pos, window);
      auto seg = col->OpenSegment(window.rg, ctx);
      const auto rg_count =
        static_cast<duckdb::idx_t>(window.end - window.begin);
      duckdb::ColumnScanState state{nullptr};
      seg->InitializeScan(state);
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      duckdb::idx_t scanned = 0;
      while (scanned < rg_count) {
        const auto take =
          std::min<duckdb::idx_t>(rg_count - scanned, STANDARD_VECTOR_SIZE);
        seg->Scan(state, take, out, 0,
                  duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
        state.offset_in_column += take;
        auto* data = duckdb::FlatVector::GetData<duckdb::string_t>(out);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          const auto& expected = inputs[verified + k];
          ASSERT_EQ(data[k].GetSize(), expected.size())
            << "row " << (verified + k);
          EXPECT_EQ(std::string_view(data[k].GetData(), data[k].GetSize()),
                    std::string_view(expected.data(), expected.size()))
            << "row " << (verified + k);
        }
        scanned += take;
      }
      verified += rg_count;
      row_pos = window.end;
    }
    EXPECT_EQ(verified, kRowCount);
  }
}

// Per-doc point access via PointReadCursor. Writes enough rows to span
// multiple row groups, then reads them back in three patterns:
//   1) sequential -- verifies in-order reads share the cached segment.
//   2) random -- verifies the cursor releases / re-opens segments
//      correctly across row-group boundaries.
//   3) repeated within one rg -- verifies the cached open segment is
//      reused (we don't have a cheap counter to assert reuse, but the
//      bytes round-trip; functional correctness implies the dispatch
//      worked).
TEST_F(IRSColumnstoreTest, PointReadCursorAcrossRowGroups) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "point_read";
  // Two full row groups + a tail. Default DEFAULT_ROW_GROUP_SIZE = 122880;
  // a smaller row group is fine here since codec selection is what
  // matters, not size.
  constexpr uint64_t kRowCount = 5000;
  constexpr uint32_t kRowGroupSize = 1000;

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(
      /*id=*/9, duckdb::LogicalType::BIGINT,
      /*skip_validity=*/true, /*row_group_size=*/kRowGroupSize,
      duckdb::CompressionType::COMPRESSION_AUTO, false);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        data[k] = static_cast<int64_t>((produced + k) * 13 + 7);
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }

  // Read
  {
    irs::ColReader r{dir, std::string{kSegmentName}, Db()};
    const auto* col = r.Column(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    EXPECT_GE(CountRowGroups(*col), 5u);  // 5000 / 1000 = at least 5

    TypedPointCursor cursor{r, *col};
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* outd = duckdb::FlatVector::GetDataMutable<int64_t>(out);

    auto expected = [](uint64_t row) -> int64_t {
      return static_cast<int64_t>(row * 13 + 7);
    };

    // 1) sequential -- exercises in-rg cache reuse + boundary crossings.
    for (uint64_t row = 0; row < kRowCount; ++row) {
      cursor.FetchRow(row, out, 0);
      ASSERT_EQ(outd[0], expected(row)) << "sequential row=" << row;
    }

    // 2) random -- exercises arbitrary rg opens. Use a fixed seed so test
    //    is deterministic.
    std::mt19937 rng{0xc0ffee};
    for (int i = 0; i < 500; ++i) {
      const uint64_t row = rng() % kRowCount;
      cursor.FetchRow(row, out, 0);
      ASSERT_EQ(outd[0], expected(row)) << "random row=" << row;
    }

    // 3) repeated reads of the same row -- worst case for the cursor: it
    //    should reuse the cached open segment and the same fetch state.
    for (int i = 0; i < 100; ++i) {
      cursor.FetchRow(42, out, 0);
      ASSERT_EQ(outd[0], expected(42));
    }
  }
}

// PointReadCursor on a VARCHAR column with strings that span the overflow
// path. Verifies the cursor + IndexInputOverflowReader interplay:
// the cached ColumnSegment carries our overflow_reader, and per-doc
// FetchRow resolves long strings via the (block_id = file_offset)
// tight-packed format.
TEST_F(IRSColumnstoreTest, PointReadCursorVarcharWithOverflow) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "point_read_str";
  constexpr uint64_t kRowCount = 100;

  std::vector<std::string> inputs(kRowCount);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    if (i % 4 == 0) {
      inputs[i] = "tiny" + std::to_string(i);
    } else if (i % 4 == 2) {
      inputs[i] = std::string(800, char('a' + (i % 26))) + std::to_string(i);
    } else {
      // Force overflow: > inline budget.
      inputs[i] = std::string(80 * 1024, char('A' + (i % 26)));
    }
  }

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/11, duckdb::LogicalType::VARCHAR);
    duckdb::Vector batch{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto& s = inputs[produced + k];
        slots[k] =
          duckdb::StringVector::AddStringOrBlob(batch, s.data(), s.size());
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }

  // Read via cursor
  {
    irs::ColReader r{dir, std::string{kSegmentName}, Db()};
    const auto* col = r.Column(11);
    ASSERT_NE(col, nullptr);
    TypedPointCursor cursor{r, *col};
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    auto* outd = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(out);

    // Random-order fetches force cache misses + reopens.
    std::mt19937 rng{0xfeedface};
    for (int i = 0; i < 200; ++i) {
      const uint64_t row = rng() % kRowCount;
      cursor.FetchRow(row, out, 0);
      const auto& expected = inputs[row];
      ASSERT_EQ(outd[0].GetSize(), expected.size()) << "row=" << row;
      EXPECT_EQ(std::string_view(outd[0].GetData(), outd[0].GetSize()),
                std::string_view(expected.data(), expected.size()))
        << "row=" << row;
    }
  }
}

// LIST<BLOB> round-trip. Mirrors how composite stored-list columns will
// land in new cs: each row carries a variable-length list of byte
// strings; the level itself stores compressed UBIGINT lengths, the
// element child holds the flattened BLOB bytes. Sized + row-group-sized
// to span at least 2 row groups so the per-RG cumulative-offset
// accounting and validity-pointer arithmetic both get exercised.
TEST_F(IRSColumnstoreTest, RoundTripListBlob) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "list_blob";
  constexpr uint64_t kRowCount = 200;
  constexpr uint32_t kRowGroupSize = 64;  // forces 4 row groups

  // Build per-row lists of varying length. Element bytes are
  // deterministic so we can verify byte-identity post round-trip.
  std::vector<std::vector<std::string>> inputs(kRowCount);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    const auto count = (i % 5) + 1;  // 1..5 elements per row
    for (uint64_t k = 0; k < count; ++k) {
      inputs[i].push_back("row" + std::to_string(i) + "_elem" +
                          std::to_string(k));
    }
  }

  const auto list_type = duckdb::LogicalType::LIST(duckdb::LogicalType::BLOB);

  // Write
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(
      /*id=*/77, list_type, /*skip_validity=*/false, kRowGroupSize,
      duckdb::CompressionType::COMPRESSION_AUTO, false);

    duckdb::Vector batch{list_type, STANDARD_VECTOR_SIZE};
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(batch);
    auto& child = duckdb::ListVector::GetChildMutable(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      // Compute per-batch total elements + size the child vector.
      uint64_t total_elems = 0;
      for (duckdb::idx_t k = 0; k < take; ++k) {
        total_elems += inputs[produced + k].size();
      }
      duckdb::ListVector::Reserve(batch, total_elems);
      duckdb::ListVector::SetListSize(batch, total_elems);

      uint64_t offset = 0;
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto& list = inputs[produced + k];
        entries[k] = duckdb::list_entry_t{offset, list.size()};
        for (const auto& s : list) {
          duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child)[offset] =
            duckdb::StringVector::AddStringOrBlob(child, s.data(), s.size());
          ++offset;
        }
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }

  // Read: verify the LIST node has the right shape (UBIGINT lengths self
  // data + BLOB element child) and that the per-row lengths + element
  // bytes round-trip.
  {
    irs::ColReader r{dir, std::string{kSegmentName}, Db()};
    const auto* col = r.Column(77);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->Type().id(), duckdb::LogicalTypeId::LIST);
    EXPECT_EQ(col->RowCount(), kRowCount);
    // 200 rows / 64 RG size = at least 4 row groups.
    EXPECT_GE(CountRowGroups(*col), 4u);

    const auto* element = col->Child();
    ASSERT_NE(element, nullptr);
    EXPECT_EQ(element->Type().id(), duckdb::LogicalTypeId::BLOB);

    // The LIST level stores cumulative end-offsets into the element
    // child: `offsets[i]` is the one-past-end position of row `i`'s
    // elements. Per-row lengths are recovered as
    // `offsets[i] - offsets[i-1]` (with `offsets[-1] = 0`).
    std::vector<uint64_t> offsets;
    offsets.reserve(kRowCount);
    irs::ReadContext ctx{r};
    irs::RgWindow lwindow{};
    for (uint64_t row_pos = 0; row_pos < col->RowCount();) {
      lwindow = col->Locate(row_pos, lwindow);
      auto seg = col->OpenSegment(lwindow.rg, ctx);
      const auto rg_count =
        static_cast<duckdb::idx_t>(lwindow.end - lwindow.begin);
      duckdb::ColumnScanState state{nullptr};
      seg->InitializeScan(state);
      duckdb::Vector out{duckdb::LogicalType::UBIGINT, STANDARD_VECTOR_SIZE};
      duckdb::idx_t scanned = 0;
      while (scanned < rg_count) {
        const auto take =
          std::min<duckdb::idx_t>(rg_count - scanned, STANDARD_VECTOR_SIZE);
        seg->Scan(state, take, out, 0,
                  duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
        state.offset_in_column += take;
        auto* data = duckdb::FlatVector::GetData<uint64_t>(out);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          offsets.push_back(data[k]);
        }
        scanned += take;
      }
      row_pos = lwindow.end;
    }
    ASSERT_EQ(offsets.size(), kRowCount);
    uint64_t prev = 0;
    for (uint64_t i = 0; i < kRowCount; ++i) {
      EXPECT_EQ(offsets[i] - prev, inputs[i].size()) << "row=" << i;
      prev = offsets[i];
    }

    // Verify the child element data round-trips. Use the child's point
    // cursor; offset N corresponds to cumulative sum of prior lengths.
    TypedPointCursor child_cursor{r, *element};
    duckdb::Vector elem_out{duckdb::LogicalType::BLOB, 1};
    uint64_t element_offset = 0;
    for (uint64_t i = 0; i < kRowCount; ++i) {
      for (uint64_t k = 0; k < inputs[i].size(); ++k) {
        child_cursor.FetchRow(element_offset++, elem_out, 0);
        const auto& slot =
          duckdb::FlatVector::GetData<duckdb::string_t>(elem_out)[0];
        const auto& expected = inputs[i][k];
        ASSERT_EQ(slot.GetSize(), expected.size())
          << "row=" << i << " elem=" << k;
        EXPECT_EQ(std::string_view(slot.GetData(), slot.GetSize()),
                  std::string_view(expected.data(), expected.size()))
          << "row=" << i << " elem=" << k;
      }
    }
  }
}

// ============================================================================
// Re-ported coverage from the deleted formats_test_case_base.cpp suite
// (`columns_rw_*` family). The originals targeted a different on-disk shape;
// these target the same *invariants* (empty rounds, all-null validity,
// dense alternating mask, writer rollback) against the new cs.
// ============================================================================

// `columns_rw_empty` analogue: open a typed column but never append. After
// Commit(0) the column exists with RowCount() == 0 and HasValidity() == false.
TEST_F(IRSColumnstoreTest, EmptyTypedColumnRoundTrip) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "empty_col";
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
    w.Commit(/*target_row=*/0);
  }
  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  ASSERT_TRUE(r.HasColumn(1));
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), 0u);
  EXPECT_FALSE(col->HasValidity());
  EXPECT_EQ(CountRowGroups(*col), 0u);
}

// `columns_rw_writer_reuse` analogue: a Writer that's rolled back leaves
// the eagerly-created `.col` file behind. The file is not referenced from
// any committed SegmentMeta and the directory cleaner sweeps it on the
// next pass -- same lifecycle the legacy `.csd` writer relied on.
TEST_F(IRSColumnstoreTest, WriterRollbackLeavesOrphanFile) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "rollback_seg";
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, 4};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (int i = 0; i < 4; ++i) {
      data[i] = i;
    }
    cw.Append(0, batch, 4);
    w.Rollback();
  }
  // Post-Rollback: orphan file is left on disk. The IndexWriter-driven
  // path (see index_tests `clear_writer` / `compact_*`) tracks the
  // file via `dir.attributes().refs()` and DirectoryCleaner::clean()
  // reaps it on a later pass; in this isolated columnstore-only test
  // we only assert that Rollback itself does not remove the file.
  bool present = false;
  ASSERT_TRUE(dir.exists(present, "rollback_seg.col"));
  EXPECT_TRUE(present);
}

// `columns_rw_dense_mask` analogue: every row null, exercises the validity
// codec on the all-zero mask path. Verifies the column reports HasValidity()
// and that range scans surface all-invalid rows.
TEST_F(IRSColumnstoreTest, AllNullColumnRoundTrip) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "all_null";
  constexpr uint64_t kRowCount = 200;
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto& valid = duckdb::FlatVector::ValidityMutable(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      valid.SetAllInvalid(take);
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }
  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), kRowCount);
  EXPECT_TRUE(col->HasValidity());

  irs::ReadContext ctx{r};
  irs::ColumnReader::RangeScan vscan{*col, ctx,
                                     /*validity_side=*/true};
  duckdb::Vector vbatch{duckdb::LogicalType::BIGINT, /*capacity=*/0};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  uint64_t scanned = 0;
  while (scanned < kRowCount) {
    const auto take =
      std::min<duckdb::idx_t>(kRowCount - scanned, STANDARD_VECTOR_SIZE);
    vscan.Scan(scanned, take, vbatch, 0);
    const auto& vmask = vbatch.Buffer().GetValidityMask();
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_FALSE(vmask.RowIsValid(k))
        << "row " << (scanned + k) << " should be invalid";
    }
    scanned += take;
  }
}

// `columns_rw_bit_mask` analogue: alternating valid/invalid pattern across
// a multi-row-group span. Verifies the validity codec preserves per-bit
// values and that the value at valid rows round-trips.
TEST_F(IRSColumnstoreTest, AlternatingValidityRoundTrip) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "alt_validity";
  constexpr uint64_t kRowCount = 5000;
  constexpr uint32_t kRowGroupSize = 1000;
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT,
                            /*skip_validity=*/false, kRowGroupSize,
                            duckdb::CompressionType::COMPRESSION_AUTO, false);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    auto& valid = duckdb::FlatVector::ValidityMutable(batch);
    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        data[k] = static_cast<int64_t>((produced + k) * 11 + 1);
        if ((produced + k) % 2 == 0) {
          valid.SetValid(k);
        } else {
          valid.SetInvalid(k);
        }
      }
      cw.Append(produced, batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }
  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), kRowCount);
  EXPECT_TRUE(col->HasValidity());

  irs::ReadContext ctx{r};
  irs::ColumnReader::RangeScan vscan{*col, ctx, true};
  duckdb::Vector vbatch{duckdb::LogicalType::BIGINT, 0};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  uint64_t scanned = 0;
  while (scanned < kRowCount) {
    const auto take =
      std::min<duckdb::idx_t>(kRowCount - scanned, STANDARD_VECTOR_SIZE);
    vscan.Scan(scanned, take, vbatch, 0);
    const auto& vmask = vbatch.Buffer().GetValidityMask();
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const bool expect_valid = ((scanned + k) % 2 == 0);
      EXPECT_EQ(vmask.RowIsValid(k), expect_valid) << "row " << (scanned + k);
    }
    scanned += take;
  }
}

// `columns_rw_writer_reuse` second analogue: a Writer rolled back, then a
// fresh Writer opened on the same segment name, commits successfully and
// is readable. Exercises directory cleanup + fresh open path.
TEST_F(IRSColumnstoreTest, FreshWriterAfterRollback) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "rollback_then_commit";
  constexpr uint64_t kRowCount = 16;
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, 4};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (int i = 0; i < 4; ++i) {
      data[i] = i;
    }
    cw.Append(0, batch, 4);
    w.Rollback();
  }
  // Second writer: should succeed and yield a readable segment.
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowCount; ++k) {
      data[k] = static_cast<int64_t>(k * 100);
    }
    cw.Append(0, batch, kRowCount);
    w.Commit(kRowCount);
  }
  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  const auto* col = r.Column(1);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), kRowCount);

  TypedPointCursor cursor{r, *col};
  duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
  auto* outd = duckdb::FlatVector::GetDataMutable<int64_t>(out);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    cursor.FetchRow(i, out, 0);
    EXPECT_EQ(outd[0], static_cast<int64_t>(i * 100)) << "row=" << i;
  }
}

// `test_merge_writer_columns` analogue: drives `MergeInto`
// directly across two source segments holding the same column id and
// verifies the merged output contains all source values in source order
// (dense, no deletes).
TEST_F(IRSColumnstoreTest, MergeIntoTwoSegmentsNoDeletes) {
  irs::MemoryDirectory dir{};
  constexpr uint64_t kRowsA = 100;
  constexpr uint64_t kRowsB = 150;

  // Source segment A: ids 1..100 stored as i*2.
  {
    irs::ColWriter w{dir, "src_a", Db()};
    auto& cw = w.OpenColumn(/*id=*/7, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsA; ++k) {
      data[k] = static_cast<int64_t>(k * 2);
    }
    cw.Append(0, batch, kRowsA);
    w.Commit(kRowsA);
  }
  // Source segment B: ids 0..149 stored as 1000 + i.
  {
    irs::ColWriter w{dir, "src_b", Db()};
    auto& cw = w.OpenColumn(/*id=*/7, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsB; ++k) {
      data[k] = static_cast<int64_t>(1000 + k);
    }
    cw.Append(0, batch, kRowsB);
    w.Commit(kRowsB);
  }

  irs::ColReader ra{dir, "src_a", Db()};
  irs::ColReader rb{dir, "src_b", Db()};

  // Merge into "merged".
  {
    irs::ColWriter w{dir, "merged", Db()};
    irs::MergeSource sources[2] = {
      {.reader = nullptr,
       .col_reader = &ra,
       .mask = nullptr,
       .alive_count = kRowsA},
      {.reader = nullptr,
       .col_reader = &rb,
       .mask = nullptr,
       .alive_count = kRowsB},
    };
    irs::MergeInto(sources, w, /*field_options=*/nullptr);
    w.Commit(kRowsA + kRowsB);
  }

  irs::ColReader r{dir, std::string{"merged"}, Db()};
  const auto* col = r.Column(7);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), kRowsA + kRowsB);

  TypedPointCursor cursor{r, *col};
  duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
  auto* outd = duckdb::FlatVector::GetDataMutable<int64_t>(out);
  for (uint64_t i = 0; i < kRowsA; ++i) {
    cursor.FetchRow(i, out, 0);
    EXPECT_EQ(outd[0], static_cast<int64_t>(i * 2)) << "A row=" << i;
  }
  for (uint64_t i = 0; i < kRowsB; ++i) {
    cursor.FetchRow(kRowsA + i, out, 0);
    EXPECT_EQ(outd[0], static_cast<int64_t>(1000 + i)) << "B row=" << i;
  }
}

// `test_merge_writer_columns_remove` analogue: each source carries a
// DocumentMask covering its odd-offset docs; the surviving values land
// contiguously in the merged column.
TEST_F(IRSColumnstoreTest, MergeIntoTwoSegmentsWithDeletes) {
  irs::MemoryDirectory dir{};
  constexpr uint64_t kRowsA = 50;
  constexpr uint64_t kRowsB = 60;

  {
    irs::ColWriter w{dir, "src_a", Db()};
    auto& cw = w.OpenColumn(/*id=*/3, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsA; ++k) {
      data[k] = static_cast<int64_t>(k);
    }
    cw.Append(0, batch, kRowsA);
    w.Commit(kRowsA);
  }
  {
    irs::ColWriter w{dir, "src_b", Db()};
    auto& cw = w.OpenColumn(/*id=*/3, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsB; ++k) {
      data[k] = static_cast<int64_t>(500 + k);
    }
    cw.Append(0, batch, kRowsB);
    w.Commit(kRowsB);
  }

  irs::ColReader ra{dir, "src_a", Db()};
  irs::ColReader rb{dir, "src_b", Db()};

  // Mask the odd-offset source docs in each segment. `DocumentMask`'s
  // `ManagedTypedAllocator` defaults to `gForbidden` under SDB_DEV, so
  // we plumb a noop manager explicitly.
  irs::DocumentMask mask_a{{irs::IResourceManager::gNoop}};
  for (uint64_t off = 1; off < kRowsA; off += 2) {
    mask_a.insert(static_cast<irs::doc_id_t>(irs::doc_limits::min() + off));
  }
  irs::DocumentMask mask_b{{irs::IResourceManager::gNoop}};
  for (uint64_t off = 1; off < kRowsB; off += 2) {
    mask_b.insert(static_cast<irs::doc_id_t>(irs::doc_limits::min() + off));
  }
  const uint64_t alive_a = kRowsA - mask_a.size();
  const uint64_t alive_b = kRowsB - mask_b.size();

  {
    irs::ColWriter w{dir, "merged", Db()};
    irs::MergeSource sources[2] = {
      {.reader = nullptr,
       .col_reader = &ra,
       .mask = &mask_a,
       .alive_count = alive_a},
      {.reader = nullptr,
       .col_reader = &rb,
       .mask = &mask_b,
       .alive_count = alive_b},
    };
    irs::MergeInto(sources, w, /*field_options=*/nullptr);
    w.Commit(alive_a + alive_b);
  }

  irs::ColReader r{dir, std::string{"merged"}, Db()};
  const auto* col = r.Column(3);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), alive_a + alive_b);

  std::vector<int64_t> expected;
  expected.reserve(alive_a + alive_b);
  for (uint64_t k = 0; k < kRowsA; ++k) {
    if ((k % 2) == 0) {
      expected.push_back(static_cast<int64_t>(k));
    }
  }
  for (uint64_t k = 0; k < kRowsB; ++k) {
    if ((k % 2) == 0) {
      expected.push_back(static_cast<int64_t>(500 + k));
    }
  }
  ASSERT_EQ(expected.size(), alive_a + alive_b);

  TypedPointCursor cursor{r, *col};
  duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
  auto* outd = duckdb::FlatVector::GetDataMutable<int64_t>(out);
  for (size_t i = 0; i < expected.size(); ++i) {
    cursor.FetchRow(i, out, 0);
    EXPECT_EQ(outd[0], expected[i]) << "merged row=" << i;
  }
}

uint64_t ReferenceDistinctCount(const std::vector<int64_t>& values) {
  duckdb::HyperLogLog hll;
  duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
  duckdb::Vector h{duckdb::LogicalType::HASH, STANDARD_VECTOR_SIZE};
  auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(v);
  for (size_t i = 0; i < values.size(); ++i) {
    d[i] = values[i];
  }
  duckdb::VectorOperations::Hash(v, h, values.size());
  hll.Update(v, h);
  return hll.Count();
}

TEST_F(IRSColumnstoreTest, MergeIntoReusesHyperLogLog) {
  irs::MemoryDirectory dir{};
  constexpr irs::field_id kId = 9;
  constexpr uint64_t kRowsA = 40;
  constexpr uint64_t kRowsB = 50;

  irs::ColumnOptionsProvider column_options =
    [](irs::field_id) -> irs::ColumnOptions { return {.hyperloglog = true}; };
  irs::FunctionFieldOptions field_options{column_options, nullptr};

  {
    irs::ColWriter w{dir, "src_a", Db()};
    w.SetFieldOptions(&field_options);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsA; ++k) {
      data[k] = static_cast<int64_t>(k % 5);
    }
    cw.Append(0, batch, kRowsA);
    w.Commit(kRowsA);
  }
  {
    irs::ColWriter w{dir, "src_b", Db()};
    w.SetFieldOptions(&field_options);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsB; ++k) {
      data[k] = static_cast<int64_t>((k % 5) + 3);
    }
    cw.Append(0, batch, kRowsB);
    w.Commit(kRowsB);
  }

  irs::ColReader ra{dir, "src_a", Db()};
  irs::ColReader rb{dir, "src_b", Db()};
  ASSERT_NE(ra.Column(kId)->HyperLogLog(), nullptr);
  ASSERT_NE(rb.Column(kId)->HyperLogLog(), nullptr);

  {
    irs::ColWriter w{dir, "merged", Db()};
    w.SetFieldOptions(&field_options);
    irs::MergeSource sources[2] = {
      {.reader = nullptr,
       .col_reader = &ra,
       .mask = nullptr,
       .alive_count = kRowsA},
      {.reader = nullptr,
       .col_reader = &rb,
       .mask = nullptr,
       .alive_count = kRowsB},
    };
    irs::MergeInto(sources, w, &field_options);
    w.Commit(kRowsA + kRowsB);
  }

  irs::ColReader r{dir, std::string{"merged"}, Db()};
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  const auto* hll = col->HyperLogLog();
  ASSERT_NE(hll, nullptr);
  EXPECT_EQ(hll->Count(), ReferenceDistinctCount({0, 1, 2, 3, 4, 5, 6, 7}));
}

TEST_F(IRSColumnstoreTest, MergeIntoRebuildsHyperLogLogWithDeletes) {
  irs::MemoryDirectory dir{};
  constexpr irs::field_id kId = 9;
  constexpr uint64_t kRowsA = 8;
  constexpr uint64_t kRowsB = 8;

  irs::ColumnOptionsProvider column_options =
    [](irs::field_id) -> irs::ColumnOptions { return {.hyperloglog = true}; };
  irs::FunctionFieldOptions field_options{column_options, nullptr};

  {
    irs::ColWriter w{dir, "src_a", Db()};
    w.SetFieldOptions(&field_options);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsA; ++k) {
      data[k] = static_cast<int64_t>(k);
    }
    cw.Append(0, batch, kRowsA);
    w.Commit(kRowsA);
  }
  {
    irs::ColWriter w{dir, "src_b", Db()};
    w.SetFieldOptions(&field_options);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsB; ++k) {
      data[k] = static_cast<int64_t>(100 + k);
    }
    cw.Append(0, batch, kRowsB);
    w.Commit(kRowsB);
  }

  irs::ColReader ra{dir, "src_a", Db()};
  irs::ColReader rb{dir, "src_b", Db()};

  irs::DocumentMask mask_a{{irs::IResourceManager::gNoop}};
  for (uint64_t off = 1; off < kRowsA; off += 2) {
    mask_a.insert(static_cast<irs::doc_id_t>(irs::doc_limits::min() + off));
  }
  irs::DocumentMask mask_b{{irs::IResourceManager::gNoop}};
  for (uint64_t off = 1; off < kRowsB; off += 2) {
    mask_b.insert(static_cast<irs::doc_id_t>(irs::doc_limits::min() + off));
  }
  const uint64_t alive_a = kRowsA - mask_a.size();
  const uint64_t alive_b = kRowsB - mask_b.size();

  {
    irs::ColWriter w{dir, "merged", Db()};
    w.SetFieldOptions(&field_options);
    irs::MergeSource sources[2] = {
      {.reader = nullptr,
       .col_reader = &ra,
       .mask = &mask_a,
       .alive_count = alive_a},
      {.reader = nullptr,
       .col_reader = &rb,
       .mask = &mask_b,
       .alive_count = alive_b},
    };
    irs::MergeInto(sources, w, &field_options);
    w.Commit(alive_a + alive_b);
  }

  irs::ColReader r{dir, std::string{"merged"}, Db()};
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  const auto* hll = col->HyperLogLog();
  ASSERT_NE(hll, nullptr);
  EXPECT_EQ(hll->Count(),
            ReferenceDistinctCount({0, 2, 4, 6, 100, 102, 104, 106}));
}

TEST_F(IRSColumnstoreTest, MergeIntoMixedHyperLogLog) {
  irs::MemoryDirectory dir{};
  constexpr irs::field_id kId = 9;
  constexpr uint64_t kRowsA = 4;
  constexpr uint64_t kRowsB = 6;

  irs::ColumnOptionsProvider column_options =
    [](irs::field_id) -> irs::ColumnOptions { return {.hyperloglog = true}; };
  irs::FunctionFieldOptions field_options{column_options, nullptr};

  {
    irs::ColWriter w{dir, "src_a", Db()};
    w.SetFieldOptions(&field_options);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsA; ++k) {
      data[k] = static_cast<int64_t>(k);
    }
    cw.Append(0, batch, kRowsA);
    w.Commit(kRowsA);
  }
  {
    irs::ColWriter w{dir, "src_b", Db()};
    w.SetFieldOptions(&field_options);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < kRowsB; ++k) {
      data[k] = static_cast<int64_t>(10 + k);
    }
    cw.Append(0, batch, kRowsB);
    w.Commit(kRowsB);
  }

  irs::ColReader ra{dir, "src_a", Db()};
  irs::ColReader rb{dir, "src_b", Db()};

  irs::DocumentMask mask_b{{irs::IResourceManager::gNoop}};
  for (uint64_t off = 1; off < kRowsB; off += 2) {
    mask_b.insert(static_cast<irs::doc_id_t>(irs::doc_limits::min() + off));
  }
  const uint64_t alive_b = kRowsB - mask_b.size();

  {
    irs::ColWriter w{dir, "merged", Db()};
    w.SetFieldOptions(&field_options);
    irs::MergeSource sources[2] = {
      {.reader = nullptr,
       .col_reader = &ra,
       .mask = nullptr,
       .alive_count = kRowsA},
      {.reader = nullptr,
       .col_reader = &rb,
       .mask = &mask_b,
       .alive_count = alive_b},
    };
    irs::MergeInto(sources, w, &field_options);
    w.Commit(kRowsA + alive_b);
  }

  irs::ColReader r{dir, std::string{"merged"}, Db()};
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  const auto* hll = col->HyperLogLog();
  ASSERT_NE(hll, nullptr);
  EXPECT_EQ(hll->Count(), ReferenceDistinctCount({0, 1, 2, 3, 10, 12, 14}));
}

// Merge consults the IndexWriter callback (provider B), not whatever the
// source segments were written with (provider A). The footer no longer
// persists `row_group_size`, so this is the regression guard for the
// callback-driven design.
TEST_F(IRSColumnstoreTest, MergeIntoUsesCallbackRowGroupSize) {
  irs::MemoryDirectory dir{};
  constexpr uint64_t kRowsA = 200;
  constexpr uint64_t kRowsB = 300;
  constexpr irs::field_id kId = 11;
  constexpr uint32_t kSmallRowGroup = 64;

  // Both source segments are written under the default (large) row-group
  // size, so each ends up as a single row group.
  for (auto [name, rows] : {std::pair{std::string_view{"src_a"}, kRowsA},
                            std::pair{std::string_view{"src_b"}, kRowsB}}) {
    irs::ColWriter w{dir, name, Db()};
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (duckdb::idx_t k = 0; k < rows; ++k) {
      data[k] = static_cast<int64_t>(k);
    }
    cw.Append(0, batch, rows);
    w.Commit(rows);
  }

  irs::ColReader ra{dir, "src_a", Db()};
  irs::ColReader rb{dir, "src_b", Db()};
  ASSERT_EQ(ra.Column(kId)->DataRgCount(), 1u);
  ASSERT_EQ(rb.Column(kId)->DataRgCount(), 1u);

  // Provider used at merge time. Returns a small row-group size; we expect
  // the merged column to have ceil((kRowsA + kRowsB) / kSmallRowGroup)
  // row groups even though both sources had only one.
  irs::ColumnOptionsProvider column_options =
    [](irs::field_id) -> irs::ColumnOptions {
    return {
      .row_group_size = kSmallRowGroup,
      .compression = duckdb::CompressionType::COMPRESSION_AUTO,
    };
  };
  irs::FunctionFieldOptions field_options{column_options, nullptr};

  {
    irs::ColWriter w{dir, "merged", Db()};
    w.SetFieldOptions(&field_options);
    irs::MergeSource sources[2] = {
      {.reader = nullptr,
       .col_reader = &ra,
       .mask = nullptr,
       .alive_count = kRowsA},
      {.reader = nullptr,
       .col_reader = &rb,
       .mask = nullptr,
       .alive_count = kRowsB},
    };
    irs::MergeInto(sources, w, &field_options);
    w.Commit(kRowsA + kRowsB);
  }

  irs::ColReader r{dir, std::string{"merged"}, Db()};
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->RowCount(), kRowsA + kRowsB);

  constexpr size_t kExpectedRgs =
    (kRowsA + kRowsB + kSmallRowGroup - 1) / kSmallRowGroup;
  EXPECT_EQ(col->DataRgCount(), kExpectedRgs);

  TypedPointCursor cursor{r, *col};
  duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
  auto* outd = duckdb::FlatVector::GetDataMutable<int64_t>(out);
  for (uint64_t i = 0; i < kRowsA; ++i) {
    cursor.FetchRow(i, out, 0);
    EXPECT_EQ(outd[0], static_cast<int64_t>(i)) << "A row=" << i;
  }
  for (uint64_t i = 0; i < kRowsB; ++i) {
    cursor.FetchRow(kRowsA + i, out, 0);
    EXPECT_EQ(outd[0], static_cast<int64_t>(i)) << "B row=" << i;
  }
}

// `columns_rw` multi-column variant: two columns of different types in the
// same .col file, independent row counts, both round-trip. Exercises the
// per-column footer entry + independent codec selection.
TEST_F(IRSColumnstoreTest, TwoColumnsDifferentTypesSameSegment) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "two_cols";
  constexpr uint64_t kRowCount = 500;
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw_int = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
    auto& cw_str = w.OpenColumn(/*id=*/2, duckdb::LogicalType::VARCHAR);

    duckdb::Vector int_batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    duckdb::Vector str_batch{duckdb::LogicalType::VARCHAR,
                             STANDARD_VECTOR_SIZE};
    auto* int_data = duckdb::FlatVector::GetDataMutable<int64_t>(int_batch);
    auto* str_slots =
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(str_batch);

    uint64_t produced = 0;
    while (produced < kRowCount) {
      const auto take =
        std::min<duckdb::idx_t>(kRowCount - produced, STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        int_data[k] = static_cast<int64_t>((produced + k) * 3);
      }
      cw_int.Append(produced, int_batch, take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto s = "row_" + std::to_string(produced + k);
        str_slots[k] =
          duckdb::StringVector::AddStringOrBlob(str_batch, s.data(), s.size());
      }
      cw_str.Append(produced, str_batch, take);
      produced += take;
    }
    w.Commit(kRowCount);
  }
  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  EXPECT_TRUE(r.HasColumn(1));
  EXPECT_TRUE(r.HasColumn(2));
  const auto* int_col = r.Column(1);
  const auto* str_col = r.Column(2);
  ASSERT_NE(int_col, nullptr);
  ASSERT_NE(str_col, nullptr);
  EXPECT_EQ(int_col->RowCount(), kRowCount);
  EXPECT_EQ(str_col->RowCount(), kRowCount);

  TypedPointCursor int_cursor{r, *int_col};
  TypedPointCursor str_cursor{r, *str_col};
  duckdb::Vector int_out{duckdb::LogicalType::BIGINT, 1};
  duckdb::Vector str_out{duckdb::LogicalType::VARCHAR, 1};
  auto* int_d = duckdb::FlatVector::GetDataMutable<int64_t>(int_out);
  auto* str_d = duckdb::FlatVector::GetData<duckdb::string_t>(str_out);
  for (uint64_t i = 0; i < kRowCount; i += 47) {
    int_cursor.FetchRow(i, int_out, 0);
    str_cursor.FetchRow(i, str_out, 0);
    EXPECT_EQ(int_d[0], static_cast<int64_t>(i * 3)) << "int i=" << i;
    const auto expected = "row_" + std::to_string(i);
    EXPECT_EQ(std::string_view(str_d[0].GetData(), str_d[0].GetSize()),
              std::string_view(expected.data(), expected.size()))
      << "str i=" << i;
  }
}

// Pins the lazy `OpenNormColumn` contract: when a schema declares many
// norm-featured fields but only a few ever index a doc,
// `Writer::Impl::norm_writers` stays linear in the *indexed* count, not
// the *declared* count. `FieldData::compute_features` and
// `MergeNormColumnFromSources` are both lazy callers of
// `Writer::OpenNormColumn`; this test exercises the contract at the
// Writer surface directly (declaring an id is not enough to register a
// writer -- only `OpenNormColumn` is).
TEST_F(IRSColumnstoreTest, LazyNormOpenSparseSchema) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "lazy_norms";
  constexpr size_t kDeclaredFields = 100;
  constexpr size_t kIndexedFields = 5;
  constexpr uint64_t kRowCount = 10;

  std::vector<irs::field_id> declared_ids;
  std::vector<irs::field_id> indexed_ids;
  declared_ids.reserve(kDeclaredFields);
  indexed_ids.reserve(kIndexedFields);

  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    // Declare 100 column ids (mirrors FieldsData::emplace allocating an
    // id eagerly per norm-featured field).
    for (size_t i = 0; i < kDeclaredFields; ++i) {
      declared_ids.push_back(static_cast<irs::field_id>(i));
    }
    // Only 5 of them actually receive Appends (mirrors the field-data
    // path calling OpenNormColumn lazily inside `compute_features`).
    for (size_t i = 0; i < kIndexedFields; ++i) {
      const auto id = declared_ids[i * 17 % kDeclaredFields];
      indexed_ids.push_back(id);
      auto& nw = w.OpenNormColumn(id, DEFAULT_ROW_GROUP_SIZE);
      for (uint64_t r = 0; r < kRowCount; ++r) {
        nw.Append(r, static_cast<uint32_t>((r + 1) * (i + 1)));
      }
    }
    EXPECT_EQ(w.NormWriters().size(), kIndexedFields)
      << "norm_writers must be linear in indexed-fields count, not "
         "declared-fields count";
    w.Commit(kRowCount);
  }

  // After commit: every Allocated-but-never-Opened id must be absent
  // from the segment footer.
  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  std::set<irs::field_id> indexed_set{indexed_ids.begin(), indexed_ids.end()};
  for (auto id : declared_ids) {
    const bool expect_present = indexed_set.contains(id);
    EXPECT_EQ(r.HasNormColumn(id), expect_present)
      << "id=" << id << " expect_present=" << expect_present;
    if (expect_present) {
      const auto* col = r.NormColumn(id);
      ASSERT_NE(col, nullptr);
      EXPECT_EQ(col->RowCount(), kRowCount);
    } else {
      EXPECT_EQ(r.NormColumn(id), nullptr);
    }
  }
}

// Reproduces the inverted-index merge validity drift. A merged segment carries
// several columns in one `.col` file; an INCLUDE column larger than
// STANDARD_VECTOR_SIZE is later materialized in vector-sized batches (the
// MergeInto read shape) when that segment is re-merged. The second batch starts
// at row STANDARD_VECTOR_SIZE, so RangeScan seeks forward before scanning the
// tail. The batched validity read of that tail must match the per-row point
// read; a mismatch is a dropped validity bit that surfaces as a spurious NULL
// in the columnstore.
TEST_F(IRSColumnstoreTest, MergeSegmentBatchedTailValidityMatchesPointRead) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "merge_tail";
  // 2052 = STANDARD_VECTOR_SIZE + 4: a full first batch then a 4-row tail.
  constexpr uint64_t kHead = 4;
  constexpr uint64_t kRowCount = STANDARD_VECTOR_SIZE + kHead;
  const auto type = duckdb::LogicalType::VARCHAR;

  // INCLUDE column under test (id 2). The crux: a row in the FIRST batch's
  // [0, tail) window is NULL while the corresponding tail row in the SECOND
  // batch is valid. The validity scan AND-combines into the reused out_vec, so
  // a stale NULL from the first batch would spuriously clear the tail row. Here
  // row 1 is the only NULL, so second-batch row STANDARD_VECTOR_SIZE+1 (valid)
  // lands on the stale-NULL slot 1. Low-cardinality VARCHAR values mirror the
  // FTS `note` column; a leading pk-like blob column (id 1) makes the `.col`
  // multi-column, matching the FTS segment shape.
  auto is_valid = [](uint64_t i) { return i != 1; };
  auto value_of = [](uint64_t i) { return "n" + std::to_string(i % 16); };

  // Write the merged segment the way MergeInto does: append source-by-source,
  // so the second source (STANDARD_VECTOR_SIZE rows) lands at output row 4 and
  // its batch spans the growing chunk-capacity boundaries.
  {
    irs::ColWriter w{dir, kSegmentName, Db()};
    auto& cw_pk = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BLOB);
    auto& cw_val = w.OpenColumn(/*id=*/2, type);

    const uint64_t sizes[] = {kHead, STANDARD_VECTOR_SIZE};
    uint64_t out_row = 0;
    for (const auto sz : sizes) {
      uint64_t pos = 0;
      while (pos < sz) {
        const auto take =
          std::min<duckdb::idx_t>(sz - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector pk_batch{duckdb::LogicalType::BLOB,
                                STANDARD_VECTOR_SIZE};
        duckdb::Vector val_batch{type, STANDARD_VECTOR_SIZE};
        auto* pk_slots =
          duckdb::FlatVector::GetDataMutable<duckdb::string_t>(pk_batch);
        auto* val_slots =
          duckdb::FlatVector::GetDataMutable<duckdb::string_t>(val_batch);
        auto& val_valid = duckdb::FlatVector::ValidityMutable(val_batch);
        val_valid.Reset(STANDARD_VECTOR_SIZE);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          const auto g = out_row + k;
          const auto pk = "pk_" + std::to_string(g);
          pk_slots[k] = duckdb::StringVector::AddStringOrBlob(
            pk_batch, pk.data(), pk.size());
          if (is_valid(g)) {
            const auto v = value_of(g);
            val_slots[k] = duckdb::StringVector::AddStringOrBlob(
              val_batch, v.data(), v.size());
            val_valid.SetValid(k);
          } else {
            val_valid.SetInvalid(k);
          }
        }
        cw_pk.Append(out_row, pk_batch, take);
        cw_val.Append(out_row, val_batch, take);
        out_row += take;
        pos += take;
      }
    }
    ASSERT_EQ(out_row, kRowCount);
    w.Commit(kRowCount);
  }

  irs::ColReader r{dir, std::string{kSegmentName}, Db()};
  const auto* col = r.Column(2);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRowCount);

  // Batched read in vector-sized chunks, reusing one MaterializeState across
  // batches (the MergeInto read shape). The second chunk seeks past
  // STANDARD_VECTOR_SIZE before scanning the tail.
  irs::ReadContext ctx{r};
  auto state = irs::MakeMaterializeState(*col, ctx);
  std::vector<bool> batched(kRowCount, false);
  duckdb::Vector batch{type, STANDARD_VECTOR_SIZE,
                       duckdb::VectorDataInitialization::UNINITIALIZED};
  uint64_t pos = 0;
  while (pos < kRowCount) {
    const auto take =
      std::min<duckdb::idx_t>(kRowCount - pos, STANDARD_VECTOR_SIZE);
    irs::MaterializeNode(*col, *state, irs::IotaRange{pos, take}, batch,
                         /*output_start=*/0);
    const auto& v = duckdb::FlatVector::Validity(batch);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      batched[pos + k] = v.RowIsValid(k);
    }
    pos += take;
  }

  irs::ColumnReader::PointReader cursor{r, *col};
  duckdb::Vector out{type, 1};
  for (uint64_t i = 0; i < kRowCount; ++i) {
    duckdb::FlatVector::ValidityMutable(out).Reset();
    cursor.FetchRow(i, out, 0);
    const bool point_valid = duckdb::FlatVector::Validity(out).RowIsValid(0);
    EXPECT_EQ(point_valid, is_valid(i)) << "point read wrong at row " << i;
    EXPECT_EQ(batched[i], point_valid)
      << "batched validity != point validity at row " << i;
  }
}

}  // namespace
