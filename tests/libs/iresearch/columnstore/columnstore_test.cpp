////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <random>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/columnstore/norm_writer.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

class IRSColumnstoreTest : public ::testing::Test {
 protected:
  IRSColumnstoreTest() : _db{nullptr} {}

  duckdb::DatabaseInstance& Db() { return *_db.instance; }

 private:
  duckdb::DuckDB _db;
};

TEST_F(IRSColumnstoreTest, RoundTripInt64Dense) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "test_segment";
  constexpr uint64_t kRowCount = 5000;

  // Write
  {
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, "col_a", duckdb::LogicalType::BIGINT);

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
    auto filename = w.Commit();
    ASSERT_FALSE(filename.empty());
  }

  // Read
  {
    irs::SegmentMeta meta;
    meta.name = std::string{kSegmentName};
    irs::columnstore::Reader r{dir, meta.name, Db()};

    ASSERT_TRUE(r.HasColumn(1));
    const auto* col = r.Column(1);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    EXPECT_EQ(col->RowGroupCount(), 1u);

    auto seg = col->OpenSegment(0);
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

  // Write only even rows; odd rows arrive via gap-padding as nulls.
  {
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/1, "col_a", duckdb::LogicalType::BIGINT);

    duckdb::Vector single{duckdb::LogicalType::BIGINT, 1};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(single);
    for (uint64_t i = 0; i < kRowCount; i += 2) {
      data[0] = static_cast<int64_t>(i);
      cw.Append(i, single, 1);
    }
    // Force the trailing odd doc to be padded as null in the row group.
    duckdb::FlatVector::SetNull(single, 0, true);
    cw.Append(kRowCount - 1, single, 1);
    w.Commit();
  }

  // Read both data and validity segments; mirror duckdb::StandardColumnData
  // by scanning data into a result Vector, then scanning validity into the
  // same Vector to populate its FlatVector::Validity mask.
  {
    irs::SegmentMeta meta;
    meta.name = std::string{kSegmentName};
    irs::columnstore::Reader r{dir, meta.name, Db()};
    const auto* col = r.Column(1);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);

    auto data_seg = col->OpenSegment(0);
    auto validity_seg = col->OpenValiditySegment(0);

    duckdb::ColumnScanState data_state{nullptr};
    duckdb::ColumnScanState validity_state{nullptr};
    data_seg->InitializeScan(data_state);
    validity_seg->InitializeScan(validity_state);

    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    data_seg->Scan(data_state, kRowCount, out, 0,
                   duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    validity_seg->Scan(validity_state, kRowCount, out, 0,
                       duckdb::ScanVectorType::SCAN_FLAT_VECTOR);

    auto* data = duckdb::FlatVector::GetData<int64_t>(out);
    auto& validity = duckdb::FlatVector::Validity(out);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      if (i % 2 == 0) {
        EXPECT_TRUE(validity.RowIsValid(i)) << "i=" << i;
        EXPECT_EQ(data[i], static_cast<int64_t>(i));
      } else {
        EXPECT_FALSE(validity.RowIsValid(i)) << "i=" << i;
      }
    }
  }
}

// Norm columns share the .cs file but bypass the duckdb codec pipeline:
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
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& nw =
      w.OpenNormColumn(/*id=*/42, "norm_a", /*row_group_size=*/kPerGroup);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      uint32_t v;
      if (i < kPerGroup) {
        v = static_cast<uint32_t>(i % 200);  // 1-byte range
      } else if (i < 2 * kPerGroup) {
        v = 300 + static_cast<uint32_t>(i);  // 2-byte range
      } else {
        v = 100000 + static_cast<uint32_t>(i);  // 4-byte range
      }
      nw.Append(v);
    }
    auto filename = w.Commit();
    ASSERT_FALSE(filename.empty());
  }

  // Read + verify each row group's encoding + per-row values.
  {
    irs::columnstore::Reader r{dir, kSegmentName, Db()};
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
      const auto v = irs::columnstore::ReadNormValue(
        bytes.data() + in_rg * byte_size, byte_size);

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
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& nw = w.OpenNormColumn(/*id=*/7, "norm_b",
                                /*row_group_size=*/kRowCount + 1);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      // All values fit in 1 byte -- this exercises the byte_size==1 path
      // and the stride-1 BM25 fast path the user described.
      nw.Append(static_cast<uint32_t>(i % 250));
    }
    w.Commit();
  }

  {
    irs::columnstore::Reader r{dir, kSegmentName, Db()};
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

// The SegmentWriter flush integration relies on this: postings writers
// read per-doc norms during the same flush cycle in which the values
// are being appended. NormColumnWriter must serve Get(row_pos) reads
// from the in-memory accumulator before Finalize is called -- once
// Finalize runs the values are flushed to the IndexOutput and Get
// returns 0 (callers switch to a NormColumnReader against the new file).
TEST_F(IRSColumnstoreTest, NormColumnWriterReadsDuringWrite) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "norm_in_flight";
  constexpr uint64_t kRowCount = 250;

  // Construct the columnstore Writer (it owns the IndexOutput) and then
  // open a norm column. We don't Commit until after we've exercised the
  // read-during-write path.
  irs::columnstore::Writer w{dir, kSegmentName, Db()};
  auto& nw = w.OpenNormColumn(/*id=*/3, "norm_inflight",
                              /*row_group_size=*/100);

  // Phase 1: append every value, then verify Get / Sum / NonZeroCount
  // match a manual rollup. This is the postings-writer view: values
  // present in memory, file not yet written.
  uint64_t expected_sum = 0;
  uint64_t expected_non_zero = 0;
  for (uint64_t i = 0; i < kRowCount; ++i) {
    const uint32_t v = (i % 7 == 0) ? 0 : static_cast<uint32_t>(i + 1);
    nw.Append(v);
    expected_sum += v;
    expected_non_zero += static_cast<uint64_t>(v != 0);
  }
  EXPECT_EQ(nw.Size(), kRowCount);
  EXPECT_EQ(nw.Sum(), expected_sum);
  EXPECT_EQ(nw.NonZeroCount(), expected_non_zero);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    const uint32_t expected = (i % 7 == 0) ? 0 : static_cast<uint32_t>(i + 1);
    EXPECT_EQ(nw.Get(i), expected) << "i=" << i;
  }
  EXPECT_EQ(nw.Get(kRowCount), 0u) << "out-of-range -> 0 sentinel";

  // Phase 2: Commit, then open a Reader against the freshly-written .cs
  // and verify the on-disk view matches what Get() returned in flight.
  auto filename = w.Commit();
  ASSERT_FALSE(filename.empty());

  irs::columnstore::Reader r{dir, kSegmentName, Db()};
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

// Forward-compat: opening a `.cs` file that has no norm_columns list
// (older writes) returns no norm readers and HasNormColumn=false.
TEST_F(IRSColumnstoreTest, NormColumnAbsentOnTypedOnlySegment) {
  irs::MemoryDirectory dir{};
  constexpr std::string_view kSegmentName = "typed_only";
  {
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(1, "col", duckdb::LogicalType::BIGINT);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, 8};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(batch);
    for (int i = 0; i < 8; ++i) {
      data[i] = i;
    }
    cw.Append(0, batch, 8);
    w.Commit();
  }
  irs::columnstore::Reader r{dir, kSegmentName, Db()};
  EXPECT_FALSE(r.HasNormColumn(1));
  EXPECT_EQ(r.NormColumn(1), nullptr);
  EXPECT_TRUE(r.HasColumn(1));
}

// ARRAY(FLOAT, dim) round-trip. Mirrors how HNSW vectors will be stored:
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
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/42, "vec", array_type);

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
    auto filename = w.Commit();
    ASSERT_FALSE(filename.empty());
  }

  // Read: parent ARRAY column exposes child ColumnReader; element data is
  // a flat FLOAT primitive segment with kRowCount * kDim entries.
  {
    irs::columnstore::Reader r{dir, kSegmentName, Db()};
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
    // vectors -- mirrors how HNSWIndexReader will pull per-doc bytes.
    auto seg = element->OpenSegment(0);
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
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/7, "txt", duckdb::LogicalType::VARCHAR);

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
    auto filename = w.Commit();
    ASSERT_FALSE(filename.empty());
  }

  // Read
  {
    irs::columnstore::Reader r{dir, std::string{kSegmentName}, Db()};
    ASSERT_TRUE(r.HasColumn(7));
    const auto* col = r.Column(7);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->Type().id(), duckdb::LogicalTypeId::VARCHAR);
    EXPECT_EQ(col->RowCount(), kRowCount);

    // Some codecs split the row group into multiple data segments. Walk
    // every data row group, scan all rows, and verify byte-identity.
    duckdb::idx_t verified = 0;
    for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
      auto seg = col->OpenSegment(rg);
      const auto rg_count = col->RowGroupRowCount(rg);
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
  // Two full row groups + a tail. Default kDefaultRowGroupSize = 122880;
  // a smaller row group is fine here since codec selection is what
  // matters, not size.
  constexpr uint64_t kRowCount = 5000;
  constexpr uint64_t kRowGroupSize = 1000;

  // Write
  {
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw =
      w.OpenColumn(/*id=*/9, "ints", duckdb::LogicalType::BIGINT,
                   /*row_group_size=*/kRowGroupSize, /*skip_validity=*/true);
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
    auto filename = w.Commit();
    ASSERT_FALSE(filename.empty());
  }

  // Read
  {
    irs::columnstore::Reader r{dir, std::string{kSegmentName}, Db()};
    const auto* col = r.Column(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    EXPECT_GE(col->RowGroupCount(), 5u);  // 5000 / 1000 = at least 5

    auto cursor = col->NewPointCursor();
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
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/11, "txt", duckdb::LogicalType::VARCHAR);
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
    w.Commit();
  }

  // Read via cursor
  {
    irs::columnstore::Reader r{dir, std::string{kSegmentName}, Db()};
    const auto* col = r.Column(11);
    ASSERT_NE(col, nullptr);
    auto cursor = col->NewPointCursor();
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
  constexpr uint64_t kRowGroupSize = 64;  // forces 4 row groups

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
    irs::columnstore::Writer w{dir, kSegmentName, Db()};
    auto& cw = w.OpenColumn(/*id=*/77, "tags", list_type, kRowGroupSize);

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
    auto filename = w.Commit();
    ASSERT_FALSE(filename.empty());
  }

  // Read: verify the LIST node has the right shape (UBIGINT lengths self
  // data + BLOB element child) and that the per-row lengths + element
  // bytes round-trip.
  {
    irs::columnstore::Reader r{dir, std::string{kSegmentName}, Db()};
    const auto* col = r.Column(77);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->Type().id(), duckdb::LogicalTypeId::LIST);
    EXPECT_EQ(col->RowCount(), kRowCount);
    // 200 rows / 64 RG size = at least 4 row groups.
    EXPECT_GE(col->RowGroupCount(), 4u);

    const auto* element = col->Child();
    ASSERT_NE(element, nullptr);
    EXPECT_EQ(element->Type().id(), duckdb::LogicalTypeId::BLOB);

    // Reconstruct per-row lengths by scanning the LIST level's data
    // segments (UBIGINT lengths). Cumulative-summing them gives offsets
    // into the element child.
    std::vector<uint64_t> lengths;
    lengths.reserve(kRowCount);
    for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
      auto seg = col->OpenSegment(rg);
      const auto rg_count = col->RowGroupRowCount(rg);
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
          lengths.push_back(data[k]);
        }
        scanned += take;
      }
    }
    ASSERT_EQ(lengths.size(), kRowCount);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      EXPECT_EQ(lengths[i], inputs[i].size()) << "row=" << i;
    }

    // Verify the child element data round-trips. Use the child's point
    // cursor; offset N corresponds to cumulative sum of prior lengths.
    auto child_cursor = element->NewPointCursor();
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

}  // namespace
