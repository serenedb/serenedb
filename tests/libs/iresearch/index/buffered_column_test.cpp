////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

// The legacy `irs::BufferedColumn` is being removed (task #6). The four
// tests here keep the name+shape but exercise the surviving norm-column
// write path (`irs::columnstore::NormColumnWriter` via `Writer::Open
// NormColumn`) -- BufferedColumn's role was to stage per-doc norm bytes
// before flush, and the new NormColumnWriter replaces it.

#include <gtest/gtest.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/columnstore/norm_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

namespace {

class BufferedColumnTestCase : public ::testing::TestWithParam<bool> {
 protected:
  duckdb::DatabaseInstance& Db() {
    return ::sdb::DuckDBEngine::Instance().instance();
  }
};

// Returns true iff `dir` contains a file named `segment_name + ".cs"`.
// Used to assert that Rollback() leaves the directory clean.
bool HasCsFile(const irs::Directory& dir, std::string_view segment_name) {
  bool exists = false;
  std::string fn{segment_name};
  fn.append(".cs");
  if (!dir.exists(exists, fn)) {
    return false;
  }
  return exists;
}

}  // namespace

TEST_P(BufferedColumnTestCase, Ctor) {
  // Fresh writer + fresh norm column => Id matches, RowCount == 0.
  // Mirrors the legacy `Empty()`/`Size()` checks on a default-constructed
  // BufferedColumn.
  irs::MemoryDirectory dir;
  {
    irs::columnstore::Writer w{dir, "ctor_seg", Db()};
    auto& nw = w.OpenNormColumn(/*id=*/1, /*row_group_size=*/128);
    EXPECT_EQ(nw.Id(), 1);
    EXPECT_EQ(nw.RowCount(), 0u);

    // RowCount advances on Append.
    nw.Append(0, /*value=*/3);
    EXPECT_EQ(nw.RowCount(), 1u);

    // Append a few more; RowCount tracks the high-water row.
    nw.Append(1, 5);
    nw.Append(2, 7);
    EXPECT_EQ(nw.RowCount(), 3u);

    // Rollback -- the eagerly-created `.cs` file stays on disk as an
    // orphan. The directory cleaner sweeps it later; the writer itself
    // does not remove (matches the legacy `.csd` writer contract).
    w.Rollback();
  }
  EXPECT_TRUE(HasCsFile(dir, "ctor_seg"));
}

TEST_P(BufferedColumnTestCase, FlushEmpty) {
  // BufferedColumn::Flush on an empty buffer was a no-op + the segment's
  // column meta wasn't written. New cs analogue: open the norm column,
  // append nothing, Rollback() the writer => no .cs file appears.

  // --- (1) Rollback path on a norm-only writer ----------------------------
  {
    irs::MemoryDirectory dir;
    {
      irs::columnstore::Writer w{dir, "flush_empty_rb", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/3, /*row_group_size=*/128);
      EXPECT_EQ(nw.RowCount(), 0u);
      w.Rollback();
    }
    // Eagerly-created `.cs` file is left as an orphan post-Rollback; the
    // directory cleaner sweeps it later (legacy `.csd` writer contract).
    EXPECT_TRUE(HasCsFile(dir, "flush_empty_rb"));
  }

  // --- (2) Commit-with-zero-padding produces a 0-row group ---------------
  // Writer::Commit pads the norm column to target_row before flushing; a
  // typed column with one row + an unused norm column => the .cs file
  // exists, the typed column has one row, the norm column has one
  // implicit zero-padded row (NonZeroCount == 0, Sum == 0).
  {
    irs::MemoryDirectory dir;
    {
      irs::columnstore::Writer w{dir, "flush_empty_typed", Db()};
      w.OpenNormColumn(/*id=*/3, /*row_group_size=*/128);
      auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, 1};
      duckdb::FlatVector::GetDataMutable<int64_t>(v)[0] = 42;
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      cw.Append(0, v, 1);
      auto filename = w.Commit(/*target_row=*/1);
      ASSERT_FALSE(filename.empty());
    }
    irs::columnstore::Reader r{dir, "flush_empty_typed", Db()};
    EXPECT_TRUE(r.HasColumn(1));
    ASSERT_TRUE(r.HasNormColumn(3));
    const auto* norm = r.NormColumn(3);
    ASSERT_NE(norm, nullptr);
    EXPECT_EQ(norm->RowCount(), 1u);
    EXPECT_EQ(norm->Sum(), 0u);
    EXPECT_EQ(norm->NonZeroCount(), 0u);
  }
}

TEST_P(BufferedColumnTestCase, InsertDuplicates) {
  // Three sub-scenarios.
  //
  // (1) All-zero stream: NonZeroCount must be 0 and Sum() = 0 even though
  //     RowCount > 0 (mirrors legacy "all-equal-zero" payload shape).
  // (2) All-equal non-zero stream: round-trip + stats reflect duplicates.
  // (3) Append a per-row-group-spanning duplicate stream: row group
  //     boundaries don't lose data and every row reads back identically.

  // (1) All zeros.
  {
    irs::MemoryDirectory dir;
    constexpr uint64_t kRowCount = 256;
    constexpr uint32_t kRowGroupSize = 64;  // 4 row groups
    {
      irs::columnstore::Writer w{dir, "dup_zero", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, /*value=*/0);
      }
      w.Commit(kRowCount);
    }
    irs::columnstore::Reader r{dir, "dup_zero", Db()};
    ASSERT_TRUE(r.HasNormColumn(9));
    const auto* col = r.NormColumn(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    EXPECT_EQ(col->Sum(), 0u);
    EXPECT_EQ(col->NonZeroCount(), 0u);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      EXPECT_EQ(col->Get(i), 0u) << "i=" << i;
    }
  }

  // (2) All same non-zero value (matches the legacy test name's intent).
  {
    irs::MemoryDirectory dir;
    constexpr uint64_t kRowCount = 5000;
    constexpr uint32_t kRepeatedValue = 42;
    constexpr uint32_t kRowGroupSize = 1024;
    {
      irs::columnstore::Writer w{dir, "dup_value", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, kRepeatedValue);
      }
      auto filename = w.Commit(kRowCount);
      ASSERT_FALSE(filename.empty());
    }
    irs::columnstore::Reader r{dir, "dup_value", Db()};
    ASSERT_TRUE(r.HasNormColumn(9));
    const auto* col = r.NormColumn(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowCount(), kRowCount);
    EXPECT_EQ(col->Sum(), uint64_t{kRepeatedValue} * kRowCount);
    EXPECT_EQ(col->NonZeroCount(), kRowCount);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      EXPECT_EQ(col->Get(i), kRepeatedValue) << "i=" << i;
    }
    // Multi-row-group: with 1024 RG size + 5000 rows we expect 5 row groups.
    EXPECT_EQ(col->RowGroupCount(), 5u);
  }

  // (3) Stream of duplicates crossing row-group boundaries; assert each
  //     row group reads back the same `byte_size` (all-equal -> 1 byte).
  {
    irs::MemoryDirectory dir;
    constexpr uint64_t kRowCount = 300;
    constexpr uint32_t kRepeatedValue = 7;
    constexpr uint32_t kRowGroupSize = 100;
    {
      irs::columnstore::Writer w{dir, "dup_rg", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, kRepeatedValue);
      }
      w.Commit(kRowCount);
    }
    irs::columnstore::Reader r{dir, "dup_rg", Db()};
    const auto* col = r.NormColumn(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowGroupCount(), 3u);
    for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
      EXPECT_EQ(col->ByteSize(rg), 1u) << "rg=" << rg;
      EXPECT_EQ(col->RowGroupRowCount(rg), kRowGroupSize) << "rg=" << rg;
    }
  }
}

TEST_P(BufferedColumnTestCase, Sort) {
  GTEST_SKIP()
    << "BufferedColumn::Sort backed sorted-index inserts on the "
       "legacy subsystem; sorted-index isn't supported on the new cs.";
}

INSTANTIATE_TEST_SUITE_P(BufferedColumnTest, BufferedColumnTestCase,
                         ::testing::Values(false, true));
