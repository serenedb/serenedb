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
// write path (`irs::NormColumnWriter` via `Writer::Open
// NormColumn`) -- BufferedColumn's role was to stage per-doc norm bytes
// before flush, and the new NormColumnWriter replaces it.

#include <gtest/gtest.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/formats/column/norm_reader.hpp>
#include <iresearch/formats/column/norm_writer.hpp>
#include <iresearch/formats/norm_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "tests_shared.hpp"

namespace {

class BufferedColumnTestCase : public ::testing::TestWithParam<bool> {
 protected:
  duckdb::DatabaseInstance& Db() {
    return ::irs::DuckDBEngine::Instance().instance();
  }
};

// Returns true iff `dir` contains a file named `segment_name + ".col"`.
// Used to assert that Rollback() leaves the directory clean.
bool HasCsFile(const irs::Directory& dir, std::string_view segment_name) {
  bool exists = false;
  std::string fn{segment_name};
  fn.append(".col");
  if (!dir.exists(exists, fn)) {
    return false;
  }
  return exists;
}

void AssertNormReads(const irs::NormColumnReader& col,
                     const std::vector<uint32_t>& expected) {
  ASSERT_EQ(col.RowCount(), expected.size());
  uint64_t sum = 0;
  for (uint64_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(col.Get(i), expected[i]) << "i=" << i;
    sum += expected[i];
  }
  EXPECT_EQ(col.Sum(), sum);

  std::vector<uint32_t> decoded;
  for (size_t rg = 0; rg < col.RowGroupCount(); ++rg) {
    const auto first = col.RowGroupFirstRow(rg);
    decoded.assign(col.RowGroupRowCount(rg), 0);
    col.Decode(rg, decoded.data());
    for (size_t i = 0; i < decoded.size(); ++i) {
      ASSERT_EQ(decoded[i], expected[first + i]) << "rg=" << rg << " i=" << i;
    }
  }

  const auto doc = [](uint64_t row) {
    return static_cast<irs::doc_id_t>(row + irs::doc_limits::min());
  };
  auto reader = irs::MakePersistedNormReader(col);
  ASSERT_NE(nullptr, reader);
  for (uint64_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(reader->Get(doc(i)), expected[i]) << "i=" << i;
  }

  std::array<irs::doc_id_t, irs::kPostingBlock> block_docs;
  std::array<uint32_t, irs::kPostingBlock> block_values;
  for (uint64_t first = 0; first + block_docs.size() <= expected.size();
       first += 100) {
    for (size_t i = 0; i < block_docs.size(); ++i) {
      block_docs[i] = doc(first + i);
    }
    reader->GetPostingBlock(block_docs, block_values);
    for (size_t i = 0; i < block_docs.size(); ++i) {
      ASSERT_EQ(block_values[i], expected[first + i])
        << "first=" << first << " i=" << i;
    }
  }

  std::array<irs::doc_id_t, irs::kScoreBlock> score_docs;
  std::array<uint32_t, irs::kScoreBlock> score_values;
  for (uint64_t first = 0; first + 3 * score_docs.size() <= expected.size();
       first += 250) {
    for (size_t i = 0; i < score_docs.size(); ++i) {
      score_docs[i] = doc(first + 3 * i);
    }
    reader->GetScoreBlock(score_docs, score_values);
    for (size_t i = 0; i < score_docs.size(); ++i) {
      ASSERT_EQ(score_values[i], expected[first + 3 * i])
        << "first=" << first << " i=" << i;
    }
  }

  std::vector<irs::doc_id_t> sparse_docs;
  for (uint64_t i = 0; i < expected.size(); ++i) {
    if (expected[i] > 255 || i % 97 == 0) {
      sparse_docs.push_back(doc(i));
    }
  }
  std::vector<uint32_t> sparse_values(sparse_docs.size());
  reader->Get(sparse_docs, sparse_values);
  for (size_t i = 0; i < sparse_docs.size(); ++i) {
    ASSERT_EQ(sparse_values[i],
              expected[sparse_docs[i] - irs::doc_limits::min()])
      << "doc=" << sparse_docs[i];
  }
}

}  // namespace

TEST_P(BufferedColumnTestCase, Ctor) {
  // Fresh writer + fresh norm column => Id matches, RowCount == 0.
  // Mirrors the legacy `Empty()`/`Size()` checks on a default-constructed
  // BufferedColumn.
  irs::MemoryDirectory dir;
  {
    irs::ColWriter w{dir, "ctor_seg", Db()};
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

    w.Rollback();
  }
  EXPECT_FALSE(HasCsFile(dir, "ctor_seg"));
}

TEST_P(BufferedColumnTestCase, FlushEmpty) {
  // BufferedColumn::Flush on an empty buffer was a no-op + the segment's
  // column meta wasn't written. New cs analogue: open the norm column,
  // append nothing, Rollback() the writer => no .col file appears.

  // --- (1) Rollback path on a norm-only writer ----------------------------
  {
    irs::MemoryDirectory dir;
    {
      irs::ColWriter w{dir, "flush_empty_rb", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/3, /*row_group_size=*/128);
      EXPECT_EQ(nw.RowCount(), 0u);
      w.Rollback();
    }
    EXPECT_FALSE(HasCsFile(dir, "flush_empty_rb"));
  }

  // --- (2) Commit-with-zero-padding produces a 0-row group ---------------
  // Writer::Commit pads the norm column to target_row before flushing; a
  // typed column with one row + an unused norm column => the .col file
  // exists, the typed column has one row, the norm column has one
  // implicit zero-padded row (NonZeroCount == 0, Sum == 0).
  {
    irs::MemoryDirectory dir;
    {
      irs::ColWriter w{dir, "flush_empty_typed", Db()};
      w.OpenNormColumn(/*id=*/3, /*row_group_size=*/128);
      auto& cw = w.OpenColumn(/*id=*/1, duckdb::LogicalType::BIGINT);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, 1};
      duckdb::FlatVector::GetDataMutable<int64_t>(v)[0] = 42;
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      cw.Append(0, v, 1);
      w.Commit(/*target_row=*/1);
    }
    irs::ColReader r{dir, "flush_empty_typed", Db()};
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
      irs::ColWriter w{dir, "dup_zero", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, /*value=*/0);
      }
      w.Commit(kRowCount);
    }
    irs::ColReader r{dir, "dup_zero", Db()};
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
      irs::ColWriter w{dir, "dup_value", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, kRepeatedValue);
      }
      w.Commit(kRowCount);
    }
    irs::ColReader r{dir, "dup_value", Db()};
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

  {
    irs::MemoryDirectory dir;
    constexpr uint64_t kRowCount = 300;
    constexpr uint32_t kRepeatedValue = 7;
    constexpr uint32_t kRowGroupSize = 100;
    {
      irs::ColWriter w{dir, "dup_rg", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, kRepeatedValue);
      }
      w.Commit(kRowCount);
    }
    irs::ColReader r{dir, "dup_rg", Db()};
    const auto* col = r.NormColumn(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowGroupCount(), 3u);
    for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
      EXPECT_EQ(col->Bits(rg), 8u) << "rg=" << rg;
      EXPECT_EQ(col->RowGroupRowCount(rg), kRowGroupSize) << "rg=" << rg;
    }
    EXPECT_TRUE(col->Uniform());
    EXPECT_FALSE(col->HasExceptions());
  }
}

TEST_P(BufferedColumnTestCase, RareLargeValues) {
  constexpr uint32_t kRowGroupSize = 1024;
  constexpr uint64_t kRowCount = 3000;
  std::vector<uint32_t> expected(kRowCount);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    expected[i] = static_cast<uint32_t>(i % 200);
  }
  expected[5] = 255;
  expected[700] = 300;
  expected[1023] = 70000;
  for (uint64_t i = 1024; i < 2048; ++i) {
    expected[i] = static_cast<uint32_t>(256 + i % 1000);
  }
  expected[2058] = 255;

  irs::MemoryDirectory dir;
  {
    irs::ColWriter w{dir, "rare", Db()};
    auto& nw = w.OpenNormColumn(/*id=*/9, kRowGroupSize);
    for (uint64_t i = 0; i < kRowCount; ++i) {
      nw.Append(i, expected[i]);
    }
    w.Commit(kRowCount);
  }
  irs::ColReader r{dir, "rare", Db()};
  const auto* col = r.NormColumn(9);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowGroupCount(), 3u);
  EXPECT_EQ(col->Bits(0), 8u);
  EXPECT_EQ(col->Bits(1), 16u);
  EXPECT_EQ(col->Bits(2), 8u);
  EXPECT_TRUE(col->HasExceptions());
  EXPECT_FALSE(col->Uniform());
  AssertNormReads(*col, expected);
}

TEST_P(BufferedColumnTestCase, RareLargeValuesEveryRowGroup) {
  constexpr uint64_t kRowCount = 2048;
  std::vector<uint32_t> expected(kRowCount);
  for (uint64_t i = 0; i < kRowCount; ++i) {
    expected[i] = static_cast<uint32_t>(i * 7 % 250);
  }
  for (uint64_t rg = 0; rg < 4; ++rg) {
    expected[rg * 512 + 3] = static_cast<uint32_t>(1000 + rg);
    expected[rg * 512 + 511] = 255;
  }

  for (const uint32_t row_group_size : {512u, 4096u}) {
    irs::MemoryDirectory dir;
    {
      irs::ColWriter w{dir, "rare_all", Db()};
      auto& nw = w.OpenNormColumn(/*id=*/9, row_group_size);
      for (uint64_t i = 0; i < kRowCount; ++i) {
        nw.Append(i, expected[i]);
      }
      w.Commit(kRowCount);
    }
    irs::ColReader r{dir, "rare_all", Db()};
    const auto* col = r.NormColumn(9);
    ASSERT_NE(col, nullptr);
    EXPECT_EQ(col->RowGroupCount(),
              kRowCount / std::min<uint64_t>(kRowCount, row_group_size));
    for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
      EXPECT_EQ(col->Bits(rg), 8u) << "rg=" << rg;
    }
    EXPECT_TRUE(col->HasExceptions());
    EXPECT_TRUE(col->Uniform());
    AssertNormReads(*col, expected);
  }
}

TEST_P(BufferedColumnTestCase, Sort) {
  GTEST_SKIP()
    << "BufferedColumn::Sort backed sorted-index inserts on the "
       "legacy subsystem; sorted-index isn't supported on the new cs.";
}

INSTANTIATE_TEST_SUITE_P(BufferedColumnTest, BufferedColumnTestCase,
                         ::testing::Values(false, true));
