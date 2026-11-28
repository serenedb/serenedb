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
#include <absl/hash/hash.h>
#include <absl/strings/str_cat.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/page_residency.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <functional>
#include <iresearch/formats/column/codecs/numeric/numeric_layout.hpp>
#include <iresearch/formats/column/codecs/registry.hpp>
#include <iresearch/formats/column/codecs/text/string_tuning.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_patch.hpp>
#include <iresearch/formats/column/hot_column_reader.hpp>
#include <iresearch/formats/column/internal/gather_arms.hpp>
#include <iresearch/formats/column/merge.hpp>
#include <iresearch/formats/column/norm_column_reader.hpp>
#include <iresearch/formats/column/norm_writer.hpp>
#include <iresearch/formats/column/variant_column_reader.hpp>
#include <iresearch/formats/format_utils.hpp>
#include <iresearch/formats/ivf/ivf_reader.hpp>
#include <iresearch/formats/norm_reader_impl.hpp>
#include <iresearch/index/column_extract.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <latch>
#include <limits>
#include <magic_enum/magic_enum.hpp>
#include <numeric>
#include <random>
#include <set>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "gtest/gtest.h"
#include "hook_file_system.hpp"
#include "index_dir.hpp"
#include "tests_shared.hpp"

#ifdef SERENEDB_HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace {

class ColumnReaderTest : public ::testing::Test {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }
  duckdb::DuckDB _db;
};

// Verify a BIGINT column read back through a ColumnReader matches `value_of`
// (NULL where !is_valid), scanning in vector-sized batches with a reused state.
void VerifyBigint(const irs::ColumnReader& col, irs::ReadContext& ctx,
                  uint64_t rows, const std::function<bool(uint64_t)>& is_valid,
                  const std::function<int64_t(uint64_t)>& value_of) {
  auto state = col.InitScan(ctx);
  uint64_t pos = 0;
  while (pos < rows) {
    const auto take = std::min<duckdb::idx_t>(rows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col.Scan(state, result, take);
    result.Flatten(take);
    const auto* rd = duckdb::FlatVector::GetData<int64_t>(result);
    const auto& rv = duckdb::FlatVector::Validity(result);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      if (!is_valid(g)) {
        EXPECT_FALSE(rv.RowIsValid(k)) << "row " << g;
      } else {
        ASSERT_TRUE(rv.RowIsValid(k)) << "row " << g;
        EXPECT_EQ(rd[k], value_of(g)) << "row " << g;
      }
    }
    pos += take;
  }
}

// Multi-column, multi-row-group round-trip through the full segment container
// (header + per-row-group column runs + footer). Small row-group size forces
// several row-groups, exercising the multi-segment scan-crossing path.
TEST_F(ColumnReaderTest, SegmentRoundTripMultiColumnMultiRowGroup) {
  constexpr uint64_t kRows = 10000;
  constexpr uint32_t kRgSize = 4096;  // multiple of STANDARD_VECTOR_SIZE
  constexpr irs::field_id kA = 1;     // BIGINT, every 10th NULL
  constexpr irs::field_id kB = 2;     // BIGINT, every 7th NULL, value = g*3

  auto a_null = [](uint64_t g) { return g % 10 != 0; };
  auto a_val = [](uint64_t g) { return static_cast<int64_t>(g); };
  auto b_null = [](uint64_t g) { return g % 7 != 0; };
  auto b_val = [](uint64_t g) { return static_cast<int64_t>(g) * 3; };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             {.row_group_size = kRgSize});
    auto& cwB = w.OpenColumn(kB, duckdb::LogicalType::BIGINT,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector va{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      duckdb::Vector vb{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(va);
      auto* db = duckdb::FlatVector::GetDataMutable<int64_t>(vb);
      auto& vaa = duckdb::FlatVector::ValidityMutable(va);
      auto& vbb = duckdb::FlatVector::ValidityMutable(vb);
      vaa.Reset(STANDARD_VECTOR_SIZE);
      vbb.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (a_null(g)) {
          da[k] = a_val(g);
        } else {
          vaa.SetInvalid(k);
        }
        if (b_null(g)) {
          db[k] = b_val(g);
        } else {
          vbb.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(va, take);
      duckdb::FlatVector::SetSize(vb, take);
      cwA.Append(va, take);
      cwB.Append(vb, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ca = r.Column(kA);
  const auto* cb = r.Column(kB);
  ASSERT_NE(ca, nullptr);
  ASSERT_NE(cb, nullptr);
  ASSERT_EQ(ca->RowCount(), kRows);
  ASSERT_EQ(cb->RowCount(), kRows);

  VerifyBigint(*ca, r.Ctx(), kRows, a_null, a_val);
  VerifyBigint(*cb, r.Ctx(), kRows, b_null, b_val);
}

// VARCHAR with a mix of inline-short, overflow-long, and NULL strings across
// multiple row-groups: exercises the overflow string writer + reader.
TEST_F(ColumnReaderTest, SegmentRoundTripVarcharOverflow) {
  constexpr uint64_t kRows = 6000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kS = 5;

  auto s_null = [](uint64_t g) { return g % 13 != 0; };
  auto s_val = [](uint64_t g) -> std::string {
    if (g % 3 == 0) {  // > 12 bytes -> overflow path
      return "long_string_value_" + std::to_string(g) + "_padding_padding_zzz";
    }
    return "s" + std::to_string(g % 100);  // short -> inline
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      auto& val = duckdb::FlatVector::ValidityMutable(v);
      val.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (s_null(g)) {
          d[k] = duckdb::StringVector::AddString(v, s_val(g));
        } else {
          val.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwS.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  ASSERT_EQ(cs->RowCount(), kRows);

  auto state = cs->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    cs->Scan(state, result, take);
    result.Flatten(take);
    const auto* rd = duckdb::FlatVector::GetData<duckdb::string_t>(result);
    const auto& rv = duckdb::FlatVector::Validity(result);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      if (!s_null(g)) {
        EXPECT_FALSE(rv.RowIsValid(k)) << "row " << g;
      } else {
        ASSERT_TRUE(rv.RowIsValid(k)) << "row " << g;
        EXPECT_EQ(rd[k].GetString(), s_val(g)) << "row " << g;
      }
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, CopiedBlocksReadBackBetweenAppendedRows) {
  constexpr uint64_t kRows = 10000;
  constexpr uint64_t kBefore = 50;
  constexpr uint64_t kAfter = 70;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kA = 1;
  const auto append = [](irs::ColumnWriter& cw, uint64_t rows,
                         const std::function<bool(uint64_t)>& is_valid,
                         const std::function<int64_t(uint64_t)>& value_of) {
    for (uint64_t pos = 0; pos < rows;) {
      const auto take =
        std::min<duckdb::idx_t>(rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      auto& validity = duckdb::FlatVector::ValidityMutable(v);
      validity.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        if (is_valid(pos + k)) {
          data[k] = value_of(pos + k);
        } else {
          validity.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
  };
  const auto source_valid = [](uint64_t g) { return g % 10 != 0; };
  const auto source_value = [](uint64_t g) { return static_cast<int64_t>(g); };
  const auto always = [](uint64_t) { return true; };
  const auto before_value = [](uint64_t g) {
    return static_cast<int64_t>(1'000'000 + g);
  };
  const auto after_value = [](uint64_t g) {
    return static_cast<int64_t>(2'000'000 + g);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    append(w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                        {.row_group_size = kRgSize}),
           kRows, source_valid, source_value);
    w.Commit(0);
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto* source = src.Column(kA);
  ASSERT_NE(source, nullptr);
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                            {.row_group_size = kRgSize});
    append(cw, kBefore, always, before_value);
    ASSERT_TRUE(cw.CopyableFrom(*source));
    ASSERT_TRUE(cw.CopyBlocks(*source, src.Ctx(), [] { return true; }));
    append(cw, kAfter, always, after_value);
    w.Commit(0);
  }

  irs::ColReader dst = irs::tests::OpenColReader(dir, "dst");
  const auto* copied = dst.Column(kA);
  ASSERT_NE(copied, nullptr);
  ASSERT_EQ(copied->RowCount(), kBefore + kRows + kAfter);
  VerifyBigint(
    *copied, dst.Ctx(), kBefore + kRows + kAfter,
    [&](uint64_t g) {
      return g < kBefore || g >= kBefore + kRows || source_valid(g - kBefore);
    },
    [&](uint64_t g) {
      if (g < kBefore) {
        return before_value(g);
      }
      if (g < kBefore + kRows) {
        return source_value(g - kBefore);
      }
      return after_value(g - kBefore - kRows);
    });
  const auto source_blocks = source->DataBlocks();
  const auto copied_blocks = copied->DataBlocks();
  ASSERT_GE(copied_blocks.size(), source_blocks.size() + 2);
  for (size_t i = 0; i < source_blocks.size(); ++i) {
    EXPECT_EQ(copied_blocks[i + 1].tuple_count, source_blocks[i].tuple_count);
    EXPECT_EQ(copied_blocks[i + 1].byte_size, source_blocks[i].byte_size);
  }
}

class MergedSegment final : public irs::SubReader {
 public:
  explicit MergedSegment(uint64_t docs) {
    _meta.docs_count = docs;
    _meta.live_docs_count = docs;
  }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  std::span<const irs::TermReader* const> fields(irs::field_id) const final {
    return {};
  }
  irs::NormReader::ptr norms(irs::field_id) const final { return nullptr; }

 private:
  irs::SegmentInfo _meta;
};

TEST_F(ColumnReaderTest, MergeStopsCopyingBlocksOnceProgressFails) {
  constexpr uint64_t kRows = 2 * DEFAULT_ROW_GROUP_SIZE;
  constexpr irs::field_id kA = 1;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    auto& cw = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                            {.row_group_size = DEFAULT_ROW_GROUP_SIZE});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        data[k] = static_cast<int64_t>(pos + k);
      }
      duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
      cw.Append(v, STANDARD_VECTOR_SIZE);
    }
    w.Commit(kRows);
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto* column = src.Column(kA);
  ASSERT_NE(column, nullptr);
  ASSERT_GE(column->DataBlocks().size(), 2);
  const MergedSegment segment{kRows};
  const std::array sources{
    irs::MergeSource{
      .reader = &segment, .col_reader = &src, .alive_count = kRows},
    irs::MergeSource{
      .reader = &segment, .col_reader = &src, .alive_count = kRows},
  };
  irs::ColWriter probe{Db(), dir.Tracker("probe")};
  ASSERT_TRUE(probe
                .OpenColumn(kA, duckdb::LogicalType::BIGINT,
                            {.row_group_size = DEFAULT_ROW_GROUP_SIZE})
                .CopyableFrom(*column));
  irs::ColWriter out{Db(), dir.Tracker("dst")};
  uint64_t checks = 0;
  EXPECT_FALSE(
    irs::MergeInto(sources, out, nullptr, [&checks] { return ++checks == 1; }));
  EXPECT_EQ(checks, 2);
}

TEST_F(ColumnReaderTest, UncompressedOverflowStringsAreNotCopied) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kS = 5;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    auto& cw =
      w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                   {.row_group_size = kRgSize,
                    .compression = irs::codecs::CodecType::Uncompressed});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        data[k] = duckdb::StringVector::AddString(
          v, "long_string_value_" + std::to_string(pos + k) + "_padding_zzz");
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto* source = src.Column(kS);
  ASSERT_NE(source, nullptr);
  irs::ColWriter w{Db(), dir.Tracker("dst")};
  EXPECT_FALSE(
    w.OpenColumn(kS, duckdb::LogicalType::VARCHAR, {.row_group_size = kRgSize})
      .CopyableFrom(*source));
}

TEST_F(ColumnReaderTest, InMemoryColumnsAreNotCopied) {
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kA = 6;
  irs::StagedColumn staged{.id = kA, .type = duckdb::LogicalType::BIGINT};
  for (uint64_t first = 0; first < 3 * kRgSize; first += STANDARD_VECTOR_SIZE) {
    duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(v);
    for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
      data[k] = static_cast<int64_t>(first + k);
    }
    staged.chunks.push_back(
      irs::WriteChunk{std::move(v), STANDARD_VECTOR_SIZE});
  }
  const auto hot = irs::HotColumnReader::Make(Db(), std::move(staged), nullptr);
  ASSERT_GE(hot->RowCount(), kRgSize);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  irs::ColWriter w{Db(), dir.Tracker("dst")};
  EXPECT_FALSE(
    w.OpenColumn(kA, duckdb::LogicalType::BIGINT, {.row_group_size = kRgSize})
      .CopyableFrom(*hot));
}

TEST_F(ColumnReaderTest, CopiedRowsReadBackLikeTheirSource) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 10000;
  struct Case {
    irs::field_id id;
    duckdb::LogicalType type;
    irs::codecs::CodecType compression;
    std::function<duckdb::Value(uint64_t)> value_of;
  };
  const std::vector<Case> cases = {
    {.id = 1,
     .type = duckdb::LogicalType::BIGINT,
     .compression = irs::codecs::CodecType::Auto,
     .value_of =
       [](uint64_t g) {
         return g % 7 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                           : duckdb::Value::BIGINT(static_cast<int64_t>(g * 3));
       }},
    {.id = 2,
     .type = duckdb::LogicalType::VARCHAR,
     .compression = irs::codecs::CodecType::Auto,
     .value_of =
       [](uint64_t g) {
         return g % 5 == 0
                  ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                  : duckdb::Value{absl::StrCat("copied_value_", g % 37)};
       }},
    {.id = 3,
     .type = duckdb::LogicalType::VARCHAR,
     .compression = irs::codecs::CodecType::Zstd,
     .value_of =
       [](uint64_t g) {
         return g % 3 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                           : duckdb::Value{absl::StrCat("zstd_value_", g)};
       }},
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    for (const auto& c : cases) {
      auto& cw =
        w.OpenColumn(c.id, c.type,
                     {.row_group_size = kRgSize, .compression = c.compression});
      for (uint64_t pos = 0; pos < kRows;) {
        const auto take =
          std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector v{c.type, STANDARD_VECTOR_SIZE};
        for (duckdb::idx_t k = 0; k < take; ++k) {
          v.SetValue(k, c.value_of(pos + k));
        }
        duckdb::FlatVector::SetSize(v, take);
        cw.Append(v, take);
        pos += take;
      }
    }
    w.Commit(0);
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");

  struct Run {
    uint64_t first;
    uint64_t count;
  };
  const std::vector<Run> runs = {
    {.first = 0, .count = kRgSize},
    {.first = kRgSize + 100, .count = kRgSize - 100},
    {.first = 2 * kRgSize, .count = kRows - 2 * kRgSize},
    {.first = 50, .count = kRgSize - (kRows - 2 * kRgSize - 100)},
    {.first = 2 * kRgSize, .count = kRows - 2 * kRgSize},
  };
  std::vector<uint64_t> rows;
  for (const auto& run : runs) {
    for (uint64_t row = run.first; row < run.first + run.count; ++row) {
      rows.push_back(row);
    }
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    for (const auto& c : cases) {
      auto& cw =
        w.OpenColumn(c.id, c.type,
                     {.row_group_size = kRgSize, .compression = c.compression});
      irs::ReadContext context{src};
      uint64_t row = 0;
      for (const auto& run : runs) {
        cw.CopyRows(row, *src.Column(c.id), context, run.first, run.count);
        row += run.count;
      }
    }
    w.Commit(0);
  }
  irs::ColReader dst = irs::tests::OpenColReader(dir, "dst");

  for (const auto& c : cases) {
    const auto* column = dst.Column(c.id);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(rows.size(), column->RowCount());
    auto state = column->InitScan(dst.Ctx());
    for (uint64_t pos = 0; pos < rows.size();) {
      const auto take =
        std::min<duckdb::idx_t>(rows.size() - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector batch{c.type, STANDARD_VECTOR_SIZE};
      column->Scan(state, batch, take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto expected = c.value_of(rows[pos + k]);
        EXPECT_TRUE(duckdb::Value::NotDistinctFrom(expected, batch.GetValue(k)))
          << "column " << c.id << " row " << pos + k << ": "
          << expected.ToString() << " != " << batch.GetValue(k).ToString();
      }
      pos += take;
    }
  }

  const auto source = src.Column(1)->DataBlocks();
  const auto copied = dst.Column(1)->DataBlocks();
  ASSERT_EQ(4, copied.size());
  for (size_t block = 0; block != 3; ++block) {
    EXPECT_EQ(kRgSize, copied[block].tuple_count);
  }
  for (const auto [from, to] : {std::pair{0, 0}, std::pair{2, 3}}) {
    EXPECT_EQ(source[from].tuple_count, copied[to].tuple_count);
    EXPECT_EQ(source[from].byte_size, copied[to].byte_size);
    EXPECT_EQ(source[from].codec, copied[to].codec);
  }
}

TEST_F(ColumnReaderTest, CopiesOnlyBlocksInTheForcedCodec) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kId = 1;
  const auto value_of = [](uint64_t g) {
    return g % 5 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                      : duckdb::Value{absl::StrCat("forced_value_", g % 37)};
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::VARCHAR,
                            {.row_group_size = kRgSize});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, value_of(pos + k));
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto& source = *src.Column(kId);
  ASSERT_TRUE(absl::c_none_of(source.DataBlocks(), [](const auto& block) {
    return block.codec->type == irs::codecs::CodecType::Zstd;
  }));

  {
    irs::ColWriter w{Db(), dir.Tracker("probe")};
    EXPECT_TRUE(w.OpenColumn(kId, duckdb::LogicalType::VARCHAR,
                             {.row_group_size = kRgSize})
                  .CopyableFrom(source));
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw = w.OpenColumn(
      kId, duckdb::LogicalType::VARCHAR,
      {.row_group_size = kRgSize, .compression = irs::codecs::CodecType::Zstd});
    EXPECT_FALSE(cw.CopyableFrom(source));
    irs::ReadContext context{src};
    cw.CopyRows(0, source, context, 0, kRows);
    w.Commit(0);
  }
  irs::ColReader dst = irs::tests::OpenColReader(dir, "dst");
  const auto* column = dst.Column(kId);
  ASSERT_NE(nullptr, column);
  ASSERT_EQ(kRows, column->RowCount());
  for (const auto& block : column->DataBlocks()) {
    EXPECT_EQ(irs::codecs::CodecType::Zstd, block.codec->type);
  }
  auto state = column->InitScan(dst.Ctx());
  for (uint64_t pos = 0; pos < kRows;) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector batch{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    column->Scan(state, batch, take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_TRUE(
        duckdb::Value::NotDistinctFrom(value_of(pos + k), batch.GetValue(k)))
        << "row " << pos + k;
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, ForcedCodecTheTypeCannotUseKeepsBlocksCopyable) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kId = 1;
  const auto value_of = [](uint64_t g) {
    return g % 5 == 0
             ? duckdb::Value{duckdb::LogicalType::BIGINT}
             : duckdb::Value::BIGINT(static_cast<int64_t>(g * 7919 % 1000));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT,
                            {.row_group_size = kRgSize});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, value_of(pos + k));
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto& source = *src.Column(kId);
  ASSERT_TRUE(absl::c_none_of(source.DataBlocks(), [](const auto& block) {
    return block.codec->type == irs::codecs::CodecType::Constant;
  }));

  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw = w.OpenColumn(
      kId, duckdb::LogicalType::BIGINT,
      {.row_group_size = kRgSize, .compression = irs::codecs::CodecType::Fsst});
    EXPECT_TRUE(cw.CopyableFrom(source));
    irs::ReadContext context{src};
    cw.CopyRows(0, source, context, 0, kRows);
    w.Commit(0);
  }
  irs::ColReader dst = irs::tests::OpenColReader(dir, "dst");
  const auto* column = dst.Column(kId);
  ASSERT_NE(nullptr, column);
  ASSERT_EQ(kRows, column->RowCount());
  const auto blocks = source.DataBlocks();
  const auto copied = column->DataBlocks();
  ASSERT_EQ(blocks.size(), copied.size());
  for (size_t b = 0; b != blocks.size(); ++b) {
    EXPECT_EQ(blocks[b].byte_size, copied[b].byte_size) << "block " << b;
    EXPECT_EQ(blocks[b].codec, copied[b].codec) << "block " << b;
  }
  auto state = column->InitScan(dst.Ctx());
  for (uint64_t pos = 0; pos < kRows;) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector batch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    column->Scan(state, batch, take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_TRUE(
        duckdb::Value::NotDistinctFrom(value_of(pos + k), batch.GetValue(k)))
        << "row " << pos + k;
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, CopiesBlocksOfAnyTierAtItsLevel) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kId = 1;
  const auto type = duckdb::LogicalType::VARCHAR;
  const auto value_of = [](uint64_t g) {
    return g % 9 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                      : duckdb::Value{absl::StrCat("tier_value_", g)};
  };
  const auto append = [&](irs::ColumnWriter& cw, uint64_t first,
                          uint64_t rows) {
    for (uint64_t pos = 0; pos < rows;) {
      const auto take =
        std::min<duckdb::idx_t>(rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{type, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, value_of(first + pos + k));
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
  };
  const irs::ColumnWriterOptions plain{.row_group_size = kRgSize};
  const irs::ColumnWriterOptions zstd{
    .row_group_size = kRgSize,
    .compression = irs::codecs::CodecType::Zstd,
    .codec_params = {.compression_level = 3},
  };
  const irs::ColumnWriterOptions zstd9{
    .row_group_size = kRgSize,
    .compression = irs::codecs::CodecType::Zstd,
    .codec_params = {.compression_level = 9},
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  const auto write = [&](std::string_view name, irs::WriteTier tier,
                         const irs::ColumnWriterOptions& options) {
    irs::ColWriter w{Db(), dir.Tracker(name), tier};
    append(w.OpenColumn(kId, type, options), 0, kRows);
    w.Commit(0);
  };
  write("flush", irs::WriteTier::Flush, plain);
  write("merge", irs::WriteTier::Merge, plain);
  write("merge_zstd", irs::WriteTier::Merge, zstd);
  irs::ColReader flush = irs::tests::OpenColReader(dir, "flush");
  irs::ColReader merge = irs::tests::OpenColReader(dir, "merge");
  irs::ColReader merge_zstd = irs::tests::OpenColReader(dir, "merge_zstd");
  EXPECT_EQ(0, flush.Column(kId)->CompressionLevel());
  EXPECT_EQ(0, merge.Column(kId)->CompressionLevel());
  EXPECT_EQ(3, merge_zstd.Column(kId)->CompressionLevel());

  {
    irs::ColWriter w{Db(), dir.Tracker("probe_merge"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kId, type, plain);
    EXPECT_TRUE(cw.CopyableFrom(*flush.Column(kId)));
    EXPECT_TRUE(cw.CopyableFrom(*merge.Column(kId)));
    EXPECT_FALSE(cw.CopyableFrom(*merge_zstd.Column(kId)));
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("probe_flush"), irs::WriteTier::Flush};
    auto& cw = w.OpenColumn(kId, type, plain);
    EXPECT_TRUE(cw.CopyableFrom(*flush.Column(kId)));
    EXPECT_TRUE(cw.CopyableFrom(*merge.Column(kId)));
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("probe_level"), irs::WriteTier::Merge};
    EXPECT_TRUE(
      w.OpenColumn(kId, type, zstd).CopyableFrom(*merge_zstd.Column(kId)));
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("probe_level9"), irs::WriteTier::Merge};
    EXPECT_FALSE(
      w.OpenColumn(kId, type, zstd9).CopyableFrom(*merge_zstd.Column(kId)));
  }

  const auto expect_rows = [&](const irs::ColReader& reader, uint64_t rows,
                               const std::function<uint64_t(uint64_t)>& row) {
    const auto* column = reader.Column(kId);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(rows, column->RowCount());
    irs::ReadContext context{reader};
    auto state = column->InitScan(context);
    for (uint64_t pos = 0; pos < rows;) {
      const auto take =
        std::min<duckdb::idx_t>(rows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
      column->Scan(state, batch, take);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        EXPECT_TRUE(duckdb::Value::NotDistinctFrom(value_of(row(pos + k)),
                                                   batch.GetValue(k)))
          << "row " << pos + k;
      }
      pos += take;
    }
  };
  {
    irs::ColWriter w{Db(), dir.Tracker("copied"), irs::WriteTier::Flush};
    auto& cw = w.OpenColumn(kId, type, plain);
    irs::ReadContext context{merge};
    ASSERT_TRUE(
      cw.CopyBlocks(*merge.Column(kId), context, [] { return true; }));
    w.Commit(0);
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("mixed"), irs::WriteTier::Flush};
    auto& cw = w.OpenColumn(kId, type, plain);
    append(cw, 0, 100);
    irs::ReadContext context{merge};
    ASSERT_TRUE(
      cw.CopyBlocks(*merge.Column(kId), context, [] { return true; }));
    w.Commit(0);
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("merged"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kId, type, plain);
    irs::ReadContext context{flush};
    cw.CopyRows(0, *flush.Column(kId), context, 0, kRows);
    w.Commit(0);
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("reencoded"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kId, type, zstd9);
    irs::ReadContext context{merge_zstd};
    cw.CopyRows(0, *merge_zstd.Column(kId), context, 0, kRows);
    w.Commit(0);
  }
  irs::ColReader copied = irs::tests::OpenColReader(dir, "copied");
  irs::ColReader mixed = irs::tests::OpenColReader(dir, "mixed");
  irs::ColReader merged = irs::tests::OpenColReader(dir, "merged");
  irs::ColReader reencoded = irs::tests::OpenColReader(dir, "reencoded");
  EXPECT_EQ(9, reencoded.Column(kId)->CompressionLevel());
  expect_rows(copied, kRows, [](uint64_t g) { return g; });
  expect_rows(mixed, kRows + 100,
              [](uint64_t g) { return g < 100 ? g : g - 100; });
  expect_rows(merged, kRows, [](uint64_t g) { return g; });
  expect_rows(reencoded, kRows, [](uint64_t g) { return g; });
  const auto expect_same_blocks = [&](const irs::ColReader& source,
                                      const irs::ColReader& target) {
    const auto source_blocks = source.Column(kId)->DataBlocks();
    const auto target_blocks = target.Column(kId)->DataBlocks();
    ASSERT_EQ(source_blocks.size(), target_blocks.size());
    for (size_t i = 0; i < source_blocks.size(); ++i) {
      EXPECT_EQ(source_blocks[i].byte_size, target_blocks[i].byte_size);
      EXPECT_EQ(source_blocks[i].codec, target_blocks[i].codec);
    }
  };
  expect_same_blocks(merge, copied);
  expect_same_blocks(flush, merged);
}

struct HotLayout {
  std::vector<duckdb::idx_t> chunks;
  uint64_t rows;
};

void ExpectConcatReadsItsParts(
  duckdb::DatabaseInstance& db, const duckdb::LogicalType& type,
  const std::function<duckdb::Value(uint64_t)>& value_of) {
  constexpr irs::field_id kId = 7;
  const std::vector<HotLayout> layouts = {
    {.chunks = {100, STANDARD_VECTOR_SIZE}, .rows = 100 + STANDARD_VECTOR_SIZE},
    {.chunks = {}, .rows = 30},
    {.chunks = {STANDARD_VECTOR_SIZE, 5}, .rows = STANDARD_VECTOR_SIZE + 9},
    {.chunks = {1}, .rows = 1},
    {.chunks = {7, 3}, .rows = 10},
  };
  std::vector<duckdb::Value> expected;
  std::vector<std::unique_ptr<irs::ColumnReader>> sources;
  std::vector<irs::HotColumnPart> parts;
  irs::StagedColumn all{
    .id = kId, .type = type, .distinct = irs::DistinctStats::Exact};
  irs::HotChunkStats stats;
  for (const auto& layout : layouts) {
    const auto first = expected.size();
    const irs::ColumnReader* column = nullptr;
    if (!layout.chunks.empty()) {
      irs::StagedColumn staged{
        .id = kId, .type = type, .distinct = irs::DistinctStats::Exact};
      for (const auto count : layout.chunks) {
        duckdb::Vector v{type, count};
        for (duckdb::idx_t k = 0; k < count; ++k) {
          expected.push_back(value_of(expected.size()));
          v.SetValue(k, expected.back());
        }
        duckdb::FlatVector::SetSize(v, count);
        all.chunks.push_back(irs::WriteChunk{duckdb::Vector::Ref(v), count});
        staged.chunks.push_back(irs::WriteChunk{std::move(v), count});
      }
      column = sources
                 .emplace_back(
                   irs::HotColumnReader::Make(db, std::move(staged), &stats))
                 .get();
    }
    expected.resize(first + layout.rows, duckdb::Value{type});
    parts.push_back({.column = column, .rows = layout.rows});
  }
  irs::HotChunkStats all_stats;
  const auto reference =
    irs::HotColumnReader::Make(db, std::move(all), &all_stats);

  std::vector<std::unique_ptr<irs::ColumnReader>> columns;
  columns.push_back(irs::HotColumnReader::Concat(
    db, kId, type, irs::DistinctStats::Exact, parts));
  irs::ColReader reader{db, std::move(columns)};
  const auto* concat = reader.Column(kId);
  ASSERT_NE(nullptr, concat);
  ASSERT_EQ(expected.size(), concat->RowCount());

  auto state = concat->InitScan(reader.Ctx());
  for (uint64_t pos = 0; pos < expected.size();) {
    const auto take =
      std::min<duckdb::idx_t>(expected.size() - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
    concat->Scan(state, batch, take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_TRUE(
        duckdb::Value::NotDistinctFrom(expected[pos + k], batch.GetValue(k)))
        << "row " << pos + k << ": " << expected[pos + k].ToString()
        << " != " << batch.GetValue(k).ToString();
    }
    pos += take;
  }

  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    const auto blocks = concat->DataBlocks();
    for (size_t i = 0; i < blocks.size(); ++i) {
      const auto first = concat->DataBlockFirstRow(i);
      const auto& block = concat->DataBlocks()[i].statistics;
      for (uint64_t row = first; row < first + blocks[i].tuple_count; ++row) {
        if (expected[row].IsNull()) {
          EXPECT_TRUE(block.CanHaveNull()) << "row " << row;
          continue;
        }
        const auto text = expected[row].GetValue<std::string>();
        EXPECT_NE(duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE,
                  duckdb::StringStats::CheckZonemap(
                    block, duckdb::ExpressionType::COMPARE_EQUAL,
                    duckdb::string_t{text}))
          << "row " << row;
      }
    }
  }

  ASSERT_NE(nullptr, reference->Distinct());
  ASSERT_NE(nullptr, concat->Distinct());
  EXPECT_EQ(reference->Distinct()->GetCount(), concat->Distinct()->GetCount());
}

TEST_F(ColumnReaderTest, ConcatenatedHotStringsReadAsTheirParts) {
  ExpectConcatReadsItsParts(
    Db(), duckdb::LogicalType::VARCHAR, [](uint64_t g) -> duckdb::Value {
      if (g % 11 == 0) {
        return duckdb::Value{duckdb::LogicalType::VARCHAR};
      }
      return duckdb::Value{absl::StrCat("concatenated_value_", g % 97)};
    });
}

TEST_F(ColumnReaderTest, ConcatenatedHotStructsReadAsTheirParts) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"a", duckdb::LogicalType::BIGINT}, {"b", duckdb::LogicalType::VARCHAR}});
  ExpectConcatReadsItsParts(Db(), type, [&](uint64_t g) -> duckdb::Value {
    if (g % 11 == 0) {
      return duckdb::Value{type};
    }
    duckdb::child_list_t<duckdb::Value> children;
    children.emplace_back("a", duckdb::Value::BIGINT(static_cast<int64_t>(g)));
    children.emplace_back(
      "b", g % 7 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                      : duckdb::Value{absl::StrCat("nested_value_", g % 13)});
    return duckdb::Value::STRUCT(std::move(children));
  });
}

// A nullable VARCHAR column compresses with dict_fsst, which self-carries nulls
// (dict slot 0 == NULL) and reconstructs them in-band on scan; the validity
// stream is therefore absorbed to a single EMPTY segment (mirror DuckDB
// ValidityCoveredByBasedata). NullsInData() reports this, and a normal Scan
// still yields the correct null mask + values.
TEST_F(ColumnReaderTest, DictFsstValidityAbsorbed) {
  constexpr uint64_t kRows = 6000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kS = 5;
  auto s_valid = [](uint64_t g) { return g % 7 != 0; };
  auto s_val = [](uint64_t g) -> std::string {
    static const char* const vals[] = {"cat", "dog", "bird", "fish", "owl"};
    return vals[g % 5];
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      auto& vv = duckdb::FlatVector::ValidityMutable(v);
      vv.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (s_valid(g)) {
          d[k] = duckdb::StringVector::AddString(v, s_val(g));
        } else {
          vv.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwS.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  // dict_fsst self-carries nulls -> validity absorbed to EMPTY; NullsInData()
  // reports it so readers scan the data (not the empty validity) for nullness.
  ASSERT_TRUE(irs::tests::NullsInData(*cs));

  auto state = cs->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    cs->Scan(state, result, take);
    result.Flatten(take);
    const auto* rd = duckdb::FlatVector::GetData<duckdb::string_t>(result);
    const auto& rv = duckdb::FlatVector::Validity(result);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      if (s_valid(g)) {
        ASSERT_TRUE(rv.RowIsValid(k)) << "row " << g;
        EXPECT_EQ(rd[k].GetString(), s_val(g)) << "row " << g;
      } else {
        EXPECT_FALSE(rv.RowIsValid(k)) << "row " << g;
      }
    }
    pos += take;
  }
}

// An all-NULL column's merged stats must report has_no_null=false (mirror
// DuckDB, which merges the all-null data + validity segment stats and never
// unconditionally sets has_no_null). Regression for the over-approximation that
// flipped it to true.
TEST_F(ColumnReaderTest, AllNullColumnMergedStatsHasNoNullFalse) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kA = 1;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto& vv = duckdb::FlatVector::ValidityMutable(v);
      vv.Reset(STANDARD_VECTOR_SIZE);
      vv.SetAllInvalid(static_cast<duckdb::idx_t>(take));
      duckdb::FlatVector::SetSize(v, take);
      cwA.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kA);
  ASSERT_NE(cs, nullptr);
  const auto& stats = cs->MergedStatistics();
  EXPECT_TRUE(stats.CanHaveNull());
  EXPECT_FALSE(stats.CanHaveNoNull());
}

TEST_F(ColumnReaderTest, DictFsstReusedBatchAcrossChunks) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 8192;
  constexpr irs::field_id kS = 5;
  auto s_valid = [](uint64_t g) { return g % 7 != 0; };
  auto s_val = [](uint64_t g) -> std::string {
    static const char* const v[] = {"a", "bb", "ccc", "dddd", "ee"};
    return v[g % 5];
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                            {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      auto& vv = duckdb::FlatVector::ValidityMutable(v);
      vv.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (s_valid(g)) {
          d[k] = duckdb::StringVector::AddString(v, s_val(g));
        } else {
          vv.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  ASSERT_TRUE(irs::tests::NullsInData(*cs));
  auto state = cs->InitScan(r.Ctx());
  duckdb::Vector batch{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  batch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    auto* words = batch.BufferMutable().GetValidityMask().GetData();
    std::memset(words, 0xFF, ((take + 63) / 64) * sizeof(*words));
    cs->ScanCount(state, batch, take, /*result_offset=*/0);
    const auto& m = duckdb::FlatVector::Validity(batch);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      EXPECT_EQ(m.RowIsValid(k), s_valid(g)) << "row " << g;
    }
    pos += take;
  }
}

// Non-contiguous DocIds duck-type for gather.
struct Rows {
  std::vector<uint64_t> v;
  size_t size() const noexcept { return v.size(); }
  uint64_t operator[](size_t i) const noexcept { return v[i]; }
};

// Gather an ascending mix of runs + isolated rows spanning several row-groups,
// including NULL rows.
TEST_F(ColumnReaderTest, GatherSparseAndDenseAcrossRowGroups) {
  constexpr uint64_t kRows = 10000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kA = 1;
  auto is_valid = [](uint64_t g) { return g % 10 != 0; };
  auto val = [](uint64_t g) { return static_cast<int64_t>(g); };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      auto& vv = duckdb::FlatVector::ValidityMutable(v);
      vv.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (is_valid(g)) {
          d[k] = val(g);
        } else {
          vv.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwA.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kA);
  ASSERT_NE(cs, nullptr);

  Rows rows{{3, 4, 5, 100, 101, 4096, 4097, 8000, 9000, 9999}};
  auto state = cs->InitScan(r.Ctx());
  duckdb::Vector result{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(*cs, state, rows, result, /*out_offset=*/0);
  result.Flatten(rows.size());
  const auto* rd = duckdb::FlatVector::GetData<int64_t>(result);
  const auto& rv = duckdb::FlatVector::Validity(result);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto g = rows[i];
    if (!is_valid(g)) {
      EXPECT_FALSE(rv.RowIsValid(i)) << "gathered row " << g;
    } else {
      ASSERT_TRUE(rv.RowIsValid(i)) << "gathered row " << g;
      EXPECT_EQ(rd[i], val(g)) << "gathered row " << g;
    }
  }
}

using ValueOf = std::function<duckdb::Value(uint64_t)>;

void WriteColumn(duckdb::DatabaseInstance& db, const tests::IndexDir& dir,
                 irs::field_id id, const duckdb::LogicalType& type,
                 uint64_t rows, uint32_t rg_size, const ValueOf& value_of) {
  irs::ColWriter w{db, dir.Tracker("seg")};
  auto& cw = w.OpenColumn(id, type, {.row_group_size = rg_size});
  for (uint64_t pos = 0; pos < rows;) {
    const auto take = std::min<duckdb::idx_t>(rows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector v{type, STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t k = 0; k < take; ++k) {
      v.SetValue(k, value_of(pos + k));
    }
    duckdb::FlatVector::SetSize(v, take);
    cw.Append(v, take);
    pos += take;
  }
  w.Commit(0);
}

bool CrossesBlock(const irs::ColumnReader& col, uint64_t anchor,
                  uint64_t span) {
  for (size_t b = 1; b < col.DataBlocks().size(); ++b) {
    const auto first = col.DataBlockFirstRow(b);
    if (first > anchor && first < anchor + span) {
      return true;
    }
  }
  return false;
}

enum class GatherMode : uint8_t {
  Dense,
  Scatter,
  ScatterAppend,
  Mixed,
};

struct GatherStats {
  size_t windows = 0;
  size_t crossing = 0;
};

void CheckGathers(const irs::ColumnReader& col, irs::ReadContext& ctx,
                  const ValueOf& value_of, GatherMode mode, uint32_t per_mille,
                  GatherStats& stats) {
  const auto rows = col.RowCount();
  std::mt19937_64 rng{per_mille * 7 + static_cast<uint32_t>(mode)};
  constexpr std::array<uint64_t, 4> kSpans{STANDARD_VECTOR_SIZE, 1000, 37,
                                           3 * STANDARD_VECTOR_SIZE + 500};
  auto state = col.InitScan(ctx);
  auto out = std::make_unique<duckdb::Vector>(col.Type(), STANDARD_VECTOR_SIZE);
  std::vector<uint64_t> expected;
  auto verify = [&] {
    for (size_t i = 0; i < expected.size(); ++i) {
      const auto got = out->GetValue(i);
      const auto want = value_of(expected[i]);
      ASSERT_TRUE(duckdb::Value::NotDistinctFrom(got, want))
        << "row " << expected[i] << " got " << got.ToString() << " want "
        << want.ToString();
    }
    expected.clear();
  };
  for (uint64_t anchor = rng() % 64; anchor < rows;) {
    auto dense = mode == GatherMode::Dense;
    if (mode == GatherMode::Mixed) {
      dense = rng() % 2 == 0;
    }
    auto span =
      std::min(rows - anchor,
               kSpans[rng() % (dense ? kSpans.size() - 1 : kSpans.size())]);
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    duckdb::idx_t hits = 0;
    for (duckdb::idx_t i = 0; i < span && hits < STANDARD_VECTOR_SIZE; ++i) {
      if (rng() % 1000 < per_mille) {
        sel.set_index(hits++, i);
      }
    }
    if (hits == 0) {
      sel.set_index(hits++, span - 1);
    }
    span =
      std::min<uint64_t>(span, sel.get_index(hits - 1) + STANDARD_VECTOR_SIZE);
    ++stats.windows;
    stats.crossing += CrossesBlock(col, anchor, span);
    if (mode == GatherMode::ScatterAppend) {
      if (expected.size() + hits > STANDARD_VECTOR_SIZE) {
        verify();
        out =
          std::make_unique<duckdb::Vector>(col.Type(), STANDARD_VECTOR_SIZE);
      }
      col.GatherScatter(state, anchor, sel, hits, *out, expected.size());
    } else {
      expected.clear();
      out = std::make_unique<duckdb::Vector>(col.Type(), STANDARD_VECTOR_SIZE);
      if (dense) {
        col.GatherDense(state, anchor, sel, hits, span, *out);
      } else {
        col.GatherScatter(state, anchor, sel, hits, *out, 0);
      }
    }
    for (duckdb::idx_t i = 0; i < hits; ++i) {
      expected.push_back(anchor + sel.get_index(i));
    }
    if (mode != GatherMode::ScatterAppend) {
      verify();
    }
    anchor += span + rng() % 3000;
  }
  verify();
}

void ScanRuns(const irs::ColumnReader& col, irs::ColumnReader::ScanState& state,
              uint64_t& cursor, uint64_t anchor,
              const duckdb::SelectionVector& sel, duckdb::idx_t hits,
              duckdb::Vector& out) {
  for (duckdb::idx_t i = 0; i < hits;) {
    duckdb::idx_t run = 1;
    while (i + run < hits &&
           sel.get_index(i + run) == sel.get_index(i + run - 1) + 1) {
      ++run;
    }
    const auto target = anchor + sel.get_index(i);
    col.Skip(state, target - cursor);
    col.ScanCount(state, out, run, i);
    cursor = target + run;
    i += run;
  }
}

std::string RandomText(uint64_t seed, size_t len) {
  std::mt19937_64 rng{seed};
  std::string s(len, ' ');
  for (auto& c : s) {
    c = static_cast<char>('a' + rng() % 26);
  }
  return s;
}

struct GatherCase {
  std::string name;
  duckdb::LogicalType type;
  uint64_t rows;
  ValueOf value_of;
  std::vector<std::string> codecs;
};

TEST_F(ColumnReaderTest, GathersMatchScanAcrossBlocks) {
  const std::vector<GatherCase> cases{
    {"bigint_random",
     duckdb::LogicalType::BIGINT,
     30000,
     [](uint64_t g) {
       return g % 10 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                          : duckdb::Value::BIGINT(
                              static_cast<int64_t>(std::mt19937_64{g}()));
     },
     {"uncompressed"}},
    {"bigint_narrow",
     duckdb::LogicalType::BIGINT,
     30000,
     [](uint64_t g) {
       return duckdb::Value::BIGINT(static_cast<int64_t>(g % 1000));
     },
     {"bitpacking"}},
    {"bigint_runs",
     duckdb::LogicalType::BIGINT,
     30000,
     [](uint64_t g) {
       return g / 500 % 7 == 3
                ? duckdb::Value{duckdb::LogicalType::BIGINT}
                : duckdb::Value::BIGINT(static_cast<int64_t>(g / 500));
     },
     {"rle", "bitpacking"}},
    {"double",
     duckdb::LogicalType::DOUBLE,
     30000,
     [](uint64_t g) { return duckdb::Value::DOUBLE(g * 0.37); },
     {"uncompressed", "alprd"}},
    {"varchar_dict",
     duckdb::LogicalType::VARCHAR,
     30000,
     [](uint64_t g) {
       static const char* const kWords[] = {"cat", "dog", "bird", "fish",
                                            "owl"};
       return g % 9 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                         : duckdb::Value{kWords[g % 5]};
     },
     {"zstd", "uncompressed", "dict_fsst", "fsst", "lz4", "fsst12", "fsst_plus",
      "onpair"}},
    {"varchar_unique",
     duckdb::LogicalType::VARCHAR,
     30000,
     [](uint64_t g) {
       return duckdb::Value{"value_" + std::to_string(g) + "_" +
                            RandomText(g, 24)};
     },
     {"zstd", "uncompressed", "dict_fsst", "fsst", "lz4", "fsst12", "fsst_plus",
      "onpair"}},
    {"varchar_long",
     duckdb::LogicalType::VARCHAR,
     6000,
     [](uint64_t g) {
       return g % 11 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                          : duckdb::Value{RandomText(g, 500 + g % 2000)};
     },
     {"zstd", "uncompressed", "dict_fsst", "fsst", "lz4", "fsst12", "fsst_plus",
      "onpair"}},
  };
  constexpr irs::field_id kId = 1;
  duckdb::Connection con{Db()};
  for (const auto& c : cases) {
    std::vector<std::string> codecs{"auto"};
    codecs.insert(codecs.end(), c.codecs.begin(), c.codecs.end());
    for (const auto& codec : codecs) {
      SCOPED_TRACE(c.name + " " + codec);
      auto set = con.Query("SET force_compression = '" + codec + "'");
      ASSERT_FALSE(set->HasError()) << set->GetError();
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      WriteColumn(Db(), dir, kId, c.type, c.rows, 4096, c.value_of);
      irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
      const auto* col = r.Column(kId);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->RowCount(), c.rows);
      if (codec != "auto") {
        const auto forced = irs::codecs::FindCodec(codec);
        ASSERT_NE(forced, irs::codecs::kNoCodec);
        EXPECT_TRUE(absl::c_any_of(col->DataBlocks(), [&](const auto& b) {
          return b.codec->type == forced;
        }));
      }
      GatherStats stats;
      for (const auto mode : {GatherMode::Dense, GatherMode::Scatter,
                              GatherMode::ScatterAppend, GatherMode::Mixed}) {
        for (const uint32_t per_mille : {1u, 10u, 50u, 200u, 600u, 1000u}) {
          SCOPED_TRACE(testing::Message() << "mode " << static_cast<int>(mode)
                                          << " per_mille " << per_mille);
          CheckGathers(*col, r.Ctx(), c.value_of, mode, per_mille, stats);
          if (HasFatalFailure()) {
            con.Query("SET force_compression = 'auto'");
            return;
          }
        }
      }
      EXPECT_GT(stats.crossing, 0u) << "of " << stats.windows << " windows";
    }
  }
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, RepeatedMapGathersMatchValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 30000;
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::VARCHAR);
  const auto list_of = [](uint64_t g) -> uint64_t {
    return g % 7 == 0 ? kRows + g : g * 2654435761 % 997;
  };
  const ValueOf value_of = [&](uint64_t g) {
    if (g % 23 == 0) {
      return duckdb::Value{type};
    }
    const auto list = list_of(g);
    std::vector<duckdb::Value> keys;
    std::vector<duckdb::Value> values;
    for (uint64_t k = 0; k < list % 4; ++k) {
      keys.emplace_back(absl::StrCat("key-", k));
      values.emplace_back(absl::StrCat("value-of-list-", list, "-", k));
    }
    return duckdb::Value::MAP(duckdb::LogicalType::VARCHAR,
                              duckdb::LogicalType::VARCHAR, std::move(keys),
                              std::move(values));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, type, kRows, 4096, value_of);
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  uint64_t elements = 0;
  for (uint64_t g = 0; g < kRows; ++g) {
    elements += g % 23 == 0 ? 0 : list_of(g) % 4;
  }
  EXPECT_LT(col->Child()->RowCount() * 2, elements);
  GatherStats stats;
  for (const auto mode : {GatherMode::Dense, GatherMode::Scatter,
                          GatherMode::ScatterAppend, GatherMode::Mixed}) {
    for (const uint32_t per_mille : {1u, 10u, 50u, 200u, 600u, 1000u}) {
      SCOPED_TRACE(testing::Message() << "mode " << static_cast<int>(mode)
                                      << " per_mille " << per_mille);
      CheckGathers(*col, r.Ctx(), value_of, mode, per_mille, stats);
      if (HasFatalFailure()) {
        return;
      }
    }
  }
  EXPECT_GT(stats.crossing, 0u) << "of " << stats.windows << " windows";
}

TEST_F(ColumnReaderTest, FrontCodedRowsGatherForEverySmallSelectionOfAGroup) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 5 * STANDARD_VECTOR_SIZE;
  constexpr uint32_t kGroup = 16;
  constexpr uint32_t kShared[kGroup] = {0,  12, 12, 5, 9, 9,  9, 2,
                                        30, 31, 32, 1, 0, 20, 3, 20};
  constexpr std::string_view kBase =
    "the-quick-brown-fox-jumps-over-the-lazy-dog";
  const ValueOf value_of = [&](uint64_t g) {
    return duckdb::Value{
      absl::StrCat(kBase.substr(0, kShared[g % kGroup]), "#", g * 7919)};
  };
  duckdb::Connection con{Db()};
  for (const std::string codec : {"fsst", "fsst12"}) {
    SCOPED_TRACE(codec);
    auto set = con.Query("SET force_compression = '" + codec + "'");
    ASSERT_FALSE(set->HasError()) << set->GetError();
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, 4096,
                value_of);
    irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
    const auto* col = r.Column(kId);
    ASSERT_NE(col, nullptr);
    for (uint32_t rows = 1; rows < (1u << kGroup); ++rows) {
      if (std::popcount(rows) > 3 && rows % 97 != 0) {
        continue;
      }
      SCOPED_TRACE(testing::Message() << "rows " << rows);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{col->Type(), STANDARD_VECTOR_SIZE};
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      duckdb::idx_t hits = 0;
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
        if ((rows >> (i % kGroup) & 1) != 0) {
          sel.set_index(hits++, i);
        }
      }
      const uint64_t anchor = STANDARD_VECTOR_SIZE + rows % kGroup;
      col->GatherDense(state, anchor, sel, hits, STANDARD_VECTOR_SIZE, out);
      for (duckdb::idx_t i = 0; i < hits; ++i) {
        const auto row = anchor + sel.get_index(i);
        ASSERT_TRUE(
          duckdb::Value::NotDistinctFrom(out.GetValue(i), value_of(row)))
          << "row " << row << " got " << out.GetValue(i).ToString();
      }
      duckdb::Vector scanned{col->Type(), STANDARD_VECTOR_SIZE};
      const uint64_t next = anchor + STANDARD_VECTOR_SIZE;
      uint64_t cursor = next;
      ScanRuns(*col, state, cursor, next, sel, hits, scanned);
      for (duckdb::idx_t i = 0; i < hits; ++i) {
        const auto row = next + sel.get_index(i);
        ASSERT_TRUE(
          duckdb::Value::NotDistinctFrom(scanned.GetValue(i), value_of(row)))
          << "run by run: row " << row << " got "
          << scanned.GetValue(i).ToString();
      }
      duckdb::Vector appended{col->Type(), STANDARD_VECTOR_SIZE};
      const uint64_t last = next + STANDARD_VECTOR_SIZE;
      const auto half = hits / 2;
      col->GatherScatter(state, last, sel, half, appended, 0);
      const duckdb::SelectionVector rest{sel.data() + half, hits - half};
      col->GatherScatter(state, last, rest, hits - half, appended, half);
      for (duckdb::idx_t i = 0; i < hits; ++i) {
        const auto row = last + sel.get_index(i);
        ASSERT_TRUE(
          duckdb::Value::NotDistinctFrom(appended.GetValue(i), value_of(row)))
          << "appended: row " << row << " got "
          << appended.GetValue(i).ToString();
      }
    }
  }
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, AppendingGathersStopAtTheLastStoredRow) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE;
  const ValueOf value_of = [](uint64_t g) {
    return duckdb::Value::BIGINT(static_cast<int64_t>(g / 100));
  };
  duckdb::Connection con{Db()};
  for (const std::string codec : {"rle", "bitpacking"}) {
    SCOPED_TRACE(codec);
    auto set = con.Query("SET force_compression = '" + codec + "'");
    ASSERT_FALSE(set->HasError()) << set->GetError();
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    WriteColumn(Db(), dir, kId, duckdb::LogicalType::BIGINT, kRows, 4096,
                value_of);
    irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
    const auto* col = r.Column(kId);
    ASSERT_NE(col, nullptr);
    for (const duckdb::idx_t stored : {duckdb::idx_t{10}, duckdb::idx_t{100}}) {
      SCOPED_TRACE(stored);
      auto state = col->InitScan(r.Ctx());
      duckdb::Vector out{col->Type(), STANDARD_VECTOR_SIZE};
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      duckdb::idx_t hits = 0;
      for (; hits < stored; ++hits) {
        sel.set_index(hits, hits * 3);
      }
      for (duckdb::idx_t i = 0; i < 5; ++i) {
        sel.set_index(hits++, 3 * STANDARD_VECTOR_SIZE + i);
      }
      const uint64_t anchor = kRows - 3 * stored;
      col->GatherScatter(state, anchor, sel, hits, out, 0);
      for (duckdb::idx_t i = 0; i < stored; ++i) {
        const auto row = anchor + sel.get_index(i);
        ASSERT_TRUE(
          duckdb::Value::NotDistinctFrom(out.GetValue(i), value_of(row)))
          << "row " << row << " got " << out.GetValue(i).ToString();
      }
    }
  }
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, FrontCodedRunsOfEveryLengthScanRunByRun) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 6 * STANDARD_VECTOR_SIZE;
  constexpr std::string_view kBase =
    "the-quick-brown-fox-jumps-over-the-lazy-dog";
  const ValueOf value_of = [&](uint64_t g) {
    return duckdb::Value{
      absl::StrCat(kBase.substr(0, (g * 11) % 40), "#", g * 7919)};
  };
  std::vector<uint64_t> selected;
  for (uint64_t row = 3, k = 0; row < kRows; ++k) {
    const uint64_t run = 1 + (k * 7) % 70;
    for (uint64_t i = 0; i < run && row + i < kRows; ++i) {
      selected.push_back(row + i);
    }
    row += run + 1 + (k * 3) % 5;
  }
  duckdb::Connection con{Db()};
  for (const std::string codec : {"fsst", "fsst12", "fsst_plus", "onpair"}) {
    SCOPED_TRACE(codec);
    auto set = con.Query("SET force_compression = '" + codec + "'");
    ASSERT_FALSE(set->HasError()) << set->GetError();
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, 4096,
                value_of);
    irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
    const auto* col = r.Column(kId);
    ASSERT_NE(col, nullptr);
    auto state = col->InitScan(r.Ctx());
    uint64_t cursor = 0;
    size_t at = 0;
    for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      duckdb::idx_t hits = 0;
      for (;
           at < selected.size() && selected[at] < anchor + STANDARD_VECTOR_SIZE;
           ++at) {
        sel.set_index(hits++, selected[at] - anchor);
      }
      ASSERT_GT(hits, 64u);
      duckdb::Vector out{col->Type(), STANDARD_VECTOR_SIZE};
      ScanRuns(*col, state, cursor, anchor, sel, hits, out);
      for (duckdb::idx_t i = 0; i < hits; ++i) {
        const auto row = anchor + sel.get_index(i);
        ASSERT_TRUE(
          duckdb::Value::NotDistinctFrom(out.GetValue(i), value_of(row)))
          << "row " << row << " got " << out.GetValue(i).ToString();
      }
    }
  }
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, GatherFilterReleasesPassedBlocks) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 60000;
  const std::string bound = "https://example.com/n";
  const ValueOf value_of = [](uint64_t g) {
    return duckdb::Value{
      absl::StrCat("https://example.com/", RandomText(g % 5000, 40))};
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'dict_fsst'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, 4096,
              value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 4u);

  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::VARCHAR, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{bound}))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
    const auto span = std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
    duckdb::Vector scratch{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < span; ++i) {
      sel.set_index(i, i);
    }
    const auto approved =
      col->GatherFilter(state, anchor, span, sel, span, filter, *filter_state,
                        irs::NullCheckKind::None, scratch);
    std::vector<uint64_t> want;
    for (duckdb::idx_t i = 0; i < span; ++i) {
      if (value_of(anchor + i).GetValue<std::string>() >= bound) {
        want.push_back(i);
      }
    }
    ASSERT_EQ(approved, want.size()) << "span at " << anchor;
    for (duckdb::idx_t i = 0; i < approved; ++i) {
      ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
    }
    ASSERT_LE(state.segments.size(), 2u) << "span at " << anchor;
    ASSERT_LE(state.st.previous_states.size(), 1u) << "span at " << anchor;
  }
}

std::shared_ptr<const irs::ColumnPatch> MergePatch(
  const irs::ColumnPatch* base, const duckdb::LogicalType& type,
  const std::vector<uint64_t>& rows, const ValueOf& value_of) {
  duckdb::Vector values{type, rows.size()};
  for (size_t i = 0; i < rows.size(); ++i) {
    values.SetValue(i, value_of(rows[i]));
  }
  duckdb::FlatVector::SetSize(values, rows.size());
  return irs::ColumnPatch::Merge(base, type, rows, values);
}

void CheckPatchedFilter(duckdb::ClientContext& context,
                        const irs::ColumnReader& col, irs::ReadContext& ctx,
                        const ValueOf& value_of,
                        duckdb::unique_ptr<duckdb::Expression> predicate,
                        irs::NullCheckKind null_check,
                        const std::function<bool(const duckdb::Value&)>& keep) {
  const duckdb::ExpressionFilter filter{std::move(predicate)};
  auto filter_state = duckdb::TableFilterState::Initialize(context, filter);
  auto state = col.InitScan(ctx);
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  const auto rows = col.RowCount();
  for (uint64_t anchor = 0; anchor < rows; anchor += STANDARD_VECTOR_SIZE) {
    const auto span = std::min<uint64_t>(rows - anchor, STANDARD_VECTOR_SIZE);
    duckdb::Vector scratch{col.Type(), STANDARD_VECTOR_SIZE};
    duckdb::idx_t count = 0;
    for (duckdb::idx_t i = 0; i < span; ++i) {
      if (i % 3 != 1) {
        sel.set_index(count++, i);
      }
    }
    const auto approved =
      col.GatherFilter(state, anchor, span, sel, count, filter, *filter_state,
                       null_check, scratch);
    std::vector<uint64_t> want;
    for (duckdb::idx_t i = 0; i < span; ++i) {
      if (i % 3 != 1 && keep(value_of(anchor + i))) {
        want.push_back(i);
      }
    }
    ASSERT_EQ(approved, want.size()) << "span at " << anchor;
    for (duckdb::idx_t i = 0; i < approved; ++i) {
      ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
      ASSERT_TRUE(duckdb::Value::NotDistinctFrom(scratch.GetValue(want[i]),
                                                 value_of(anchor + want[i])))
        << "filtered row " << anchor + want[i];
    }
  }
}

TEST_F(ColumnReaderTest, PatchedColumnsReadTheirPatchedValues) {
  struct PatchCase {
    std::string name;
    duckdb::LogicalType type;
    ValueOf base;
    ValueOf first;
    ValueOf second;
    duckdb::Value bound;
    std::vector<std::string> codecs;
  };
  const std::vector<PatchCase> cases{
    {"bigint",
     duckdb::LogicalType::BIGINT,
     [](uint64_t g) {
       return g % 10 == 0
                ? duckdb::Value{duckdb::LogicalType::BIGINT}
                : duckdb::Value::BIGINT(static_cast<int64_t>(g % 1000));
     },
     [](uint64_t g) {
       return g % 5 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                         : duckdb::Value::BIGINT(int64_t{1} << 40 | int64_t(g));
     },
     [](uint64_t g) { return duckdb::Value::BIGINT(-static_cast<int64_t>(g)); },
     duckdb::Value::BIGINT(500),
     {"bitpacking", "rle", "uncompressed", "missing"}},
    {"varchar",
     duckdb::LogicalType::VARCHAR,
     [](uint64_t g) {
       static const char* const kWords[] = {"cat", "dog", "bird", "fish",
                                            "owl"};
       return g % 9 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                         : duckdb::Value{kWords[g % 5]};
     },
     [](uint64_t g) {
       return g % 4 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                         : duckdb::Value{"patched_" + RandomText(g, 20)};
     },
     [](uint64_t g) { return duckdb::Value{"again_" + std::to_string(g)}; },
     duckdb::Value{"dog"},
     {"dict_fsst", "zstd", "uncompressed", "missing"}},
  };
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 30000;
  duckdb::Connection con{Db()};
  for (const auto& c : cases) {
    for (const auto& codec : c.codecs) {
      SCOPED_TRACE(c.name + " " + codec);
      const bool missing = codec == "missing";
      if (!missing) {
        auto set = con.Query("SET force_compression = '" + codec + "'");
        ASSERT_FALSE(set->HasError()) << set->GetError();
      }
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      WriteColumn(Db(), dir, kId, c.type, kRows, 4096, c.base);
      con.Query("SET force_compression = 'auto'");
      irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
      const irs::NullColumnReader absent{Db(), kId, c.type, kRows};
      const auto* col = missing ? &absent : r.Column(kId);
      ASSERT_NE(col, nullptr);
      const ValueOf base =
        missing ? ValueOf{[&](uint64_t) { return duckdb::Value{c.type}; }}
                : c.base;

      std::vector<uint64_t> first_rows;
      std::vector<uint64_t> second_rows;
      for (uint64_t g = 0; g < kRows; ++g) {
        if (g % 13 == 0 || (g >= 5000 && g < 5100) || g == kRows - 1) {
          first_rows.push_back(g);
        }
        if (g % 26 == 0 || (g >= 9000 && g < 11100)) {
          second_rows.push_back(g);
        }
      }
      const auto in = [](const std::vector<uint64_t>& rows, uint64_t g) {
        return absl::c_binary_search(rows, g);
      };
      const ValueOf model = [&](uint64_t g) {
        if (in(second_rows, g)) {
          return c.second(g);
        }
        return in(first_rows, g) ? c.first(g) : base(g);
      };
      const auto first = MergePatch(nullptr, c.type, first_rows, c.first);
      const auto patch = MergePatch(first.get(), c.type, second_rows, c.second);
      std::vector<uint64_t> both;
      absl::c_set_union(first_rows, second_rows, std::back_inserter(both));
      ASSERT_EQ(first->Count(), first_rows.size());
      ASSERT_EQ(patch->Count(), both.size());

      const irs::PatchedColumnReader patched{*col, patch};
      ASSERT_EQ(patched.RowCount(), kRows);
      {
        auto state = patched.InitScan(r.Ctx());
        for (uint64_t pos = 0; pos < kRows;) {
          const auto take =
            std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
          duckdb::Vector result{c.type, STANDARD_VECTOR_SIZE};
          patched.Scan(state, result, take);
          for (duckdb::idx_t k = 0; k < take; ++k) {
            ASSERT_TRUE(duckdb::Value::NotDistinctFrom(result.GetValue(k),
                                                       model(pos + k)))
              << "scanned row " << pos + k;
          }
          pos += take;
        }
      }
      {
        auto state = patched.InitScan(r.Ctx());
        patched.Skip(state, 8990);
        duckdb::Vector result{c.type, STANDARD_VECTOR_SIZE};
        patched.ScanCount(state, result, 100, 7);
        for (duckdb::idx_t k = 0; k < 100; ++k) {
          ASSERT_TRUE(duckdb::Value::NotDistinctFrom(result.GetValue(7 + k),
                                                     model(8990 + k)))
            << "counted row " << 8990 + k;
        }
      }
      GatherStats stats;
      for (const auto mode : {GatherMode::Dense, GatherMode::Scatter,
                              GatherMode::ScatterAppend, GatherMode::Mixed}) {
        for (const uint32_t per_mille : {1u, 50u, 600u, 1000u}) {
          SCOPED_TRACE(testing::Message() << "mode " << static_cast<int>(mode)
                                          << " per_mille " << per_mille);
          CheckGathers(patched, r.Ctx(), model, mode, per_mille, stats);
          ASSERT_FALSE(HasFatalFailure());
        }
      }
      {
        std::vector<uint64_t> order(kRows);
        std::iota(order.begin(), order.end(), uint64_t{0});
        std::shuffle(order.begin(), order.end(), std::mt19937_64{7});
        irs::ColumnReader::PointReader reader{r, patched};
        duckdb::Vector out{c.type, 1};
        for (const auto g : order) {
          duckdb::FlatVector::ValidityMutable(out).Reset();
          const bool valid = reader.FetchRow(g, out, 0);
          const auto want = model(g);
          ASSERT_EQ(valid, !want.IsNull()) << "point row " << g;
          if (valid) {
            ASSERT_TRUE(duckdb::Value::NotDistinctFrom(out.GetValue(0), want))
              << "point row " << g;
          }
        }
      }
      const auto& merged = patched.MergedStatistics();
      ASSERT_TRUE(merged.CanHaveNull());
      for (uint64_t g = 0; g < kRows; g += 997) {
        const auto value = model(g);
        if (value.IsNull()) {
          continue;
        }
        const auto block = patched.Locate(g).block;
        const auto& rg = patched.DataBlocks()[block].statistics;
        const duckdb::ExpressionFilter eq{
          duckdb::BoundComparisonExpression::Create(
            duckdb::ExpressionType::COMPARE_EQUAL,
            duckdb::make_uniq<duckdb::BoundReferenceExpression>(c.type, 0),
            duckdb::make_uniq<duckdb::BoundConstantExpression>(value))};
        ASSERT_NE(eq.CheckStatistics(rg),
                  duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE)
          << "zonemap of row " << g;
        ASSERT_NE(eq.CheckStatistics(merged),
                  duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE)
          << "merged stats of row " << g;
      }
      CheckPatchedFilter(
        *con.context, patched, r.Ctx(), model,
        duckdb::BoundComparisonExpression::Create(
          duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
          duckdb::make_uniq<duckdb::BoundReferenceExpression>(c.type, 0),
          duckdb::make_uniq<duckdb::BoundConstantExpression>(c.bound)),
        irs::NullCheckKind::None,
        [&](const duckdb::Value& v) { return !v.IsNull() && v >= c.bound; });
      ASSERT_FALSE(HasFatalFailure());
      auto is_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
        duckdb::ExpressionType::OPERATOR_IS_NULL, duckdb::LogicalType::BOOLEAN);
      is_null->GetChildrenMutable().push_back(
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(c.type, 0));
      CheckPatchedFilter(*con.context, patched, r.Ctx(), model,
                         std::move(is_null), irs::NullCheckKind::IsNull,
                         [](const duckdb::Value& v) { return v.IsNull(); });
      ASSERT_FALSE(HasFatalFailure());
    }
  }
}

TEST_F(ColumnReaderTest, NestedGathersMatchScan) {
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);
  const auto ltype = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
  const auto atype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::INTEGER, 3);
  const std::vector<GatherCase> cases{
    {"struct",
     stype,
     20000,
     [stype](uint64_t g) {
       if (g % 13 == 0) {
         return duckdb::Value{stype};
       }
       return duckdb::Value::STRUCT(
         stype, {g % 10 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                             : duckdb::Value::BIGINT(static_cast<int64_t>(g)),
                 duckdb::Value{"value_" + std::to_string(g) + "_" +
                               RandomText(g, 8)}});
     },
     {"uncompressed", "dict_fsst"}},
    {"struct with a nullable field in every other vector",
     stype,
     20000,
     [stype](uint64_t g) {
       const bool null = (g / STANDARD_VECTOR_SIZE) % 2 == 0 && g % 500 == 3;
       return duckdb::Value::STRUCT(
         stype, {null ? duckdb::Value{duckdb::LogicalType::BIGINT}
                      : duckdb::Value::BIGINT(static_cast<int64_t>(g)),
                 duckdb::Value{"value_" + std::to_string(g)}});
     },
     {"uncompressed"}},
    {"list",
     ltype,
     20000,
     [](uint64_t g) {
       if (g % 17 == 0) {
         return duckdb::Value{
           duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT)};
       }
       duckdb::vector<duckdb::Value> items;
       for (uint64_t i = 0; i < g % 5; ++i) {
         items.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(g + i)));
       }
       return duckdb::Value::LIST(duckdb::LogicalType::BIGINT,
                                  std::move(items));
     },
     {"uncompressed"}},
    {"array",
     atype,
     20000,
     [atype](uint64_t g) {
       if (g % 19 == 0) {
         return duckdb::Value{atype};
       }
       return duckdb::Value::ARRAY(
         duckdb::LogicalType::INTEGER,
         {duckdb::Value::INTEGER(static_cast<int32_t>(g)),
          duckdb::Value::INTEGER(static_cast<int32_t>(g * 2)),
          g % 7 == 0 ? duckdb::Value{duckdb::LogicalType::INTEGER}
                     : duckdb::Value::INTEGER(static_cast<int32_t>(g * 3))});
     },
     {"uncompressed"}},
  };
  constexpr irs::field_id kId = 1;
  duckdb::Connection con{Db()};
  for (const auto& c : cases) {
    std::vector<std::string> codecs{"auto"};
    codecs.insert(codecs.end(), c.codecs.begin(), c.codecs.end());
    for (const auto& codec : codecs) {
      SCOPED_TRACE(c.name + " " + codec);
      auto set = con.Query("SET force_compression = '" + codec + "'");
      ASSERT_FALSE(set->HasError()) << set->GetError();
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      WriteColumn(Db(), dir, kId, c.type, c.rows, 4096, c.value_of);
      irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
      const auto* col = r.Column(kId);
      ASSERT_NE(col, nullptr);
      ASSERT_EQ(col->RowCount(), c.rows);
      GatherStats stats;
      for (const auto mode : {GatherMode::Dense, GatherMode::Scatter,
                              GatherMode::ScatterAppend, GatherMode::Mixed}) {
        for (const uint32_t per_mille : {1u, 10u, 50u, 200u, 1000u}) {
          SCOPED_TRACE(testing::Message() << "mode " << static_cast<int>(mode)
                                          << " per_mille " << per_mille);
          CheckGathers(*col, r.Ctx(), c.value_of, mode, per_mille, stats);
          if (HasFatalFailure()) {
            con.Query("SET force_compression = 'auto'");
            return;
          }
        }
      }
    }
  }
  con.Query("SET force_compression = 'auto'");
}

// Point fetches (FetchRow) of scattered single rows on BIGINT and VARCHAR
// (incl. overflow + NULL), across row-group boundaries.
TEST_F(ColumnReaderTest, PointFetchRow) {
  constexpr uint64_t kRows = 6000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kA = 1;  // BIGINT
  constexpr irs::field_id kS = 5;  // VARCHAR

  auto a_valid = [](uint64_t g) { return g % 10 != 0; };
  auto a_val = [](uint64_t g) { return static_cast<int64_t>(g); };
  auto s_valid = [](uint64_t g) { return g % 13 != 0; };
  auto s_val = [](uint64_t g) -> std::string {
    return g % 3 == 0 ? "long_string_value_" + std::to_string(g) + "_pad_pad_zz"
                      : "s" + std::to_string(g % 100);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             {.row_group_size = kRgSize});
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector va{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      duckdb::Vector vs{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(va);
      auto* ds = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vs);
      auto& maska = duckdb::FlatVector::ValidityMutable(va);
      auto& masks = duckdb::FlatVector::ValidityMutable(vs);
      maska.Reset(STANDARD_VECTOR_SIZE);
      masks.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (a_valid(g)) {
          da[k] = a_val(g);
        } else {
          maska.SetInvalid(k);
        }
        if (s_valid(g)) {
          ds[k] = duckdb::StringVector::AddString(vs, s_val(g));
        } else {
          masks.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(va, take);
      duckdb::FlatVector::SetSize(vs, take);
      cwA.Append(va, take);
      cwS.Append(vs, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ca = r.Column(kA);
  const auto* cs = r.Column(kS);
  ASSERT_NE(ca, nullptr);
  ASSERT_NE(cs, nullptr);

  const uint64_t probes[] = {0, 1, 30, 100, 4095, 4096, 4097, 5999};
  duckdb::Vector outa{duckdb::LogicalType::BIGINT, 1};
  duckdb::Vector outs{duckdb::LogicalType::VARCHAR, 1};
  irs::ColumnReader::PointReader pa{r, *ca};
  irs::ColumnReader::PointReader ps{r, *cs};
  for (const auto g : probes) {
    duckdb::FlatVector::ValidityMutable(outa).Reset();
    pa.FetchRow(g, outa, 0);
    if (!a_valid(g)) {
      EXPECT_FALSE(duckdb::FlatVector::Validity(outa).RowIsValid(0)) << g;
    } else {
      ASSERT_TRUE(duckdb::FlatVector::Validity(outa).RowIsValid(0)) << g;
      EXPECT_EQ(duckdb::FlatVector::GetData<int64_t>(outa)[0], a_val(g)) << g;
    }

    duckdb::FlatVector::ValidityMutable(outs).Reset();
    ps.FetchRow(g, outs, 0);
    if (!s_valid(g)) {
      EXPECT_FALSE(duckdb::FlatVector::Validity(outs).RowIsValid(0)) << g;
    } else {
      ASSERT_TRUE(duckdb::FlatVector::Validity(outs).RowIsValid(0)) << g;
      EXPECT_EQ(
        duckdb::FlatVector::GetData<duckdb::string_t>(outs)[0].GetString(),
        s_val(g))
        << g;
    }
  }
}

struct PointCodecCase {
  std::string name;
  duckdb::LogicalType type;
  irs::codecs::CodecType codec;
  std::function<bool(uint64_t)> valid;
  std::function<duckdb::Value(uint64_t)> value;
  std::optional<irs::codecs::CodecType> validity_codec = std::nullopt;
};

void CheckPointFetch(duckdb::DatabaseInstance& db, const PointCodecCase& c) {
  constexpr uint64_t kRows = 20000;
  constexpr uint32_t kRgSize = 8192;
  constexpr irs::field_id kField = 1;
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{db, dir.Tracker("seg")};
    auto& cw = w.OpenColumn(
      kField, c.type, {.row_group_size = kRgSize, .compression = c.codec});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{c.type, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        v.SetValue(k, c.valid(g) ? c.value(g) : duckdb::Value{c.type});
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr) << c.name;
  std::string codecs;
  for (const auto& block : col->DataBlocks()) {
    absl::StrAppend(&codecs, magic_enum::enum_name(block.codec->type), " ");
  }
  ASSERT_TRUE(std::ranges::any_of(col->DataBlocks(),
                                  [&](const auto& block) {
                                    return magic_enum::enum_name(
                                             block.codec->type) ==
                                           magic_enum::enum_name(c.codec);
                                  }))
    << c.name << ": " << codecs;
  if (c.validity_codec) {
    ASSERT_NE(col->Validity(), nullptr) << c.name;
    ASSERT_TRUE(
      std::ranges::any_of(col->Validity()->DataBlocks(),
                          [&](const auto& block) {
                            return magic_enum::enum_name(block.codec->type) ==
                                   magic_enum::enum_name(*c.validity_codec);
                          }))
      << c.name;
  }

  std::vector<uint64_t> order(kRows);
  std::iota(order.begin(), order.end(), uint64_t{0});
  std::shuffle(order.begin(), order.end(), std::mt19937_64{42});
  for (uint64_t g = kRows; g-- > 0;) {
    order.push_back(g);
  }
  for (uint64_t g = 0; g < kRows; ++g) {
    order.push_back(g);
  }
  irs::ColumnReader::PointReader reader{r, *col};
  duckdb::Vector out{c.type, 1};
  for (const auto g : order) {
    duckdb::FlatVector::ValidityMutable(out).Reset();
    const bool valid = reader.FetchRow(g, out, 0);
    ASSERT_EQ(valid, c.valid(g)) << c.name << " row " << g;
    if (valid) {
      const auto got = out.GetValue(0);
      const auto want = c.value(g);
      ASSERT_TRUE(duckdb::Value::NotDistinctFrom(got, want))
        << c.name << " row " << g << ": " << got.ToString()
        << " != " << want.ToString();
    }
  }
}

TEST_F(ColumnReaderTest, ReopenedBlockKeepsItsId) {
  constexpr irs::field_id kField = 1;
  constexpr uint32_t kRgSize = 2048;
  constexpr uint64_t kRows = 4 * kRgSize;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kField, duckdb::LogicalType::BIGINT, kRows, kRgSize,
              [](uint64_t g) {
                return duckdb::Value::BIGINT(static_cast<int64_t>(g * g));
              });
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  const auto blocks = col->DataBlocks().size();
  ASSERT_GT(blocks, 1);
  const auto last = [](const irs::codecs::ColumnSegment& segment) {
    irs::codecs::ColumnFetchState state;
    duckdb::Vector out{duckdb::LogicalType::BIGINT, 1};
    const duckdb::idx_t row = segment.count - 1;
    segment.function.fetch_rows(
      segment, state, &row, 1, out,
      *duckdb::FlatVector::IncrementalSelectionVector(), 0);
    return duckdb::FlatVector::GetData<int64_t>(out)[0];
  };
  irs::ReadContext ctx{r, /*random_access=*/true};
  std::set<duckdb::block_id_t> ids;
  for (int round = 0; round < 3; ++round) {
    for (size_t block = 0; block != blocks; ++block) {
      SCOPED_TRACE(absl::StrCat("round ", round, " block ", block));
      const auto segment = col->OpenSegment(block, ctx);
      ASSERT_TRUE(segment->block);
      ids.insert(segment->block->BlockId());
      const auto again = col->OpenSegment(block, ctx);
      EXPECT_EQ(again->block, segment->block);
      const auto g = col->DataBlockFirstRow(block) + segment->count - 1;
      const auto want = static_cast<int64_t>(g * g);
      EXPECT_EQ(last(*segment), want);
      EXPECT_EQ(last(*again), want);
    }
  }
  EXPECT_EQ(ids.size(), blocks);
}

TEST_F(ColumnReaderTest, PointFetchEveryCodec) {
  using duckdb::LogicalType;
  using duckdb::Value;
  using irs::codecs::CodecType;
  const auto all = [](uint64_t) { return true; };
  const auto sparse = [](uint64_t g) { return g % 997 != 0; };
  const auto periodic = [](uint64_t g) { return g % 17 != 0; };
  const auto dense = [](uint64_t g) { return (g * 2654435761U) % 3 != 0; };
  const auto runs = [](uint64_t g) { return (g / 300) % 5 != 0; };
  const auto short_runs = [](uint64_t g) { return (g / 50) % 4 != 0; };
  const auto rare = [](uint64_t g) { return g % 50 == 0; };
  const auto mix = [](uint64_t g) { return g * 0x9E3779B97F4A7C15ULL; };
  const std::vector<PointCodecCase> cases{
    {"bigint for wide", LogicalType::BIGINT, irs::codecs::CodecType::Bitpacking,
     periodic,
     [&](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>(mix(g) >> 4));
     }},
    {"integer for wide", LogicalType::INTEGER,
     irs::codecs::CodecType::Bitpacking, all,
     [&](uint64_t g) {
       return Value::INTEGER(static_cast<int32_t>(mix(g) >> 33));
     }},
    {"smallint for", LogicalType::SMALLINT, irs::codecs::CodecType::Bitpacking,
     sparse,
     [](uint64_t g) {
       return Value::SMALLINT(static_cast<int16_t>((g * 31) % 30000) - 15000);
     },
     irs::codecs::CodecType::Roaring},
    {"bigint short null runs", LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, short_runs,
     [](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(g * 3)); },
     irs::codecs::CodecType::Roaring},
    {"bigint mostly null", LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, rare,
     [](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(g % 777)); },
     irs::codecs::CodecType::Roaring},
    {"double alprd exceptions", LogicalType::DOUBLE,
     irs::codecs::CodecType::Alprd, all,
     [](uint64_t g) {
       return g % 97 == 0 ? Value::DOUBLE(-1e300 * static_cast<double>(g))
                          : Value::DOUBLE(std::sin(static_cast<double>(g)));
     }},
    {"bigint for", LogicalType::BIGINT, irs::codecs::CodecType::Bitpacking,
     periodic,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>((g * 7919) % 1000) - 500);
     }},
    {"bigint delta for", LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, all,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>(g * 3 + (g * 7919) % 5));
     }},
    {"bigint delta for nulls", LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, sparse,
     [](uint64_t g) {
       return Value::BIGINT(1'000'000'000'000 -
                            static_cast<int64_t>(g * 1000 + (g * 31) % 7));
     }},
    {"bigint constant delta", LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, all,
     [](uint64_t g) {
       return Value::BIGINT(1000 + static_cast<int64_t>(g) * 5);
     }},
    {"bigint constant groups", LogicalType::BIGINT,
     irs::codecs::CodecType::Bitpacking, runs,
     [](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(g / 5000)); }},
    {"bigint rle", LogicalType::BIGINT, irs::codecs::CodecType::Rle, dense,
     [](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>((g / 37) % 11));
     }},
    {"bigint rle wide", LogicalType::BIGINT, irs::codecs::CodecType::Rle, all,
     [&](uint64_t g) {
       return Value::BIGINT(static_cast<int64_t>(mix(g / 9)));
     }},
    {"bigint uncompressed", LogicalType::BIGINT,
     irs::codecs::CodecType::Uncompressed, periodic,
     [&](uint64_t g) { return Value::BIGINT(static_cast<int64_t>(mix(g))); }},
    {"integer for", LogicalType::INTEGER, irs::codecs::CodecType::Bitpacking,
     sparse,
     [](uint64_t g) {
       return Value::INTEGER(static_cast<int32_t>(g % 3000) - 1500);
     }},
    {"smallint rle", LogicalType::SMALLINT, irs::codecs::CodecType::Rle, all,
     [](uint64_t g) {
       return Value::SMALLINT(static_cast<int16_t>((g / 13) % 200));
     }},
    {"tinyint for", LogicalType::TINYINT, irs::codecs::CodecType::Bitpacking,
     runs,
     [](uint64_t g) { return Value::TINYINT(static_cast<int8_t>(g % 100)); }},
    {"hugeint for", LogicalType::HUGEINT, irs::codecs::CodecType::Bitpacking,
     periodic,
     [](uint64_t g) {
       return Value::HUGEINT(
         duckdb::hugeint_t{static_cast<int64_t>((g * 31) % 100000)});
     }},
    {"hugeint delta for", LogicalType::HUGEINT,
     irs::codecs::CodecType::Bitpacking, all,
     [](uint64_t g) {
       return Value::HUGEINT(
         duckdb::hugeint_t{static_cast<int64_t>(g * 7 + g % 3)});
     }},
    {"double alp", LogicalType::DOUBLE, irs::codecs::CodecType::Alp, periodic,
     [](uint64_t g) {
       return g % 101 == 0
                ? Value::DOUBLE(std::sqrt(static_cast<double>(g)) * 1.000001)
                : Value::DOUBLE(static_cast<double>(g % 10000) / 100.0);
     }},
    {"float alp", LogicalType::FLOAT, irs::codecs::CodecType::Alp, all,
     [](uint64_t g) {
       return Value::FLOAT(static_cast<float>(g % 1000) / 10.0F);
     }},
    {"double alprd", LogicalType::DOUBLE, irs::codecs::CodecType::Alprd, dense,
     [](uint64_t g) {
       return Value::DOUBLE(std::sin(static_cast<double>(g)) * 12345.678901234);
     }},
    {"float alprd", LogicalType::FLOAT, irs::codecs::CodecType::Alprd, sparse,
     [](uint64_t g) {
       return Value::FLOAT(std::sin(static_cast<float>(g)) * 123.456F);
     }},
    {"double rle", LogicalType::DOUBLE, irs::codecs::CodecType::Rle, runs,
     [](uint64_t g) {
       return Value::DOUBLE(static_cast<double>((g / 50) % 7) + 0.5);
     }},
    {"varchar dictionary", LogicalType::VARCHAR,
     irs::codecs::CodecType::DictFsst, periodic,
     [](uint64_t g) { return Value{"category_" + std::to_string(g % 50)}; }},
    {"varchar fsst", LogicalType::VARCHAR, irs::codecs::CodecType::Fsst, sparse,
     [](uint64_t g) {
       return Value{"https://example.org/items/" + std::to_string(g * 7919) +
                    "/detail"};
     }},
    {"varchar zstd", LogicalType::VARCHAR, irs::codecs::CodecType::Zstd, dense,
     [](uint64_t g) {
       return Value{std::string(200, static_cast<char>('a' + g % 26)) +
                    std::to_string(g)};
     }},
    {"varchar uncompressed", LogicalType::VARCHAR,
     irs::codecs::CodecType::Uncompressed, runs,
     [](uint64_t g) { return Value{"u" + std::to_string(g)}; }},
    {"boolean rle", LogicalType::BOOLEAN, irs::codecs::CodecType::Rle, periodic,
     [](uint64_t g) { return Value::BOOLEAN((g / 100) % 2 == 0); }},
    {"boolean bitpacking", LogicalType::BOOLEAN,
     irs::codecs::CodecType::Bitpacking, all,
     [](uint64_t g) { return Value::BOOLEAN(g % 3 == 0); }},
    {"boolean roaring", LogicalType::BOOLEAN, irs::codecs::CodecType::Roaring,
     dense, [](uint64_t g) { return Value::BOOLEAN(g % 97 == 0); }},
    {"boolean uncompressed", LogicalType::BOOLEAN,
     irs::codecs::CodecType::Uncompressed, sparse,
     [](uint64_t g) { return Value::BOOLEAN(g % 5 < 2); }},
  };
  for (const auto& c : cases) {
    CheckPointFetch(Db(), c);
  }
}

TEST_F(ColumnReaderTest, BlobPointReaderHoldsOnlyTheLastValue) {
#ifndef SERENEDB_HAVE_JEMALLOC
  GTEST_SKIP() << "needs jemalloc's per-thread counters";
#else
  constexpr uint64_t kRows = 20000;
  constexpr irs::field_id kS = 1;
  const auto value = [](uint64_t g) {
    return absl::StrCat("value-", g, "-", std::string(300, 'x'));
  };
  const auto live_bytes = [] {
    uint64_t epoch = 1;
    size_t epoch_size = sizeof(epoch);
    mallctl("epoch", &epoch, &epoch_size, &epoch, sizeof(epoch));
    uint64_t allocated = 0;
    uint64_t deallocated = 0;
    size_t size = sizeof(uint64_t);
    mallctl("thread.allocated", &allocated, &size, nullptr, 0);
    mallctl("thread.deallocated", &deallocated, &size, nullptr, 0);
    return static_cast<int64_t>(allocated - deallocated);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw =
      w.OpenColumn(kS, duckdb::LogicalType::VARCHAR, {.row_group_size = 4096});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vs{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* ds = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vs);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ds[k] = duckdb::StringVector::AddString(vs, value(pos + k));
      }
      duckdb::FlatVector::SetSize(vs, take);
      cw.Append(vs, take);
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  irs::ColumnReader::BlobPointReader reader{r, *r.Column(kS)};
  const auto walk = [&] {
    for (uint64_t g = 0; g != kRows; ++g) {
      const auto got = reader.FetchRow(g);
      ASSERT_EQ(value(g),
                std::string_view(reinterpret_cast<const char*>(got.data()),
                                 got.size()));
    }
  };
  ASSERT_NO_FATAL_FAILURE(walk());
  const auto before = live_bytes();
  ASSERT_NO_FATAL_FAILURE(walk());
  EXPECT_LT(live_bytes() - before, static_cast<int64_t>(kRows * 30));
#endif
}

struct PrefetchRange {
  uint64_t offset = 0;
  uint64_t size = 0;
};

class ScriptedMapping final : public duckdb::MemoryMappedFile {
 public:
  explicit ScriptedMapping(irs::bstring bytes)
    : MemoryMappedFile{{},
                       duckdb::FileOpenFlags::FILE_FLAGS_READ,
                       Reserve(bytes.size()),
                       static_cast<duckdb::idx_t>(bytes.size())},
      _bytes{std::move(bytes)} {
    KeepUntil(size);
  }
  ~ScriptedMapping() final { ::munmap(data, Reserved(size)); }

  void Sync() final {}
  bool Trim(duckdb::idx_t /*offset*/, duckdb::idx_t /*length*/) final {
    return false;
  }
  void Close() final {}
  void Advise(duckdb::MappedFileAccess /*access*/) final {}
  void Prefetch(duckdb::idx_t location, duckdb::idx_t nr_bytes) final {
    prefetched.emplace_back(location, nr_bytes);
    Load(location / Page(), Pages(location + nr_bytes));
  }
  void Evict(duckdb::idx_t /*location*/, duckdb::idx_t /*nr_bytes*/) final {}

  void Drop(uint64_t offset, uint64_t length) {
    const auto first = offset / Page();
    const auto end = Pages(std::min<uint64_t>(offset + length, size));
    ASSERT_EQ(
      ::madvise(data + first * Page(), (end - first) * Page(), MADV_DONTNEED),
      0);
  }

  void KeepUntil(uint64_t end) {
    const auto all = Pages(size);
    const auto kept = Pages(std::min<uint64_t>(end, size));
    Load(0, kept);
    if (kept != all) {
      ASSERT_EQ(
        ::madvise(data + kept * Page(), (all - kept) * Page(), MADV_DONTNEED),
        0);
    }
  }

  std::vector<PrefetchRange> prefetched;

 private:
  static uint64_t Page() noexcept {
    return static_cast<uint64_t>(::sysconf(_SC_PAGESIZE));
  }
  static uint64_t Pages(uint64_t bytes) noexcept {
    return (bytes + Page() - 1) / Page();
  }
  static uint64_t Reserved(uint64_t bytes) noexcept {
    return std::max<uint64_t>(Pages(bytes), 1) * Page();
  }
  static duckdb::data_ptr_t Reserve(uint64_t bytes) {
    auto* const mapped =
      ::mmap(nullptr, Reserved(bytes), PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    SDB_ENSURE(mapped != MAP_FAILED, "ScriptedMapping: mmap failed");
    ::madvise(mapped, Reserved(bytes), MADV_NOHUGEPAGE);
    return static_cast<duckdb::data_ptr_t>(mapped);
  }
  void Load(uint64_t first, uint64_t end) noexcept {
    const auto from = first * Page();
    const auto to = std::min<uint64_t>(end * Page(), size);
    if (from < to) {
      std::memcpy(data + from, _bytes.data() + from, to - from);
    }
  }

  irs::bstring _bytes;
};

class ScriptedFileSystem final : public tests::HookFileSystem {
 public:
  using HookFileSystem::HookFileSystem;

  duckdb::shared_ptr<duckdb::InputFile> OpenBlockFile(
    const std::string& /*path*/) final {
    return nullptr;
  }
  duckdb::unique_ptr<duckdb::MemoryMappedFile> MemoryMapFile(
    const duckdb::OpenFileInfo& path, duckdb::FileOpenFlags /*flags*/,
    const duckdb::MMapOptions& /*options*/,
    duckdb::optional_ptr<duckdb::FileOpener> /*opener*/) final {
    auto mapping = duckdb::make_uniq<ScriptedMapping>(
      irs::ReadWhole<irs::bstring>(duckdb::QueryContext{}, *this, path.path));
    mapped = mapping.get();
    return mapping;
  }

  ScriptedMapping* mapped = nullptr;
};

TEST_F(ColumnReaderTest, PointReadsPrefetchTheirBlockUnlessRandomAccess) {
  constexpr uint64_t kRows = 20000;
  constexpr irs::field_id kS = 1;
  tests::IndexDir mem{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), mem.Tracker("seg")};
    auto& cw =
      w.OpenColumn(kS, duckdb::LogicalType::VARCHAR, {.row_group_size = 4096});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector vs{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* ds = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vs);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        ds[k] = duckdb::StringVector::AddString(
          vs, absl::StrCat("value-", pos + k, "-", std::string(300, 'x')));
      }
      duckdb::FlatVector::SetSize(vs, take);
      cw.Append(vs, take);
    }
    w.Commit(0);
  }

  ScriptedFileSystem fs{mem.Fs()};
  irs::ColReader r{Db(),
                   fs,
                   mem.Root(),
                   "seg",
                   mem.Sizes("seg", irs::kColFile),
                   duckdb::MappedFileAccess::SEQUENTIAL};
  ASSERT_NE(fs.mapped, nullptr);
  auto& prefetched = fs.mapped->prefetched;
  const auto& col = *r.Column(kS);
  const auto& block = col.DataBlocks().back();
  const auto covers_block = [&](const PrefetchRange& p) {
    return p.offset <= block.file_offset &&
           block.file_offset + block.byte_size <= p.offset + p.size;
  };
  duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};

  prefetched.clear();
  {
    irs::ColumnReader::PointReader reader{r, col, true};
    ASSERT_TRUE(reader.FetchRow(kRows - 1, out, 0));
  }
  EXPECT_TRUE(absl::c_none_of(prefetched, covers_block));

  prefetched.clear();
  ASSERT_NO_FATAL_FAILURE(fs.mapped->KeepUntil(0));
  irs::ColumnReader::PointReader reader{r, col};
  ASSERT_TRUE(reader.FetchRow(kRows - 1, out, 0));
  EXPECT_TRUE(absl::c_any_of(prefetched, covers_block));
}

TEST_F(ColumnReaderTest, ReadsPrefetchTheSharedBlockOfTheirRowGroup) {
  constexpr uint32_t kRgSize = 4 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kS = 1;
  tests::IndexDir mem{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), mem.Tracker("seg"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                            {.row_group_size = kRgSize,
                             .compression = irs::codecs::CodecType::Fsst12,
                             .codec_params = {.segment_target = 32 * 1024}});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector vs{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* ds = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vs);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ds[k] = duckdb::StringVector::AddString(
          vs, absl::StrCat("https://example.org/", (pos + k) % 977, "/item/",
                           ((pos + k) * 7919) % 100003, "?ref=", pos + k));
      }
      duckdb::FlatVector::SetSize(vs, STANDARD_VECTOR_SIZE);
      cw.Append(vs, STANDARD_VECTOR_SIZE);
    }
    w.Commit(0);
  }

  ScriptedFileSystem fs{mem.Fs()};
  irs::ColReader r{Db(),
                   fs,
                   mem.Root(),
                   "seg",
                   mem.Sizes("seg", irs::kColFile),
                   duckdb::MappedFileAccess::SEQUENTIAL};
  ASSERT_NE(fs.mapped, nullptr);
  auto& prefetched = fs.mapped->prefetched;
  const auto& col = *r.Column(kS);
  const auto blocks = col.DataBlocks();
  ASSERT_GT(blocks.size(), 6);
  ASSERT_TRUE(absl::c_all_of(
    blocks, [](const auto& block) { return block.shared_size != 0; }));
  ASSERT_NE(blocks.front().shared_offset, blocks.back().shared_offset);
  ASSERT_EQ(blocks[blocks.size() - 2].shared_offset,
            blocks.back().shared_offset);
  const auto prefetches = [&](const irs::ColumnBlockMeta& block) {
    return absl::c_count_if(prefetched, [&](const PrefetchRange& p) {
      return p.offset <= block.shared_offset &&
             block.shared_offset + block.shared_size <= p.offset + p.size;
    });
  };

  ASSERT_NO_FATAL_FAILURE(fs.mapped->KeepUntil(0));
  {
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
    irs::ColumnReader::PointReader reader{r, col};
    ASSERT_TRUE(reader.FetchRow(kRows - 1, out, 0));
  }
  EXPECT_EQ(prefetches(blocks.back()), 1);
  EXPECT_EQ(prefetches(blocks.front()), 0);

  prefetched.clear();
  ASSERT_NO_FATAL_FAILURE(fs.mapped->KeepUntil(0));
  auto state = col.InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector result{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    col.Scan(state, result, STANDARD_VECTOR_SIZE);
  }
  for (const auto& block : blocks) {
    EXPECT_NE(prefetches(block), 0);
  }
  EXPECT_EQ(prefetches(blocks.back()), 1);
}

TEST_F(ColumnReaderTest, ScansTrustTheWarmMapBetweenTwoResidentEnds) {
  constexpr uint32_t kRgSize = STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 640 * kRgSize;
  constexpr uint64_t kRun = 256 * duckdb::PageResidency::PAGE_SIZE;
  constexpr uint64_t kEverything = std::numeric_limits<uint64_t>::max();
  constexpr irs::field_id kPlain = 1;
  constexpr irs::field_id kRuns = 2;
  const auto plain = [](uint64_t row) { return static_cast<int64_t>(row * 7); };
  const auto runs = [](uint64_t row) {
    const auto group = row / kRgSize;
    return static_cast<int64_t>(group % 2 == 0 ? row * 3 : group);
  };
  tests::IndexDir mem{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), mem.Tracker("seg"), irs::WriteTier::Merge};
    auto& plain_writer =
      w.OpenColumn(kPlain, duckdb::LogicalType::BIGINT,
                   {.row_group_size = kRgSize,
                    .compression = irs::codecs::CodecType::Uncompressed});
    auto& runs_writer = w.OpenColumn(kRuns, duckdb::LogicalType::BIGINT,
                                     {.row_group_size = kRgSize});
    for (uint64_t pos = 0; pos < kRows; pos += kRgSize) {
      duckdb::Vector plain_values{duckdb::LogicalType::BIGINT, kRgSize};
      duckdb::Vector runs_values{duckdb::LogicalType::BIGINT, kRgSize};
      auto* plain_data =
        duckdb::FlatVector::GetDataMutable<int64_t>(plain_values);
      auto* runs_data =
        duckdb::FlatVector::GetDataMutable<int64_t>(runs_values);
      for (duckdb::idx_t k = 0; k < kRgSize; ++k) {
        plain_data[k] = plain(pos + k);
        runs_data[k] = runs(pos + k);
      }
      duckdb::FlatVector::SetSize(plain_values, kRgSize);
      duckdb::FlatVector::SetSize(runs_values, kRgSize);
      plain_writer.Append(plain_values, kRgSize);
      runs_writer.Append(runs_values, kRgSize);
    }
    ASSERT_TRUE(w.Commit(0));
  }

  ScriptedFileSystem fs{mem.Fs()};
  irs::ColReader r{Db(),
                   fs,
                   mem.Root(),
                   "seg",
                   mem.Sizes("seg", irs::kColFile),
                   duckdb::MappedFileAccess::SEQUENTIAL};
  ASSERT_NE(fs.mapped, nullptr);
  auto& mapping = *fs.mapped;
  ASSERT_TRUE(absl::c_any_of(r.Column(kRuns)->DataBlocks(),
                             [](const auto& b) { return b.byte_size == 0; }));
  const auto check = [&](irs::field_id field,
                         const std::function<int64_t(uint64_t)>& value_of) {
    const auto& col = *r.Column(field);
    const auto groups = col.DataBlocks();
    std::vector<PrefetchRange> blocks;
    for (const auto& b : groups) {
      if (b.byte_size != 0) {
        blocks.emplace_back(b.file_offset, b.byte_size);
      }
    }
    ASSERT_GE(blocks.size(), 8);
    std::vector<bool> asked;
    const auto scan = [&] {
      mapping.prefetched.clear();
      asked.clear();
      auto state = col.InitScan(r.Ctx());
      size_t advised_end = 0;
      for (uint64_t pos = 0; pos < kRows; pos += kRgSize) {
        duckdb::Vector result{duckdb::LogicalType::BIGINT, kRgSize};
        col.Scan(state, result, kRgSize);
        result.Flatten(kRgSize);
        const auto* values = duckdb::FlatVector::GetData<int64_t>(result);
        for (duckdb::idx_t k = 0; k < kRgSize; ++k) {
          ASSERT_EQ(values[k], value_of(pos + k)) << "row " << pos + k;
        }
        const auto group = pos / kRgSize;
        if (groups[group].byte_size != 0) {
          asked.push_back(group >= advised_end);
        }
        advised_end = state.advised_end;
      }
    };
    const auto resident = [&](const PrefetchRange& b) {
      return duckdb::PageResidency::IsResident(r.File()->Data() + b.offset,
                                               b.size);
    };
    const auto prefetched = [&](const PrefetchRange& b) {
      return absl::c_any_of(mapping.prefetched, [&](const PrefetchRange& p) {
        return p.offset <= b.offset && b.offset + b.size <= p.offset + p.size;
      });
    };

    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(kEverything));
    duckdb::PageResidency::Invalidate();
    auto epoch = duckdb::PageResidency::Epoch();
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_EQ(duckdb::PageResidency::Epoch(), epoch);
    EXPECT_TRUE(absl::c_none_of(blocks, prefetched));
    EXPECT_EQ(static_cast<size_t>(absl::c_count(asked, true)), blocks.size());

    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_EQ(duckdb::PageResidency::Epoch(), epoch);
    EXPECT_TRUE(absl::c_none_of(blocks, prefetched));
    ASSERT_EQ(asked.size(), blocks.size());
    EXPECT_LT(static_cast<size_t>(absl::c_count(asked, true)),
              blocks.size() / 2);
    EXPECT_TRUE(asked.front());
    uint64_t run = 0;
    for (size_t b = 0; b != blocks.size(); ++b) {
      run = asked[b] ? blocks[b].size : run + blocks[b].size;
      EXPECT_LE(run, kRun);
    }
    size_t tail = 0;
    for (size_t b = 1; b != blocks.size() && !asked[b]; ++b) {
      tail = b;
    }
    ASSERT_GT(tail, 0);

    const auto half = blocks.size() / 2 + 1;
    ASSERT_NO_FATAL_FAILURE(
      mapping.KeepUntil(blocks[half - 1].offset + blocks[half - 1].size));
    ASSERT_TRUE(resident(blocks[half - 1]));
    ASSERT_FALSE(resident(blocks[half]));
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_NE(duckdb::PageResidency::Epoch(), epoch);
    for (size_t b = 0; b < blocks.size(); ++b) {
      EXPECT_EQ(prefetched(blocks[b]), b >= half) << "block " << b;
    }

    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(kEverything));
    ASSERT_NO_FATAL_FAILURE(scan());
    epoch = duckdb::PageResidency::Epoch();
    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(0));
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_NE(duckdb::PageResidency::Epoch(), epoch);
    EXPECT_TRUE(absl::c_all_of(blocks, prefetched));

    epoch = duckdb::PageResidency::Epoch();
    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(0));
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_EQ(duckdb::PageResidency::Epoch(), epoch);
    EXPECT_TRUE(absl::c_all_of(blocks, prefetched));

    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(kEverything));
    ASSERT_NO_FATAL_FAILURE(scan());
    epoch = duckdb::PageResidency::Epoch();
    ASSERT_NO_FATAL_FAILURE(mapping.Drop(blocks.front().offset, 1));
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_NE(duckdb::PageResidency::Epoch(), epoch);
    EXPECT_TRUE(prefetched(blocks.front()));

    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(kEverything));
    ASSERT_NO_FATAL_FAILURE(scan());
    epoch = duckdb::PageResidency::Epoch();
    ASSERT_NO_FATAL_FAILURE(
      mapping.Drop(blocks[tail].offset + blocks[tail].size - 1, 1));
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_NE(duckdb::PageResidency::Epoch(), epoch);
    EXPECT_TRUE(prefetched(blocks[tail]));

    ASSERT_NO_FATAL_FAILURE(mapping.KeepUntil(kEverything));
    ASSERT_NO_FATAL_FAILURE(scan());
    ASSERT_NO_FATAL_FAILURE(
      mapping.KeepUntil(blocks[half - 1].offset + blocks[half - 1].size));
    ASSERT_NO_FATAL_FAILURE(scan());
    ASSERT_TRUE(resident(blocks[half + 1]));
    ASSERT_NO_FATAL_FAILURE(
      mapping.Drop(blocks[half + 1].offset + blocks[half + 1].size / 2, 1));
    ASSERT_NO_FATAL_FAILURE(scan());
    EXPECT_TRUE(prefetched(blocks[half + 1]));
  };
  ASSERT_NO_FATAL_FAILURE(check(kPlain, plain));
  ASSERT_NO_FATAL_FAILURE(check(kRuns, runs));
}

TEST_F(ColumnReaderTest, ScansAskAboutABlockLargerThanARunInFull) {
  constexpr uint32_t kRgSize = 128 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr uint64_t kRun = 256 * duckdb::PageResidency::PAGE_SIZE;
  constexpr irs::field_id kWide = 1;
  const auto value = [](uint64_t row) {
    return static_cast<int64_t>(absl::HashOf(row));
  };
  tests::IndexDir mem{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), mem.Tracker("seg"), irs::WriteTier::Merge};
    auto& writer =
      w.OpenColumn(kWide, duckdb::LogicalType::BIGINT,
                   {.row_group_size = kRgSize,
                    .compression = irs::codecs::CodecType::Bitpacking});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector values{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(values);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        data[k] = value(pos + k);
      }
      duckdb::FlatVector::SetSize(values, STANDARD_VECTOR_SIZE);
      writer.Append(values, STANDARD_VECTOR_SIZE);
    }
    ASSERT_TRUE(w.Commit(0));
  }

  ScriptedFileSystem fs{mem.Fs()};
  irs::ColReader r{Db(),
                   fs,
                   mem.Root(),
                   "seg",
                   mem.Sizes("seg", irs::kColFile),
                   duckdb::MappedFileAccess::SEQUENTIAL};
  ASSERT_NE(fs.mapped, nullptr);
  auto& mapping = *fs.mapped;
  const auto& col = *r.Column(kWide);
  const auto blocks = col.DataBlocks();
  ASSERT_EQ(blocks.size(), 3);
  ASSERT_TRUE(absl::c_all_of(
    blocks, [&](const auto& block) { return block.byte_size > kRun; }));
  const auto scan = [&] {
    mapping.prefetched.clear();
    auto state = col.InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector result{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      col.Scan(state, result, STANDARD_VECTOR_SIZE);
      result.Flatten(STANDARD_VECTOR_SIZE);
      const auto* values = duckdb::FlatVector::GetData<int64_t>(result);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ASSERT_EQ(values[k], value(pos + k)) << "row " << pos + k;
      }
    }
  };
  const auto prefetched = [&](const irs::ColumnBlockMeta& block) {
    return absl::c_any_of(mapping.prefetched, [&](const PrefetchRange& p) {
      return p.offset <= block.file_offset &&
             block.file_offset + block.byte_size <= p.offset + p.size;
    });
  };

  ASSERT_NO_FATAL_FAILURE(scan());
  ASSERT_NO_FATAL_FAILURE(scan());
  EXPECT_TRUE(mapping.prefetched.empty());

  ASSERT_NO_FATAL_FAILURE(
    mapping.Drop(blocks[1].file_offset + blocks[1].byte_size / 2, 1));
  ASSERT_NO_FATAL_FAILURE(scan());
  EXPECT_TRUE(prefetched(blocks[1]));
  EXPECT_FALSE(prefetched(blocks[0]));
}

TEST_F(ColumnReaderTest, PointReaderReopensBlocksItClosed) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kGroups = 20;
  constexpr uint64_t kRows = kGroups * kRgSize;
  constexpr size_t kValueSize = 1024;
  constexpr irs::field_id kS = 1;
  const auto value = [](uint64_t row) {
    std::string v(kValueSize, ' ');
    std::mt19937_64 random{row};
    for (auto& c : v) {
      c = static_cast<char>('a' + random() % 26);
    }
    return v;
  };
  tests::IndexDir mem{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), mem.Tracker("seg")};
    auto& cw =
      w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                   {.row_group_size = kRgSize,
                    .compression = irs::codecs::CodecType::Uncompressed});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector vs{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* ds = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vs);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        ds[k] = duckdb::StringVector::AddString(vs, value(pos + k));
      }
      duckdb::FlatVector::SetSize(vs, STANDARD_VECTOR_SIZE);
      cw.Append(vs, STANDARD_VECTOR_SIZE);
    }
    w.Commit(0);
  }
  irs::ColReader r = irs::tests::OpenColReader(mem, "seg");
  const auto& col = *r.Column(kS);
  std::vector<uint64_t> first_rows;
  uint64_t stored = 0;
  uint64_t rows = 0;
  for (const auto& block : col.DataBlocks()) {
    ASSERT_GT(block.tuple_count, 1);
    first_rows.push_back(rows);
    rows += block.tuple_count;
    stored += block.byte_size;
  }
  ASSERT_EQ(rows, kRows);
  ASSERT_GT(stored, uint64_t{64} << 20);
  irs::ColumnReader::PointReader reader{r, col};
  duckdb::Vector out{duckdb::LogicalType::VARCHAR, 1};
  for (const uint64_t pass : {uint64_t{0}, uint64_t{1}}) {
    for (const auto first : first_rows) {
      const auto row = first + pass;
      ASSERT_TRUE(reader.FetchRow(row, out, 0));
      ASSERT_EQ(
        duckdb::FlatVector::GetData<duckdb::string_t>(out)[0].GetString(),
        value(row))
        << "row " << row;
    }
  }
}

TEST_F(ColumnReaderTest, SameColumnsGiveTheSameFileSize) {
  constexpr irs::field_id kS = 1;
  constexpr uint64_t kRows = 1000;
  tests::IndexDir mem{Db(), TestEnv::MemoryRoot()};
  std::vector<uint64_t> lengths;
  for (int file = 0; file != 16; ++file) {
    const auto name = absl::StrCat("seg", file);
    {
      irs::ColWriter w{Db(), mem.Tracker(name)};
      auto& cw =
        w.OpenColumn(kS, duckdb::LogicalType::BIGINT,
                     {.row_group_size = 2048,
                      .compression = irs::codecs::CodecType::Uncompressed});
      duckdb::Vector values{duckdb::LogicalType::BIGINT, kRows};
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(values);
      for (uint64_t row = 0; row != kRows; ++row) {
        data[row] = static_cast<int64_t>(row * 7);
      }
      duckdb::FlatVector::SetSize(values, kRows);
      cw.Append(values, kRows);
      ASSERT_TRUE(w.Commit(0));
    }
    lengths.push_back(
      mem.Open(irs::FileName(name), duckdb::MappedFileAccess::NORMAL)
        ->GetSize());
  }
  EXPECT_EQ(lengths.size(), absl::c_count(lengths, lengths.front()));
}

TEST_F(ColumnReaderTest, SegmentRoundTripStruct) {
  constexpr uint64_t kRows = 6000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kT = 9;
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);

  auto s_valid = [](uint64_t g) { return g % 11 != 0; };
  auto a_valid = [](uint64_t g) { return g % 10 != 0; };
  auto a_val = [](uint64_t g) { return static_cast<int64_t>(g); };
  auto b_valid = [](uint64_t g) { return g % 7 != 0; };
  auto b_val = [](uint64_t g) { return "v" + std::to_string(g); };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwT = w.OpenColumn(kT, stype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{stype, STANDARD_VECTOR_SIZE};
      auto& entries = duckdb::StructVector::GetEntries(v);
      auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
      auto* db =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(entries[1]);
      auto& va = duckdb::FlatVector::ValidityMutable(entries[0]);
      auto& vb = duckdb::FlatVector::ValidityMutable(entries[1]);
      auto& vs = duckdb::FlatVector::ValidityMutable(v);
      va.Reset(STANDARD_VECTOR_SIZE);
      vb.Reset(STANDARD_VECTOR_SIZE);
      vs.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (!s_valid(g)) {
          vs.SetInvalid(k);
        }
        if (a_valid(g)) {
          da[k] = a_val(g);
        } else {
          va.SetInvalid(k);
        }
        if (b_valid(g)) {
          db[k] = duckdb::StringVector::AddString(entries[1], b_val(g));
        } else {
          vb.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwT.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ct = r.Column(kT);
  ASSERT_NE(ct, nullptr);
  ASSERT_EQ(ct->RowCount(), kRows);

  auto state = ct->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{stype, STANDARD_VECTOR_SIZE};
    ct->Scan(state, result, take);
    result.Flatten(take);
    auto& entries = duckdb::StructVector::GetEntries(result);
    const auto* da = duckdb::FlatVector::GetData<int64_t>(entries[0]);
    const auto* db = duckdb::FlatVector::GetData<duckdb::string_t>(entries[1]);
    const auto& sv = duckdb::FlatVector::Validity(result);
    const auto& va = duckdb::FlatVector::Validity(entries[0]);
    const auto& vb = duckdb::FlatVector::Validity(entries[1]);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      EXPECT_EQ(sv.RowIsValid(k), s_valid(g)) << "struct row " << g;
      if (a_valid(g)) {
        ASSERT_TRUE(va.RowIsValid(k)) << "a row " << g;
        EXPECT_EQ(da[k], a_val(g)) << "a row " << g;
      } else {
        EXPECT_FALSE(va.RowIsValid(k)) << "a row " << g;
      }
      if (b_valid(g)) {
        ASSERT_TRUE(vb.RowIsValid(k)) << "b row " << g;
        EXPECT_EQ(db[k].GetString(), b_val(g)) << "b row " << g;
      } else {
        EXPECT_FALSE(vb.RowIsValid(k)) << "b row " << g;
      }
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, SegmentRoundTripTuple) {
  constexpr uint64_t kRows = 6000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kT = 9;
  const auto ttype =
    duckdb::LogicalType::TUPLE(duckdb::vector<duckdb::LogicalType>{
      duckdb::LogicalType::BIGINT, duckdb::LogicalType::DOUBLE});
  ASSERT_EQ(ttype.id(), duckdb::LogicalTypeId::TUPLE);

  auto t_valid = [](uint64_t g) { return g % 11 != 0; };
  auto x_valid = [](uint64_t g) { return g % 10 != 0; };
  auto x_val = [](uint64_t g) { return static_cast<int64_t>(g); };
  auto y_val = [](uint64_t g) { return static_cast<double>(g) / 4; };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwT = w.OpenColumn(kT, ttype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{ttype, STANDARD_VECTOR_SIZE};
      auto& entries = duckdb::StructVector::GetEntries(v);
      auto* dx = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
      auto* dy = duckdb::FlatVector::GetDataMutable<double>(entries[1]);
      auto& vx = duckdb::FlatVector::ValidityMutable(entries[0]);
      auto& vt = duckdb::FlatVector::ValidityMutable(v);
      vx.Reset(STANDARD_VECTOR_SIZE);
      vt.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (!t_valid(g)) {
          vt.SetInvalid(k);
        }
        if (x_valid(g)) {
          dx[k] = x_val(g);
        } else {
          vx.SetInvalid(k);
        }
        dy[k] = y_val(g);
      }
      duckdb::FlatVector::SetSize(v, take);
      cwT.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ct = r.Column(kT);
  ASSERT_NE(ct, nullptr);
  ASSERT_EQ(ct->RowCount(), kRows);
  EXPECT_EQ(ct->StructFieldCount(), 2);

  auto state = ct->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{ttype, STANDARD_VECTOR_SIZE};
    ct->Scan(state, result, take);
    result.Flatten(take);
    auto& entries = duckdb::StructVector::GetEntries(result);
    const auto* dx = duckdb::FlatVector::GetData<int64_t>(entries[0]);
    const auto* dy = duckdb::FlatVector::GetData<double>(entries[1]);
    const auto& tv = duckdb::FlatVector::Validity(result);
    const auto& vx = duckdb::FlatVector::Validity(entries[0]);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      EXPECT_EQ(tv.RowIsValid(k), t_valid(g)) << "tuple row " << g;
      if (x_valid(g)) {
        ASSERT_TRUE(vx.RowIsValid(k)) << "x row " << g;
        EXPECT_EQ(dx[k], x_val(g)) << "x row " << g;
      } else {
        EXPECT_FALSE(vx.RowIsValid(k)) << "x row " << g;
      }
      EXPECT_EQ(dy[k], y_val(g)) << "y row " << g;
    }
    pos += take;
  }
}

// Gather (sparse, multi-run, multi-row-group) a top-level STRUCT column.
// Regression: a top-level STRUCT/ARRAY reader carries no data segments, so its
// row cursor lives entirely in the child states. GatherCursor must reflect that
// real position; if it were stuck at 0 the 2nd+ run would re-Skip from the
// origin and desync the children -> wrong rows.
TEST_F(ColumnReaderTest, GatherStructAcrossRowGroups) {
  constexpr uint64_t kRows = 10000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kT = 9;
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);

  auto s_valid = [](uint64_t g) { return g % 11 != 0; };
  auto a_valid = [](uint64_t g) { return g % 10 != 0; };
  auto a_val = [](uint64_t g) { return static_cast<int64_t>(g); };
  auto b_val = [](uint64_t g) { return "v" + std::to_string(g); };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwT = w.OpenColumn(kT, stype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{stype, STANDARD_VECTOR_SIZE};
      auto& entries = duckdb::StructVector::GetEntries(v);
      auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
      auto* db =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(entries[1]);
      auto& va = duckdb::FlatVector::ValidityMutable(entries[0]);
      auto& vs = duckdb::FlatVector::ValidityMutable(v);
      va.Reset(STANDARD_VECTOR_SIZE);
      vs.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (!s_valid(g)) {
          vs.SetInvalid(k);
        }
        if (a_valid(g)) {
          da[k] = a_val(g);
        } else {
          va.SetInvalid(k);
        }
        db[k] = duckdb::StringVector::AddString(entries[1], b_val(g));
      }
      duckdb::FlatVector::SetSize(v, take);
      cwT.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ct = r.Column(kT);
  ASSERT_NE(ct, nullptr);

  Rows rows{{3, 4, 5, 100, 101, 4096, 4097, 8000, 9000, 9999}};
  auto state = ct->InitScan(r.Ctx());
  duckdb::Vector result{stype, STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(*ct, state, rows, result, /*out_offset=*/0);
  result.Flatten(rows.size());
  auto& entries = duckdb::StructVector::GetEntries(result);
  const auto* da = duckdb::FlatVector::GetData<int64_t>(entries[0]);
  const auto* db = duckdb::FlatVector::GetData<duckdb::string_t>(entries[1]);
  const auto& sv = duckdb::FlatVector::Validity(result);
  const auto& va = duckdb::FlatVector::Validity(entries[0]);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto g = rows[i];
    EXPECT_EQ(sv.RowIsValid(i), s_valid(g)) << "struct row " << g;
    if (a_valid(g)) {
      ASSERT_TRUE(va.RowIsValid(i)) << "a row " << g;
      EXPECT_EQ(da[i], a_val(g)) << "a row " << g;
    } else {
      EXPECT_FALSE(va.RowIsValid(i)) << "a row " << g;
    }
    EXPECT_EQ(db[i].GetString(), b_val(g)) << "b row " << g;
  }
}

TEST_F(ColumnReaderTest, GatherStructDenseKeepsFieldsOnOneRow) {
  constexpr uint64_t kRows = 12288;
  constexpr irs::field_id kT = 9;
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);
  auto b_val = [](uint64_t g) {
    return "value_" + std::to_string(g) + "_padding";
  };
  duckdb::Connection con{Db()};
  for (const bool nulls : {false, true}) {
    for (const std::string codec : {"auto", "uncompressed"}) {
      SCOPED_TRACE(testing::Message()
                   << "nulls " << nulls << " codec " << codec);
      auto set = con.Query("SET force_compression = '" + codec + "'");
      ASSERT_FALSE(set->HasError()) << set->GetError();
      auto s_valid = [&](uint64_t g) { return !nulls || g % 7 != 0; };
      tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
      {
        irs::ColWriter w{Db(), dir.Tracker("seg")};
        auto& cw = w.OpenColumn(kT, stype, {.row_group_size = 4096});
        for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
          duckdb::Vector v{stype, STANDARD_VECTOR_SIZE};
          auto& entries = duckdb::StructVector::GetEntries(v);
          auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
          auto* db =
            duckdb::FlatVector::GetDataMutable<duckdb::string_t>(entries[1]);
          auto& vs = duckdb::FlatVector::ValidityMutable(v);
          vs.Reset(STANDARD_VECTOR_SIZE);
          for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
            const auto g = pos + k;
            if (!s_valid(g)) {
              vs.SetInvalid(k);
            }
            da[k] = static_cast<int64_t>(g);
            db[k] = duckdb::StringVector::AddString(entries[1], b_val(g));
          }
          duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
          cw.Append(v, STANDARD_VECTOR_SIZE);
        }
        w.Commit(0);
      }
      irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
      const auto* ct = r.Column(kT);
      ASSERT_NE(ct, nullptr);
      auto state = ct->InitScan(r.Ctx());
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      auto gather = [&](uint64_t anchor, duckdb::idx_t hits,
                        duckdb::idx_t step) {
        for (duckdb::idx_t i = 0; i < hits; ++i) {
          sel.set_index(i, i * step);
        }
        duckdb::Vector out{stype, STANDARD_VECTOR_SIZE};
        ct->GatherDense(state, anchor, sel, hits, STANDARD_VECTOR_SIZE, out);
        out.Flatten(hits);
        auto& entries = duckdb::StructVector::GetEntries(out);
        const auto* da = duckdb::FlatVector::GetData<int64_t>(entries[0]);
        const auto* db =
          duckdb::FlatVector::GetData<duckdb::string_t>(entries[1]);
        const auto& sv = duckdb::FlatVector::Validity(out);
        for (duckdb::idx_t i = 0; i < hits; ++i) {
          const auto g = anchor + i * step;
          ASSERT_EQ(sv.RowIsValid(i), s_valid(g)) << "row " << g;
          if (s_valid(g)) {
            ASSERT_EQ(da[i], static_cast<int64_t>(g)) << "row " << g;
            ASSERT_EQ(db[i].GetString(), b_val(g)) << "row " << g;
          }
        }
      };
      gather(0, 1900, 1);
      gather(2048, STANDARD_VECTOR_SIZE, 1);
      gather(4096, 1024, 2);
      gather(6144, STANDARD_VECTOR_SIZE, 1);
      gather(8192, 20, 97);
      gather(10240, 60, 33);
    }
  }
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, ScanStructAfterNullBlockKeepsFieldsOnOneRow) {
  constexpr uint64_t kRows = 8192;
  constexpr uint64_t kNullRows = 4096;
  constexpr irs::field_id kT = 9;
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);
  auto b_val = [](uint64_t g) {
    return "value_" + std::to_string(g) + "_padding";
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw = w.OpenColumn(kT, stype, {.row_group_size = 4096});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{stype, STANDARD_VECTOR_SIZE};
      auto& entries = duckdb::StructVector::GetEntries(v);
      auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
      auto* db =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(entries[1]);
      auto& vs = duckdb::FlatVector::ValidityMutable(v);
      vs.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        const auto g = pos + k;
        if (g < kNullRows) {
          vs.SetInvalid(k);
        }
        da[k] = static_cast<int64_t>(g);
        db[k] = duckdb::StringVector::AddString(entries[1], b_val(g));
      }
      duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
      cw.Append(v, STANDARD_VECTOR_SIZE);
    }
    w.Commit(0);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ct = r.Column(kT);
  ASSERT_NE(ct, nullptr);
  auto state = ct->InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector out{stype, STANDARD_VECTOR_SIZE};
    ASSERT_EQ(ct->Scan(state, out, STANDARD_VECTOR_SIZE), STANDARD_VECTOR_SIZE);
    out.Flatten(STANDARD_VECTOR_SIZE);
    auto& entries = duckdb::StructVector::GetEntries(out);
    const auto* da = duckdb::FlatVector::GetData<int64_t>(entries[0]);
    const auto* db = duckdb::FlatVector::GetData<duckdb::string_t>(entries[1]);
    const auto& sv = duckdb::FlatVector::Validity(out);
    for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
      const auto g = pos + i;
      ASSERT_EQ(sv.RowIsValid(i), g >= kNullRows) << "row " << g;
      if (g >= kNullRows) {
        ASSERT_EQ(da[i], static_cast<int64_t>(g)) << "row " << g;
        ASSERT_EQ(db[i].GetString(), b_val(g)) << "row " << g;
      }
    }
  }
}

// Fixed-size ARRAY(BIGINT, 3) with array-level + element-level NULLs, multi-rg.
TEST_F(ColumnReaderTest, SegmentRoundTripArray) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kArr = 14;
  constexpr duckdb::idx_t kDim = 3;
  const auto atype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::BIGINT, kDim);

  auto arr_valid = [](uint64_t g) { return g % 9 != 0; };
  auto elem_valid = [](uint64_t g, uint64_t i) {
    return (g * kDim + i) % 5 != 0;
  };
  auto elem_val = [](uint64_t g, uint64_t i) {
    return static_cast<int64_t>(g * 100 + i);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwArr = w.OpenColumn(kArr, atype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{atype, STANDARD_VECTOR_SIZE};
      auto& child = duckdb::ArrayVector::GetChildMutable(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<int64_t>(child);
      auto& av = duckdb::FlatVector::ValidityMutable(v);
      auto& cv = duckdb::FlatVector::ValidityMutable(child);
      av.Reset(STANDARD_VECTOR_SIZE);
      cv.Reset(STANDARD_VECTOR_SIZE * kDim);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (!arr_valid(g)) {
          av.SetInvalid(k);
        }
        for (duckdb::idx_t i = 0; i < kDim; ++i) {
          if (elem_valid(g, i)) {
            cd[k * kDim + i] = elem_val(g, i);
          } else {
            cv.SetInvalid(k * kDim + i);
          }
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwArr.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ca = r.Column(kArr);
  ASSERT_NE(ca, nullptr);
  ASSERT_EQ(ca->RowCount(), kRows);
  auto state = ca->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{atype, STANDARD_VECTOR_SIZE};
    ca->Scan(state, result, take);
    result.Flatten(take);
    const auto& child = duckdb::ArrayVector::GetChild(result);
    const auto* cd = duckdb::FlatVector::GetData<int64_t>(child);
    const auto& av = duckdb::FlatVector::Validity(result);
    const auto& cv = duckdb::FlatVector::Validity(child);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      EXPECT_EQ(av.RowIsValid(k), arr_valid(g)) << "array row " << g;
      for (duckdb::idx_t i = 0; i < kDim; ++i) {
        if (elem_valid(g, i)) {
          ASSERT_TRUE(cv.RowIsValid(k * kDim + i)) << "elem " << g << ":" << i;
          EXPECT_EQ(cd[k * kDim + i], arr_valid(g) ? elem_val(g, i) : 0)
            << "elem " << g << ":" << i;
        } else {
          EXPECT_FALSE(cv.RowIsValid(k * kDim + i)) << "elem " << g << ":" << i;
        }
      }
    }
    pos += take;
  }
}

// Gather (sparse, multi-run, multi-row-group) a top-level ARRAY column. Same
// regression class as GatherStructAcrossRowGroups: an ARRAY reader has no data
// segments of its own; its row cursor is the element child's cursor /
// array_size.
TEST_F(ColumnReaderTest, GatherArrayAcrossRowGroups) {
  constexpr uint64_t kRows = 10000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kArr = 14;
  constexpr duckdb::idx_t kDim = 3;
  const auto atype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::BIGINT, kDim);

  auto arr_valid = [](uint64_t g) { return g % 9 != 0; };
  auto elem_val = [](uint64_t g, uint64_t i) {
    return static_cast<int64_t>(g * 100 + i);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwArr = w.OpenColumn(kArr, atype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{atype, STANDARD_VECTOR_SIZE};
      auto& child = duckdb::ArrayVector::GetChildMutable(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<int64_t>(child);
      auto& av = duckdb::FlatVector::ValidityMutable(v);
      auto& cv = duckdb::FlatVector::ValidityMutable(child);
      av.Reset(STANDARD_VECTOR_SIZE);
      cv.Reset(STANDARD_VECTOR_SIZE * kDim);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (!arr_valid(g)) {
          av.SetInvalid(k);
        }
        for (duckdb::idx_t i = 0; i < kDim; ++i) {
          cd[k * kDim + i] = elem_val(g, i);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwArr.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ca = r.Column(kArr);
  ASSERT_NE(ca, nullptr);
  Rows rows{{3, 4, 5, 100, 101, 4096, 4097, 8000, 9000, 9999}};
  auto state = ca->InitScan(r.Ctx());
  duckdb::Vector result{atype, STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(*ca, state, rows, result, /*out_offset=*/0);
  result.Flatten(rows.size());
  const auto& child = duckdb::ArrayVector::GetChild(result);
  const auto* cd = duckdb::FlatVector::GetData<int64_t>(child);
  const auto& av = duckdb::FlatVector::Validity(result);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto g = rows[i];
    EXPECT_EQ(av.RowIsValid(i), arr_valid(g)) << "array row " << g;
    for (duckdb::idx_t e = 0; e < kDim; ++e) {
      EXPECT_EQ(cd[i * kDim + e], arr_valid(g) ? elem_val(g, e) : 0)
        << "elem " << g << ":" << e;
    }
  }
}

TEST_F(ColumnReaderTest, SkipLandsOnItsRowFromAnywhere) {
  constexpr uint64_t kRows = 20000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kField = 1;
  const auto value_of = [](uint64_t g) {
    return static_cast<int64_t>(g * 7919 % 100003);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw = w.OpenColumn(kField, duckdb::LogicalType::BIGINT,
                            {.row_group_size = kRgSize});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        d[k] = value_of(pos + k);
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 8U);
  const auto row_at = [&](irs::ColumnReader::ScanState& state) {
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, 1);
    out.Flatten(1);
    return duckdb::FlatVector::GetData<int64_t>(out)[0];
  };
  for (const uint64_t row :
       {uint64_t{5}, uint64_t{kRgSize}, uint64_t{kRgSize + 3},
        uint64_t{7 * kRgSize - 1}, kRows / 2, kRows - 1}) {
    auto state = col->InitScan(r.Ctx());
    col->Skip(state, row);
    EXPECT_EQ(col->GatherCursor(state), row);
    EXPECT_EQ(row_at(state), value_of(row)) << "row " << row;
    if (row + 1 + 2 * kRgSize < kRows) {
      col->Skip(state, 2 * kRgSize);
      EXPECT_EQ(row_at(state), value_of(row + 1 + 2 * kRgSize))
        << "row " << row;
    }
  }

  auto state = col->InitScan(r.Ctx());
  duckdb::Vector block{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
  col->Scan(state, block, kRgSize);
  col->Skip(state, 0);
  EXPECT_EQ(row_at(state), value_of(kRgSize));
  col->Skip(state, kRows);
  EXPECT_EQ(col->GatherCursor(state), kRows);
}

TEST_F(ColumnReaderTest, VectorReadsInRandomOrderLandOnTheirRows) {
  constexpr uint64_t kRows = 8000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kField = 1;
  constexpr duckdb::idx_t kDim = 16;
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  const auto value_of = [](uint64_t g, duckdb::idx_t i) {
    return std::sin(static_cast<float>(g * kDim + i)) * 123.456F;
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw = w.OpenColumn(kField, vtype,
                            {.row_group_size = kRgSize,
                             .compression = irs::codecs::CodecType::Alprd});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{vtype, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<float>(
        duckdb::ArrayVector::GetChildMutable(v));
      for (duckdb::idx_t k = 0; k < take; ++k) {
        for (duckdb::idx_t i = 0; i < kDim; ++i) {
          d[k * kDim + i] = value_of(pos + k, i);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kField);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->Child()->DataBlocks().size(), 4U);
  irs::ReadContext ctx{r, /*random_access=*/true};
  irs::IvfVectorReader reader{*col, ctx};
  std::vector<uint64_t> rows(kRows);
  std::iota(rows.begin(), rows.end(), uint64_t{0});
  std::shuffle(rows.begin(), rows.end(), std::mt19937_64{7});
  for (const auto row : rows) {
    const auto* v = reader.ReadDocBatch(
      static_cast<irs::doc_id_t>(row + irs::doc_limits::min()), 1);
    for (duckdb::idx_t i = 0; i < kDim; ++i) {
      ASSERT_EQ(v[i], value_of(row, i)) << "row " << row << " elem " << i;
    }
  }
}

// LIST(BIGINT) with variable lengths + list-level NULLs across row-groups
// (exercises globally-cumulative offsets + element child crossing).
TEST_F(ColumnReaderTest, SegmentRoundTripList) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kL = 15;
  const auto ltype = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);

  auto list_valid = [](uint64_t g) { return g % 8 != 0; };
  auto list_len = [](uint64_t g) { return g % 4; };  // 0..3
  auto elem_val = [](uint64_t g, uint64_t j) {
    return static_cast<int64_t>(g * 1000 + j);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwL = w.OpenColumn(kL, ltype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
      auto* le = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(v);
      auto& lv = duckdb::FlatVector::ValidityMutable(v);
      lv.Reset(STANDARD_VECTOR_SIZE);
      uint64_t child_off = 0;
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        const uint64_t len = list_valid(g) ? list_len(g) : 0;
        le[k] = duckdb::list_entry_t{child_off, len};
        child_off += len;
        if (!list_valid(g)) {
          lv.SetInvalid(k);
        }
      }
      duckdb::ListVector::Reserve(v, child_off);
      auto& child = duckdb::ListVector::GetChildMutable(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<int64_t>(child);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (list_valid(g)) {
          for (uint64_t j = 0; j < list_len(g); ++j) {
            cd[le[k].offset + j] = elem_val(g, j);
          }
        }
      }
      duckdb::ListVector::SetListSize(v, child_off);
      duckdb::FlatVector::SetSize(v, take);
      cwL.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cl = r.Column(kL);
  ASSERT_NE(cl, nullptr);
  ASSERT_EQ(cl->RowCount(), kRows);
  auto state = cl->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
    cl->Scan(state, result, take);
    result.Flatten(take);
    const auto* le = duckdb::FlatVector::GetData<duckdb::list_entry_t>(result);
    const auto& lv = duckdb::FlatVector::Validity(result);
    const auto& child = duckdb::ListVector::GetChild(result);
    const auto* cd = duckdb::FlatVector::GetData<int64_t>(child);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      if (!list_valid(g)) {
        EXPECT_FALSE(lv.RowIsValid(k)) << "list row " << g;
        continue;
      }
      ASSERT_TRUE(lv.RowIsValid(k)) << "list row " << g;
      ASSERT_EQ(le[k].length, list_len(g)) << "list row " << g;
      for (uint64_t j = 0; j < list_len(g); ++j) {
        EXPECT_EQ(cd[le[k].offset + j], elem_val(g, j))
          << "list " << g << ":" << j;
      }
    }
    pos += take;
  }
}

// Doubly-nested: list<list<bigint>>. The OUTER list's child (the inner lists)
// exceeds STANDARD_VECTOR_SIZE within a row group, so SealList must slice that
// child into >1 chunk. Each sliced view carries absolute grandchild offsets, so
// serializing the grandchild from offset 0 (rather than the view's base) would
// corrupt every inner list past the first 2048.
TEST_F(ColumnReaderTest, SegmentRoundTripNestedList) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kL = 21;
  const auto inner = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
  const auto ltype = duckdb::LogicalType::LIST(inner);

  auto make_row = [&](uint64_t g) {
    duckdb::vector<duckdb::Value> inner_lists;
    const uint64_t n = 1 + (g % 3);  // 1..3 inner lists per outer row
    for (uint64_t i = 0; i < n; ++i) {
      duckdb::vector<duckdb::Value> ints;
      for (uint64_t j = 0; j < ((g + i) % 3); ++j) {
        ints.push_back(duckdb::Value::BIGINT(
          static_cast<int64_t>(g * 100000 + i * 100 + j)));
      }
      inner_lists.push_back(
        duckdb::Value::LIST(duckdb::LogicalType::BIGINT, ints));
    }
    return duckdb::Value::LIST(inner, inner_lists);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwL = w.OpenColumn(kL, ltype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, make_row(pos + k));
      }
      v.Flatten(take);
      cwL.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cl = r.Column(kL);
  ASSERT_NE(cl, nullptr);
  ASSERT_EQ(cl->RowCount(), kRows);
  auto state = cl->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
    cl->Scan(state, result, take);
    result.Flatten(take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_EQ(result.GetValue(k).ToString(), make_row(pos + k).ToString())
        << "nested list row " << (pos + k);
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, ListsWhoseElementsArriveInUnevenChunksReadBack) {
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE;
  constexpr irs::field_id kL = 21;
  const auto ltype = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
  const auto make_row = [](uint64_t g) {
    duckdb::vector<duckdb::Value> items;
    for (uint64_t j = 0; j <= g % 3; ++j) {
      items.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(g * 4 + j)));
    }
    return duckdb::Value::LIST(duckdb::LogicalType::BIGINT, items);
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw = w.OpenColumn(
      kL, ltype,
      {.row_group_size = kRows, .distinct = irs::DistinctStats::Exact});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        v.SetValue(k, make_row(pos + k));
      }
      v.Flatten(STANDARD_VECTOR_SIZE);
      cw.Append(v, STANDARD_VECTOR_SIZE);
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cl = r.Column(kL);
  ASSERT_NE(cl, nullptr);
  ASSERT_EQ(cl->RowCount(), kRows);
  auto state = cl->InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
    duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
    cl->Scan(state, result, STANDARD_VECTOR_SIZE);
    result.Flatten(STANDARD_VECTOR_SIZE);
    for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
      EXPECT_EQ(result.GetValue(k).ToString(), make_row(pos + k).ToString())
        << "list row " << (pos + k);
    }
  }
}

TEST_F(ColumnReaderTest, ListsUnderANamedDeltaReadBackARowGroupOfEmptyLists) {
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE;
  constexpr irs::field_id kL = 21;
  for (const auto id :
       {duckdb::LogicalTypeId::BIGINT, duckdb::LogicalTypeId::DOUBLE}) {
    const duckdb::LogicalType child{id};
    SCOPED_TRACE(child.ToString());
    const auto ltype = duckdb::LogicalType::LIST(child);
    const auto make_row = [&](uint64_t g) {
      duckdb::vector<duckdb::Value> items;
      if (g / STANDARD_VECTOR_SIZE != 1) {
        for (uint64_t j = 0; j <= g % 3; ++j) {
          items.push_back(duckdb::Value::BIGINT(static_cast<int64_t>(g * 4 + j))
                            .DefaultCastAs(child));
        }
      }
      return duckdb::Value::LIST(child, items);
    };
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    {
      irs::ColWriter w{Db(), dir.Tracker("seg")};
      auto& cw = w.OpenColumn(kL, ltype,
                              {.row_group_size = STANDARD_VECTOR_SIZE,
                               .compression = irs::codecs::CodecType::Delta});
      for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
        duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
        for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
          v.SetValue(k, make_row(pos + k));
        }
        v.Flatten(STANDARD_VECTOR_SIZE);
        cw.Append(v, STANDARD_VECTOR_SIZE);
      }
      ASSERT_TRUE(w.Commit(0));
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
    const auto* cl = r.Column(kL);
    ASSERT_NE(cl, nullptr);
    ASSERT_EQ(cl->RowCount(), kRows);
    auto state = cl->InitScan(r.Ctx());
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
      cl->Scan(state, result, STANDARD_VECTOR_SIZE);
      result.Flatten(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        EXPECT_EQ(result.GetValue(k).ToString(), make_row(pos + k).ToString())
          << "list row " << (pos + k);
      }
    }
  }
}

// Same nested-child-view path as SegmentRoundTripNestedList, but the inner
// collection is a fixed-size ARRAY (positional child) -- verifies SealArray's
// child slicing under sliced parent views.
TEST_F(ColumnReaderTest, SegmentRoundTripNestedArray) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kDim = 2;
  constexpr irs::field_id kL = 22;
  const auto arr =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::BIGINT, kDim);
  const auto ltype = duckdb::LogicalType::LIST(arr);

  auto make_row = [&](uint64_t g) {
    duckdb::vector<duckdb::Value> arrays;
    const uint64_t n = 1 + (g % 3);
    for (uint64_t i = 0; i < n; ++i) {
      duckdb::vector<duckdb::Value> ints;
      for (uint64_t j = 0; j < kDim; ++j) {
        ints.push_back(duckdb::Value::BIGINT(
          static_cast<int64_t>(g * 100000 + i * 100 + j)));
      }
      arrays.push_back(duckdb::Value::ARRAY(duckdb::LogicalType::BIGINT, ints));
    }
    return duckdb::Value::LIST(arr, arrays);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwL = w.OpenColumn(kL, ltype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, make_row(pos + k));
      }
      v.Flatten(take);
      cwL.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cl = r.Column(kL);
  ASSERT_NE(cl, nullptr);
  ASSERT_EQ(cl->RowCount(), kRows);
  auto state = cl->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
    cl->Scan(state, result, take);
    result.Flatten(take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_EQ(result.GetValue(k).ToString(), make_row(pos + k).ToString())
        << "nested array row " << (pos + k);
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, ListGatherSparse) {
  constexpr uint64_t kRows = 600;
  constexpr uint32_t kRgSize = 128;
  constexpr irs::field_id kL = 15;
  const auto ltype = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);

  auto list_valid = [](uint64_t g) { return g % 8 != 0; };
  auto list_len = [](uint64_t g) { return (g % 4) + 1; };  // 1..4 (non-empty)
  auto elem_val = [](uint64_t g, uint64_t j) {
    return static_cast<int64_t>(g * 1000 + j);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("lseg")};
    auto& cwL = w.OpenColumn(kL, ltype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
      auto* le = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(v);
      auto& lv = duckdb::FlatVector::ValidityMutable(v);
      lv.Reset(STANDARD_VECTOR_SIZE);
      uint64_t child_off = 0;
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        const uint64_t len = list_valid(g) ? list_len(g) : 0;
        le[k] = duckdb::list_entry_t{child_off, len};
        child_off += len;
        if (!list_valid(g)) {
          lv.SetInvalid(k);
        }
      }
      duckdb::ListVector::Reserve(v, child_off);
      auto& child = duckdb::ListVector::GetChildMutable(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<int64_t>(child);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (list_valid(g)) {
          for (uint64_t j = 0; j < list_len(g); ++j) {
            cd[le[k].offset + j] = elem_val(g, j);
          }
        }
      }
      duckdb::ListVector::SetListSize(v, child_off);
      duckdb::FlatVector::SetSize(v, take);
      cwL.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
  const auto* cl = r.Column(kL);
  ASSERT_NE(cl, nullptr);
  ASSERT_EQ(cl->RowCount(), kRows);

  // Non-contiguous ascending ids spanning all four row-groups (rg_size 128),
  // with NULL lists (g % 8 == 0: rows 0, 256, 400) interleaved.
  Rows rows{{0, 1, 2, 130, 131, 256, 257, 258, 400, 599}};
  auto state = cl->InitScan(r.Ctx());
  duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(*cl, state, rows, result, /*out_offset=*/0);
  result.Flatten(rows.size());
  const auto* le = duckdb::FlatVector::GetData<duckdb::list_entry_t>(result);
  const auto& lv = duckdb::FlatVector::Validity(result);
  const auto& child = duckdb::ListVector::GetChild(result);
  const auto* cd = duckdb::FlatVector::GetData<int64_t>(child);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto g = rows[i];
    if (!list_valid(g)) {
      EXPECT_FALSE(lv.RowIsValid(i)) << "gathered list row " << g;
      continue;
    }
    ASSERT_TRUE(lv.RowIsValid(i)) << "gathered list row " << g;
    ASSERT_EQ(le[i].length, list_len(g)) << "gathered list row " << g;
    for (uint64_t j = 0; j < list_len(g); ++j) {
      EXPECT_EQ(cd[le[i].offset + j], elem_val(g, j))
        << "gathered list " << g << ":" << j;
    }
  }
}

TEST_F(ColumnReaderTest, PointReadsAfterASpreadVectorReturnTheirOwnLists) {
  constexpr irs::field_id kL = 16;
  constexpr uint64_t kLists = 4200;
  constexpr uint64_t kListLen = 128;
  constexpr uint64_t kAlternating = 2 * kLists;
  constexpr uint64_t kFirstRepeat = 600;
  constexpr uint64_t kRepeats = 4;
  constexpr uint64_t kRows = kAlternating + kRepeats;
  constexpr uint64_t kSpreadFirst = 6200;
  const auto ltype = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
  auto list_of = [](uint64_t row) -> uint64_t {
    if (row >= kAlternating) {
      return kFirstRepeat + (row - kAlternating);
    }
    return row % 2 == 0 ? 0 : (row + 1) / 2;
  };
  auto elem_val = [](uint64_t list, uint64_t j) {
    return static_cast<int64_t>(list * 1000 + j);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("pseg")};
    auto& cw =
      w.OpenColumn(kL, ltype, {.row_group_size = static_cast<uint32_t>(kRows)});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{ltype, STANDARD_VECTOR_SIZE};
      duckdb::ListVector::Reserve(v, take * kListLen);
      auto* le = duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<int64_t>(
        duckdb::ListVector::GetChildMutable(v));
      for (duckdb::idx_t k = 0; k < take; ++k) {
        le[k] = duckdb::list_entry_t{k * kListLen, kListLen};
        for (uint64_t j = 0; j < kListLen; ++j) {
          cd[k * kListLen + j] = elem_val(list_of(pos + k), j);
        }
      }
      duckdb::ListVector::SetListSize(v, take * kListLen);
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "pseg");
  const auto* col = r.Column(kL);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  ASSERT_EQ(col->Child()->RowCount(), (kLists + 1) * kListLen);

  auto state = col->InitScan(r.Ctx());
  uint64_t at = 0;
  auto read = [&](uint64_t first, duckdb::idx_t count) {
    col->Skip(state, first - at);
    duckdb::Vector result{ltype, STANDARD_VECTOR_SIZE};
    ASSERT_EQ(col->Scan(state, result, count), count);
    at = first + count;
    result.Flatten(count);
    const auto* le = duckdb::FlatVector::GetData<duckdb::list_entry_t>(result);
    const auto* cd = duckdb::FlatVector::GetData<int64_t>(
      duckdb::ListVector::GetChild(result));
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const auto row = first + i;
      ASSERT_EQ(le[i].length, kListLen) << "row " << row;
      for (uint64_t j = 0; j < kListLen; ++j) {
        ASSERT_EQ(cd[le[i].offset + j], elem_val(list_of(row), j))
          << "row " << row << " element " << j;
      }
    }
  };
  read(1, 2);
  read(kSpreadFirst, STANDARD_VECTOR_SIZE);
  read(kAlternating, kRepeats);
}

// Build a VARIANT column from a SQL query (each chunk Append-ed), collecting
// the per-row expected Values. Mirrors variant_shredding_test.cpp's
// construction but drives the new ColWriter.
void WriteVariantViaSql(duckdb::DatabaseInstance& db,
                        const tests::IndexDir& dir, std::string_view segment,
                        irs::field_id id, const std::string& sql,
                        uint32_t rg_size,
                        std::vector<duckdb::Value>& expected) {
  irs::ColWriter w{db, dir.Tracker(segment), irs::WriteTier::Merge};
  auto& cw = w.OpenColumn(id, duckdb::LogicalType::VARIANT(),
                          {.row_group_size = rg_size});
  duckdb::Connection con{db};
  auto result = con.Query(sql);
  ASSERT_FALSE(result->HasError()) << result->GetError();
  while (auto chunk = result->Fetch()) {
    if (chunk->size() == 0) {
      continue;
    }
    ASSERT_EQ(chunk->data[0].GetType().id(), duckdb::LogicalTypeId::VARIANT);
    cw.Append(chunk->data[0], chunk->size());
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      expected.emplace_back(chunk->GetValue(0, k));
    }
  }
  w.Commit(0);
}

void SetShreddingSize(duckdb::DatabaseInstance& db, int64_t value) {
  duckdb::Connection con{db};
  auto r =
    con.Query("SET variant_minimum_shredding_size = " + std::to_string(value));
  ASSERT_FALSE(r->HasError()) << r->GetError();
}

void ExpectVariantValuesEqual(const std::vector<duckdb::Value>& expected,
                              const std::vector<duckdb::Value>& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i].IsNull(), actual[i].IsNull()) << "row=" << i;
    if (!expected[i].IsNull()) {
      EXPECT_EQ(expected[i].ToString(), actual[i].ToString()) << "row=" << i;
    }
  }
}

// Contiguous scan of a whole VARIANT column into Values.
std::vector<duckdb::Value> ScanVariantAll(const irs::ColumnReader& col,
                                          irs::ReadContext& ctx) {
  auto state = col.InitScan(ctx);
  std::vector<duckdb::Value> out;
  const auto total = col.RowCount();
  out.reserve(total);
  uint64_t pos = 0;
  while (pos < total) {
    const auto take =
      std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
    col.Scan(state, result, take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      out.emplace_back(result.GetValue(k));
    }
    pos += take;
  }
  return out;
}

void WriteViaSql(duckdb::DatabaseInstance& db, const tests::IndexDir& dir,
                 std::string_view segment, irs::field_id id,
                 const duckdb::LogicalType& type, const std::string& sql,
                 uint32_t rg_size, std::vector<duckdb::Value>& expected,
                 irs::ColumnWriterOptions options = {}) {
  irs::ColWriter w{db, dir.Tracker(segment), options.codec_params.tier};
  options.row_group_size = rg_size;
  auto& cw = w.OpenColumn(id, type, options);
  duckdb::Connection con{db};
  auto result = con.Query(sql);
  ASSERT_FALSE(result->HasError()) << result->GetError();
  while (auto chunk = result->Fetch()) {
    if (chunk->size() == 0) {
      continue;
    }
    ASSERT_EQ(chunk->data[0].GetType(), type);
    cw.Append(chunk->data[0], chunk->size());
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      expected.emplace_back(chunk->GetValue(0, k));
    }
  }
  w.Commit(0);
}

void WriteSparseViaSql(duckdb::DatabaseInstance& db, const tests::IndexDir& dir,
                       std::string_view segment, irs::field_id id,
                       const duckdb::LogicalType& type, const std::string& sql,
                       uint32_t rg_size, uint64_t gap,
                       std::vector<duckdb::Value>& expected) {
  irs::ColWriter w{db, dir.Tracker(segment)};
  auto& cw = w.OpenColumn(id, type, {.row_group_size = rg_size});
  duckdb::Connection con{db};
  auto result = con.Query(sql);
  ASSERT_FALSE(result->HasError()) << result->GetError();
  uint64_t chunks = 0;
  while (auto chunk = result->Fetch()) {
    if (chunk->size() == 0) {
      continue;
    }
    ASSERT_EQ(chunk->data[0].GetType(), type);
    if (chunks++ % 3 == 0) {
      expected.resize(expected.size() + gap, duckdb::Value{type});
    }
    cw.Append(expected.size(), chunk->data[0], chunk->size());
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      expected.emplace_back(chunk->GetValue(0, k));
    }
  }
  w.Commit(0);
}

std::vector<duckdb::Value> ScanValues(const irs::ColumnReader& col,
                                      irs::ReadContext& ctx) {
  auto state = col.InitScan(ctx);
  std::vector<duckdb::Value> out;
  const auto total = col.RowCount();
  out.reserve(total);
  uint64_t pos = 0;
  while (pos < total) {
    const auto take =
      std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{col.Type(), STANDARD_VECTOR_SIZE};
    col.Scan(state, result, take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      out.emplace_back(result.GetValue(k));
    }
    pos += take;
  }
  return out;
}

void ExpectGathered(const irs::ColumnReader& col, irs::ReadContext& ctx,
                    const std::vector<duckdb::Value>& expected,
                    const Rows& rows, bool whole_output) {
  auto state = col.InitScan(ctx);
  duckdb::Vector result{col.Type(), STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(col, state, rows, result, 0, whole_output);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto& want = expected[rows[i]];
    const auto got = result.GetValue(i);
    EXPECT_EQ(want.IsNull(), got.IsNull()) << "row " << rows[i];
    if (!want.IsNull()) {
      EXPECT_EQ(want.ToString(), got.ToString()) << "row " << rows[i];
    }
  }
}

uint64_t ElementCount(const std::vector<duckdb::Value>& values) {
  uint64_t n = 0;
  for (const auto& v : values) {
    if (!v.IsNull()) {
      n += duckdb::ListValue::GetChildren(v).size();
    }
  }
  return n;
}

TEST_F(ColumnReaderTest, RepeatedMapsAreStoredOnce) {
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "mseg", 40, type,
              "SELECT CASE WHEN i % 11 = 0 THEN NULL WHEN i % 17 = 0 THEN "
              "map([], [])::MAP(VARCHAR, VARCHAR) ELSE MAP {'service': 'svc-' "
              "|| (i // 997 % 7), 'host': 'h-' || (i % 13), 'env': 'prod'} END "
              "FROM range(20000) t(i)",
              4096, expected);
  ASSERT_EQ(expected.size(), 20000u);

  irs::ColReader r = irs::tests::OpenColReader(dir, "mseg");
  const auto* col = r.Column(40);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 20000u);
  ASSERT_NE(col->Child(), nullptr);
  EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 2, 11, 17, 4095, 4096, 4097, 9000, 13000, 19999}},
                 false);
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{8200, 8201, 8205, 8250, 8300, 9000, 10000}}, true);
  ExpectGathered(*col, r.Ctx(), expected, Rows{{8200, 8201, 8206, 8210, 8230}},
                 true);
}

TEST_F(ColumnReaderTest, RepeatedListsGatherAndSkip) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "lseg", 41, type,
              "SELECT CASE WHEN i % 9 = 0 THEN NULL ELSE ['a' || (i % 5), 'b' "
              "|| (i % 3), repeat('x', (i % 4) * 20)] END FROM range(12000) "
              "t(i)",
              1024, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
  const auto* col = r.Column(41);
  ASSERT_NE(col, nullptr);
  EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  Rows sparse;
  for (uint64_t g = 3; g < 12000; g += 97) {
    sparse.v.push_back(g);
  }
  ExpectGathered(*col, r.Ctx(), expected, sparse, false);
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{5000, 5001, 5002, 5003, 5100, 6000, 7000}}, true);
  ExpectGathered(*col, r.Ctx(), expected, Rows{{5000, 5001, 5004, 5010, 5020}},
                 true);
}

TEST_F(ColumnReaderTest, OneRepeatedListAmongUniqueListsScans) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "lseg", 41, type,
              "SELECT CASE WHEN i % 2 = 0 THEN [] ELSE ['value-' || i, 'tail-' "
              "|| (i * 7)] END FROM range(40000) t(i)",
              40000, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
  const auto* col = r.Column(41);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  Rows sparse;
  for (uint64_t g = 1; g < 40000; g += 61) {
    sparse.v.push_back(g);
  }
  ExpectGathered(*col, r.Ctx(), expected, sparse, false);
}

TEST_F(ColumnReaderTest, RepeatedNestedLists) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "nseg", 42, type,
              "SELECT CASE WHEN i % 13 = 0 THEN NULL ELSE [[i % 3, i % 2], "
              "[i % 4], []] END FROM range(9000) t(i)",
              2048, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "nseg");
  const auto* col = r.Column(42);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{1, 2, 13, 2047, 2048, 2049, 4500, 8999}}, false);
}

TEST_F(ColumnReaderTest, UniqueNestedListsKeepEveryRow) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "unseg", 47, type,
              "SELECT CASE WHEN i % 11 = 0 THEN NULL ELSE [[i, i * 7], [], "
              "[i % 5]] END FROM range(12000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "unseg");
  const auto* col = r.Column(47);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Child()->RowCount(), ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 11, 4095, 4096, 8191, 11999}}, false);
}

void CollectDoubleBits(const duckdb::Value& v, std::vector<uint64_t>& out) {
  if (v.IsNull()) {
    out.emplace_back(0x5a5a5a5a5a5a5a5aULL);
    return;
  }
  const auto id = v.type().id();
  if (id == duckdb::LogicalTypeId::DOUBLE) {
    out.emplace_back(std::bit_cast<uint64_t>(v.GetValue<double>()));
    return;
  }
  if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::MAP) {
    for (const auto& c : duckdb::ListValue::GetChildren(v)) {
      CollectDoubleBits(c, out);
    }
    return;
  }
  if (id == duckdb::LogicalTypeId::STRUCT) {
    for (const auto& c : duckdb::StructValue::GetChildren(v)) {
      CollectDoubleBits(c, out);
    }
  }
}

void ExpectDoubleBitsRoundTrip(duckdb::DatabaseInstance& db,
                               const duckdb::LogicalType& type,
                               const std::vector<duckdb::Value>& rows) {
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{db, dir.Tracker("zseg")};
    auto& cw = w.OpenColumn(60, type, {.row_group_size = 4096});
    duckdb::Vector vec{type, rows.size()};
    for (size_t i = 0; i < rows.size(); ++i) {
      vec.SetValue(i, rows[i]);
    }
    cw.Append(vec, rows.size());
    w.Commit(0);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "zseg");
  const auto* col = r.Column(60);
  ASSERT_NE(col, nullptr);
  const auto got = ScanValues(*col, r.Ctx());
  ASSERT_EQ(got.size(), rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    std::vector<uint64_t> want_bits;
    std::vector<uint64_t> got_bits;
    CollectDoubleBits(rows[i], want_bits);
    CollectDoubleBits(got[i], got_bits);
    EXPECT_EQ(want_bits, got_bits) << "row " << i;
  }
}

std::vector<double> SignedZerosAndNaNs() {
  return {0.0, -0.0, std::bit_cast<double>(0x7ff8000000000000ULL),
          std::bit_cast<double>(0x7ff8000000000001ULL),
          std::bit_cast<double>(0xfff8000000000000ULL)};
}

TEST_F(ColumnReaderTest, NestedListsKeepSignedZerosAndNaNPayloads) {
  const auto inner = duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE);
  const auto type = duckdb::LogicalType::LIST(inner);
  std::vector<duckdb::Value> rows;
  for (const auto d : SignedZerosAndNaNs()) {
    rows.emplace_back(duckdb::Value::LIST(
      inner, {duckdb::Value::LIST(duckdb::LogicalType::DOUBLE,
                                  {duckdb::Value::DOUBLE(d)})}));
  }
  ExpectDoubleBitsRoundTrip(Db(), type, rows);
}

TEST_F(ColumnReaderTest, MapsWithListValuesKeepSignedZerosAndNaNPayloads) {
  const auto value_type =
    duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE);
  const auto type =
    duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, value_type);
  std::vector<duckdb::Value> rows;
  for (const auto d : SignedZerosAndNaNs()) {
    rows.emplace_back(duckdb::Value::MAP(
      duckdb::LogicalType::VARCHAR, value_type, {duckdb::Value{"k"}},
      {duckdb::Value::LIST(duckdb::LogicalType::DOUBLE,
                           {duckdb::Value::DOUBLE(d)})}));
  }
  ExpectDoubleBitsRoundTrip(Db(), type, rows);
}

TEST_F(ColumnReaderTest, NestedListsKeepEqualComparingIntervalsApart) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTERVAL));
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "iseg", 61, type,
              "SELECT CASE i % 3 WHEN 0 THEN [[INTERVAL '1 month']] WHEN 1 "
              "THEN [[INTERVAL '30 days']] ELSE [[INTERVAL '720 hours']] END "
              "FROM range(300) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "iseg");
  const auto* col = r.Column(61);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, ListsInsideStructs) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"tags", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
     {"attrs", duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                        duckdb::LogicalType::VARCHAR)}});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "sseg", 48, type,
              "SELECT CASE WHEN i % 17 = 0 THEN NULL ELSE {'tags': CASE WHEN "
              "i % 9 = 0 THEN NULL WHEN i < 10000 THEN ['t' || (i % 5), 'x'] "
              "ELSE ['u' || i] END, 'attrs': map_from_entries(list_transform("
              "range(3), lambda k: {'key': 'a' || k, 'value': CASE WHEN i < "
              "10000 THEN 'v' || ((i // 50) % 7) ELSE 'w' || i || k END}))} "
              "END FROM range(20000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "sseg");
  const auto* col = r.Column(48);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 20000u);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 9, 17, 4095, 4096, 9999, 10000, 19999}}, false);
}

TEST_F(ColumnReaderTest, RepeatedMapsWithDistinctStatistics) {
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::INTEGER);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "hseg", 49, type,
              "SELECT CASE WHEN i % 17 = 0 THEN NULL ELSE map_from_entries("
              "list_transform(range(4), lambda k: {'key': 'k' || k, 'value': "
              "((i // 25) % 40)::INTEGER})) END FROM range(20000) t(i)",
              4096, expected, {.distinct = irs::DistinctStats::Exact});
  irs::ColReader r = irs::tests::OpenColReader(dir, "hseg");
  const auto* col = r.Column(49);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 20000u);
  EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 24, 25, 4095, 4096, 12287, 12288, 19999}}, false);
  const auto* distinct = col->Distinct();
  ASSERT_NE(distinct, nullptr);
  EXPECT_GE(distinct->GetCount(), 20u);
  EXPECT_LE(distinct->GetCount(), 80u);
}

TEST_F(ColumnReaderTest, SparseRepeatedMapsPadNulls) {
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteSparseViaSql(Db(), dir, "pseg", 50, type,
                    "SELECT CASE WHEN i % 29 = 0 THEN NULL ELSE "
                    "map_from_entries(list_transform(range(3), lambda k: "
                    "{'key': 'a' || k, 'value': 'v' || ((i // 40) % 6) || k}))"
                    " END FROM range(12000) t(i)",
                    4096, 1500, expected);
  ASSERT_GT(expected.size(), 12000u);
  irs::ColReader r = irs::tests::OpenColReader(dir, "pseg");
  const auto* col = r.Column(50);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), expected.size());
  EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1499, 1500, 4095, 4096, 7643, 7644, 8191, 8192, 9143,
                       9144, 12287, 12288, expected.size() - 1}},
                 false);
}

TEST_F(ColumnReaderTest, RewrittenRepeatedListsKeepTheirValues) {
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> source;
  WriteViaSql(Db(), dir, "rsrc", 51, type,
              "SELECT CASE WHEN i % 23 = 0 THEN NULL WHEN i % 31 = 0 THEN "
              "map([], [])::MAP(VARCHAR, VARCHAR) ELSE map_from_entries("
              "list_transform(range(3), lambda k: {'key': 'attr.' || k, "
              "'value': 'v' || ((i // 7) % 50) || '-' || k})) END "
              "FROM range(30000) t(i)",
              8192, source);
  irs::ColReader src = irs::tests::OpenColReader(dir, "rsrc");
  const auto* col = src.Column(51);
  ASSERT_NE(col, nullptr);
  for (const bool sliced : {false, true}) {
    SCOPED_TRACE(sliced ? "every other row" : "every row");
    const std::string segment = sliced ? "rdst_sliced" : "rdst";
    std::vector<duckdb::Value> expected;
    {
      irs::ColWriter w{Db(), dir.Tracker(segment)};
      auto& cw = w.OpenColumn(52, type, {.row_group_size = 8192});
      auto state = col->InitScan(src.Ctx());
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      uint64_t pos = 0;
      while (pos < col->RowCount()) {
        const auto take =
          std::min<uint64_t>(col->RowCount() - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
        col->Scan(state, batch, take);
        if (!sliced) {
          cw.Append(batch, take);
          expected.insert(expected.end(), source.begin() + pos,
                          source.begin() + pos + take);
        } else {
          duckdb::idx_t kept = 0;
          for (duckdb::idx_t k = 0; k < take; k += 2) {
            sel.set_index(kept++, k);
            expected.emplace_back(source[pos + k]);
          }
          const duckdb::Vector selected{batch, sel, kept};
          cw.Append(selected, kept);
        }
        pos += take;
      }
      w.Commit(0);
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, segment);
    const auto* out = r.Column(52);
    ASSERT_NE(out, nullptr);
    ASSERT_EQ(out->RowCount(), expected.size());
    EXPECT_LT(out->Child()->RowCount() * 10, ElementCount(expected));
    ExpectVariantValuesEqual(expected, ScanValues(*out, r.Ctx()));
    ExpectGathered(*out, r.Ctx(), expected,
                   Rows{{0, 1, 22, 23, 8191, 8192, expected.size() - 1}},
                   false);
  }
}

TEST_F(ColumnReaderTest, RewrittenStructsKeepTheirListFieldsDeduplicated) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"id", duckdb::LogicalType::BIGINT},
     {"tags", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)},
     {"attrs", duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                        duckdb::LogicalType::VARCHAR)}});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> source;
  WriteViaSql(Db(), dir, "ssrc", 51, type,
              "SELECT CASE WHEN i % 23 = 0 THEN NULL ELSE {'id': i, 'tags': "
              "CASE WHEN i % 29 = 0 THEN NULL WHEN i % 31 = 0 THEN "
              "[]::VARCHAR[] ELSE ['tag.' || (i % 40), 'env.' || (i % 3)] "
              "END, 'attrs': map_from_entries(list_transform(range(3), lambda "
              "k: {'key': 'attr.' || k, 'value': 'v' || ((i // 7) % 50) || "
              "'-' || k}))} END FROM range(30000) t(i)",
              8192, source);
  const auto tag_count = [](const std::vector<duckdb::Value>& values) {
    std::vector<duckdb::Value> tags;
    for (const auto& v : values) {
      if (!v.IsNull()) {
        tags.emplace_back(duckdb::StructValue::GetChildren(v)[1]);
      }
    }
    return ElementCount(tags);
  };
  irs::ColReader src = irs::tests::OpenColReader(dir, "ssrc");
  const auto* col = src.Column(51);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->StructFieldCount(), 3u);
  EXPECT_LT(col->StructField(1).Child()->RowCount() * 10, tag_count(source));
  for (const bool sliced : {false, true}) {
    SCOPED_TRACE(sliced ? "every other row" : "every row");
    const std::string segment = sliced ? "sdst_sliced" : "sdst";
    std::vector<duckdb::Value> expected;
    {
      irs::ColWriter w{Db(), dir.Tracker(segment)};
      auto& cw = w.OpenColumn(52, type, {.row_group_size = 8192});
      auto state = col->InitScan(src.Ctx());
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      uint64_t pos = 0;
      while (pos < col->RowCount()) {
        const auto take =
          std::min<uint64_t>(col->RowCount() - pos, STANDARD_VECTOR_SIZE);
        duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
        col->Scan(state, batch, take);
        if (!sliced) {
          cw.Append(batch, take);
          expected.insert(expected.end(), source.begin() + pos,
                          source.begin() + pos + take);
        } else {
          duckdb::idx_t kept = 0;
          for (duckdb::idx_t k = 0; k < take; k += 2) {
            sel.set_index(kept++, k);
            expected.emplace_back(source[pos + k]);
          }
          const duckdb::Vector selected{batch, sel, kept};
          cw.Append(selected, kept);
        }
        pos += take;
      }
      w.Commit(0);
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, segment);
    const auto* out = r.Column(52);
    ASSERT_NE(out, nullptr);
    ASSERT_EQ(out->RowCount(), expected.size());
    ASSERT_EQ(out->StructFieldCount(), 3u);
    EXPECT_LT(out->StructField(1).Child()->RowCount() * 10,
              tag_count(expected));
    ExpectVariantValuesEqual(expected, ScanValues(*out, r.Ctx()));
    ExpectGathered(*out, r.Ctx(), expected,
                   Rows{{0, 1, 22, 23, 8191, 8192, expected.size() - 1}},
                   false);
  }
}

TEST_F(ColumnReaderTest, RepeatedListsStayDeduplicatedAcrossSplitAppends) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteSparseViaSql(Db(), dir, "split", 51, type,
                    "SELECT CASE WHEN i % 17 = 0 THEN NULL ELSE ['tag.' || (i "
                    "% 3), 'env'] END FROM range(40000) t(i)",
                    8192, 1, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "split");
  const auto* col = r.Column(51);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), expected.size());
  EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, ListDictionariesKeepTheirValuesAcrossAppends) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  constexpr duckdb::idx_t kLists = 64;
  constexpr duckdb::idx_t kRows = 1500;
  const auto list_of = [](uint64_t generation, duckdb::idx_t i) {
    return duckdb::Value::LIST(duckdb::LogicalType::VARCHAR,
                               {duckdb::Value{"g" + std::to_string(generation) +
                                              "-" + std::to_string(i % 8)},
                                duckdb::Value{"x"}});
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  for (const bool reusable : {false, true}) {
    SCOPED_TRACE(reusable ? "reusable dictionaries" : "refilled vector");
    const std::string segment = reusable ? "reused" : "refilled";
    std::vector<duckdb::Value> expected;
    {
      irs::ColWriter w{Db(), dir.Tracker(segment)};
      auto& cw = w.OpenColumn(51, type, {.row_group_size = 8192});
      const duckdb::VectorCache cache{duckdb::Allocator::DefaultAllocator(),
                                      type};
      duckdb::Vector lists{cache};
      duckdb::buffer_ptr<duckdb::DictionaryEntry> dictionary;
      duckdb::SelectionVector sel{kRows};
      for (uint64_t round = 0; round != 6; ++round) {
        const uint64_t generation = reusable ? round / 2 : round;
        if (!reusable) {
          lists.ResetFromCache(cache);
          for (duckdb::idx_t i = 0; i != kLists; ++i) {
            lists.SetValue(i, list_of(generation, i));
          }
          duckdb::FlatVector::SetSize(lists, kLists);
        } else if (round % 2 == 0) {
          dictionary =
            duckdb::DictionaryVector::CreateReusableDictionary(type, kLists);
          for (duckdb::idx_t i = 0; i != kLists; ++i) {
            dictionary->data.SetValue(i, list_of(generation, i));
          }
        }
        for (duckdb::idx_t k = 0; k != kRows; ++k) {
          const auto i = (k * 7 + round) % kLists;
          sel.set_index(k, i);
          expected.emplace_back(list_of(generation, i));
        }
        duckdb::Vector selected{type};
        if (reusable) {
          selected.Dictionary(dictionary, sel, kRows);
        } else {
          selected.Slice(lists, sel, kRows);
        }
        cw.Append(selected, kRows);
      }
      w.Commit(0);
    }
    irs::ColReader r = irs::tests::OpenColReader(dir, segment);
    const auto* col = r.Column(51);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), expected.size());
    EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
    ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  }
}

TEST_F(ColumnReaderTest,
       RewrittenListsKeepTheirValuesAcrossReaderDictionaries) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> source;
  WriteViaSql(Db(), dir, "lsrc", 51, type,
              "SELECT CASE WHEN i % 29 = 0 THEN NULL ELSE ['loc' || (i // 4), "
              "'z'] END FROM range(40000) t(i)",
              8192, source);
  irs::ColReader src = irs::tests::OpenColReader(dir, "lsrc");
  const auto* col = src.Column(51);
  ASSERT_NE(col, nullptr);
  {
    irs::ColWriter w{Db(), dir.Tracker("plain")};
    auto& cw = w.OpenColumn(52, type, {.row_group_size = 8192});
    auto state = col->InitScan(std::make_shared<irs::ReadContext>(src));
    uint64_t pos = 0;
    while (pos < col->RowCount()) {
      const auto take =
        std::min<uint64_t>(col->RowCount() - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, batch, take);
      cw.Append(batch, take);
      pos += take;
    }
    w.Commit(0);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "plain");
  const auto* out = r.Column(52);
  ASSERT_NE(out, nullptr);
  ASSERT_EQ(out->RowCount(), source.size());
  ExpectVariantValuesEqual(source, ScanValues(*out, r.Ctx()));
}

TEST_F(ColumnReaderTest, UncompressedListElementsKeepTheirValuesAcrossScans) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "lseg", 41, type,
    "SELECT ['first-' || l || repeat('x', 40), 'second-' || l || repeat('y', "
    "40)] FROM (SELECT 100 * (i // 1024) + i % 100 AS l FROM range(6144) t(i))",
    1024, expected, {.compression = irs::codecs::CodecType::Uncompressed});
  irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
  const auto* col = r.Column(41);
  ASSERT_NE(col, nullptr);
  EXPECT_LT(col->Child()->RowCount() * 10, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, RowGroupsWithoutListElementsWriteNoLz4Segment) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "lseg", 41, type,
              "SELECT CASE WHEN i % 3 = 0 THEN NULL WHEN i < 2048 THEN [] ELSE "
              "['tag-' || (i % 7), 'value-' || i] END FROM range(3072) t(i)",
              1024, expected, {.compression = irs::codecs::CodecType::Lz4});
  irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
  const auto* col = r.Column(41);
  ASSERT_NE(col, nullptr);
  const auto* elements = col->Child();
  ASSERT_NE(elements, nullptr);
  ASSERT_FALSE(elements->DataBlocks().empty());
  for (const auto& block : elements->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Lz4);
    EXPECT_NE(block.tuple_count, 0);
  }
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 1023, 1024, 2047, 2048, 2049, 3071}}, false);
}

TEST_F(ColumnReaderTest,
       UncompressedNestedListElementsKeepTheirValuesAcrossRereads) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "nseg", 42, type,
    "SELECT [['v-' || (CASE WHEN o < 500 THEN o ELSE (o - 500) * 7 % 500 END) "
    "|| repeat('p', 40)], ['u-' || o || repeat('q', 40)]] FROM (SELECT CASE "
    "WHEN i < 1000 THEN i WHEN i % 2 = 0 THEN i // 2 % 50 ELSE 500 + i // 2 % "
    "50 END AS o FROM range(8192) t(i))",
    8192, expected, {.compression = irs::codecs::CodecType::Uncompressed});
  irs::ColReader r = irs::tests::OpenColReader(dir, "nseg");
  const auto* col = r.Column(42);
  ASSERT_NE(col, nullptr);
  EXPECT_LT(col->Child()->RowCount() * 4, ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, SparseListsInsideStructsPadNulls) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"tags", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteSparseViaSql(
    Db(), dir, "sps", 51, type,
    "SELECT CASE WHEN i % 31 = 0 THEN NULL ELSE {'tags': CASE "
    "WHEN i % 11 = 0 THEN NULL WHEN i < 6000 THEN ['t' || (i "
    "% 4), 'x'] ELSE ['u' || i] END} END FROM range(12000) t(i)",
    4096, 1500, expected);
  ASSERT_GT(expected.size(), 12000u);
  irs::ColReader r = irs::tests::OpenColReader(dir, "sps");
  const auto* col = r.Column(51);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), expected.size());
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1499, 1500, 4095, 4096, 7643, 7644, 8191, 8192, 9143,
                       9144, 12287, 12288, expected.size() - 1}},
                 false);
}

TEST_F(ColumnReaderTest, LongListsInsideStructs) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"tags", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "lls", 52, type,
              "SELECT {'tags': CASE WHEN i < 4096 THEN CASE WHEN i % 50 = 0 "
              "THEN list_transform(range(2500), lambda k: 'e' || k) ELSE ['r' "
              "|| (i % 3)] END WHEN i % 7 = 0 THEN list_transform(range(2100 + "
              "i % 5), lambda k: 'u' || i || '-' || k) ELSE ['s' || i] END} "
              "FROM range(8192) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "lls");
  const auto* col = r.Column(52);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 8192u);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(
    *col, r.Ctx(), expected,
    Rows{{0, 1, 50, 51, 2050, 4095, 4096, 4102, 4103, 6146, 8189, 8191}},
    false);
}

TEST_F(ColumnReaderTest, UniqueListsKeepEveryRow) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "useg", 43, type,
              "SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE ['u' || i, 'v' || (i "
              "* 31)] END FROM range(10000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "useg");
  const auto* col = r.Column(43);
  ASSERT_NE(col, nullptr);
  EXPECT_EQ(col->Child()->RowCount(), ElementCount(expected));
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected, Rows{{1, 7, 8, 4096, 9999}}, false);
}

TEST_F(ColumnReaderTest, UniqueListRowGroupsStoreTheirCodesAsSteps) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "sseg", 62, type,
              "SELECT CASE WHEN i % 7 = 0 THEN NULL WHEN i = 11 THEN "
              "[]::VARCHAR[] ELSE ['u' || i, 'v' || (i * 31)] END FROM "
              "range(10000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "sseg");
  const auto* col = r.Column(62);
  ASSERT_NE(col, nullptr);
  ASSERT_FALSE(col->DataBlocks().empty());
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Delta);
  }
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 7, 11, 12, 4095, 4096, 4097, 8191, 9999}}, false);
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{3000, 3001, 3002, 3010, 5000, 5001}}, true);
}

TEST_F(ColumnReaderTest, ListRowGroupsOfRunsRunLengthEncodeTheirCodes) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "sseg", 62, type,
              "SELECT CASE WHEN i % 500 = 0 THEN NULL WHEN i // 37 = 11 THEN "
              "[]::VARCHAR[] ELSE ['u' || (i // 37), 'v' || ((i // 37) * 31)] "
              "END FROM range(10000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "sseg");
  const auto* col = r.Column(62);
  ASSERT_NE(col, nullptr);
  ASSERT_FALSE(col->DataBlocks().empty());
  for (const auto& block : col->DataBlocks()) {
    EXPECT_EQ(block.codec->type, irs::codecs::CodecType::Rle);
  }
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 36, 37, 407, 444, 4095, 4096, 4097, 8191, 9999}},
                 false);
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{3000, 3001, 3002, 3010, 5000, 5001}}, true);
}

TEST_F(ColumnReaderTest, ListCodesStayConsecutiveOnceDedupTurnsOff) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  irs::ListIngest ingest{type, /*borrow=*/false};
  duckdb::Connection con{Db()};
  auto result = con.Query(
    "SELECT CASE WHEN i % 7 = 0 THEN NULL WHEN i % 13 = 0 THEN "
    "[]::VARCHAR[] ELSE ['u' || i, 'v' || (i * 31)] END FROM range(8192) "
    "t(i)");
  ASSERT_FALSE(result->HasError()) << result->GetError();
  while (auto chunk = result->Fetch()) {
    ingest.Add(chunk->data[0], 0, chunk->size());
    ingest.CheckDistinct();
  }
  const auto parts = ingest.Take();
  const auto& codes = parts.codes;
  ASSERT_EQ(codes.size(), 8192u);
  for (size_t i = STANDARD_VECTOR_SIZE + 1; i < codes.size(); ++i) {
    ASSERT_EQ(codes[i], codes[i - 1] + 1) << "row " << i;
  }
}

TEST_F(ColumnReaderTest, EqualListsShareACodeInEveryElementVectorForm) {
  constexpr duckdb::idx_t kLists = 8;
  constexpr duckdb::idx_t kShortest = 60;
  constexpr duckdb::idx_t kRows = 64;
  constexpr duckdb::idx_t kElements =
    kRows * kShortest + kRows / kLists * (kLists * (kLists - 1) / 2);
  enum class Form : uint8_t {
    Flat,
    Selected,
    Nullable,
  };
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT);
  irs::ListIngest ingest{type, /*borrow=*/false};
  const auto add = [&](Form form) {
    const bool reversed = form == Form::Selected;
    const duckdb::idx_t rows = kRows + (form == Form::Nullable);
    duckdb::Vector lists{type, STANDARD_VECTOR_SIZE};
    duckdb::ListVector::Reserve(lists, kElements + 1);
    auto* entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
    auto& child = duckdb::ListVector::GetChildMutable(lists);
    auto* values = duckdb::FlatVector::GetDataMutable<int64_t>(child);
    duckdb::idx_t element = 0;
    for (duckdb::idx_t row = 0; row < kRows; ++row) {
      const auto list = row % kLists;
      const auto length = kShortest + list;
      entries[row] = duckdb::list_entry_t{element, length};
      for (duckdb::idx_t j = 0; j < length; ++j, ++element) {
        values[reversed ? kElements - 1 - element : element] =
          static_cast<int64_t>(list * 1000 + j);
      }
    }
    ASSERT_EQ(element, kElements);
    if (form == Form::Nullable) {
      entries[kRows] = duckdb::list_entry_t{kElements, 1};
      values[kElements] = 0;
      auto& validity = duckdb::FlatVector::ValidityMutable(child);
      validity.Reset(kElements + 1);
      validity.SetInvalid(kElements);
      ++element;
    }
    duckdb::ListVector::SetListSize(lists, element);
    duckdb::FlatVector::SetSize(lists, rows);
    if (reversed) {
      duckdb::SelectionVector backwards{kElements};
      for (duckdb::idx_t i = 0; i < kElements; ++i) {
        backwards.set_index(i, kElements - 1 - i);
      }
      child.Slice(backwards, kElements);
    }
    ingest.Add(lists, 0, rows);
  };
  add(Form::Flat);
  add(Form::Selected);
  add(Form::Nullable);
  const auto parts = ingest.Take();
  ASSERT_EQ(parts.codes.size(), 3 * kRows + 1);
  EXPECT_EQ(parts.distinct, kLists + 1);
  for (duckdb::idx_t row = 0; row < 3 * kRows; ++row) {
    ASSERT_EQ(parts.codes[row], row % kRows % kLists) << "row " << row;
  }
  EXPECT_EQ(parts.codes.back(), kLists);
  EXPECT_EQ(parts.elem_count,
            kLists * kShortest + kLists * (kLists - 1) / 2 + 1);
}

TEST_F(ColumnReaderTest, RepeatedListRowGroupsKeepTheirCodes) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "kseg", 63, type,
              "SELECT CASE WHEN i < 4096 THEN ['u' || i] ELSE ['r' || (i % 5)] "
              "END FROM range(8192) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "kseg");
  const auto* col = r.Column(63);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), 2u);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected, Rows{{0, 4095, 4096, 4097, 8191}},
                 false);
}

void ScanCorruptedLists(duckdb::DatabaseInstance& db, std::string_view segment,
                        std::span<const uint64_t> stored, size_t word,
                        uint64_t value, const char* error) {
  constexpr irs::field_id kId = 71;
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  {
    duckdb::Connection con{db};
    ASSERT_FALSE(
      con.Query("SET force_compression = 'uncompressed'")->HasError());
    WriteViaSql(db, dir, segment, kId, type,
                "SELECT ['r' || (i % 5), 'x'] FROM range(4096) t(i)", 4096,
                expected);
    con.Query("SET force_compression = 'auto'");
  }
  const auto file = irs::FileName(segment);
  const auto whole = dir.ReadWhole(file);
  std::string bytes{reinterpret_cast<const char*>(whole.data()), whole.size()};
  std::string needle(stored.size() * sizeof(uint64_t), '\0');
  std::memcpy(needle.data(), stored.data(), needle.size());
  const auto at = bytes.find(needle);
  ASSERT_NE(at, std::string::npos);
  std::memcpy(bytes.data() + at + word * sizeof(uint64_t), &value,
              sizeof(value));
  {
    auto out = dir.Create(file);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(bytes.data()),
                   bytes.size());
    out->Close();
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, segment);
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
#ifdef SDB_DEV
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(ScanValues(*col, r.Ctx()), error);
#else
  EXPECT_ANY_THROW(ScanValues(*col, r.Ctx()));
#endif
}

TEST_F(ColumnReaderTest, CorruptedListCodesAreRejected) {
  constexpr uint64_t kCodes[] = {0, 1, 2, 3, 4, 0, 1, 2, 3, 4};
  ScanCorruptedLists(Db(), "cseg", kCodes, 7, 1000, "corrupted list codes");
}

TEST_F(ColumnReaderTest, CorruptedListEndsAreRejected) {
  constexpr uint64_t kEnds[] = {2, 4, 6, 8, 10};
  ScanCorruptedLists(Db(), "eseg", kEnds, 2, 1, "corrupted list ends");
}

TEST_F(ColumnReaderTest, ListEndsPastTheElementsAreRejected) {
  constexpr uint64_t kEnds[] = {2, 4, 6, 8, 10};
  ScanCorruptedLists(Db(), "pseg", kEnds, 4, 1000, "corrupted list ends");
}

void OpenWithCorruptedFooter(duckdb::DatabaseInstance& db, const char* error,
                             auto corrupt) {
  constexpr irs::field_id kId = 1;
  constexpr duckdb::idx_t kRows = 100;
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{db, dir.Tracker("fseg")};
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT);
    duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(v);
    for (duckdb::idx_t i = 0; i < kRows; ++i) {
      data[i] = static_cast<int64_t>(i);
    }
    duckdb::FlatVector::SetSize(v, kRows);
    cw.Append(v, kRows);
    w.Commit(kRows);
  }
  const auto file = irs::FileName("fseg");
  std::string bytes;
  std::vector<irs::ColumnMeta> metas;
  {
    const auto in = dir.Open(file, duckdb::MappedFileAccess::SEQUENTIAL);
    const auto stored = irs::format_utils::ReadFooter(
      *in, file, dir.Sizes("fseg", irs::kColFile),
      [&](duckdb::BinaryDeserializer& footer, uint64_t) {
        footer.Set<duckdb::DatabaseInstance&>(db);
        footer.ReadOptionalList(
          irs::kColFieldColumns, "columns",
          [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
            list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
              metas.emplace_back(irs::DeserializeColumnMeta(obj));
            });
          });
        footer.Unset<duckdb::DatabaseInstance>();
      });
    bytes.resize(stored.data_len);
    duckdb::InputFileReader reader{*in};
    reader.ReadData(reinterpret_cast<irs::byte_type*>(bytes.data()),
                    bytes.size());
  }
  ASSERT_EQ(metas.size(), 1);
  corrupt(metas.front());
  {
    auto out = dir.Create(file);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(bytes.data()),
                   bytes.size());
    const auto footer = irs::format_utils::WriteFooter(
      *out, [&](duckdb::BinarySerializer& footer) {
        footer.WriteList(
          irs::kColFieldColumns, "columns", metas.size(),
          [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
            list.WriteObject([&](duckdb::BinarySerializer& obj) {
              irs::SerializeColumnMeta(obj, metas[i]);
            });
          });
      });
    dir.Adopt("fseg", irs::kColFile,
              {.file = out->Position(), .footer = footer});
    out->Close();
  }
  try {
    irs::tests::OpenColReader(dir, "fseg");
    ADD_FAILURE() << "a footer with " << error << " was accepted";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED);
    EXPECT_NE(std::string_view{e.message()}.find(error), std::string_view::npos)
      << e.message();
  }
}

void FetchWithCorruptedFooter(duckdb::DatabaseInstance& db,
                              const duckdb::LogicalType& type,
                              irs::codecs::CodecType codec,
                              std::string_view error, uint64_t row,
                              auto corrupt) {
  constexpr irs::field_id kId = 1;
  constexpr irs::field_id kPad = 2;
  constexpr duckdb::idx_t kRows = STANDARD_VECTOR_SIZE;
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{db, dir.Tracker("cseg")};
    auto& cw = w.OpenColumn(kId, type, {.compression = codec});
    duckdb::Vector v{type, STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < kRows; ++i) {
      v.SetValue(i, duckdb::Value::BIGINT(static_cast<int64_t>(i % 1000))
                      .DefaultCastAs(type));
    }
    duckdb::FlatVector::SetSize(v, kRows);
    cw.Append(v, kRows);
    cw.Finish();
    auto& pad =
      w.OpenColumn(kPad, duckdb::LogicalType::BIGINT,
                   {.compression = irs::codecs::CodecType::Uncompressed});
    for (int64_t chunk = 0; chunk < 64; ++chunk) {
      duckdb::Vector p{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(p);
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
        data[i] = static_cast<int64_t>((chunk * STANDARD_VECTOR_SIZE + i) *
                                       0x9E3779B97F4A7C15ULL);
      }
      duckdb::FlatVector::SetSize(p, STANDARD_VECTOR_SIZE);
      pad.Append(p, STANDARD_VECTOR_SIZE);
    }
    w.Commit(kRows);
  }
  const auto file = irs::FileName("cseg");
  std::string bytes;
  std::vector<irs::ColumnMeta> metas;
  {
    const auto in = dir.Open(file, duckdb::MappedFileAccess::SEQUENTIAL);
    const auto stored = irs::format_utils::ReadFooter(
      *in, file, dir.Sizes("cseg", irs::kColFile),
      [&](duckdb::BinaryDeserializer& footer, uint64_t) {
        footer.Set<duckdb::DatabaseInstance&>(db);
        footer.ReadOptionalList(
          irs::kColFieldColumns, "columns",
          [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
            list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
              metas.emplace_back(irs::DeserializeColumnMeta(obj));
            });
          });
        footer.Unset<duckdb::DatabaseInstance>();
      });
    bytes.resize(stored.data_len);
    duckdb::InputFileReader reader{*in};
    reader.ReadData(reinterpret_cast<irs::byte_type*>(bytes.data()),
                    bytes.size());
  }
  ASSERT_EQ(metas.size(), 2);
  ASSERT_EQ(metas.front().id, kId);
  ASSERT_EQ(magic_enum::enum_name(metas.front().data.front().codec->type),
            magic_enum::enum_name(codec));
  corrupt(metas.front());
  {
    auto out = dir.Create(file);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(bytes.data()),
                   bytes.size());
    const auto footer = irs::format_utils::WriteFooter(
      *out, [&](duckdb::BinarySerializer& footer) {
        footer.WriteList(
          irs::kColFieldColumns, "columns", metas.size(),
          [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
            list.WriteObject([&](duckdb::BinarySerializer& obj) {
              irs::SerializeColumnMeta(obj, metas[i]);
            });
          });
      });
    dir.Adopt("cseg", irs::kColFile,
              {.file = out->Position(), .footer = footer});
    out->Close();
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "cseg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  try {
    irs::ColumnReader::PointReader cursor{r, *col};
    duckdb::Vector out{type, 1};
    cursor.FetchRow(row, out, 0);
    ADD_FAILURE() << "the point read accepted a corrupted footer";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
    EXPECT_EQ(e.message(), error);
  }
}

TEST_F(ColumnReaderTest, FooterBlocksWithoutBytesAreIndexCorruption) {
  FetchWithCorruptedFooter(
    Db(), duckdb::LogicalType::BIGINT, irs::codecs::CodecType::Bitpacking,
    "Corrupted BitPacking segment: segment has no bytes", 10,
    [](irs::ColumnMeta& meta) { meta.data.front().byte_size = 0; });
}

TEST_F(ColumnReaderTest, FooterRowCountsBeyondUncompressedBlocks) {
  constexpr uint64_t kCount = (uint64_t{1} << 61) + 512;
  FetchWithCorruptedFooter(
    Db(), duckdb::LogicalType::BIGINT, irs::codecs::CodecType::Uncompressed,
    "Corrupted uncompressed segment: rows exceed the segment size", 100000,
    [&](irs::ColumnMeta& meta) {
      meta.data.front().tuple_count = kCount;
      meta.validity.front().tuple_count = kCount;
    });
}

TEST_F(ColumnReaderTest, FooterRowCountsBeyondAlpBlocks) {
  constexpr uint64_t kCount = std::numeric_limits<uint64_t>::max();
  FetchWithCorruptedFooter(
    Db(), duckdb::LogicalType::DOUBLE, irs::codecs::CodecType::Alp,
    "Corrupted ALP segment: metadata offset table exceeds the segment",
    10'000'000, [&](irs::ColumnMeta& meta) {
      meta.data.front().tuple_count = kCount;
      meta.validity.front().tuple_count = kCount;
    });
}

TEST_F(ColumnReaderTest, FooterRowCountsBeyondAlpRdBlocks) {
  constexpr uint64_t kCount = std::numeric_limits<uint64_t>::max();
  FetchWithCorruptedFooter(
    Db(), duckdb::LogicalType::DOUBLE, irs::codecs::CodecType::Alprd,
    "Corrupted ALPRD segment: metadata offset table exceeds the segment",
    10'000'000, [&](irs::ColumnMeta& meta) {
      meta.data.front().tuple_count = kCount;
      meta.validity.front().tuple_count = kCount;
    });
}

TEST_F(ColumnReaderTest, FooterBlocksOutsideTheFileAreRefusedAsCorrupted) {
  constexpr auto kError = "column data on column id 1 out of range";
  OpenWithCorruptedFooter(Db(), kError, [](irs::ColumnMeta& meta) {
    auto& block = meta.data.front();
    block.file_offset =
      std::numeric_limits<uint64_t>::max() - block.byte_size + 1;
  });
  OpenWithCorruptedFooter(Db(), kError, [](irs::ColumnMeta& meta) {
    meta.data.front().byte_size = std::numeric_limits<uint64_t>::max();
  });
  OpenWithCorruptedFooter(Db(), kError, [](irs::ColumnMeta& meta) {
    meta.data.front().shared_offset = std::numeric_limits<uint64_t>::max();
  });
}

void WriteNormRegion(duckdb::BinarySerializer& s,
                     const irs::NormRegionMeta& r) {
  s.WriteProperty(0, "rows", r.stats.rows);
  s.WriteProperty(1, "sum", r.stats.sum);
  s.WriteProperty(2, "non_zero", r.stats.non_zero);
  s.WriteProperty(3, "wide8", r.stats.wide8);
  s.WriteProperty(4, "wide16", r.stats.wide16);
  s.WriteProperty(5, "min", r.stats.min);
  s.WriteProperty(6, "max", r.stats.max);
  s.WriteProperty(7, "bits", r.bits);
  if (r.bits == 0) {
    s.WriteProperty(8, "value", r.value);
    return;
  }
  s.WriteProperty(9, "file_offset", r.file_offset);
  s.WritePropertyWithDefault<uint32_t>(10, "exceptions", r.exceptions, 0);
  if (r.exceptions != 0) {
    s.WriteProperty(11, "overflow", r.overflow);
    s.WriteProperty(12, "shift", r.shift);
    s.WriteProperty(13, "exception_bytes", r.exception_bytes);
    s.WriteProperty(14, "table_offset", r.table_offset);
  }
}

void OpenWithFooter(duckdb::DatabaseInstance& db, std::string_view error,
                    const std::vector<irs::NormColumnSpec>& norms,
                    const std::vector<uint64_t>& starts) {
  constexpr size_t kData = 8192;
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  {
    auto out = dir.Create(irs::FileName("nseg"));
    const std::string data(kData, '\0');
    out->WriteData(reinterpret_cast<const irs::byte_type*>(data.data()),
                   data.size());
    const auto footer = irs::format_utils::WriteFooter(
      *out, [&](duckdb::BinarySerializer& footer) {
        footer.WriteOptionalList(
          irs::kColFieldNormColumns, "norm_columns", norms.size(),
          [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
            list.WriteObject([&](duckdb::BinarySerializer& obj) {
              obj.WriteProperty(0, "id", static_cast<uint64_t>(norms[i].id));
              obj.WriteList(
                1, "regions", norms[i].meta.regions.size(),
                [&](duckdb::BinarySerializer::List& regions, duckdb::idx_t k) {
                  regions.WriteObject([&](duckdb::BinarySerializer& region) {
                    WriteNormRegion(region, norms[i].meta.regions[k]);
                  });
                });
            });
          });
        footer.WriteOptionalList(
          irs::kColFieldRowGroupStarts, "row_group_starts", starts.size(),
          [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
            list.WriteElement(starts[i]);
          });
      });
    dir.Adopt("nseg", irs::kColFile,
              {.file = out->Position(), .footer = footer});
    out->Close();
  }
  if (error.empty()) {
    const irs::ColReader r = irs::tests::OpenColReader(dir, "nseg");
    for (const auto& norm : norms) {
      EXPECT_NE(r.NormColumn(norm.id), nullptr);
    }
    return;
  }
  try {
    irs::tests::OpenColReader(dir, "nseg");
    ADD_FAILURE() << "a footer with " << error << " was accepted";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED);
    EXPECT_NE(std::string_view{e.message()}.find(error), std::string_view::npos)
      << e.message();
  }
}

TEST_F(ColumnReaderTest, FooterNormsAndRowGroupStartsOutOfShapeAreCorrupted) {
  constexpr irs::field_id kId = 7;
  const auto region = [](uint8_t bits) {
    irs::NormRegionMeta r;
    r.stats.rows = 100;
    r.stats.min = 1;
    r.stats.max = bits == 0 ? 1 : 200;
    r.bits = bits;
    r.value = 1;
    return r;
  };
  const auto excepted = [&](uint8_t exception_bytes) {
    auto r = region(8);
    r.exceptions = 1;
    r.exception_bytes = exception_bytes;
    r.table_offset = 1024;
    return r;
  };
  const auto norm = [&](std::vector<irs::NormRegionMeta> regions) {
    irs::NormColumnSpec spec{.id = kId};
    for (const auto& r : regions) {
      spec.meta.row_count += r.stats.rows;
    }
    spec.meta.regions = std::move(regions);
    return std::vector<irs::NormColumnSpec>{std::move(spec)};
  };
  for (const uint8_t bits : {0, 8, 16, 32}) {
    SCOPED_TRACE(static_cast<int>(bits));
    OpenWithFooter(Db(), "", norm({region(bits)}), {0, 100});
  }
  for (const uint8_t bytes : {2, 4}) {
    OpenWithFooter(Db(), "", norm({excepted(bytes)}), {});
  }
  constexpr std::string_view kRegion = "norm region on column id 7 is corrupt";
  auto r = region(8);
  r.stats.rows = 0;
  OpenWithFooter(Db(), kRegion, norm({r}), {});
  r = region(8);
  r.stats.rows = uint64_t{irs::doc_limits::kMaxDocs} + 1;
  OpenWithFooter(Db(), kRegion, norm({r}), {});
  r = region(8);
  r.stats.min = 300;
  OpenWithFooter(Db(), kRegion, norm({r}), {});
  OpenWithFooter(Db(), kRegion, norm({region(1)}), {});
  r = region(8);
  r.file_offset = 8190;
  OpenWithFooter(Db(), "norm data on column id 7 out of range", norm({r}), {});
  constexpr std::string_view kExceptions =
    "norm exceptions on column id 7 are corrupt";
  r = excepted(2);
  r.bits = 32;
  OpenWithFooter(Db(), kExceptions, norm({r}), {});
  r = excepted(2);
  r.exceptions = 101;
  OpenWithFooter(Db(), kExceptions, norm({r}), {});
  r = excepted(2);
  r.overflow = 2;
  OpenWithFooter(Db(), kExceptions, norm({r}), {});
  r = excepted(2);
  r.shift = irs::kNormMaxShift + 1;
  OpenWithFooter(Db(), kExceptions, norm({r}), {});
  OpenWithFooter(Db(), kExceptions, norm({excepted(3)}), {});
  r = excepted(2);
  r.table_offset = 8190;
  OpenWithFooter(Db(), "norm exceptions on column id 7 out of range", norm({r}),
                 {});
  OpenWithFooter(Db(), "norm column id 7 holds 0 rows across 0 regions",
                 norm({}), {});
  r = region(0);
  r.stats.rows = irs::doc_limits::kMaxDocs;
  OpenWithFooter(Db(), "norm column id 7 holds", norm({r, r}), {});
  constexpr std::string_view kStarts =
    "row group starts are not increasing from 0";
  OpenWithFooter(Db(), kStarts, norm({region(0)}), {1});
  OpenWithFooter(Db(), kStarts, norm({region(0)}), {0, 0});
  auto twice = norm({region(0)});
  twice.push_back(twice.front());
  OpenWithFooter(Db(), "duplicate norm field_id 7", twice, {});
}

TEST_F(ColumnReaderTest, ListsTurningRepetitiveAreDeduplicatedAgain) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "aseg", 64, type,
    "SELECT CASE WHEN i % 17 = 0 THEN NULL WHEN i < 4096 THEN ['u' || "
    "i] ELSE ['r' || (i % 5), 'x'] END FROM range(65536) t(i)",
    32768, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "aseg");
  const auto* col = r.Column(64);
  ASSERT_NE(col, nullptr);
  EXPECT_LT(col->Child()->RowCount(), 40000u);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 4095, 4096, 4097, 9000, 40000, 65535}}, false);
}

TEST_F(ColumnReaderTest, StructListsTurningRepetitiveAreDeduplicatedAgain) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"tags", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "bseg", 65, type,
              "SELECT {'tags': CASE WHEN i % 17 = 0 THEN NULL WHEN i < 4096 "
              "THEN ['u' || i] ELSE ['r' || (i % 5), 'x'] END} FROM "
              "range(32768) t(i)",
              32768, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "bseg");
  const auto* col = r.Column(65);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 4095, 4096, 4097, 9000, 20000, 32767}}, false);
}

TEST_F(ColumnReaderTest, RepeatedWideMapsAcrossRowGroups) {
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "wseg", 44, type,
              "SELECT CASE WHEN i % 101 = 0 THEN NULL WHEN i % 13 = 0 THEN "
              "map([], [])::MAP(VARCHAR, VARCHAR) ELSE map_from_entries("
              "list_transform(range(8), lambda k: {'key': 'attribute.' || k, "
              "'value': 'v' || ((i // 37) % 300) || '-' || k})) END "
              "FROM range(30000) t(i)",
              12288, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "wseg");
  const auto* col = r.Column(44);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 30000u);
  EXPECT_LT(col->Child()->RowCount(), ElementCount(expected) / 10);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 36, 37, 12287, 12288, 29999}}, false);
}

TEST_F(ColumnReaderTest, ListsLongerThanAVector) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "lseg", 45, type,
    "SELECT CASE WHEN i % 9 = 0 THEN list_transform(range(3000 + i "
    "% 5), lambda k: 'e' || k) WHEN i % 9 = 1 THEN NULL ELSE ['a' || (i "
    "% 4), 'b'] END FROM range(20000) t(i)",
    8192, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
  const auto* col = r.Column(45);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected, Rows{{0, 1, 2, 9, 8191, 19998}},
                 false);
}

TEST_F(ColumnReaderTest, EmptyMapsOnly) {
  const auto type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR,
                                             duckdb::LogicalType::VARCHAR);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "eseg", 46, type,
              "SELECT CASE WHEN i % 5 = 0 THEN NULL ELSE map([], "
              "[])::MAP(VARCHAR, VARCHAR) END FROM range(10000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "eseg");
  const auto* col = r.Column(46);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 10000u);
  EXPECT_EQ(col->Child()->RowCount(), 0u);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected, Rows{{0, 1, 4095, 4096, 9999}},
                 false);
}

// VARIANT, shredding disabled: many unshredded row groups round-trip.
TEST_F(ColumnReaderTest, VariantRoundTripUnshredded) {
  SetShreddingSize(Db(), -1);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/30,
                     "SELECT i::VARIANT FROM range(200) t(i)", /*rg_size=*/64,
                     expected);
  ASSERT_EQ(expected.size(), 200u);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(30);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->Type().id(), duckdb::LogicalTypeId::VARIANT);
  ASSERT_EQ(col->RowCount(), 200u);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

void TextChoices(const irs::ColumnReader& col, irs::ReadContext& ctx,
                 std::vector<std::string>& choices) {
  if (col.Type().InternalType() == duckdb::PhysicalType::VARCHAR) {
    for (size_t block = 0; block != col.DataBlocks().size(); ++block) {
      const auto segment = col.OpenSegment(block, ctx);
      auto info = segment->function.get_segment_info(*segment);
      choices.push_back(
        absl::StrCat(magic_enum::enum_name(col.DataBlocks()[block].codec->type),
                     " level ", info["level"], " sorted ", info["sorted"]));
    }
  }
  col.VisitSubColumns([&](const irs::ColumnReader::SubColumn& sub) {
    TextChoices(sub.column, ctx, choices);
  });
}

TEST_F(ColumnReaderTest,
       VariantRowGroupsRepeatTheirStringChoiceUntilTheTextGrows) {
  constexpr irs::field_id kId = 31;
  constexpr uint32_t kRowGroup = 16384;
  SetShreddingSize(Db(), -1);
  {
    duckdb::Connection con{Db()};
    auto set = con.Query("SET disabled_compression_methods = 'fsst,dict_fsst'");
    ASSERT_FALSE(set->HasError()) << set->GetError();
  }
  const std::string clustered =
    "repeat(md5(((hash(i) % 4000) // 4)::VARCHAR), 6) || '/' || "
    "((hash(i) % 4000) % 4)";
  const std::string tenants = "'tenant-' || ((i * 7919) % 3000) || '/item'";
  const auto choices = [&](const std::string& first, const std::string& then,
                           uint64_t groups) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    std::vector<duckdb::Value> expected;
    WriteVariantViaSql(
      Db(), dir, "vtune", kId,
      absl::StrCat("SELECT (CASE WHEN i < ", kRowGroup, " THEN ", first,
                   " ELSE ", then, " END)::VARIANT FROM range(",
                   groups * kRowGroup, ") t(i)"),
      kRowGroup, expected);
    EXPECT_EQ(expected.size(), groups * kRowGroup);
    irs::ColReader r = irs::tests::OpenColReader(dir, "vtune");
    const auto* col = r.Column(kId);
    EXPECT_NE(col, nullptr);
    ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
    std::vector<std::string> found;
    TextChoices(*col, r.Ctx(), found);
    return found;
  };
  const auto clustered_alone = choices(clustered, clustered, 1);
  const auto tenants_alone = choices(tenants, tenants, 1);
  ASSERT_EQ(clustered_alone.size(), 1u)
    << ::testing::PrintToString(clustered_alone);
  ASSERT_EQ(tenants_alone.size(), 1u)
    << ::testing::PrintToString(tenants_alone);
  ASSERT_NE(clustered_alone, tenants_alone);
  EXPECT_EQ(choices(tenants, clustered, 4),
            (std::vector{tenants_alone[0], clustered_alone[0],
                         clustered_alone[0], clustered_alone[0]}));
  EXPECT_EQ(choices(clustered, tenants, 4), std::vector(4, clustered_alone[0]));
}

TEST_F(ColumnReaderTest, VariantRowGroupsOfOneShapeShredOnOneType) {
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  irs::ColWriter w{Db(), dir.Tracker("vshape"), irs::WriteTier::Merge};
  auto& cw = w.OpenColumn(/*id=*/36, duckdb::LogicalType::VARIANT(),
                          {.row_group_size = 256});
  duckdb::Connection con{Db()};
  auto result = con.Query(
    "SELECT {'f': i, 'e': 'e' || i, 'd': i % 7, 'c': 'c' || (i % 13), "
    "'b': i * 3, 'a': 'a' || i}::VARIANT FROM range(8192) t(i)");
  ASSERT_FALSE(result->HasError()) << result->GetError();
  while (auto chunk = result->Fetch()) {
    cw.Append(chunk->data[0], chunk->size());
  }
  const auto& rgs = cw.Meta().variant_rgs;
  ASSERT_GT(rgs.size(), 16u);
  for (const auto& rg : rgs) {
    ASSERT_TRUE(rg.shredded);
    EXPECT_EQ(rg.shredded->type, rgs.front().shredded->type)
      << rg.shredded->type.ToString();
  }
  ASSERT_TRUE(w.Commit(0));
}

// VARIANT, shredding forced on: shredded row groups reconstruct via unshred.
TEST_F(ColumnReaderTest, VariantRoundTripShredded) {
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/31,
                     "SELECT {'a': i, 'b': ('v' || i)}::VARIANT "
                     "FROM range(500) t(i)",
                     /*rg_size=*/256, expected);
  ASSERT_EQ(expected.size(), 500u);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(31);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 500u);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, SubColumnsOfAVariantAreVisitedRowGroupByRowGroup) {
  constexpr irs::field_id kId = 34;
  constexpr uint32_t kRowGroup = 256;
  constexpr uint64_t kRows = 600;
  SetShreddingSize(Db(), kRowGroup);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", kId,
                     absl::StrCat("SELECT md5(i::VARCHAR)::VARIANT FROM range(",
                                  kRows, ") t(i)"),
                     kRowGroup, expected);
  ASSERT_EQ(expected.size(), kRows);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->Validity(), nullptr);
  ASSERT_TRUE(col->DataBlocks().empty());
  std::vector<std::string> sub_columns;
  col->VisitSubColumns([&](const irs::ColumnReader::SubColumn& sub) {
    sub_columns.emplace_back(absl::StrCat(sub.index, "@", sub.first_row));
  });
  EXPECT_EQ((std::vector<std::string>{"1@0", "2@0", "1@256", "2@256", "1@512"}),
            sub_columns);
}

// VARIANT with NULL rows + leftover (mixed types) under shredding: exercises
// Partial/Full row groups and the VARIANT-level null mask.
TEST_F(ColumnReaderTest, VariantNullAndLeftover) {
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/32,
                     "SELECT CASE WHEN i % 3 = 0 THEN NULL "
                     "WHEN i % 7 = 0 THEN ('str_' || i)::VARIANT "
                     "ELSE i::VARIANT END FROM range(500) t(i)",
                     /*rg_size=*/128, expected);
  ASSERT_EQ(expected.size(), 500u);
  ASSERT_TRUE(expected[0].IsNull());

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(32);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

// Sparse Gather of a VARIANT column across row-group boundaries (forces the
// SeekVariantRg skip-to-position path), with shredding on.
TEST_F(ColumnReaderTest, VariantGatherSparse) {
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(
    Db(), dir, "vseg", /*id=*/33,
    "SELECT CASE WHEN i % 5 = 0 THEN NULL "
    "ELSE {'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT END "
    "FROM range(600) t(i)",
    /*rg_size=*/128, expected);
  ASSERT_EQ(expected.size(), 600u);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(33);
  ASSERT_NE(col, nullptr);

  Rows rows{{0, 1, 2, 130, 131, 256, 257, 258, 400, 599}};
  auto state = col->InitScan(r.Ctx());
  duckdb::Vector result{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(*col, state, rows, result, /*out_offset=*/0);
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto g = rows[i];
    const auto got = result.GetValue(i);
    EXPECT_EQ(got.IsNull(), expected[g].IsNull()) << "gathered row " << g;
    if (!expected[g].IsNull()) {
      EXPECT_EQ(got.ToString(), expected[g].ToString()) << "gathered row " << g;
    }
  }
}

size_t PinnedSegments(const irs::ColumnReader::ScanState& s) {
  auto n = s.segments.size();
  for (const auto& child : s.child_states) {
    n += PinnedSegments(child);
  }
  if (s.variant) {
    for (const auto& rg : s.variant->rgs) {
      for (const auto* state :
           {rg.unshredded.get(), rg.shredded.get(), rg.leaf.get()}) {
        if (state) {
          n += PinnedSegments(*state);
        }
      }
    }
  }
  return n;
}

TEST_F(ColumnReaderTest, VariantGathersReleaseBlocks) {
  SetShreddingSize(Db(), -1);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/35,
                     "SELECT (SELECT string_agg(md5((i * 8 + j)::VARCHAR), '') "
                     "FROM range(8) u(j))::VARIANT FROM range(20000) t(i)",
                     /*rg_size=*/8192, expected);
  ASSERT_EQ(expected.size(), 20000u);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(35);
  ASSERT_NE(col, nullptr);

  auto state = col->InitScan(r.Ctx());
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  sel.set_index(0, 0);
  constexpr uint64_t kRowGroup = 8192;
  size_t first = 0;
  for (uint64_t row = 0; row < expected.size(); row += 97) {
    duckdb::Vector out{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
    col->GatherScatter(state, row, sel, 1, out, 0);
    ASSERT_EQ(out.GetValue(0).ToString(), expected[row].ToString())
      << "row=" << row;
    const auto pinned = PinnedSegments(state);
    if (row == 0) {
      first = pinned;
    }
    if (row % kRowGroup >= 97) {
      EXPECT_LE(pinned, first + 1) << "row=" << row;
    }
  }
}

TEST_F(ColumnReaderTest, VariantGatherDenseThenAppend) {
  SetShreddingSize(Db(), -1);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", 36,
                     "SELECT ('v' || i)::VARIANT FROM range(4000) t(i)", 1024,
                     expected);
  ASSERT_EQ(expected.size(), 4000u);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(36);
  ASSERT_NE(col, nullptr);

  auto state = col->InitScan(r.Ctx());
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  for (duckdb::idx_t i = 0; i < 100; ++i) {
    sel.set_index(i, i * 3);
  }
  duckdb::Vector dense{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
  col->GatherDense(state, 1000, sel, 100, 298, dense);
  for (duckdb::idx_t i = 0; i < 100; ++i) {
    ASSERT_EQ(dense.GetValue(i).ToString(), expected[1000 + i * 3].ToString())
      << "i=" << i;
  }

  duckdb::Vector out{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
  col->GatherScatter(state, 2000, sel, 50, out, 0);
  col->GatherScatter(state, 3000, sel, 50, out, 50);
  for (duckdb::idx_t i = 0; i < 50; ++i) {
    ASSERT_EQ(out.GetValue(i).ToString(), expected[2000 + i * 3].ToString())
      << "i=" << i;
    ASSERT_EQ(out.GetValue(50 + i).ToString(),
              expected[3000 + i * 3].ToString())
      << "i=" << i;
  }
}

// Per-column distinct statistics: built from the rows at seal time, persisted
// in the footer, recovered + queryable on read.
TEST_F(ColumnReaderTest, DistinctStatistics) {
  constexpr uint64_t kRows = 20000;
  constexpr uint64_t kDistinct = 1000;  // values cycle 0..999
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kExact = 40;
  constexpr irs::field_id kSampled = 41;
  constexpr irs::field_id kNone = 42;

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& exact = w.OpenColumn(kExact, duckdb::LogicalType::BIGINT,
                               {.skip_validity = true,
                                .row_group_size = kRgSize,
                                .distinct = irs::DistinctStats::Exact});
    auto& sampled = w.OpenColumn(kSampled, duckdb::LogicalType::BIGINT,
                                 {.skip_validity = true,
                                  .row_group_size = kRgSize,
                                  .distinct = irs::DistinctStats::Sampled});
    auto& none =
      w.OpenColumn(kNone, duckdb::LogicalType::BIGINT,
                   {.skip_validity = true, .row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        d[k] = static_cast<int64_t>((pos + k) % kDistinct);
      }
      duckdb::FlatVector::SetSize(v, take);
      exact.Append(v, take);
      sampled.Append(v, take);
      none.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  ASSERT_NE(r.Column(kNone), nullptr);
  EXPECT_EQ(r.Column(kNone)->Distinct(), nullptr);
  // A generous band checks the statistics are built, persisted and recovered
  // as a real distinct counter, not the sketch's precision: no dedup gives
  // ~kRows, an empty or unserialized sketch ~0.
  const auto in_band = [](uint64_t estimate) {
    return estimate >= kDistinct / 2 && estimate <= kDistinct * 2;
  };
  ASSERT_NE(r.Column(kExact), nullptr);
  const auto* exact = r.Column(kExact)->Distinct();
  ASSERT_NE(exact, nullptr);
  EXPECT_EQ(exact->total_count.load(), kRows);
  EXPECT_EQ(exact->sample_count.load(), kRows);
  EXPECT_TRUE(in_band(exact->GetCount())) << exact->GetCount();
  ASSERT_NE(r.Column(kSampled), nullptr);
  const auto* sampled = r.Column(kSampled)->Distinct();
  ASSERT_NE(sampled, nullptr);
  EXPECT_EQ(sampled->total_count.load(), kRows);
  EXPECT_GT(sampled->sample_count.load(), 0);
  EXPECT_LT(sampled->sample_count.load(), kRows);
  EXPECT_TRUE(in_band(sampled->GetCount())) << sampled->GetCount();
}

// Norm column: per-doc uint32 FTS norm, bit-packed per row group, with sparse
// docs (gaps zero-padded to target_row), across multiple row groups.
TEST_F(ColumnReaderTest, NormColumnRoundTrip) {
  constexpr uint64_t kDocs = 5000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kN = 50;

  auto has_norm = [](uint64_t d) { return d % 3 == 0; };
  auto norm_val = [](uint64_t d) { return static_cast<uint32_t>(d * 13 + 1); };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& nw = w.OpenNormColumn(kN, kRgSize);
    for (uint64_t d = 0; d < kDocs; ++d) {
      if (has_norm(d)) {
        nw.Append(d, norm_val(d));
      }
    }
    w.Commit(kDocs);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* nr = r.NormColumn(kN);
  ASSERT_NE(nr, nullptr);
  ASSERT_EQ(nr->RowCount(), kDocs);
  const auto reader = irs::MakePersistedNormReader(*nr);
  for (uint64_t d = 0; d < kDocs; ++d) {
    const auto expected = has_norm(d) ? norm_val(d) : 0u;
    EXPECT_EQ(
      reader->Get(static_cast<irs::doc_id_t>(d + irs::doc_limits::min())),
      expected)
      << "doc " << d;
  }
}

// BlobPointReader: point-read raw bytes of a BLOB column (incl null rows),
// across row-group boundaries -- the geo/wildcard exact-match access path.
TEST_F(ColumnReaderTest, BlobPointReaderFetch) {
  constexpr uint64_t kRows = 4000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kB = 70;

  auto is_valid = [](uint64_t g) { return g % 9 != 0; };
  auto blob_val = [](uint64_t g) -> std::string {
    return "blob_" + std::to_string(g) + "_payload";
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwB =
      w.OpenColumn(kB, duckdb::LogicalType::BLOB, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      auto& val = duckdb::FlatVector::ValidityMutable(v);
      val.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (is_valid(g)) {
          d[k] = duckdb::StringVector::AddStringOrBlob(v, blob_val(g));
        } else {
          val.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwB.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cb = r.Column(kB);
  ASSERT_NE(cb, nullptr);
  irs::ColumnReader::BlobPointReader pr{r, *cb};
  const uint64_t probes[] = {0, 1, 9, 18, 1023, 1024, 2500, 3999};
  for (const auto g : probes) {
    if (!is_valid(g)) {
      EXPECT_TRUE(pr.IsNullRow(g)) << "row " << g;
    } else {
      const auto bytes = pr.FetchRow(g);
      const std::string got{reinterpret_cast<const char*>(bytes.data()),
                            bytes.size()};
      EXPECT_EQ(got, blob_val(g)) << "row " << g;
    }
  }
}

// Repro: tiny sparse BLOB column written one row at a time (valid/null/null/
// valid), point-read each via BlobPointReader. Mirrors the index-writer prefix
// column shape; IsNullRow must be true for the null rows.
TEST_F(ColumnReaderTest, ReproSparseTinyPointRead) {
  constexpr irs::field_id kName = 70;  // dense column written first
  constexpr irs::field_id kB = 71;     // sparse column
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto append_blob = [&](irs::ColumnWriter& cw, uint64_t row,
                           std::string_view s, bool valid) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      auto& val = duckdb::FlatVector::ValidityMutable(v);
      val.SetAllValid(1);
      if (valid) {
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
          duckdb::StringVector::AddStringOrBlob(v, s.data(), s.size());
      } else {
        val.SetInvalid(0);
      }
      duckdb::FlatVector::SetSize(v, 1);
      cw.Append(row, v, 1);
    };
    // A dense column written FIRST (the index-writer "name" column shape).
    auto& cw_name = w.OpenColumn(kName, duckdb::LogicalType::BLOB);
    append_blob(cw_name, 0, "A", true);
    append_blob(cw_name, 1, "B", true);
    append_blob(cw_name, 2, "C", true);
    append_blob(cw_name, 3, "D", true);
    // The sparse column: values at rows 0 and 3, nulls at 1 and 2.
    auto& cw = w.OpenColumn(kB, duckdb::LogicalType::BLOB);
    append_blob(cw, 0, "abcd", true);
    append_blob(cw, 1, "", false);
    append_blob(cw, 2, "", false);
    append_blob(cw, 3, "abcde", true);
    w.Commit(4);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cb = r.Column(kB);
  ASSERT_NE(cb, nullptr);
  EXPECT_EQ(cb->RowCount(), 4u);
  auto fetch = [&](uint64_t row) {
    irs::ColumnReader::BlobPointReader pr{r, *cb};  // fresh reader per probe
    const auto b = pr.FetchRow(row);
    return std::string{reinterpret_cast<const char*>(b.data()), b.size()};
  };
  auto is_null = [&](uint64_t row) {
    irs::ColumnReader::BlobPointReader pr{r, *cb};
    return pr.IsNullRow(row);
  };
  EXPECT_EQ(fetch(0), "abcd");
  EXPECT_TRUE(is_null(1));
  EXPECT_TRUE(is_null(2));
  EXPECT_EQ(fetch(3), "abcde");
  EXPECT_EQ(fetch(0), "abcd");  // backward jump on a fresh reader
}

// Repro: a MULTI-ROW sparse append (one Append of a 4-row vector with embedded
// nulls, the SwitchColumn/AppendToColumn shape), point-read back. Plus a
// sibling PK col. This is the connector InsertNullsColumns write shape.
TEST_F(ColumnReaderTest, ReproMultiRowSparseAppend) {
  constexpr irs::field_id kVal = 90;
  constexpr irs::field_id kPk = 91;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    // VARCHAR column read back through a (BLOB-buffer) BlobPointReader
    // exercises the VARCHAR-vs-BLOB physical-type-equal copy path.
    auto& cw_val = w.OpenColumn(kVal, duckdb::LogicalType::VARCHAR);
    // One Append of 4 rows: foo, NULL, bar, NULL.
    duckdb::Vector vec{duckdb::LogicalType::VARCHAR, 4};
    auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec);
    auto& val = duckdb::FlatVector::ValidityMutable(vec);
    val.Reset(4);
    d[0] = duckdb::StringVector::AddString(vec, "foo");
    val.SetInvalid(1);
    d[2] = duckdb::StringVector::AddString(vec, "bar");
    val.SetInvalid(3);
    duckdb::FlatVector::SetSize(vec, 4);
    cw_val.Append(/*start_row=*/0, vec, 4);
    // Dense PK col (pk1..pk4) appended per row.
    auto& cw_pk = w.OpenColumn(kPk, duckdb::LogicalType::BLOB);
    const std::vector<std::string> pks = {"pk1", "pk2", "pk3", "pk4"};
    for (size_t i = 0; i < pks.size(); ++i) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddStringOrBlob(v, pks[i].data(), pks[i].size());
      duckdb::FlatVector::SetSize(v, 1);
      cw_pk.Append(i, v, 1);
    }
    w.Commit(4);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cval = r.Column(kVal);
  const auto* cpk = r.Column(kPk);
  ASSERT_NE(cval, nullptr);
  ASSERT_NE(cpk, nullptr);
  auto fetch_val = [&](uint64_t row) {
    irs::ColumnReader::BlobPointReader pr{r, *cval};
    const auto b = pr.FetchRow(row);
    return std::string{reinterpret_cast<const char*>(b.data()), b.size()};
  };
  auto null_val = [&](uint64_t row) {
    irs::ColumnReader::BlobPointReader pr{r, *cval};
    return pr.IsNullRow(row);
  };
  EXPECT_EQ(fetch_val(0), "foo");
  EXPECT_TRUE(null_val(1));
  EXPECT_EQ(fetch_val(2), "bar");
  EXPECT_TRUE(null_val(3));
  const std::vector<std::string> pks = {"pk1", "pk2", "pk3", "pk4"};
  for (size_t i = 0; i < pks.size(); ++i) {
    irs::ColumnReader::BlobPointReader pr{r, *cpk};
    const auto b = pr.FetchRow(i);
    EXPECT_EQ((std::string{reinterpret_cast<const char*>(b.data()), b.size()}),
              pks[i])
      << "pk row " << i;
  }
}

// Repro: a DENSE dict_fsst column (pk1..pk4) read in a segment that ALSO holds
// a SPARSE column (the InsertNullsColumns shape: PK col + nullable value col).
TEST_F(ColumnReaderTest, ReproDensePkWithSparseSibling) {
  constexpr irs::field_id kVal = 80;  // sparse, written first
  constexpr irs::field_id kPk = 81;   // dense dict_fsst
  const std::vector<std::string> pks = {"pk1", "pk2", "pk3", "pk4"};
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto put = [&](irs::ColumnWriter& cw, uint64_t row, std::string_view s,
                   bool valid) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      auto& val = duckdb::FlatVector::ValidityMutable(v);
      val.SetAllValid(1);
      if (valid) {
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
          duckdb::StringVector::AddStringOrBlob(v, s.data(), s.size());
      } else {
        val.SetInvalid(0);
      }
      duckdb::FlatVector::SetSize(v, 1);
      cw.Append(row, v, 1);
    };
    auto& cw_val = w.OpenColumn(kVal, duckdb::LogicalType::BLOB);
    put(cw_val, 0, "foo", true);
    put(cw_val, 1, "", false);
    put(cw_val, 2, "bar", true);
    put(cw_val, 3, "", false);
    auto& cw_pk = w.OpenColumn(kPk, duckdb::LogicalType::BLOB);
    for (size_t i = 0; i < pks.size(); ++i) {
      put(cw_pk, i, pks[i], true);
    }
    w.Commit(4);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cpk = r.Column(kPk);
  ASSERT_NE(cpk, nullptr);
  for (size_t i = 0; i < pks.size(); ++i) {
    irs::ColumnReader::BlobPointReader pr{r, *cpk};
    const auto b = pr.FetchRow(i);
    EXPECT_EQ((std::string{reinterpret_cast<const char*>(b.data()), b.size()}),
              pks[i])
      << "pk row " << i;
  }
}

// Repro: many short all-valid BLOB strings sharing a tight common prefix
// ("row_0".."row_499") -- the shape that triggers dict_fsst. Written densely,
// point-read scattered rows. Catches dict_fsst point-read corruption.
TEST_F(ColumnReaderTest, ReproShortPkPointRead) {
  constexpr irs::field_id kB = 72;
  constexpr uint64_t kRows = 500;
  // Length-prefixed content (WriteStr layout) like the failing test.
  auto key = [](uint64_t i) {
    const std::string s = "row_" + std::to_string(i);
    return std::string(1, static_cast<char>(s.size())) + s;
  };
  constexpr irs::field_id kInt = 73;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    // A BIGINT column written FIRST, so the BLOB column's bytes land at a
    // non-zero file offset (mirrors columns_rw_typed).
    auto& cw_int = w.OpenColumn(kInt, duckdb::LogicalType::BIGINT);
    {
      duckdb::Vector b{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(b);
      uint64_t pos = 0;
      while (pos < kRows) {
        const auto take =
          std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
        for (duckdb::idx_t k = 0; k < take; ++k) {
          d[k] = static_cast<int64_t>((pos + k) * 11 + 5);
        }
        cw_int.Append(pos, b, take);
        pos += take;
      }
    }
    auto& cw = w.OpenColumn(kB, duckdb::LogicalType::BLOB);
    // Single-row appends (the AppendBlob / PushInStaging shape), 500 of them.
    for (uint64_t i = 0; i < kRows; ++i) {
      const auto s = key(i);
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddStringOrBlob(v, s.data(), s.size());
      duckdb::FlatVector::SetSize(v, 1);
      cw.Append(i, v, 1);
    }
    w.Commit(kRows);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cb = r.Column(kB);
  ASSERT_NE(cb, nullptr);
  // Scattered point reads, a fresh reader per backward jump (the
  // columns_rw_typed access shape) -- catches dict_fsst point-read decode
  // corruption.
  for (const uint64_t i : {uint64_t{0}, uint64_t{5}, uint64_t{17}, uint64_t{99},
                           uint64_t{250}, uint64_t{499}}) {
    irs::ColumnReader::BlobPointReader pr{r, *cb};
    const auto b = pr.FetchRow(i);
    EXPECT_EQ((std::string{reinterpret_cast<const char*>(b.data()), b.size()}),
              key(i))
      << "row " << i;
  }
}

// Repro: empty column, then a column with data, then another empty column. All
// three must be present in the reader (footer round-trip of empty columns).
TEST_F(ColumnReaderTest, ReproEmptyColumnsAroundData) {
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    w.OpenColumn(0, duckdb::LogicalType::BLOB);
    auto& cw1 = w.OpenColumn(1, duckdb::LogicalType::BLOB);
    w.OpenColumn(2, duckdb::LogicalType::BLOB);
    duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
    duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
      duckdb::StringVector::AddStringOrBlob(v, "hello", 5);
    duckdb::FlatVector::SetSize(v, 1);
    cw1.Append(/*start_row=*/41, v, 1);
    w.Commit(42);
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  EXPECT_TRUE(r.HasColumn(0));
  EXPECT_TRUE(r.HasColumn(1));
  EXPECT_TRUE(r.HasColumn(2));
  ASSERT_NE(r.Column(0), nullptr);
  ASSERT_NE(r.Column(2), nullptr);
  EXPECT_EQ(r.Column(0)->RowCount(), 0u);
  EXPECT_EQ(r.Column(2)->RowCount(), 0u);
  ASSERT_NE(r.Column(1), nullptr);
  EXPECT_EQ(r.Column(1)->RowCount(), 42u);
}

// A whole row group that is entirely struct-NULL: the validity scan can yield a
// constant-NULL vector, which must short-circuit child scanning (mirror
// StructColumnData::Scan). Regression test for that path.
TEST_F(ColumnReaderTest, SegmentRoundTripStructAllNullRowGroup) {
  constexpr uint64_t kRows = 256;
  constexpr uint32_t kRgSize = 64;  // rg1 = [64,128) is all-null
  constexpr irs::field_id kT = 17;
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.emplace_back("a", duckdb::LogicalType::BIGINT);
  fields.emplace_back("b", duckdb::LogicalType::VARCHAR);
  const auto stype = duckdb::LogicalType::STRUCT(fields);

  auto s_valid = [](uint64_t g) { return g < 64 || g >= 128; };
  auto a_val = [](uint64_t g) { return static_cast<int64_t>(g); };
  auto b_val = [](uint64_t g) { return "v" + std::to_string(g); };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwT = w.OpenColumn(kT, stype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{stype, STANDARD_VECTOR_SIZE};
      auto& entries = duckdb::StructVector::GetEntries(v);
      auto* da = duckdb::FlatVector::GetDataMutable<int64_t>(entries[0]);
      auto* db =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(entries[1]);
      auto& vs = duckdb::FlatVector::ValidityMutable(v);
      auto& va = duckdb::FlatVector::ValidityMutable(entries[0]);
      auto& vb = duckdb::FlatVector::ValidityMutable(entries[1]);
      vs.Reset(STANDARD_VECTOR_SIZE);
      va.Reset(STANDARD_VECTOR_SIZE);
      vb.Reset(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        if (s_valid(g)) {
          da[k] = a_val(g);
          db[k] = duckdb::StringVector::AddString(entries[1], b_val(g));
        } else {
          // Null struct row -> null children too (so Copy skips the garbage
          // string_t in the uninitialized VARCHAR child).
          vs.SetInvalid(k);
          va.SetInvalid(k);
          vb.SetInvalid(k);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cwT.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ct = r.Column(kT);
  ASSERT_NE(ct, nullptr);
  ASSERT_EQ(ct->RowCount(), kRows);

  auto state = ct->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{stype, STANDARD_VECTOR_SIZE};
    ct->Scan(state, result, take);
    result.Flatten(take);
    auto& entries = duckdb::StructVector::GetEntries(result);
    const auto* da = duckdb::FlatVector::GetData<int64_t>(entries[0]);
    const auto& sv = duckdb::FlatVector::Validity(result);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      EXPECT_EQ(sv.RowIsValid(k), s_valid(g)) << "struct row " << g;
      if (s_valid(g)) {
        EXPECT_EQ(da[k], a_val(g)) << "a row " << g;
      }
    }
    pos += take;
  }
}

// VARIANT pushdown extract (correctness path): reconstruct the full VARIANT via
// Gather, then VariantExtract(path) + cast. This is the logic the cutover's
// MaterializeExtractNode will use (the shredded fast-path is a later opt).
std::vector<duckdb::Value> ExtractVariantPath(
  const irs::ColumnReader& col, irs::ReadContext& ctx,
  duckdb::ClientContext& dctx, std::span<const std::string_view> path,
  const duckdb::LogicalType& scan_type) {
  auto state = col.InitScan(ctx);
  duckdb::vector<duckdb::VariantPathComponent> components;
  components.reserve(path.size());
  for (const auto& f : path) {
    components.emplace_back(std::string{f});
  }
  std::vector<duckdb::Value> out;
  const auto total = col.RowCount();
  out.reserve(total);
  uint64_t pos = 0;
  while (pos < total) {
    const auto take =
      std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
    Rows ids;
    ids.v.reserve(take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      ids.v.push_back(pos + k);
    }
    duckdb::Vector variant{duckdb::LogicalType::VARIANT(),
                           STANDARD_VECTOR_SIZE};
    irs::column_internal::GatherRows(col, state, ids, variant, 0);
    duckdb::Vector extracted{duckdb::LogicalType::VARIANT(),
                             STANDARD_VECTOR_SIZE};
    duckdb::VariantUtils::VariantExtract(variant, components, extracted, take);
    duckdb::Vector* result = &extracted;
    duckdb::Vector casted{scan_type, STANDARD_VECTOR_SIZE};
    if (scan_type.id() != duckdb::LogicalTypeId::VARIANT) {
      duckdb::VectorOperations::Cast(dctx, extracted, casted, take);
      result = &casted;
    }
    for (duckdb::idx_t k = 0; k < take; ++k) {
      out.emplace_back(result->GetValue(k));
    }
    pos += take;
  }
  return out;
}

TEST_F(ColumnReaderTest, VariantExtractPath) {
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  const std::string obj = "{'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/34,
                     "SELECT " + obj + " FROM range(500) t(i)",
                     /*rg_size=*/128, ignored);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* col = r.Column(34);
  ASSERT_NE(col, nullptr);

  duckdb::Connection con{Db()};
  auto qr = con.Query("SELECT (" + obj + ").a::DOUBLE FROM range(500) t(i)");
  ASSERT_FALSE(qr->HasError()) << qr->GetError();
  std::vector<duckdb::Value> expected;
  while (auto chunk = qr->Fetch()) {
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      expected.emplace_back(chunk->GetValue(0, k));
    }
  }

  const std::vector<std::string_view> path{"a"};
  const auto actual = ExtractVariantPath(*col, r.Ctx(), *con.context, path,
                                         duckdb::LogicalType::DOUBLE);
  ExpectVariantValuesEqual(expected, actual);
}

// Mixed shredding: a threshold between the full row groups and the short
// tail leaves the tail unshredded. The shredded-leaf fast path must still
// serve the shredded row groups while the tail reconstructs, driven the same
// way the connector materializer drives it.
TEST_F(ColumnReaderTest, VariantExtractPathMixedShredding) {
  SetShreddingSize(Db(), 120);  // rgs: 128,128,128,116 -> tail unshredded
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  const std::string obj = "{'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/34,
                     "SELECT " + obj + " FROM range(500) t(i)",
                     /*rg_size=*/128, ignored);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* base = r.Column(34);
  ASSERT_NE(base, nullptr);
  ASSERT_EQ(base->Type().id(), duckdb::LogicalTypeId::VARIANT);
  const auto& col = static_cast<const irs::VariantColumnReader&>(*base);

  duckdb::Connection con{Db()};
  auto qr = con.Query("SELECT (" + obj + ").a::DOUBLE FROM range(500) t(i)");
  ASSERT_FALSE(qr->HasError()) << qr->GetError();
  std::vector<duckdb::Value> expected;
  while (auto chunk = qr->Fetch()) {
    for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
      expected.emplace_back(chunk->GetValue(0, k));
    }
  }

  const std::vector<std::string_view> path{"a"};
  auto state = col.InitScan(r.Ctx());
  duckdb::LogicalType leaf_type;
  ASSERT_TRUE(col.CachedUniformShreddedLeaf(state, path, leaf_type));
  ASSERT_EQ(leaf_type, duckdb::LogicalType::DOUBLE);
  ASSERT_TRUE(col.RgLeafOk(state, 0));
  ASSERT_TRUE(col.RgLeafOk(state, 2));
  ASSERT_FALSE(col.RgLeafOk(state, 3));

  duckdb::vector<duckdb::VariantPathComponent> components;
  components.emplace_back(std::string{"a"});

  std::vector<duckdb::Value> actual;
  const auto total = col.RowCount();
  uint64_t pos = 0;
  while (pos < total) {
    const auto take =
      std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector out{duckdb::LogicalType::DOUBLE, STANDARD_VECTOR_SIZE};
    col.ForEachVariantRun(
      pos, take,
      [&](size_t rg, duckdb::idx_t local_start, duckdb::idx_t rlen,
          duckdb::idx_t done) {
        if (col.RgLeafOk(state, rg)) {
          const auto [leaf, leaf_scan] = col.ShreddedLeafScan(state, rg, path);
          const auto cursor = leaf->GatherCursor(*leaf_scan);
          ASSERT_GE(local_start, cursor);
          if (local_start > cursor) {
            leaf->Skip(*leaf_scan, local_start - cursor);
          }
          leaf->ScanCount(*leaf_scan, out, rlen, done);
          return;
        }
        duckdb::Vector variant{duckdb::LogicalType::VARIANT(),
                               STANDARD_VECTOR_SIZE};
        irs::column_internal::GatherRows(
          col, state, irs::IotaRange{pos + done, rlen}, variant, 0);
        duckdb::Vector extracted{duckdb::LogicalType::VARIANT(),
                                 STANDARD_VECTOR_SIZE};
        duckdb::VariantUtils::VariantExtract(variant, components, extracted,
                                             rlen);
        duckdb::Vector casted{duckdb::LogicalType::DOUBLE,
                              STANDARD_VECTOR_SIZE};
        duckdb::VectorOperations::Cast(*con.context, extracted, casted, rlen);
        duckdb::VectorOperations::Copy(casted, out, rlen, 0, done);
      });
    for (duckdb::idx_t k = 0; k < take; ++k) {
      actual.emplace_back(out.GetValue(k));
    }
    pos += take;
  }
  ExpectVariantValuesEqual(expected, actual);
}

TEST_F(ColumnReaderTest, VariantGatherKeepsLeafOfPassedRowGroup) {
  SetShreddingSize(Db(), 120);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  const std::string prefix(24, 'x');
  std::vector<duckdb::Value> ignored;
  WriteVariantViaSql(
    Db(), dir, "vseg", /*id=*/34,
    "SELECT {'s': repeat('x', 24) || i::VARCHAR}::VARIANT FROM range(500) t(i)",
    /*rg_size=*/128, ignored);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* base = r.Column(34);
  ASSERT_NE(base, nullptr);
  const auto& col = static_cast<const irs::VariantColumnReader&>(*base);

  const std::vector<std::string_view> path{"s"};
  auto state = col.InitScan(r.Ctx());
  duckdb::LogicalType leaf_type;
  ASSERT_TRUE(col.CachedUniformShreddedLeaf(state, path, leaf_type));
  ASSERT_EQ(leaf_type, duckdb::LogicalType::VARCHAR);
  ASSERT_TRUE(col.RgLeafOk(state, 2));
  ASSERT_FALSE(col.RgLeafOk(state, 3));

  duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  const auto [leaf, leaf_scan] = col.ShreddedLeafScan(state, 2, path);
  irs::column_internal::GatherRows(*leaf, *leaf_scan, irs::IotaRange{0, 128},
                                   out, 0);
  duckdb::Vector variant{duckdb::LogicalType::VARIANT(), STANDARD_VECTOR_SIZE};
  irs::column_internal::GatherRows(col, state, irs::IotaRange{384, 116},
                                   variant, 0, /*whole_output=*/true);

  ASSERT_NE(state.variant->rgs[2].leaf, nullptr);
  for (duckdb::idx_t k = 0; k < 128; ++k) {
    EXPECT_EQ(out.GetValue(k).ToString(), prefix + std::to_string(256 + k))
      << "row=" << k;
  }
}

TEST_F(ColumnReaderTest, VariantLeavesBeforeARowAreReleased) {
  SetShreddingSize(Db(), 120);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  const std::string prefix(24, 'x');
  std::vector<duckdb::Value> ignored;
  WriteVariantViaSql(
    Db(), dir, "vseg", /*id=*/36,
    "SELECT {'s': repeat('x', 24) || i::VARCHAR}::VARIANT FROM range(500) t(i)",
    /*rg_size=*/128, ignored);

  irs::ColReader r = irs::tests::OpenColReader(dir, "vseg");
  const auto* base = r.Column(36);
  ASSERT_NE(base, nullptr);
  const auto& col = static_cast<const irs::VariantColumnReader&>(*base);

  const std::vector<std::string_view> path{"s"};
  auto state = col.InitScan(r.Ctx());
  duckdb::LogicalType leaf_type;
  ASSERT_TRUE(col.CachedUniformShreddedLeaf(state, path, leaf_type));
  for (size_t rg = 0; rg != 3; ++rg) {
    ASSERT_TRUE(col.RgLeafOk(state, rg));
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    const auto [leaf, leaf_scan] = col.ShreddedLeafScan(state, rg, path);
    irs::column_internal::GatherRows(*leaf, *leaf_scan, irs::IotaRange{0, 128},
                                     out, 0);
  }
  col.ReleaseLeavesBefore(state, 300);
  EXPECT_EQ(state.variant->rgs[0].leaf, nullptr);
  EXPECT_EQ(state.variant->rgs[1].leaf, nullptr);
  ASSERT_NE(state.variant->rgs[2].leaf, nullptr);

  duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  const auto [leaf, leaf_scan] = col.ShreddedLeafScan(state, 0, path);
  irs::column_internal::GatherRows(*leaf, *leaf_scan, irs::IotaRange{0, 128},
                                   out, 0);
  for (duckdb::idx_t k = 0; k < 128; ++k) {
    EXPECT_EQ(out.GetValue(k).ToString(), prefix + std::to_string(k))
      << "row=" << k;
  }
}

// Sparse append: values land at non-contiguous start rows; the gaps are
// null-padded (crossing row-group boundaries), and Commit pads the tail.
TEST_F(ColumnReaderTest, SparseAppendNullPadding) {
  constexpr uint32_t kRgSize = 64;
  constexpr uint64_t kTotal = 500;
  constexpr irs::field_id kA = 60;

  // Three runs of values at sparse positions; everything else is NULL.
  struct Run {
    uint64_t start;
    uint64_t count;
  };
  const Run runs[] = {{0, 30}, {200, 40}, {410, 25}};
  auto val = [](uint64_t g) { return static_cast<int64_t>(g * 7 + 1); };
  auto is_valid = [&](uint64_t g) {
    for (const auto& r : runs) {
      if (g >= r.start && g < r.start + r.count) {
        return true;
      }
    }
    return false;
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             {.row_group_size = kRgSize});
    for (const auto& r : runs) {
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      for (uint64_t i = 0; i < r.count; ++i) {
        d[i] = val(r.start + i);
      }
      duckdb::FlatVector::SetSize(v, r.count);
      cwA.Append(r.start, v, r.count);
    }
    // Typed columns don't auto-pad at Commit (matches the old engine: only norm
    // columns do); the caller pads the tail.
    cwA.PadNullsTo(kTotal);
    w.Commit(kTotal);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ca = r.Column(kA);
  ASSERT_NE(ca, nullptr);
  ASSERT_EQ(ca->RowCount(), kTotal);
  VerifyBigint(*ca, r.Ctx(), kTotal, is_valid, val);
}

TEST_F(ColumnReaderTest, SparseArrayNullPaddingNullsChildren) {
  constexpr uint64_t kTotal = 3000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kArr = 91;
  constexpr duckdb::idx_t kDim = 4;
  const auto atype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  struct Run {
    uint64_t start;
    uint64_t count;
  };
  const std::vector<Run> runs{{0, 100}, {500, 100}, {2500, 100}};
  auto is_valid = [&](uint64_t g) {
    return absl::c_any_of(runs, [&](const Run& r) {
      return g >= r.start && g < r.start + r.count;
    });
  };
  auto elem_val = [](uint64_t g, duckdb::idx_t i) {
    return static_cast<float>(g * 10 + i);
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwArr = w.OpenColumn(kArr, atype, {.row_group_size = kRgSize});
    for (const auto& r : runs) {
      duckdb::Vector v{atype, STANDARD_VECTOR_SIZE};
      auto* cd = duckdb::FlatVector::GetDataMutable<float>(
        duckdb::ArrayVector::GetChildMutable(v));
      for (uint64_t k = 0; k < r.count; ++k) {
        for (duckdb::idx_t i = 0; i < kDim; ++i) {
          cd[k * kDim + i] = elem_val(r.start + k, i);
        }
      }
      duckdb::FlatVector::SetSize(v, r.count);
      cwArr.Append(r.start, v, r.count);
    }
    cwArr.PadNullsTo(kTotal);
    w.Commit(kTotal);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* ca = r.Column(kArr);
  ASSERT_NE(ca, nullptr);
  ASSERT_EQ(ca->RowCount(), kTotal);
  auto state = ca->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kTotal) {
    const auto take =
      std::min<duckdb::idx_t>(kTotal - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{atype, STANDARD_VECTOR_SIZE};
    ca->Scan(state, result, take);
    result.Flatten(take);
    const auto& child = duckdb::ArrayVector::GetChild(result);
    const auto* cd = duckdb::FlatVector::GetData<float>(child);
    const auto& av = duckdb::FlatVector::Validity(result);
    const auto& cv = duckdb::FlatVector::Validity(child);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      ASSERT_EQ(av.RowIsValid(k), is_valid(g)) << "array row " << g;
      for (duckdb::idx_t i = 0; i < kDim; ++i) {
        ASSERT_EQ(cv.RowIsValid(k * kDim + i), is_valid(g))
          << "elem " << g << ":" << i;
        if (is_valid(g)) {
          EXPECT_EQ(cd[k * kDim + i], elem_val(g, i))
            << "elem " << g << ":" << i;
        }
      }
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, DictFsstShortRepeatedStrings) {
  constexpr uint64_t kRows = 3000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kS = 90;
  static const char* const kVals[] = {"abcd", "abcde", "abcdef", "xyz"};
  auto val = [](uint64_t g) -> std::string { return kVals[g % 4]; };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      auto* d = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        d[k] = duckdb::StringVector::AddString(v, val(pos + k));
      }
      duckdb::FlatVector::SetSize(v, take);
      cwS.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  // Scan.
  auto state = cs->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    cs->Scan(state, result, take);
    result.Flatten(take);
    const auto* rd = duckdb::FlatVector::GetData<duckdb::string_t>(result);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      EXPECT_EQ(rd[k].GetString(), val(pos + k)) << "scan row " << (pos + k);
    }
    pos += take;
  }
  // Point reads.
  for (const uint64_t g : {uint64_t{0}, uint64_t{1}, uint64_t{2}, uint64_t{3},
                           uint64_t{1023}, uint64_t{1024}, uint64_t{2999}}) {
    irs::ColumnReader::BlobPointReader pr{r, *cs};
    const auto b = pr.FetchRow(g);
    EXPECT_EQ((std::string{reinterpret_cast<const char*>(b.data()), b.size()}),
              val(g))
      << "point row " << g;
  }
}

// Minimal exact repro of the failing IndexColumnTest prefix column: 4 rows,
// rows 0/3 present ("abcd"/"abcde"), rows 1/2 null, written one row at a time
// (the AppendBlob / PushInStaging shape) with null-padding between.
TEST_F(ColumnReaderTest, SparseDictFsstTinyRepro) {
  constexpr irs::field_id kS = 91;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwS =
      w.OpenColumn(kS, duckdb::LogicalType::VARCHAR, {.row_group_size = 1024});
    auto put = [&](uint64_t row, const std::string& s) {
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, 1};
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddString(v, s);
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::SetSize(v, 1);
      cwS.Append(row, v, 1);
    };
    put(0, "abcd");   // row 0
    put(3, "abcde");  // row 3 (pads rows 1,2 null)
    cwS.PadNullsTo(4);
    w.Commit(4);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  ASSERT_EQ(cs->RowCount(), 4u);

  // Scan.
  auto state = cs->InitScan(r.Ctx());
  duckdb::Vector result{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  cs->Scan(state, result, 4);
  result.Flatten(4);
  const auto* rd = duckdb::FlatVector::GetData<duckdb::string_t>(result);
  const auto& rv = duckdb::FlatVector::Validity(result);
  EXPECT_TRUE(rv.RowIsValid(0));
  EXPECT_EQ(rd[0].GetString(), "abcd") << "scan row 0";
  EXPECT_FALSE(rv.RowIsValid(1));
  EXPECT_FALSE(rv.RowIsValid(2));
  EXPECT_TRUE(rv.RowIsValid(3));
  EXPECT_EQ(rd[3].GetString(), "abcde") << "scan row 3";

  // Point reads.
  irs::ColumnReader::BlobPointReader p0{r, *cs};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p0.FetchRow(0).data()),
                         p0.FetchRow(0).size()}),
            "abcd");
  irs::ColumnReader::BlobPointReader p3{r, *cs};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p3.FetchRow(3).data()),
                         p3.FetchRow(3).size()}),
            "abcde");
}

// Same sparse dict_fsst column, but read the null mask via ScanCount (which
// forces SCAN_FLAT_VECTOR) -- the path the column-existence filter uses. The
// in-band dict_fsst nulls must survive the EMPTY-validity AND-combine here too.
TEST_F(ColumnReaderTest, ScanCountDictFsstNulls) {
  constexpr irs::field_id kS = 93;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             {.row_group_size = 1024,
                              .compression = irs::codecs::CodecType::DictFsst});
    auto put = [&](uint64_t row, const std::string& s) {
      duckdb::Vector v{duckdb::LogicalType::VARCHAR, 1};
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddString(v, s);
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::SetSize(v, 1);
      cwS.Append(row, v, 1);
    };
    put(0, "abcd");
    put(3, "abcde");
    cwS.PadNullsTo(4);
    w.Commit(4);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  ASSERT_TRUE(irs::tests::NullsInData(*cs));
  auto state = cs->InitScan(r.Ctx());
  duckdb::Vector result{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
  cs->ScanCount(state, result, 4, /*result_offset=*/0);
  result.Flatten(4);
  const auto& rv = duckdb::FlatVector::Validity(result);
  EXPECT_TRUE(rv.RowIsValid(0)) << "scancount row 0";
  EXPECT_FALSE(rv.RowIsValid(1)) << "scancount row 1";
  EXPECT_FALSE(rv.RowIsValid(2)) << "scancount row 2";
  EXPECT_TRUE(rv.RowIsValid(3)) << "scancount row 3";
}

// Repro of the index-writer path's AppendNullBlob: null rows are appended as
// explicit 1-row vectors whose string_t data is UNINITIALIZED (only validity is
// set null). If the dict_fsst compressor ingests that garbage from invalid
// rows, it corrupts the PRESENT rows' decoded strings.
TEST_F(ColumnReaderTest, SparseGarbageNullAppendRepro) {
  constexpr irs::field_id kS = 92;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwS =
      w.OpenColumn(kS, duckdb::LogicalType::BLOB, {.row_group_size = 1024});
    auto put = [&](uint64_t row, const std::string& s) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddStringOrBlob(v, s.data(), s.size());
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::SetSize(v, 1);
      cwS.Append(row, v, 1);
    };
    auto put_null = [&](uint64_t row) {
      // Exactly AppendNullBlob: uninitialized string_t, only validity = null.
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::SetNull(v, 0, true);
      cwS.Append(row, v, 1);
    };
    put(0, "abcd");
    put_null(1);
    put_null(2);
    put(3, "abcde");
    w.Commit(4);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  irs::ColumnReader::BlobPointReader p0{r, *cs};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p0.FetchRow(0).data()),
                         p0.FetchRow(0).size()}),
            "abcd");
  irs::ColumnReader::BlobPointReader p3{r, *cs};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p3.FetchRow(3).data()),
                         p3.FetchRow(3).size()}),
            "abcde");
}

// Two-column repro of read_write_doc_attributes: a dense single-char "name"
// column written FIRST, then a sparse "prefix" column -- so the prefix column's
// bytes land at a non-zero file offset behind another column's.
TEST_F(ColumnReaderTest, TwoColumnSparsePrefixRepro) {
  constexpr irs::field_id kName = 0;
  constexpr irs::field_id kPrefix = 1;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  auto put = [](irs::ColumnWriter& cw, uint64_t row, const std::string& s) {
    duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
      duckdb::StringVector::AddStringOrBlob(v, s.data(), s.size());
    duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
    duckdb::FlatVector::SetSize(v, 1);
    cw.Append(row, v, 1);
  };
  auto put_null = [](irs::ColumnWriter& cw, uint64_t row) {
    duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
    duckdb::FlatVector::SetNull(v, 0, true);
    cw.Append(row, v, 1);
  };
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cwName =
      w.OpenColumn(kName, duckdb::LogicalType::BLOB, {.row_group_size = 1024});
    const char* names[] = {"A", "B", "C", "D"};
    for (uint64_t i = 0; i < 4; ++i) {
      put(cwName, i, names[i]);
    }
    auto& cwPrefix = w.OpenColumn(kPrefix, duckdb::LogicalType::BLOB,
                                  {.row_group_size = 1024});
    put(cwPrefix, 0, "abcd");
    put_null(cwPrefix, 1);
    put_null(cwPrefix, 2);
    put(cwPrefix, 3, "abcde");
    w.Commit(4);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* pc = r.Column(kPrefix);
  ASSERT_NE(pc, nullptr);
  irs::ColumnReader::BlobPointReader p0{r, *pc};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p0.FetchRow(0).data()),
                         p0.FetchRow(0).size()}),
            "abcd");
  irs::ColumnReader::BlobPointReader p3{r, *pc};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p3.FetchRow(3).data()),
                         p3.FetchRow(3).size()}),
            "abcde");
}

// Same as the sparse-prefix repro, but uses the PROCESS-WIDE serenedb engine DB
// (DuckDBEngine::Instance) exactly like the failing index tests -- its
// compression config may differ from the fixture's vanilla duckdb::DuckDB and
// select a codec the read path mishandles.
TEST_F(ColumnReaderTest, SparsePrefixEngineDbRepro) {
  auto& edb = ::irs::DuckDBEngine::Instance().instance();
  constexpr irs::field_id kPrefix = 1;
  tests::IndexDir dir{edb, TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{edb, dir.Tracker("seg")};
    auto& cw0 =
      w.OpenColumn(0, duckdb::LogicalType::BLOB, {.row_group_size = 1024});
    const char* names[] = {"A", "B", "C", "D"};
    for (uint64_t i = 0; i < 4; ++i) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddStringOrBlob(v, names[i], 1);
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::SetSize(v, 1);
      cw0.Append(i, v, 1);
    }
    auto& cwPrefix = w.OpenColumn(kPrefix, duckdb::LogicalType::BLOB,
                                  {.row_group_size = 1024});
    auto put = [&](uint64_t row, const std::string& s) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddStringOrBlob(v, s.data(), s.size());
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::SetSize(v, 1);
      cwPrefix.Append(row, v, 1);
    };
    auto put_null = [&](uint64_t row) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::SetNull(v, 0, true);
      cwPrefix.Append(row, v, 1);
    };
    put(0, "abcd");
    put_null(1);
    put_null(2);
    put(3, "abcde");
    w.Commit(4);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* pc = r.Column(kPrefix);
  ASSERT_NE(pc, nullptr);
  irs::ColumnReader::BlobPointReader p0{r, *pc};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p0.FetchRow(0).data()),
                         p0.FetchRow(0).size()}),
            "abcd");
  irs::ColumnReader::BlobPointReader p3{r, *pc};
  EXPECT_EQ((std::string{reinterpret_cast<const char*>(p3.FetchRow(3).data()),
                         p3.FetchRow(3).size()}),
            "abcde");
}

// A stored FLOAT vector column is exactly what the IVF opclass writes for
// `compression = true` (CodecType::Auto). DuckDB's analyze tournament must
// compress the FLOAT element data with a floating-point codec -- ALP or ALPRD
// -- rather than leaving it uncompressed. The codec is recorded per row-group
// on the array element child and survives flush + reopen.
TEST_F(ColumnReaderTest, VectorColumnAutoPicksFloatCodec) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;  // multiple of STANDARD_VECTOR_SIZE
  constexpr irs::field_id kV = 20;
  constexpr duckdb::idx_t kDim = 8;
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);

  // Low-precision decimals (multiples of 0.25) are highly ALP-friendly.
  auto elem_val = [](uint64_t g, uint64_t i) {
    return static_cast<float>((g * kDim + i) % 997) * 0.25f;
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    // Default compression arg == CodecType::Auto (the analyze tournament).
    auto& cw = w.OpenColumn(kV, vtype, {.row_group_size = kRgSize});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{vtype, STANDARD_VECTOR_SIZE};
      auto& child = duckdb::ArrayVector::GetChildMutable(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<float>(child);
      auto& av = duckdb::FlatVector::ValidityMutable(v);
      auto& cv = duckdb::FlatVector::ValidityMutable(child);
      av.Reset(STANDARD_VECTOR_SIZE);
      cv.Reset(STANDARD_VECTOR_SIZE * kDim);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        for (duckdb::idx_t i = 0; i < kDim; ++i) {
          cd[k * kDim + i] = elem_val(g, i);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cv = r.Column(kV);
  ASSERT_NE(cv, nullptr);
  ASSERT_EQ(cv->RowCount(), kRows);
  const auto* child = cv->Child();
  ASSERT_NE(child, nullptr);
  const auto blocks = child->DataBlocks();
  ASSERT_FALSE(blocks.empty());
  for (size_t rg = 0; rg < blocks.size(); ++rg) {
    ASSERT_NE(blocks[rg].codec, nullptr) << "row-group " << rg;
    const auto codec = blocks[rg].codec->type;
    EXPECT_TRUE(codec == irs::codecs::CodecType::Alp ||
                codec == irs::codecs::CodecType::Alprd ||
                irs::codecs::IsNumeric(codec))
      << "row-group " << rg << " codec=" << magic_enum::enum_name(codec);
  }
}

// `compression = false` maps to CodecType::Uncompressed: forcing that codec on
// the stored FLOAT vector column keeps the raw bytes (every row-group reports
// UNCOMPRESSED) and the values round-trip exactly.
TEST_F(ColumnReaderTest, VectorColumnForcedUncompressed) {
  constexpr uint64_t kRows = 5000;
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kV = 21;
  constexpr duckdb::idx_t kDim = 8;
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);

  auto elem_val = [](uint64_t g, uint64_t i) {
    return static_cast<float>((g * kDim + i) % 997) * 0.25f;
  };

  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw =
      w.OpenColumn(kV, vtype,
                   {.row_group_size = kRgSize,
                    .compression = irs::codecs::CodecType::Uncompressed});
    uint64_t pos = 0;
    while (pos < kRows) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{vtype, STANDARD_VECTOR_SIZE};
      auto& child = duckdb::ArrayVector::GetChildMutable(v);
      auto* cd = duckdb::FlatVector::GetDataMutable<float>(child);
      auto& av = duckdb::FlatVector::ValidityMutable(v);
      auto& cv = duckdb::FlatVector::ValidityMutable(child);
      av.Reset(STANDARD_VECTOR_SIZE);
      cv.Reset(STANDARD_VECTOR_SIZE * kDim);
      for (duckdb::idx_t k = 0; k < take; ++k) {
        const auto g = pos + k;
        for (duckdb::idx_t i = 0; i < kDim; ++i) {
          cd[k * kDim + i] = elem_val(g, i);
        }
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* cv = r.Column(kV);
  ASSERT_NE(cv, nullptr);
  ASSERT_EQ(cv->RowCount(), kRows);
  const auto* child = cv->Child();
  ASSERT_NE(child, nullptr);
  const auto blocks = child->DataBlocks();
  ASSERT_FALSE(blocks.empty());
  for (size_t rg = 0; rg < blocks.size(); ++rg) {
    ASSERT_NE(blocks[rg].codec, nullptr) << "row-group " << rg;
    EXPECT_EQ(blocks[rg].codec->type, irs::codecs::CodecType::Uncompressed)
      << "row-group " << rg;
  }

  // Values round-trip exactly under the uncompressed codec.
  auto state = cv->InitScan(r.Ctx());
  uint64_t pos = 0;
  while (pos < kRows) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector result{vtype, STANDARD_VECTOR_SIZE};
    cv->Scan(state, result, take);
    result.Flatten(take);
    const auto& child_out = duckdb::ArrayVector::GetChild(result);
    const auto* cd = duckdb::FlatVector::GetData<float>(child_out);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto g = pos + k;
      for (duckdb::idx_t i = 0; i < kDim; ++i) {
        EXPECT_EQ(cd[k * kDim + i], elem_val(g, i)) << "elem " << g << ":" << i;
      }
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, UnknownCompressionIsRefused) {
  for (const auto type :
       {irs::codecs::CodecType{200}, irs::codecs::CodecType::Auto}) {
    const auto id = static_cast<uint32_t>(type);
    SCOPED_TRACE(id);
    auto unknown = *irs::codecs::GetCodec(irs::codecs::CodecType::Uncompressed,
                                          duckdb::PhysicalType::INT64);
    unknown.type = type;

    irs::ColumnMeta meta;
    meta.id = 7;
    meta.type = duckdb::LogicalType::BIGINT;
    meta.data.push_back(irs::ColumnBlockMeta{
      .statistics =
        duckdb::BaseStatistics::CreateEmpty(duckdb::LogicalType::BIGINT),
      .tuple_count = 1,
      .byte_size = sizeof(int64_t),
      .codec = &unknown,
    });

    duckdb::MemoryStream stream;
    duckdb::BinarySerializer serializer{stream};
    serializer.Begin();
    irs::SerializeColumnMeta(serializer, meta);
    serializer.End();

    stream.Rewind();
    duckdb::BinaryDeserializer deserializer{stream};
    deserializer.Set<duckdb::DatabaseInstance&>(Db());
    deserializer.Begin();
    std::string message;
    try {
      irs::DeserializeColumnMeta(deserializer);
    } catch (const irs::SqlException& e) {
      EXPECT_EQ(e.error().errcode, ERRCODE_FEATURE_NOT_SUPPORTED);
      message = e.what();
    }
    EXPECT_NE(std::string::npos,
              message.find(absl::StrCat("compression ", id, ",")))
      << message;
    EXPECT_NE(std::string::npos, message.find("written by a newer release"))
      << message;
  }
}

TEST_F(ColumnReaderTest, PointReadersReportWhichRowsHoldValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 6000;
  const ValueOf nullable = [](uint64_t g) {
    return g % 10 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                       : duckdb::Value::BIGINT(static_cast<int64_t>(g));
  };
  const ValueOf dense = [](uint64_t g) {
    return duckdb::Value::BIGINT(static_cast<int64_t>(g * 3));
  };
  for (const auto& value_of : {nullable, dense}) {
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    WriteColumn(Db(), dir, kId, duckdb::LogicalType::BIGINT, kRows, 4096,
                value_of);
    irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
    const auto* col = r.Column(kId);
    ASSERT_NE(col, nullptr);
    const std::vector<uint64_t> patched_rows{5, 4097};
    const auto patch = MergePatch(
      nullptr, duckdb::LogicalType::BIGINT, patched_rows,
      [](uint64_t) { return duckdb::Value{duckdb::LogicalType::BIGINT}; });
    const irs::PatchedColumnReader patched{*col, patch};
    duckdb::Vector scratch{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    irs::ColumnReader::PointReader stored{r, *col};
    irs::ColumnReader::PointReader through{r, patched};
    for (const uint64_t g : {0, 1, 5, 10, 4095, 4096, 4097, 5999}) {
      const bool holds = !value_of(g).IsNull();
      EXPECT_EQ(stored.ValidRow(g, scratch), holds) << "row " << g;
      EXPECT_EQ(through.ValidRow(g, scratch),
                holds && !absl::c_linear_search(patched_rows, g))
        << "row " << g;
    }
    EXPECT_FALSE(stored.ValidRow(kRows, scratch));
    EXPECT_FALSE(stored.ValidRow(kRows + 100, scratch));
  }
}

TEST_F(ColumnReaderTest, FilterGathersSkipBlocksWithoutOfferedRows) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 6000;
  constexpr uint64_t kBlockRows = 1024;
  const ValueOf value_of = [](uint64_t g) {
    return g % 11 == 0
             ? duckdb::Value{duckdb::LogicalType::VARCHAR}
             : duckdb::Value{absl::StrCat("tenant-", (g * 7919) % 300)};
  };
  const std::string bound = "tenant-150";
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'dict_fsst'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, kBlockRows,
              value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().size(), 4u);
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::VARCHAR, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{bound}))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  for (const bool first_block : {true, false}) {
    SCOPED_TRACE(first_block ? "first block offered" : "last block offered");
    auto state = col->InitScan(r.Ctx());
    for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
      const auto span =
        std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
      const auto offered_block =
        (first_block ? anchor : anchor + span - 1) / kBlockRows;
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      duckdb::idx_t offered = 0;
      for (duckdb::idx_t i = 0; i < span; ++i) {
        if ((anchor + i) / kBlockRows == offered_block) {
          sel.set_index(offered++, i);
        }
      }
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      const auto kept =
        col->GatherFilter(state, anchor, span, sel, offered, filter,
                          *filter_state, irs::NullCheckKind::None, out);
      std::vector<uint64_t> want;
      for (duckdb::idx_t i = 0; i < span; ++i) {
        const auto v = value_of(anchor + i);
        if ((anchor + i) / kBlockRows == offered_block && !v.IsNull() &&
            v.GetValue<std::string>() >= bound) {
          want.push_back(i);
        }
      }
      ASSERT_EQ(kept, want.size()) << "span at " << anchor;
      for (duckdb::idx_t i = 0; i < kept; ++i) {
        ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
      }
    }
  }
}

TEST_F(ColumnReaderTest, DictionaryConjunctionsIntersectTheirRanges) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 9000;
  const ValueOf value_of = [](uint64_t g) {
    return g % 13 == 0
             ? duckdb::Value{duckdb::LogicalType::VARCHAR}
             : duckdb::Value{absl::StrCat("tenant-", (g * 7919) % 300)};
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'dict_fsst'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, 4096,
              value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  using Cmp = duckdb::ExpressionType;
  const auto compare = [](Cmp cmp, std::string_view key) {
    return duckdb::BoundComparisonExpression::Create(
      cmp,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::VARCHAR, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{std::string{key}}));
  };
  const auto either = [&] {
    auto any = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
      Cmp::CONJUNCTION_OR);
    any->GetChildrenMutable().push_back(
      compare(Cmp::COMPARE_EQUAL, "tenant-1"));
    any->GetChildrenMutable().push_back(
      compare(Cmp::COMPARE_EQUAL, "tenant-200"));
    return any;
  };
  struct Conjunction {
    std::string name;
    std::function<duckdb::unique_ptr<duckdb::Expression>()> make;
    std::function<bool(std::string_view)> holds;
  };
  const Conjunction conjunctions[] = {
    {"range first",
     [&] {
       auto all = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
         Cmp::CONJUNCTION_AND);
       all->GetChildrenMutable().push_back(
         compare(Cmp::COMPARE_GREATERTHAN, "tenant-150"));
       all->GetChildrenMutable().push_back(either());
       return all;
     },
     [](std::string_view v) { return v == "tenant-200"; }},
    {"either first",
     [&] {
       auto all = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
         Cmp::CONJUNCTION_AND);
       all->GetChildrenMutable().push_back(either());
       all->GetChildrenMutable().push_back(
         compare(Cmp::COMPARE_GREATERTHAN, "tenant-150"));
       return all;
     },
     [](std::string_view v) { return v == "tenant-200"; }},
    {"disjoint",
     [&] {
       auto all = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
         Cmp::CONJUNCTION_AND);
       all->GetChildrenMutable().push_back(
         compare(Cmp::COMPARE_LESSTHAN, "tenant-100"));
       all->GetChildrenMutable().push_back(
         compare(Cmp::COMPARE_GREATERTHAN, "tenant-200"));
       return all;
     },
     [](std::string_view) { return false; }},
  };
  for (const auto& c : conjunctions) {
    SCOPED_TRACE(c.name);
    const duckdb::ExpressionFilter filter{c.make()};
    auto filter_state =
      duckdb::TableFilterState::Initialize(*con.context, filter);
    auto state = col->InitScan(r.Ctx());
    for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
      const auto span =
        std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t i = 0; i < span; ++i) {
        sel.set_index(i, i);
      }
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      const auto kept =
        col->GatherFilter(state, anchor, span, sel, span, filter, *filter_state,
                          irs::NullCheckKind::None, out);
      std::vector<uint64_t> want;
      for (duckdb::idx_t i = 0; i < span; ++i) {
        const auto v = value_of(anchor + i);
        if (!v.IsNull() && c.holds(v.GetValue<std::string>())) {
          want.push_back(i);
        }
      }
      ASSERT_EQ(kept, want.size()) << "span at " << anchor;
      for (duckdb::idx_t i = 0; i < kept; ++i) {
        ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
      }
    }
  }
}

TEST_F(ColumnReaderTest, DictionaryFiltersOnBlobsMatchTheValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 9000;
  const ValueOf value_of = [](uint64_t g) {
    return g % 13 == 0
             ? duckdb::Value{duckdb::LogicalType::BLOB}
             : duckdb::Value::BLOB(absl::StrCat("tenant-", (g * 7919) % 300));
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'dict_fsst'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::BLOB, kRows, 4096, value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().front().codec->type,
            irs::codecs::CodecType::DictFsst);
  const auto key = duckdb::Value::BLOB("tenant-42");
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_EQUAL,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::BLOB, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(key))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
    const auto span = std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < span; ++i) {
      sel.set_index(i, i);
    }
    duckdb::Vector out{duckdb::LogicalType::BLOB, STANDARD_VECTOR_SIZE};
    const auto kept =
      col->GatherFilter(state, anchor, span, sel, span, filter, *filter_state,
                        irs::NullCheckKind::None, out);
    std::vector<uint64_t> want;
    for (duckdb::idx_t i = 0; i < span; ++i) {
      if (const auto v = value_of(anchor + i); !v.IsNull() && v == key) {
        want.push_back(i);
      }
    }
    ASSERT_EQ(kept, want.size()) << "span at " << anchor;
    for (duckdb::idx_t i = 0; i < kept; ++i) {
      ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
    }
  }
}

TEST_F(ColumnReaderTest, DeferredWritersSealTheirRowsAtCommit) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 10000;
  constexpr uint32_t kRgSize = 4096;
  const ValueOf value_of = [](uint64_t g) {
    return g % 17 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                       : duckdb::Value::BIGINT(static_cast<int64_t>(g * 3));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    w.SetDeferredSeal(true);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT,
                            {.row_group_size = kRgSize});
    for (uint64_t pos = 0; pos < kRows;) {
      const auto take =
        std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < take; ++k) {
        v.SetValue(k, value_of(pos + k));
      }
      duckdb::FlatVector::SetSize(v, take);
      cw.Append(v, take);
      pos += take;
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  EXPECT_EQ(col->DataBlocks().size(), (kRows + kRgSize - 1) / kRgSize);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t pos = 0; pos < kRows;) {
    const auto take =
      std::min<duckdb::idx_t>(kRows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->Scan(state, out, take);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      ASSERT_TRUE(
        duckdb::Value::NotDistinctFrom(out.GetValue(k), value_of(pos + k)))
        << "row " << pos + k;
    }
    pos += take;
  }
}

TEST_F(ColumnReaderTest, BlobPointReadersReturnNothingForNullRows) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 5000;
  const ValueOf value_of = [](uint64_t g) {
    return g % 7 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                      : duckdb::Value{
                          absl::StrCat("value-", g, "-", std::string(20, 'x'))};
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, 4096,
              value_of);
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  irs::ColumnReader::BlobPointReader reader{r, *col};
  for (const uint64_t g : {0, 1, 7, 13, 14, 4095, 4096, 4097, 4998, 4999}) {
    const auto value = value_of(g);
    const auto got = reader.FetchRow(g);
    if (value.IsNull()) {
      EXPECT_TRUE(got.empty()) << "row " << g;
      EXPECT_TRUE(reader.IsNullRow(g)) << "row " << g;
      continue;
    }
    EXPECT_EQ(
      std::string_view(reinterpret_cast<const char*>(got.data()), got.size()),
      value.GetValue<std::string>())
      << "row " << g;
    EXPECT_FALSE(reader.IsNullRow(g)) << "row " << g;
  }
}

TEST_F(ColumnReaderTest, CollectedBlocksNameEachSharedSymbolTableOnce) {
  constexpr uint32_t kRgSize = 4 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kS = 1;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "seg", kS, duckdb::LogicalType::VARCHAR,
    absl::StrCat("SELECT 'https://example.org/' || (i % 977) || '/item/' || "
                 "(i * 7919 % 100003) || '?ref=' || i FROM range(",
                 kRows, ") t(i)"),
    kRgSize, expected,
    {.compression = irs::codecs::CodecType::Fsst12,
     .codec_params = {.segment_target = 32 * 1024}});
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto& col = *r.Column(kS);
  const auto blocks = col.DataBlocks();
  ASSERT_GT(blocks.size(), 6);
  ASSERT_TRUE(absl::c_all_of(
    blocks, [](const auto& block) { return block.shared_size != 0; }));
  ASSERT_NE(blocks.front().shared_offset, blocks.back().shared_offset);
  const auto same = [](const duckdb::FileRange& range, uint64_t offset,
                       uint64_t size) {
    return range.offset == offset && range.size == size;
  };
  for (const auto& [first, last] :
       {std::pair<uint64_t, uint64_t>{0, kRows - 1},
        std::pair<uint64_t, uint64_t>{kRgSize + 5, 2 * kRgSize + 5},
        std::pair<uint64_t, uint64_t>{kRows - 1, kRows - 1}}) {
    SCOPED_TRACE(absl::StrCat("rows ", first, "..", last));
    std::vector<duckdb::FileRange> ranges;
    col.CollectBlocks(first, last, ranges);
    std::vector<size_t> read;
    for (size_t b = 0; b != blocks.size(); ++b) {
      const auto begin = col.DataBlockFirstRow(b);
      if (begin <= last && first < begin + blocks[b].tuple_count) {
        read.push_back(b);
      }
    }
    ASSERT_FALSE(read.empty());
    size_t tables = 0;
    for (const auto b : read) {
      const auto& block = blocks[b];
      const auto data = absl::c_find_if(ranges, [&](const auto& range) {
        return same(range, block.file_offset, block.byte_size);
      });
      ASSERT_NE(data, ranges.end()) << "block " << b;
      EXPECT_EQ(absl::c_count_if(ranges,
                                 [&](const auto& range) {
                                   return same(range, block.file_offset,
                                               block.byte_size);
                                 }),
                1)
        << "block " << b;
      const auto table = absl::c_find_if(ranges, [&](const auto& range) {
        return same(range, block.shared_offset, block.shared_size);
      });
      ASSERT_NE(table, ranges.end()) << "block " << b;
      EXPECT_LT(table, data) << "block " << b;
      EXPECT_EQ(absl::c_count_if(ranges,
                                 [&](const auto& range) {
                                   return same(range, block.shared_offset,
                                               block.shared_size);
                                 }),
                1)
        << "block " << b;
      tables +=
        b == read.front() || block.shared_offset != blocks[b - 1].shared_offset;
    }
    EXPECT_EQ(ranges.size(), read.size() + tables);
  }
}

TEST_F(ColumnReaderTest, UncompressedStringsScannedForARewriteKeepTheirValues) {
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kS = 1;
  const auto type = duckdb::LogicalType::VARCHAR;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "src", kS, type,
              "SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE 'borrowed-' || i || "
              "'-' || repeat('b', 30) END FROM range(10000) t(i)",
              kRgSize, expected,
              {.compression = irs::codecs::CodecType::Uncompressed});
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto* col = src.Column(kS);
  ASSERT_NE(col, nullptr);
  ASSERT_TRUE(absl::c_all_of(col->DataBlocks(), [](const auto& block) {
    return block.codec->type == irs::codecs::CodecType::Uncompressed;
  }));
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw = w.OpenColumn(kS, type, {.row_group_size = kRgSize});
    auto state = col->InitScan(std::make_shared<irs::ReadContext>(src));
    for (uint64_t pos = 0; pos < col->RowCount();) {
      const auto take =
        std::min<uint64_t>(col->RowCount() - pos, STANDARD_VECTOR_SIZE);
      duckdb::Vector batch{type, STANDARD_VECTOR_SIZE};
      col->Scan(state, batch, take);
      cw.Append(batch, take);
      pos += take;
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "dst");
  const auto* out = r.Column(kS);
  ASSERT_NE(out, nullptr);
  ExpectVariantValuesEqual(expected, ScanValues(*out, r.Ctx()));
}

TEST_F(ColumnReaderTest, GathersOfWindowsPastTheColumnEndKeepItsRows) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 5000;
  constexpr uint64_t kWindows = 4;
  constexpr irs::field_id kS = 1;
  const auto type = duckdb::LogicalType::VARCHAR;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "seg", kS, type,
              absl::StrCat("SELECT CASE WHEN i % 11 = 0 THEN NULL ELSE "
                           "'stored-' || (i * 7919 % 1000) || '-' || i END "
                           "FROM range(",
                           kRows, ") t(i)"),
              kRgSize, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kS);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  const auto check = [&](const duckdb::Vector& out, uint64_t anchor,
                         const duckdb::SelectionVector& sel,
                         duckdb::idx_t hits) {
    for (duckdb::idx_t i = 0; i < hits; ++i) {
      const auto row = anchor + sel.get_index(i);
      if (row >= kRows) {
        continue;
      }
      const auto got = out.GetValue(i);
      EXPECT_EQ(expected[row].IsNull(), got.IsNull()) << "row " << row;
      if (!expected[row].IsNull()) {
        EXPECT_EQ(expected[row].ToString(), got.ToString()) << "row " << row;
      }
    }
  };
  for (const duckdb::idx_t step : {1, 3}) {
    SCOPED_TRACE(absl::StrCat("every ", step, " rows"));
    auto state = col->InitScan(r.Ctx());
    for (uint64_t w = 0; w < kWindows; ++w) {
      const uint64_t anchor = w * STANDARD_VECTOR_SIZE;
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      duckdb::idx_t hits = 0;
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; i += step) {
        sel.set_index(hits++, i);
      }
      const auto span = sel.get_index(hits - 1) + 1;
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->GatherDense(state, anchor, sel, hits, span, out);
      check(out, anchor, sel, hits);
    }
  }
}

TEST_F(ColumnReaderTest, FilterWindowsThatPassTheColumnEndFilterItsRows) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 5000;
  constexpr irs::field_id kId = 1;
  const ValueOf value_of = [](uint64_t g) {
    return g % 11 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                       : duckdb::Value::BIGINT(static_cast<int64_t>(g % 300));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::BIGINT, kRows, kRgSize,
              value_of);
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  duckdb::Connection con{Db()};
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_LESSTHAN,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::BIGINT, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::BIGINT(100)))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
    const duckdb::idx_t span = STANDARD_VECTOR_SIZE;
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    duckdb::idx_t offered = 0;
    for (duckdb::idx_t i = 0; i < span && anchor + i < kRows; i += 2) {
      sel.set_index(offered++, i);
    }
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    const auto kept =
      col->GatherFilter(state, anchor, span, sel, offered, filter,
                        *filter_state, irs::NullCheckKind::None, out);
    std::vector<uint64_t> want;
    for (duckdb::idx_t i = 0; i < span && anchor + i < kRows; i += 2) {
      const auto v = value_of(anchor + i);
      if (!v.IsNull() && v.GetValue<int64_t>() < 100) {
        want.push_back(i);
      }
    }
    ASSERT_EQ(kept, want.size()) << "span at " << anchor;
    for (duckdb::idx_t i = 0; i < kept; ++i) {
      ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
    }
  }
}

TEST_F(ColumnReaderTest, SelectionsOfDictionaryBlocksIgnoreTheColumnValidity) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 2 * kRgSize;
  constexpr irs::field_id kS = 1;
  const auto type = duckdb::LogicalType::VARCHAR;
  const auto values =
    [&](std::string_view segment, irs::codecs::CodecType codec,
        std::vector<duckdb::Value>& expected, tests::IndexDir& dir) {
      WriteViaSql(Db(), dir, segment, kS, type,
                  absl::StrCat("SELECT CASE WHEN i % 9 = 0 THEN NULL ELSE "
                               "'tenant-' || (i * 7919 % 40) END FROM range(",
                               kRows, ") t(i)"),
                  kRgSize, expected, {.compression = codec});
    };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  values("dict", irs::codecs::CodecType::DictFsst, expected, dir);
  values("plain", irs::codecs::CodecType::Fsst, expected, dir);
  irs::ColReader dict = irs::tests::OpenColReader(dir, "dict");
  irs::ColReader plain = irs::tests::OpenColReader(dir, "plain");
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw = w.OpenColumn(kS, type, {.row_group_size = kRgSize});
    ASSERT_TRUE(cw.CopyableFrom(*dict.Column(kS)));
    ASSERT_TRUE(
      cw.CopyBlocks(*dict.Column(kS), dict.Ctx(), [] { return true; }));
    ASSERT_TRUE(cw.CopyableFrom(*plain.Column(kS)));
    ASSERT_TRUE(
      cw.CopyBlocks(*plain.Column(kS), plain.Ctx(), [] { return true; }));
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "dst");
  const auto* col = r.Column(kS);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 2 * kRows);
  ASSERT_TRUE(col->HasValidity());
  ASSERT_EQ(col->DataBlocks().front().codec->type,
            irs::codecs::CodecType::DictFsst);
  ASSERT_EQ(col->DataBlocks().back().codec->type, irs::codecs::CodecType::Fsst);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < 2 * kRows;
       anchor += STANDARD_VECTOR_SIZE) {
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    duckdb::idx_t hits = 0;
    for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; i += 10) {
      sel.set_index(hits++, i);
    }
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    col->GatherDense(state, anchor, sel, hits, STANDARD_VECTOR_SIZE, out);
    for (duckdb::idx_t i = 0; i < hits; ++i) {
      const auto row = anchor + sel.get_index(i);
      const auto got = out.GetValue(i);
      EXPECT_EQ(expected[row].IsNull(), got.IsNull()) << "row " << row;
      if (!expected[row].IsNull()) {
        EXPECT_EQ(expected[row].ToString(), got.ToString()) << "row " << row;
      }
    }
  }
}

TEST_F(ColumnReaderTest, NestedListRowGroupsWithoutInnerListsReadBack) {
  const auto type = duckdb::LogicalType::LIST(
    duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER));
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "nseg", 44, type,
              "SELECT CASE WHEN i >= 4096 THEN [[i::INTEGER], [], [(i % "
              "7)::INTEGER, NULL]] WHEN i % 5 = 0 THEN NULL ELSE "
              "[]::INTEGER[][] END FROM range(10000) t(i)",
              4096, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "nseg");
  const auto* col = r.Column(44);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 10000u);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  ExpectGathered(*col, r.Ctx(), expected,
                 Rows{{0, 1, 5, 4095, 4096, 4097, 8191, 8192, 9999}}, false);
}

TEST_F(ColumnReaderTest, VariantRowGroupsOfChangingShapesShredEachOnItsType) {
  constexpr irs::field_id kId = 37;
  constexpr uint32_t kRowGroup = 256;
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  {
    irs::ColWriter w{Db(), dir.Tracker("vshapes"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::VARIANT(),
                            {.row_group_size = kRowGroup});
    duckdb::Connection con{Db()};
    auto result = con.Query(absl::StrCat(
      "SELECT CASE WHEN i < ", kRowGroup, " THEN i::VARIANT WHEN i < ",
      2 * kRowGroup,
      " THEN ('s-' || i)::VARIANT ELSE struct_pack()::VARIANT END FROM range(",
      3 * kRowGroup, ") t(i)"));
    ASSERT_FALSE(result->HasError()) << result->GetError();
    while (auto chunk = result->Fetch()) {
      cw.Append(chunk->data[0], chunk->size());
      for (duckdb::idx_t k = 0; k < chunk->size(); ++k) {
        expected.emplace_back(chunk->GetValue(0, k));
      }
    }
    const auto& rgs = cw.Meta().variant_rgs;
    ASSERT_EQ(rgs.size(), 3u);
    ASSERT_TRUE(rgs[0].shredded);
    ASSERT_TRUE(rgs[1].shredded);
    EXPECT_NE(rgs[0].shredded->type, rgs[1].shredded->type)
      << rgs[0].shredded->type.ToString();
    EXPECT_FALSE(rgs[2].shredded);
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "vshapes");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 3 * kRowGroup);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, DeferredWritersCopyRowsByDecodingThem) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 10000;
  constexpr uint32_t kRgSize = 4096;
  const ValueOf value_of = [](uint64_t g) {
    return g % 17 == 0 ? duckdb::Value{duckdb::LogicalType::BIGINT}
                       : duckdb::Value::BIGINT(static_cast<int64_t>(g * 3));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::BIGINT, kRows, kRgSize,
              value_of);
  irs::ColReader src = irs::tests::OpenColReader(dir, "seg");
  const auto* source = src.Column(kId);
  ASSERT_NE(source, nullptr);
  {
    irs::ColWriter w{Db(), dir.Tracker("hot")};
    w.SetDeferredSeal(true);
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::BIGINT,
                            {.row_group_size = kRgSize});
    irs::ReadContext context{src};
    cw.CopyRows(0, *source, context, 0, kRows);
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "hot");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  EXPECT_EQ(col->DataBlocks().size(), (kRows + kRgSize - 1) / kRgSize);
  const auto values = ScanValues(*col, r.Ctx());
  for (uint64_t g = 0; g < kRows; ++g) {
    ASSERT_TRUE(duckdb::Value::NotDistinctFrom(values[g], value_of(g)))
      << "row " << g;
  }
}

TEST_F(ColumnReaderTest, ColumnsWithoutValidityCopyTheirBlocksWithoutIt) {
  constexpr irs::field_id kId = 1;
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 3 * kRgSize;
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "src", kId, type,
    absl::StrCat("SELECT (i * 5 + 1)::BIGINT FROM range(", kRows, ") t(i)"),
    kRgSize, expected, {.skip_validity = true});
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto* source = src.Column(kId);
  ASSERT_NE(source, nullptr);
  ASSERT_FALSE(source->HasValidity());
  {
    irs::ColWriter w{Db(), dir.Tracker("blocks")};
    auto& cw = w.OpenColumn(kId, type,
                            {.skip_validity = true, .row_group_size = kRgSize});
    ASSERT_TRUE(cw.CopyableFrom(*source));
    ASSERT_TRUE(cw.CopyBlocks(*source, src.Ctx(), [] { return true; }));
    ASSERT_TRUE(w.Commit(0));
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("rows")};
    auto& cw = w.OpenColumn(kId, type,
                            {.skip_validity = true, .row_group_size = kRgSize});
    irs::ReadContext context{src};
    cw.CopyRows(0, *source, context, 0, kRows);
    ASSERT_TRUE(w.Commit(0));
  }
  for (const std::string_view segment : {"blocks", "rows"}) {
    SCOPED_TRACE(segment);
    irs::ColReader r = irs::tests::OpenColReader(dir, segment);
    const auto* col = r.Column(kId);
    ASSERT_NE(col, nullptr);
    EXPECT_FALSE(col->HasValidity());
    ASSERT_EQ(col->DataBlocks().size(), source->DataBlocks().size());
    for (size_t b = 0; b != source->DataBlocks().size(); ++b) {
      EXPECT_EQ(col->DataBlocks()[b].byte_size,
                source->DataBlocks()[b].byte_size);
    }
    ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
  }
}

TEST_F(ColumnReaderTest, MergesDecodeColumnsShorterThanARowGroup) {
  constexpr uint64_t kRows = DEFAULT_ROW_GROUP_SIZE;
  constexpr uint64_t kShort = 100;
  constexpr irs::field_id kFull = 1;
  constexpr irs::field_id kSparse = 2;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("src")};
    auto& full = w.OpenColumn(kFull, duckdb::LogicalType::BIGINT,
                              {.row_group_size = DEFAULT_ROW_GROUP_SIZE});
    auto& sparse = w.OpenColumn(kSparse, duckdb::LogicalType::BIGINT,
                                {.row_group_size = DEFAULT_ROW_GROUP_SIZE});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(v);
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        data[k] = static_cast<int64_t>((pos + k) * 7);
      }
      duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
      full.Append(v, STANDARD_VECTOR_SIZE);
      if (pos == 0) {
        sparse.Append(v, kShort);
      }
    }
    ASSERT_TRUE(w.Commit(kRows));
  }
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  ASSERT_EQ(src.Column(kFull)->RowCount(), kRows);
  ASSERT_EQ(src.Column(kSparse)->RowCount(), kShort);
  const MergedSegment segment{kRows};
  const std::array sources{
    irs::MergeSource{
      .reader = &segment, .col_reader = &src, .alive_count = kRows},
  };
  {
    irs::ColWriter out{Db(), dir.Tracker("dst")};
    ASSERT_TRUE(irs::MergeInto(sources, out, nullptr, [] { return true; }));
    ASSERT_TRUE(out.Commit(kRows));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "dst");
  for (const auto id : {kFull, kSparse}) {
    SCOPED_TRACE(id);
    const auto* col = r.Column(id);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), kRows);
    const auto values = ScanValues(*col, r.Ctx());
    for (uint64_t g = 0; g < kRows; ++g) {
      const auto want = id == kSparse && g >= kShort
                          ? duckdb::Value{duckdb::LogicalType::BIGINT}
                          : duckdb::Value::BIGINT(static_cast<int64_t>(g * 7));
      ASSERT_TRUE(duckdb::Value::NotDistinctFrom(values[g], want))
        << "row " << g << " got " << values[g].ToString();
    }
  }
  EXPECT_EQ(r.Column(kFull)->DataBlocks().size(),
            src.Column(kFull)->DataBlocks().size());
}

TEST_F(ColumnReaderTest, TextColumnsDeclaredUncompressedDecodeCopiedRows) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kId = 1;
  const auto type = duckdb::LogicalType::VARCHAR;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "src", kId, type,
              absl::StrCat("SELECT CASE WHEN i % 5 = 0 THEN NULL ELSE "
                           "'declared-' || (i % 37) END FROM range(",
                           kRows, ") t(i)"),
              kRgSize, expected);
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto& source = *src.Column(kId);
  ASSERT_TRUE(absl::c_none_of(source.DataBlocks(), [](const auto& block) {
    return block.codec->type == irs::codecs::CodecType::Uncompressed;
  }));
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw =
      w.OpenColumn(kId, type,
                   {.row_group_size = kRgSize,
                    .compression = irs::codecs::CodecType::Uncompressed});
    EXPECT_FALSE(cw.CopyableFrom(source));
    irs::ReadContext context{src};
    cw.CopyRows(0, source, context, 0, kRows);
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader dst = irs::tests::OpenColReader(dir, "dst");
  const auto* column = dst.Column(kId);
  ASSERT_NE(nullptr, column);
  for (const auto& block : column->DataBlocks()) {
    EXPECT_EQ(irs::codecs::CodecType::Uncompressed, block.codec->type);
  }
  ExpectVariantValuesEqual(expected, ScanValues(*column, dst.Ctx()));
}

TEST_F(ColumnReaderTest, CopiedRowsOfAColumnWithDistinctStatisticsAreCounted) {
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kId = 1;
  const ValueOf value_of = [](uint64_t g) {
    return duckdb::Value::BIGINT(static_cast<int64_t>(g % 300));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::BIGINT, kRows, kRgSize,
              value_of);
  irs::ColReader src = irs::tests::OpenColReader(dir, "seg");
  const auto& source = *src.Column(kId);
  {
    irs::ColWriter w{Db(), dir.Tracker("dst")};
    auto& cw = w.OpenColumn(
      kId, duckdb::LogicalType::BIGINT,
      {.row_group_size = kRgSize, .distinct = irs::DistinctStats::Exact});
    irs::ReadContext context{src};
    cw.CopyRows(0, source, context, 0, kRows);
    ASSERT_TRUE(w.Commit(0));
  }
  {
    irs::ColWriter w{Db(), dir.Tracker("ref")};
    auto& cw = w.OpenColumn(
      kId, duckdb::LogicalType::BIGINT,
      {.row_group_size = kRgSize, .distinct = irs::DistinctStats::Exact});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        v.SetValue(k, value_of(pos + k));
      }
      duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
      cw.Append(v, STANDARD_VECTOR_SIZE);
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader ref = irs::tests::OpenColReader(dir, "ref");
  ASSERT_NE(ref.Column(kId)->Distinct(), nullptr);
  irs::ColReader r = irs::tests::OpenColReader(dir, "dst");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_NE(col->Distinct(), nullptr);
  EXPECT_EQ(col->Distinct()->GetCount(),
            ref.Column(kId)->Distinct()->GetCount());
  const auto values = ScanValues(*col, r.Ctx());
  ASSERT_EQ(values.size(), kRows);
  for (uint64_t g = 0; g < kRows; ++g) {
    ASSERT_EQ(values[g], value_of(g)) << "row " << g;
  }
}

TEST_F(ColumnReaderTest, SparseStructsWithAListFieldPadNulls) {
  const auto type = duckdb::LogicalType::STRUCT(
    {{"id", duckdb::LogicalType::BIGINT},
     {"tags", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)}});
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteSparseViaSql(
    Db(), dir, "sps", 53, type,
    "SELECT CASE WHEN i % 31 = 0 THEN NULL ELSE {'id': i, 'tags': CASE "
    "WHEN i % 11 = 0 THEN NULL ELSE ['t' || (i % 4), 'x'] END} END FROM "
    "range(9000) t(i)",
    4096, 1500, expected);
  irs::ColReader r = irs::tests::OpenColReader(dir, "sps");
  const auto* col = r.Column(53);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), expected.size());
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

std::string ReadWithCorruptedFooter(
  duckdb::DatabaseInstance& db, const duckdb::LogicalType& type,
  irs::ColumnWriterOptions options, const ValueOf& value_of,
  const std::function<void(irs::ColumnMeta&)>& corrupt,
  const std::function<void(irs::ColReader&, const irs::ColumnReader&)>& read) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 2 * STANDARD_VECTOR_SIZE;
  tests::IndexDir dir{db, TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{db, dir.Tracker("fseg"), options.codec_params.tier};
    options.row_group_size = kRows;
    auto& cw = w.OpenColumn(kId, type, options);
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector v{type, STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t k = 0; k < STANDARD_VECTOR_SIZE; ++k) {
        v.SetValue(k, value_of(pos + k));
      }
      duckdb::FlatVector::SetSize(v, STANDARD_VECTOR_SIZE);
      cw.Append(v, STANDARD_VECTOR_SIZE);
    }
    EXPECT_TRUE(w.Commit(kRows));
  }
  const auto file = irs::FileName("fseg");
  std::string bytes;
  std::vector<irs::ColumnMeta> metas;
  {
    const auto in = dir.Open(file, duckdb::MappedFileAccess::SEQUENTIAL);
    const auto stored = irs::format_utils::ReadFooter(
      *in, file, dir.Sizes("fseg", irs::kColFile),
      [&](duckdb::BinaryDeserializer& footer, uint64_t) {
        footer.Set<duckdb::DatabaseInstance&>(db);
        footer.ReadOptionalList(
          irs::kColFieldColumns, "columns",
          [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
            list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
              metas.emplace_back(irs::DeserializeColumnMeta(obj));
            });
          });
        footer.Unset<duckdb::DatabaseInstance>();
      });
    bytes.resize(stored.data_len);
    duckdb::InputFileReader reader{*in};
    reader.ReadData(reinterpret_cast<irs::byte_type*>(bytes.data()),
                    bytes.size());
  }
  EXPECT_EQ(metas.size(), 1);
  if (metas.size() != 1) {
    return {};
  }
  corrupt(metas.front());
  {
    auto out = dir.Create(file);
    out->WriteData(reinterpret_cast<const irs::byte_type*>(bytes.data()),
                   bytes.size());
    const auto footer = irs::format_utils::WriteFooter(
      *out, [&](duckdb::BinarySerializer& footer) {
        footer.WriteList(
          irs::kColFieldColumns, "columns", metas.size(),
          [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
            list.WriteObject([&](duckdb::BinarySerializer& obj) {
              irs::SerializeColumnMeta(obj, metas[i]);
            });
          });
      });
    dir.Adopt("fseg", irs::kColFile,
              {.file = out->Position(), .footer = footer});
    out->Close();
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "fseg");
  const auto* col = r.Column(kId);
  EXPECT_NE(col, nullptr);
  if (!col) {
    return {};
  }
  try {
    read(r, *col);
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(e.error().errcode, ERRCODE_INDEX_CORRUPTED) << e.what();
    return std::string{e.message()};
  }
  return {};
}

TEST_F(ColumnReaderTest, FooterBlocksSmallerThanTheirCodecNeedsAreRejected) {
  const auto fetch = [](irs::ColReader& r, const irs::ColumnReader& col) {
    irs::ColumnReader::PointReader reader{r, col};
    duckdb::Vector out{col.Type(), 1};
    reader.FetchRow(1000, out, 0);
  };
  const auto scan_from = [](uint64_t row) {
    return [row](irs::ColReader& r, const irs::ColumnReader& col) {
      auto state = col.InitScan(r.Ctx());
      col.Skip(state, row);
      duckdb::Vector out{col.Type(), STANDARD_VECTOR_SIZE};
      col.Scan(state, out, 100);
    };
  };
  const auto data_size = [](uint64_t size) {
    return
      [size](irs::ColumnMeta& meta) { meta.data.front().byte_size = size; };
  };
  const ValueOf numbers = [](uint64_t g) {
    return duckdb::Value::BIGINT(static_cast<int64_t>(g % 1000));
  };
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::BOOLEAN,
              {.compression = irs::codecs::CodecType::Roaring},
              [](uint64_t g) { return duckdb::Value::BOOLEAN(g % 7 == 0); },
              data_size(4), fetch),
            "Corrupted roaring segment: segment is smaller than its header");
  EXPECT_EQ(
    ReadWithCorruptedFooter(
      Db(), duckdb::LogicalType::VARCHAR,
      {.compression = irs::codecs::CodecType::Uncompressed},
      [](uint64_t g) { return duckdb::Value{absl::StrCat("v-", g)}; },
      data_size(4), fetch),
    "Corrupted uncompressed string segment: header exceeds the segment");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::BIGINT,
              {.compression = irs::codecs::CodecType::Uncompressed}, numbers,
              data_size(8), scan_from(1000)),
            "Corrupted uncompressed segment: rows exceed the segment size");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::BIGINT,
              {.compression = irs::codecs::CodecType::Dict}, numbers,
              data_size(irs::codecs::kNumericHeaderSize), fetch),
            "Corrupted numeric segment: header is inconsistent");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::BIGINT,
              {.compression = irs::codecs::CodecType::Bitpacking}, numbers,
              data_size(irs::codecs::kNumericHeaderSize - 1), fetch),
            "Corrupted numeric segment: segment is smaller than its header");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::HUGEINT,
              {.compression = irs::codecs::CodecType::Bitpacking},
              [](uint64_t g) {
                return duckdb::Value::HUGEINT(
                  duckdb::hugeint_t{static_cast<int64_t>(g % 3), g * 7919});
              },
              data_size(8), fetch),
            "Corrupted numeric segment: segment is smaller than its header");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::DOUBLE,
              {.compression = irs::codecs::CodecType::Alp},
              [](uint64_t g) {
                return duckdb::Value::DOUBLE(static_cast<double>(g % 1000) / 8);
              },
              data_size(2), fetch),
            "Corrupted ALP segment: metadata ends before the segment header");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::BIGINT,
              {.compression = irs::codecs::CodecType::Bitpacking}, numbers,
              [](irs::ColumnMeta& meta) {
                meta.data.front().tuple_count =
                  uint64_t{std::numeric_limits<uint32_t>::max()} + 1;
              },
              fetch),
            "Corrupted numeric segment: header is inconsistent");
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'uncompressed'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::BIGINT, {},
              [](uint64_t g) {
                return g % 3 == 0
                         ? duckdb::Value{duckdb::LogicalType::BIGINT}
                         : duckdb::Value::BIGINT(static_cast<int64_t>(g));
              },
              [](irs::ColumnMeta& meta) {
                ASSERT_FALSE(meta.validity.empty());
                ASSERT_EQ(meta.validity.front().codec->type,
                          irs::codecs::CodecType::Uncompressed);
                meta.validity.front().byte_size = 8;
              },
              fetch),
            "Corrupted validity segment: rows exceed the segment size");
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, Fsst12BlocksWithoutTheirSharedSymbolTableAreRejected) {
  const auto lost = [](irs::ColumnMeta& meta) {
    for (auto& block : meta.data) {
      ASSERT_NE(block.shared_size, 0);
      block.shared_offset = 0;
      block.shared_size = 0;
    }
  };
  const ValueOf urls = [](uint64_t g) {
    return duckdb::Value{absl::StrCat("https://example.org/", g % 977, "/item/",
                                      (g * 7919) % 100003)};
  };
  const irs::ColumnWriterOptions fsst12{.compression =
                                          irs::codecs::CodecType::Fsst12};
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::VARCHAR, fsst12, urls, lost,
              [](irs::ColReader& r, const irs::ColumnReader& col) {
                auto state = col.InitScan(r.Ctx());
                duckdb::Vector out{col.Type(), STANDARD_VECTOR_SIZE};
                col.Scan(state, out, STANDARD_VECTOR_SIZE);
              }),
            "Corrupted text segment: entry is inconsistent");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::VARCHAR, fsst12, urls, lost,
              [](irs::ColReader& r, const irs::ColumnReader& col) {
                irs::ColumnReader::PointReader reader{r, col};
                duckdb::Vector out{col.Type(), 1};
                reader.FetchRow(3, out, 0);
              }),
            "Corrupted text segment: front coding is inconsistent");
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::VARCHAR, fsst12, urls, lost,
              [](irs::ColReader& r, const irs::ColumnReader& col) {
                irs::ColumnReader::PointReader reader{r, col};
                duckdb::Vector out{col.Type(), 1};
                reader.FetchRow(0, out, 0);
              }),
            "Corrupted text segment: entry is inconsistent");
}

TEST_F(ColumnReaderTest,
       BitsetValidityWithAWholeValidWordScansIntoFreshVectors) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 3 * STANDARD_VECTOR_SIZE;
  const ValueOf value_of = [](uint64_t g) {
    return g % STANDARD_VECTOR_SIZE >= 64 && g % 2 == 0
             ? duckdb::Value{duckdb::LogicalType::BIGINT}
             : duckdb::Value::BIGINT(static_cast<int64_t>(g * 5));
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'roaring'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::BIGINT, kRows, 4096,
              value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_NE(col->Validity(), nullptr);
  ASSERT_EQ(col->Validity()->DataBlocks().front().codec->type,
            irs::codecs::CodecType::Roaring);
  const auto values = ScanValues(*col, r.Ctx());
  ASSERT_EQ(values.size(), kRows);
  for (uint64_t g = 0; g < kRows; ++g) {
    ASSERT_TRUE(duckdb::Value::NotDistinctFrom(values[g], value_of(g)))
      << "row " << g << " got " << values[g].ToString();
  }
}

TEST_F(ColumnReaderTest, ListProbesOfOnlyNullListsKeepTheirValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kChunks = 10;
  constexpr uint64_t kRows = kChunks * STANDARD_VECTOR_SIZE;
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  const ValueOf value_of = [&](uint64_t g) {
    if (g / STANDARD_VECTOR_SIZE == 8 && g % STANDARD_VECTOR_SIZE < 600) {
      return duckdb::Value{type};
    }
    return duckdb::Value::LIST(
      duckdb::LogicalType::VARCHAR,
      {duckdb::Value{absl::StrCat("u-", g)}, duckdb::Value{"v"}});
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, type, kRows, 32768, value_of);
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  const auto values = ScanValues(*col, r.Ctx());
  ASSERT_EQ(values.size(), kRows);
  for (uint64_t g = 0; g < kRows; ++g) {
    ASSERT_TRUE(duckdb::Value::NotDistinctFrom(values[g], value_of(g)))
      << "row " << g << " got " << values[g].ToString();
  }
}

TEST_F(ColumnReaderTest, RangeFiltersOnSortedLz4DictionariesMatchTheValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 9000;
  const auto mix = [](uint64_t z) {
    z += 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
  };
  const auto clustered = [&](uint64_t k) {
    std::string s;
    for (uint64_t i = 0; i < 12; ++i) {
      absl::StrAppend(&s, absl::Hex(mix(k / 4 * 16 + i), absl::kZeroPad16));
    }
    return absl::StrCat(s, "/", k % 4);
  };
  const ValueOf value_of = [&](uint64_t g) {
    return g % 13 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                       : duckdb::Value{clustered(mix(g) % 4000)};
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'dict_lz4'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, 4096,
              value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  for (size_t block = 0; block != col->DataBlocks().size(); ++block) {
    ASSERT_EQ(col->DataBlocks()[block].codec->type,
              irs::codecs::CodecType::DictLz4);
    const auto segment = col->OpenSegment(block, r.Ctx());
    const auto info = segment->function.get_segment_info(*segment);
    ASSERT_EQ(info.at("sorted"), "true") << "block " << block;
  }
  using Cmp = duckdb::ExpressionType;
  for (const auto cmp :
       {Cmp::COMPARE_LESSTHAN, Cmp::COMPARE_GREATERTHANOREQUALTO}) {
    SCOPED_TRACE(duckdb::ExpressionTypeToString(cmp));
    const std::string bound = clustered(2001);
    const duckdb::ExpressionFilter filter{
      duckdb::BoundComparisonExpression::Create(
        cmp,
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(
          duckdb::LogicalType::VARCHAR, 0),
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          duckdb::Value{bound}))};
    auto filter_state =
      duckdb::TableFilterState::Initialize(*con.context, filter);
    auto state = col->InitScan(r.Ctx());
    for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
      const auto span =
        std::min<uint64_t>(kRows - anchor, STANDARD_VECTOR_SIZE);
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (duckdb::idx_t i = 0; i < span; ++i) {
        sel.set_index(i, i);
      }
      duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
      const auto kept =
        col->GatherFilter(state, anchor, span, sel, span, filter, *filter_state,
                          irs::NullCheckKind::None, out);
      std::vector<uint64_t> want;
      for (duckdb::idx_t i = 0; i < span; ++i) {
        const auto v = value_of(anchor + i);
        if (v.IsNull()) {
          continue;
        }
        const auto s = v.GetValue<std::string>();
        if (cmp == Cmp::COMPARE_LESSTHAN ? s < bound : s >= bound) {
          want.push_back(i);
        }
      }
      ASSERT_EQ(kept, want.size()) << "span at " << anchor;
      for (duckdb::idx_t i = 0; i < kept; ++i) {
        ASSERT_EQ(sel.get_index(i), want[i]) << "span at " << anchor;
      }
    }
  }
}

TEST_F(ColumnReaderTest,
       RangeFiltersOnASortedLz4DictionaryDecodedByAScanMatchTheValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 2 * STANDARD_VECTOR_SIZE;
  const auto mix = [](uint64_t z) {
    z += 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
  };
  const auto clustered = [&](uint64_t k) {
    std::string s;
    for (uint64_t i = 0; i < 12; ++i) {
      absl::StrAppend(&s, absl::Hex(mix(k / 4 * 16 + i), absl::kZeroPad16));
    }
    return absl::StrCat(s, "/", k % 4);
  };
  const ValueOf value_of = [&](uint64_t g) {
    return g % 13 == 0 ? duckdb::Value{duckdb::LogicalType::VARCHAR}
                       : duckdb::Value{clustered(mix(g) % 4000)};
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'dict_lz4'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, duckdb::LogicalType::VARCHAR, kRows, kRows,
              value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_GT(col->DataBlocks().front().tuple_count, STANDARD_VECTOR_SIZE);
  for (size_t block = 0; block != col->DataBlocks().size(); ++block) {
    ASSERT_EQ(col->DataBlocks()[block].codec->type,
              irs::codecs::CodecType::DictLz4);
    const auto segment = col->OpenSegment(block, r.Ctx());
    ASSERT_EQ(segment->function.get_segment_info(*segment).at("sorted"), "true")
      << "block " << block;
  }
  using Cmp = duckdb::ExpressionType;
  for (const auto cmp :
       {Cmp::COMPARE_LESSTHAN, Cmp::COMPARE_GREATERTHANOREQUALTO}) {
    SCOPED_TRACE(duckdb::ExpressionTypeToString(cmp));
    const std::string bound = clustered(2001);
    const duckdb::ExpressionFilter filter{
      duckdb::BoundComparisonExpression::Create(
        cmp,
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(
          duckdb::LogicalType::VARCHAR, 0),
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          duckdb::Value{bound}))};
    auto filter_state =
      duckdb::TableFilterState::Initialize(*con.context, filter);
    auto state = col->InitScan(r.Ctx());
    duckdb::Vector scanned{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    ASSERT_EQ(col->Scan(state, scanned, STANDARD_VECTOR_SIZE),
              STANDARD_VECTOR_SIZE);
    for (uint64_t g = 0; g < STANDARD_VECTOR_SIZE; ++g) {
      const auto got = scanned.GetValue(g);
      const auto want = value_of(g);
      ASSERT_EQ(got.IsNull(), want.IsNull()) << "row " << g;
      if (!want.IsNull()) {
        ASSERT_EQ(got.GetValue<std::string>(), want.GetValue<std::string>())
          << "row " << g;
      }
    }
    constexpr uint64_t kAnchor = STANDARD_VECTOR_SIZE;
    constexpr duckdb::idx_t kSpan = STANDARD_VECTOR_SIZE;
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < kSpan; ++i) {
      sel.set_index(i, i);
    }
    duckdb::Vector out{duckdb::LogicalType::VARCHAR, STANDARD_VECTOR_SIZE};
    const auto kept =
      col->GatherFilter(state, kAnchor, kSpan, sel, kSpan, filter,
                        *filter_state, irs::NullCheckKind::None, out);
    std::vector<uint64_t> want;
    for (duckdb::idx_t i = 0; i < kSpan; ++i) {
      const auto v = value_of(kAnchor + i);
      if (v.IsNull()) {
        continue;
      }
      const auto s = v.GetValue<std::string>();
      if (cmp == Cmp::COMPARE_LESSTHAN ? s < bound : s >= bound) {
        want.push_back(i);
      }
    }
    ASSERT_FALSE(want.empty());
    ASSERT_EQ(kept, want.size());
    for (duckdb::idx_t i = 0; i < kept; ++i) {
      ASSERT_EQ(sel.get_index(i), want[i]);
      ASSERT_EQ(out.GetValue(want[i]).GetValue<std::string>(),
                value_of(kAnchor + want[i]).GetValue<std::string>());
    }
  }
}

TEST_F(ColumnReaderTest,
       CopiedBlocksOfARowGroupSplitIntoSeveralKeepTheirValidity) {
  constexpr uint32_t kRgSize = 4 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 3 * kRgSize;
  constexpr irs::field_id kS = 1;
  const auto type = duckdb::LogicalType::VARCHAR;
  const irs::ColumnWriterOptions options{
    .row_group_size = kRgSize,
    .compression = irs::codecs::CodecType::Fsst12,
    .codec_params = {.segment_target = 32 * 1024}};
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(
    Db(), dir, "src", kS, type,
    absl::StrCat("SELECT CASE WHEN i < ", kRgSize,
                 " AND i % 7 = 0 THEN NULL ELSE 'https://example.org/' || (i % "
                 "977) || '/item/' || (i * 7919 % 100003) || '?ref=' || i END "
                 "FROM range(",
                 kRows, ") t(i)"),
    kRgSize, expected, options);
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto& source = *src.Column(kS);
  ASSERT_GT(source.DataBlocks().size(), 6);
  ASSERT_NE(source.Validity(), nullptr);
  ASSERT_LT(source.Validity()->DataBlocks().size(), source.DataBlocks().size());
  {
    irs::ColWriter w{Db(), dir.Tracker("dst"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kS, type, options);
    irs::ReadContext context{src};
    cw.CopyRows(0, source, context, 0, kRows);
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "dst");
  const auto* col = r.Column(kS);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest, FooterTextBlocksSmallerThanTheirHeaderAreRejected) {
  EXPECT_EQ(ReadWithCorruptedFooter(
              Db(), duckdb::LogicalType::VARCHAR,
              {.compression = irs::codecs::CodecType::Zstd},
              [](uint64_t g) {
                return duckdb::Value{absl::StrCat("text-", g % 300, "-value")};
              },
              [](irs::ColumnMeta& meta) { meta.data.front().byte_size = 16; },
              [](irs::ColReader& r, const irs::ColumnReader& col) {
                irs::ColumnReader::PointReader reader{r, col};
                duckdb::Vector out{col.Type(), 1};
                reader.FetchRow(10, out, 0);
              }),
            "Corrupted text segment: segment is smaller than its header");
}

TEST_F(ColumnReaderTest, SmallListGathersWidenToTheirBlockOnlyWhileItFits) {
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  const auto check = [&](uint32_t rg_size, uint64_t elems,
                         const std::vector<std::vector<uint64_t>>& gathers) {
    SCOPED_TRACE(absl::StrCat(rg_size, " lists of ", elems));
    tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
    std::vector<duckdb::Value> expected;
    WriteViaSql(Db(), dir, "lseg", 45, type,
                absl::StrCat("SELECT list_transform(range(", elems,
                             "), lambda k: (i * 7 + k)::INTEGER) FROM range(",
                             rg_size, ") t(i)"),
                rg_size, expected);
    irs::ColReader r = irs::tests::OpenColReader(dir, "lseg");
    const auto* col = r.Column(45);
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->RowCount(), rg_size);
    auto state = col->InitScan(r.Ctx());
    for (const auto& rows : gathers) {
      const auto anchor = rows.front();
      duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
      for (size_t i = 0; i < rows.size(); ++i) {
        sel.set_index(i, rows[i] - anchor);
      }
      duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
      col->GatherScatter(state, anchor, sel, rows.size(), out, 0);
      for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(out.GetValue(i).ToString(), expected[rows[i]].ToString())
          << "row " << rows[i];
      }
    }
  };
  check(8192, 80,
        {{0, 600, 1200, 1800, 2400, 3000, 3600, 4200, 4800, 5400},
         {6000, 6005},
         {6100, 6105},
         {6200, 6205},
         {6300, 6305}});
  check(16384, 2, {{100, 105}, {200, 205}, {300, 305}});
}

TEST_F(ColumnReaderTest, ListGathersAcrossDictionariesOfSeveralEndsBlocks) {
  constexpr uint64_t kChunk = STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 50 * kChunk;
  constexpr uint64_t kFresh = 1792;
  constexpr irs::field_id kId = 46;
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  const auto mix = [](uint64_t z) {
    z += 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
  };
  const irs::containers::FlatHashMap<uint64_t, uint64_t> repeats{
    {47 * kChunk + kFresh + 50, 1000},   {47 * kChunk + kFresh + 51, 1003},
    {48 * kChunk + kFresh + 100, 69990}, {48 * kChunk + kFresh + 101, 75000},
    {49 * kChunk + kFresh + 100, 70000}, {49 * kChunk + kFresh + 101, 70001},
    {49 * kChunk + kFresh + 102, 70002}, {49 * kChunk + kFresh + 103, 70003},
    {49 * kChunk + kFresh + 120, 69000}, {49 * kChunk + kFresh + 121, 69001},
    {49 * kChunk + kFresh + 122, 69002},
  };
  std::vector<uint64_t> code(kRows);
  std::vector<uint64_t> row_of;
  for (uint64_t r = 0; r < kRows; ++r) {
    if (r % kChunk < kFresh) {
      code[r] = row_of.size();
      row_of.push_back(r);
    } else if (const auto it = repeats.find(r); it != repeats.end()) {
      code[r] = it->second;
    } else {
      code[r] = mix(r) % row_of.size();
    }
  }
  ASSERT_GT(row_of.size(), 85710u);
  const ValueOf value_of = [&](uint64_t r) {
    return duckdb::Value::LIST(
      duckdb::LogicalType::INTEGER,
      {duckdb::Value::INTEGER(static_cast<int32_t>(code[r]))});
  };
  duckdb::Connection con{Db()};
  auto set = con.Query("SET force_compression = 'uncompressed'");
  ASSERT_FALSE(set->HasError()) << set->GetError();
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  WriteColumn(Db(), dir, kId, type, kRows, DEFAULT_ROW_GROUP_SIZE, value_of);
  con.Query("SET force_compression = 'auto'");
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  size_t ends_blocks = 0;
  col->VisitSubColumns([&](const irs::ColumnReader::SubColumn& sub) {
    if (sub.index == 2) {
      ends_blocks = sub.column.DataBlocks().size();
    }
  });
  ASSERT_GE(ends_blocks, 3u);
  auto state = col->InitScan(r.Ctx());
  const auto gather = [&](const std::vector<uint64_t>& rows) {
    const auto anchor = rows.front();
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    for (size_t i = 0; i < rows.size(); ++i) {
      sel.set_index(i, rows[i] - anchor);
    }
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    col->GatherScatter(state, anchor, sel, rows.size(), out, 0);
    for (size_t i = 0; i < rows.size(); ++i) {
      ASSERT_EQ(out.GetValue(i).ToString(), value_of(rows[i]).ToString())
        << "row " << rows[i];
    }
  };
  std::vector<uint64_t> spread;
  for (uint64_t k = 0; k < 40; ++k) {
    spread.push_back(row_of[80640 + 130 * k]);
  }
  ASSERT_NO_FATAL_FAILURE(gather(spread));
  ASSERT_NO_FATAL_FAILURE(
    gather({47 * kChunk + kFresh + 50, 47 * kChunk + kFresh + 51}));
  ASSERT_NO_FATAL_FAILURE(
    gather({48 * kChunk + kFresh + 100, 48 * kChunk + kFresh + 101}));
  ASSERT_NO_FATAL_FAILURE(
    gather({49 * kChunk + kFresh + 100, 49 * kChunk + kFresh + 101,
            49 * kChunk + kFresh + 102, 49 * kChunk + kFresh + 103}));
  ASSERT_NO_FATAL_FAILURE(
    gather({49 * kChunk + kFresh + 120, 49 * kChunk + kFresh + 121,
            49 * kChunk + kFresh + 122}));
  ExpectVariantValuesEqual(
    [&] {
      std::vector<duckdb::Value> all;
      for (uint64_t row = 0; row < kRows; ++row) {
        all.push_back(value_of(row));
      }
      return all;
    }(),
    ScanValues(*col, r.Ctx()));
}

std::vector<irs::ColumnMeta> ReadColumnMetas(duckdb::DatabaseInstance& db,
                                             const tests::IndexDir& dir,
                                             std::string_view segment) {
  const auto file = irs::FileName(segment);
  std::vector<irs::ColumnMeta> metas;
  const auto in = dir.Open(file, duckdb::MappedFileAccess::SEQUENTIAL);
  irs::format_utils::ReadFooter(
    *in, file, dir.Sizes(segment, irs::kColFile),
    [&](duckdb::BinaryDeserializer& footer, uint64_t) {
      footer.Set<duckdb::DatabaseInstance&>(db);
      footer.ReadOptionalList(
        irs::kColFieldColumns, "columns",
        [&](duckdb::BinaryDeserializer::List& list, duckdb::idx_t) {
          list.ReadObject([&](duckdb::BinaryDeserializer& obj) {
            metas.emplace_back(irs::DeserializeColumnMeta(obj));
          });
        });
      footer.Unset<duckdb::DatabaseInstance>();
    });
  return metas;
}

void WriteForced(duckdb::DatabaseInstance& db, const tests::IndexDir& dir,
                 std::string_view codec, irs::field_id id,
                 const duckdb::LogicalType& type, uint64_t rows,
                 uint32_t rg_size, const ValueOf& value_of) {
  duckdb::Connection con{db};
  auto set = con.Query(absl::StrCat("SET force_compression = '", codec, "'"));
  ASSERT_FALSE(set->HasError()) << set->GetError();
  WriteColumn(db, dir, id, type, rows, rg_size, value_of);
  con.Query("SET force_compression = 'auto'");
}

TEST_F(ColumnReaderTest, GathersOfACodecWithoutASelectFunctionScanTheirWindow) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 4 * STANDARD_VECTOR_SIZE;
  const ValueOf value_of = [](uint64_t g) {
    return duckdb::Value::BIGINT(static_cast<int64_t>(g * 7919 % 100003));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  ASSERT_NO_FATAL_FAILURE(WriteForced(Db(), dir, "bitpacking", kId,
                                      duckdb::LogicalType::BIGINT, kRows, kRows,
                                      value_of));
  auto metas = ReadColumnMetas(Db(), dir, "seg");
  ASSERT_EQ(metas.size(), 1);
  const auto& stored = *metas.front().data.front().codec;
  ASSERT_EQ(stored.type, irs::codecs::CodecType::Bitpacking);
  ASSERT_NE(stored.decompression.select, nullptr);
  auto scanned = stored.decompression;
  scanned.select = nullptr;
  const irs::codecs::Codec unselected{
    .type = stored.type,
    .compression = stored.compression,
    .decompression = scanned,
  };
  for (auto& block : metas.front().data) {
    block.codec = &unselected;
  }
  irs::ColReader r{
    Db(), dir.Open(irs::FileName("seg"), duckdb::MappedFileAccess::SEQUENTIAL),
    irs::ColFooter{.columns = std::move(metas)}};
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < kRows; anchor += STANDARD_VECTOR_SIZE) {
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    duckdb::idx_t hits = 0;
    for (duckdb::idx_t i = 3; i < STANDARD_VECTOR_SIZE; i += 10) {
      sel.set_index(hits++, i);
    }
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    col->GatherScatter(state, anchor, sel, hits, out, 0);
    for (duckdb::idx_t k = 0; k < hits; ++k) {
      ASSERT_EQ(out.GetValue(k).ToString(),
                value_of(anchor + sel.get_index(k)).ToString())
        << "row " << anchor + sel.get_index(k);
    }
  }
}

TEST_F(ColumnReaderTest, FiltersAcrossTheEndOfAValidityBlockReadTheirRows) {
  constexpr irs::field_id kId = 1;
  constexpr uint32_t kRg = STANDARD_VECTOR_SIZE;
  constexpr uint64_t kRows = 2 * kRg;
  constexpr uint64_t kSplit = 1000;
  const ValueOf value_of = [](uint64_t g) {
    return g >= kRg && g % 7 == 0
             ? duckdb::Value{duckdb::LogicalType::BIGINT}
             : duckdb::Value::BIGINT(static_cast<int64_t>(g * 7919 % 1000));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  ASSERT_NO_FATAL_FAILURE(WriteForced(Db(), dir, "bitpacking", kId,
                                      duckdb::LogicalType::BIGINT, kRows, kRg,
                                      value_of));
  auto metas = ReadColumnMetas(Db(), dir, "seg");
  ASSERT_EQ(metas.size(), 1);
  auto& validity = metas.front().validity;
  ASSERT_EQ(validity.size(), 2);
  ASSERT_EQ(validity.front().codec->type, irs::codecs::CodecType::Empty);
  ASSERT_EQ(validity.front().tuple_count, kRg);
  ASSERT_NE(validity.back().codec->type, irs::codecs::CodecType::Empty);
  const auto* empty = validity.front().codec;
  validity.front().tuple_count = kRg - kSplit;
  validity.insert(validity.begin(),
                  irs::ColumnBlockMeta{
                    .statistics = duckdb::BaseStatistics::CreateEmpty(
                      duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY}),
                    .tuple_count = kSplit,
                    .codec = empty,
                  });
  irs::ColReader r{
    Db(), dir.Open(irs::FileName("seg"), duckdb::MappedFileAccess::SEQUENTIAL),
    irs::ColFooter{.columns = std::move(metas)}};
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->DataBlocks().size(), 2);
  duckdb::Connection con{Db()};
  const duckdb::ExpressionFilter filter{
    duckdb::BoundComparisonExpression::Create(
      duckdb::ExpressionType::COMPARE_LESSTHAN,
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        duckdb::LogicalType::BIGINT, 0),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::BIGINT(500)))};
  auto filter_state =
    duckdb::TableFilterState::Initialize(*con.context, filter);
  auto state = col->InitScan(r.Ctx());
  for (uint64_t anchor = 0; anchor < kRows; anchor += kRg) {
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    for (duckdb::idx_t i = 0; i < kRg; ++i) {
      sel.set_index(i, i);
    }
    duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
    const auto kept =
      col->GatherFilter(state, anchor, kRg, sel, kRg, filter, *filter_state,
                        irs::NullCheckKind::None, out);
    std::vector<uint64_t> want;
    for (uint64_t g = anchor; g < anchor + kRg; ++g) {
      const auto v = value_of(g);
      if (!v.IsNull() && v.GetValue<int64_t>() < 500) {
        want.push_back(g);
      }
    }
    ASSERT_EQ(kept, want.size()) << "anchor " << anchor;
    for (duckdb::idx_t k = 0; k < kept; ++k) {
      const auto row = anchor + sel.get_index(k);
      ASSERT_EQ(row, want[k]);
      ASSERT_EQ(out.GetValue(sel.get_index(k)).ToString(),
                value_of(row).ToString())
        << "row " << row;
    }
  }
}

TEST_F(ColumnReaderTest, AFieldlessStructKeepsTheRowsOfItsValidityOnly) {
  const auto type =
    duckdb::LogicalType::STRUCT(duckdb::child_list_t<duckdb::LogicalType>{});
  const auto open = [&](uint64_t rows) {
    irs::ColumnMeta meta;
    meta.id = 9;
    meta.type = type;
    if (rows != 0) {
      const duckdb::LogicalType validity{duckdb::LogicalTypeId::VALIDITY};
      meta.validity.push_back(irs::ColumnBlockMeta{
        .statistics = duckdb::BaseStatistics::CreateEmpty(validity),
        .tuple_count = rows,
        .codec = irs::codecs::GetCodec(irs::codecs::CodecType::Empty,
                                       validity.InternalType()),
      });
    }
    irs::ColFooter footer;
    footer.columns.push_back(std::move(meta));
    return std::make_unique<irs::ColReader>(Db(), nullptr, std::move(footer));
  };
  const auto with = open(100);
  ASSERT_NE(with->Column(9), nullptr);
  EXPECT_EQ(with->Column(9)->RowCount(), 100);
  EXPECT_NE(with->Column(9)->Validity(), nullptr);
  const auto without = open(0);
  ASSERT_NE(without->Column(9), nullptr);
  EXPECT_EQ(without->Column(9)->RowCount(), 0);
  EXPECT_EQ(without->Column(9)->Validity(), nullptr);
}

TEST_F(ColumnReaderTest, GathersIntoVectorsOfAnotherTypeKeepTheirValues) {
  constexpr irs::field_id kId = 1;
  constexpr uint64_t kRows = 6 * STANDARD_VECTOR_SIZE;
  const ValueOf value_of = [](uint64_t g) {
    return duckdb::Value::BIGINT(static_cast<int64_t>(g * 7919 % 1000003));
  };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  ASSERT_NO_FATAL_FAILURE(WriteForced(Db(), dir, "bitpacking", kId,
                                      duckdb::LogicalType::BIGINT, kRows, kRows,
                                      value_of));
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  auto state = col->InitScan(r.Ctx());
  uint64_t anchor = 0;
  for (const duckdb::LogicalType type :
       {duckdb::LogicalType::BIGINT, duckdb::LogicalType::TIMESTAMP,
        duckdb::LogicalType::BIGINT}) {
    SCOPED_TRACE(type.ToString());
    duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
    duckdb::idx_t hits = 0;
    for (duckdb::idx_t i = 1; i < STANDARD_VECTOR_SIZE; i += 2) {
      sel.set_index(hits++, i);
    }
    duckdb::Vector out{type, STANDARD_VECTOR_SIZE};
    col->GatherScatter(state, anchor, sel, hits, out, 0);
    const auto* values = duckdb::FlatVector::GetData<int64_t>(out);
    for (duckdb::idx_t k = 0; k < hits; ++k) {
      ASSERT_EQ(values[k],
                value_of(anchor + sel.get_index(k)).GetValue<int64_t>())
        << "row " << anchor + sel.get_index(k);
    }
    anchor += STANDARD_VECTOR_SIZE;
  }
}

TEST_F(ColumnReaderTest, ColumnsWithoutValidityDecodeASourceThatHasIt) {
  constexpr irs::field_id kId = 1;
  constexpr uint32_t kRgSize = 4096;
  constexpr uint64_t kRows = 2 * kRgSize;
  const auto type = duckdb::LogicalType::BIGINT;
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  WriteViaSql(Db(), dir, "src", kId, type,
              absl::StrCat("SELECT CASE WHEN i % 7 = 0 THEN NULL ELSE i * 5 + "
                           "1 END::BIGINT FROM range(",
                           kRows, ") t(i)"),
              kRgSize, expected);
  irs::ColReader src = irs::tests::OpenColReader(dir, "src");
  const auto* source = src.Column(kId);
  ASSERT_NE(source, nullptr);
  ASSERT_TRUE(source->HasValidity());
  {
    irs::ColWriter w{Db(), dir.Tracker("rows")};
    auto& cw = w.OpenColumn(kId, type,
                            {.skip_validity = true, .row_group_size = kRgSize});
    EXPECT_FALSE(cw.CopyableFrom(*source));
    irs::ReadContext context{src};
    cw.CopyRows(0, *source, context, 0, kRows);
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "rows");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  EXPECT_FALSE(col->HasValidity());
  const auto values = ScanValues(*col, r.Ctx());
  ASSERT_EQ(values.size(), expected.size());
  for (size_t g = 0; g < values.size(); ++g) {
    if (!expected[g].IsNull()) {
      EXPECT_EQ(values[g].ToString(), expected[g].ToString()) << "row " << g;
    }
  }
}

TEST_F(ColumnReaderTest, ListsWhoseElementsArriveAsADictionaryKeepTheirValues) {
  constexpr irs::field_id kId = 48;
  constexpr uint64_t kRows = 8 * STANDARD_VECTOR_SIZE;
  constexpr uint64_t kPool = 16;
  const auto type = duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR);
  const auto element = [](uint64_t k) {
    return absl::StrCat("an element longer than an inlined string ", k);
  };
  const auto first = [](uint64_t g) { return g % kPool; };
  const auto second = [](uint64_t g) { return g / kPool % kPool; };
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  std::vector<duckdb::Value> expected;
  {
    irs::ColWriter w{Db(), dir.Tracker("seg")};
    auto& cw = w.OpenColumn(kId, type, {.row_group_size = kRows});
    for (uint64_t pos = 0; pos < kRows; pos += STANDARD_VECTOR_SIZE) {
      duckdb::Vector lists{type, STANDARD_VECTOR_SIZE};
      duckdb::ListVector::Reserve(lists, 2 * STANDARD_VECTOR_SIZE);
      auto& child = duckdb::ListVector::GetChildMutable(lists);
      for (uint64_t k = 0; k < kPool; ++k) {
        child.SetValue(k, duckdb::Value{element(k)});
      }
      duckdb::SelectionVector picks{2 * STANDARD_VECTOR_SIZE};
      auto* entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(lists);
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
        const auto g = pos + i;
        picks.set_index(2 * i, first(g));
        picks.set_index(2 * i + 1, second(g));
        entries[i] = {2 * i, 2};
        expected.push_back(duckdb::Value::LIST(
          duckdb::LogicalType::VARCHAR, {duckdb::Value{element(first(g))},
                                         duckdb::Value{element(second(g))}}));
      }
      child.Slice(picks, 2 * STANDARD_VECTOR_SIZE);
      ASSERT_EQ(child.GetVectorType(), duckdb::VectorType::DICTIONARY_VECTOR);
      duckdb::ListVector::SetListSize(lists, 2 * STANDARD_VECTOR_SIZE);
      duckdb::FlatVector::SetSize(lists, STANDARD_VECTOR_SIZE);
      cw.Append(lists, STANDARD_VECTOR_SIZE);
    }
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "seg");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  EXPECT_LT(col->Child()->RowCount(), 2 * kRows);
  ExpectVariantValuesEqual(expected, ScanValues(*col, r.Ctx()));
}

TEST_F(ColumnReaderTest,
       ExtractedPathsOfRowsInSeveralVariantRowGroupsReadBack) {
  constexpr irs::field_id kId = 49;
  constexpr uint32_t kRowGroup = 256;
  constexpr uint64_t kRows = 3 * kRowGroup;
  SetShreddingSize(Db(), 0);
  tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  {
    irs::ColWriter w{Db(), dir.Tracker("vpath"), irs::WriteTier::Merge};
    auto& cw = w.OpenColumn(kId, duckdb::LogicalType::VARIANT(),
                            {.row_group_size = kRowGroup});
    duckdb::Connection con{Db()};
    auto result = con.Query(absl::StrCat(
      "SELECT {'a': i * 3}::VARIANT FROM range(", kRows, ") t(i)"));
    ASSERT_FALSE(result->HasError()) << result->GetError();
    while (auto chunk = result->Fetch()) {
      cw.Append(chunk->data[0], chunk->size());
    }
    ASSERT_EQ(cw.Meta().variant_rgs.size(), 3u);
    ASSERT_TRUE(w.Commit(0));
  }
  irs::ColReader r = irs::tests::OpenColReader(dir, "vpath");
  const auto* col = r.Column(kId);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), kRows);
  duckdb::Connection con{Db()};
  irs::ReadContext ctx{r};
  const std::string_view path[] = {"a"};
  irs::ExtractBinding binding;
  binding.Bind(*col, ctx, path, duckdb::LogicalType::BIGINT, con.context.get());
  const uint64_t rows[] = {10, 200, 255, 256, 300, 511, 512, 700};
  std::vector<irs::doc_id_t> docs;
  for (const auto row : rows) {
    docs.push_back(static_cast<irs::doc_id_t>(row + irs::doc_limits::min()));
  }
  duckdb::Vector out{duckdb::LogicalType::BIGINT, STANDARD_VECTOR_SIZE};
  binding.MaterializeRows(irs::DocRows{docs}, out, 0, false);
  for (size_t k = 0; k < std::size(rows); ++k) {
    EXPECT_EQ(out.GetValue(k).ToString(), std::to_string(rows[k] * 3))
      << "row " << rows[k];
  }
}

}  // namespace
