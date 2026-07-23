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
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <duckdb/main/client_context.hpp>
#include <functional>
#include <span>
#include <string_view>
#include <vector>

#include "basics/duckdb_engine.h"
#include "gtest/gtest.h"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"
#include "iresearch/formats/column/variant_column_reader.hpp"
#include "iresearch/store/memory_directory.hpp"

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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             /*skip_validity=*/false, kRgSize);
    auto& cwB = w.OpenColumn(kB, duckdb::LogicalType::BIGINT,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
// multiple row-groups: exercises the IndexOutput overflow writer + reader.
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  // dict_fsst self-carries nulls -> validity absorbed to EMPTY; NullsInData()
  // reports it so readers scan the data (not the empty validity) for nullness.
  ASSERT_TRUE(cs->NullsInData());

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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cw = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                            /*skip_validity=*/false, kRgSize);
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
  irs::ColReader r{dir, "seg", Db()};
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  ASSERT_TRUE(cs->NullsInData());
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT, false, kRgSize);
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR, false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

// STRUCT{a: BIGINT, b: VARCHAR} with struct-level and field-level NULLs across
// multiple row-groups.
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwT = w.OpenColumn(kT, stype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwT = w.OpenColumn(kT, stype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwArr = w.OpenColumn(kArr, atype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
          EXPECT_EQ(cd[k * kDim + i], elem_val(g, i))
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwArr = w.OpenColumn(kArr, atype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
      EXPECT_EQ(cd[i * kDim + e], elem_val(g, e)) << "elem " << g << ":" << e;
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwL = w.OpenColumn(kL, ltype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwL = w.OpenColumn(kL, ltype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwL = w.OpenColumn(kL, ltype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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

// Sparse Gather of a LIST column across row-group boundaries: the doc ids are
// non-contiguous, so every run after the first lands at result_offset > 0 --
// exercising the scratch + VectorOperations::Copy reconstruction (the DuckDB
// VectorListBuffer::CopyInternal mirror; ListColumnData::ScanCount itself
// rejects result_offset > 0, same as us).
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "lseg", Db()};
    auto& cwL = w.OpenColumn(kL, ltype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "lseg", Db()};
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

// Build a VARIANT column from a SQL query (each chunk Append-ed), collecting
// the per-row expected Values. Mirrors variant_shredding_test.cpp's
// construction but drives the new ColWriter.
void WriteVariantViaSql(duckdb::DatabaseInstance& db, irs::Directory& dir,
                        std::string_view segment, irs::field_id id,
                        const std::string& sql, uint32_t rg_size,
                        std::vector<duckdb::Value>& expected) {
  irs::ColWriter w{dir, segment, db};
  auto& cw = w.OpenColumn(id, duckdb::LogicalType::VARIANT(),
                          /*skip_validity=*/false, rg_size);
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

// VARIANT, shredding disabled: many unshredded row groups round-trip.
TEST_F(ColumnReaderTest, VariantRoundTripUnshredded) {
  SetShreddingSize(Db(), -1);
  irs::MemoryDirectory dir{};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/30,
                     "SELECT i::VARIANT FROM range(200) t(i)", /*rg_size=*/64,
                     expected);
  ASSERT_EQ(expected.size(), 200u);

  irs::ColReader r{dir, "vseg", Db()};
  const auto* col = r.Column(30);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->Type().id(), duckdb::LogicalTypeId::VARIANT);
  ASSERT_EQ(col->RowCount(), 200u);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

// VARIANT, shredding forced on: shredded row groups reconstruct via unshred.
TEST_F(ColumnReaderTest, VariantRoundTripShredded) {
  SetShreddingSize(Db(), 0);
  irs::MemoryDirectory dir{};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/31,
                     "SELECT {'a': i, 'b': ('v' || i)}::VARIANT "
                     "FROM range(500) t(i)",
                     /*rg_size=*/256, expected);
  ASSERT_EQ(expected.size(), 500u);

  irs::ColReader r{dir, "vseg", Db()};
  const auto* col = r.Column(31);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), 500u);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

// VARIANT with NULL rows + leftover (mixed types) under shredding: exercises
// Partial/Full row groups and the VARIANT-level null mask.
TEST_F(ColumnReaderTest, VariantNullAndLeftover) {
  SetShreddingSize(Db(), 0);
  irs::MemoryDirectory dir{};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/32,
                     "SELECT CASE WHEN i % 3 = 0 THEN NULL "
                     "WHEN i % 7 = 0 THEN ('str_' || i)::VARIANT "
                     "ELSE i::VARIANT END FROM range(500) t(i)",
                     /*rg_size=*/128, expected);
  ASSERT_EQ(expected.size(), 500u);
  ASSERT_TRUE(expected[0].IsNull());

  irs::ColReader r{dir, "vseg", Db()};
  const auto* col = r.Column(32);
  ASSERT_NE(col, nullptr);
  ExpectVariantValuesEqual(expected, ScanVariantAll(*col, r.Ctx()));
}

// Sparse Gather of a VARIANT column across row-group boundaries (forces the
// SeekVariantRg skip-to-position path), with shredding on.
TEST_F(ColumnReaderTest, VariantGatherSparse) {
  SetShreddingSize(Db(), 0);
  irs::MemoryDirectory dir{};
  std::vector<duckdb::Value> expected;
  WriteVariantViaSql(
    Db(), dir, "vseg", /*id=*/33,
    "SELECT CASE WHEN i % 5 = 0 THEN NULL "
    "ELSE {'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT END "
    "FROM range(600) t(i)",
    /*rg_size=*/128, expected);
  ASSERT_EQ(expected.size(), 600u);

  irs::ColReader r{dir, "vseg", Db()};
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

// Per-column HyperLogLog distinct-count sketch: built by hashing rows at seal
// time, persisted in the footer, recovered + queryable on read.
TEST_F(ColumnReaderTest, HyperLogLogDistinctCount) {
  constexpr uint64_t kRows = 20000;
  constexpr uint64_t kDistinct = 1000;  // values cycle 0..999
  constexpr uint32_t kRgSize = 4096;
  constexpr irs::field_id kA = 40;

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwA = w.OpenColumn(
      kA, duckdb::LogicalType::BIGINT, /*skip_validity=*/true, kRgSize,
      duckdb::CompressionType::COMPRESSION_AUTO, /*hyperloglog=*/true);
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
      cwA.Append(v, take);
      pos += take;
    }
    w.Commit(0);
  }

  irs::ColReader r{dir, "seg", Db()};
  const auto* ca = r.Column(kA);
  ASSERT_NE(ca, nullptr);
  const auto* hll = ca->HyperLogLog();
  ASSERT_NE(hll, nullptr);
  const auto est = hll->Count();
  // This verifies the sketch is built, persisted, and recovered as a real
  // distinct-counter -- not HLL precision (that's duckdb::HyperLogLog's). A
  // generous band still catches the real failures: no dedup (~kRows) or an
  // empty/unserialized sketch (~0).
  EXPECT_GE(est, static_cast<uint64_t>(kDistinct / 2));
  EXPECT_LE(est, static_cast<uint64_t>(kDistinct * 2));
}

// Norm column: per-doc uint32 FTS norm, bit-packed per row group, with sparse
// docs (gaps zero-padded to target_row), across multiple row groups.
TEST_F(ColumnReaderTest, NormColumnRoundTrip) {
  constexpr uint64_t kDocs = 5000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kN = 50;

  // Norm for every 3rd doc (others zero-padded); value grows so byte_size
  // widens across row groups (1->2->4 byte packing).
  auto has_norm = [](uint64_t d) { return d % 3 == 0; };
  auto norm_val = [](uint64_t d) { return static_cast<uint32_t>(d * 13 + 1); };

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& nw = w.OpenNormColumn(kN, kRgSize);
    for (uint64_t d = 0; d < kDocs; ++d) {
      if (has_norm(d)) {
        nw.Append(d, norm_val(d));
      }
    }
    w.Commit(kDocs);
  }

  irs::ColReader r{dir, "seg", Db()};
  ASSERT_TRUE(r.HasNormColumn(kN));
  const auto* nr = r.NormColumn(kN);
  ASSERT_NE(nr, nullptr);
  ASSERT_EQ(nr->RowCount(), kDocs);
  for (uint64_t d = 0; d < kDocs; ++d) {
    const auto expected = has_norm(d) ? norm_val(d) : 0u;
    EXPECT_EQ(nr->Get(d), expected) << "doc " << d;
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwB = w.OpenColumn(kB, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
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
  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
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
  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
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
  irs::ColReader r{dir, "seg", Db()};
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
// point-read scattered rows. Catches dict_fsst point-read (fetch_row)
// corruption.
TEST_F(ColumnReaderTest, ReproShortPkPointRead) {
  constexpr irs::field_id kB = 72;
  constexpr uint64_t kRows = 500;
  // Length-prefixed content (irs::WriteStr layout) like the failing test.
  auto key = [](uint64_t i) {
    const std::string s = "row_" + std::to_string(i);
    return std::string(1, static_cast<char>(s.size())) + s;
  };
  constexpr irs::field_id kInt = 73;
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
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
  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
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
  irs::ColReader r{dir, "seg", Db()};
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwT = w.OpenColumn(kT, stype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  const std::string obj = "{'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/34,
                     "SELECT " + obj + " FROM range(500) t(i)",
                     /*rg_size=*/128, ignored);

  irs::ColReader r{dir, "vseg", Db()};
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
  irs::MemoryDirectory dir{};
  const std::string obj = "{'a': (i * 0.5)::DOUBLE, 'b': 'v' || i}::VARIANT";
  std::vector<duckdb::Value> ignored;
  WriteVariantViaSql(Db(), dir, "vseg", /*id=*/34,
                     "SELECT " + obj + " FROM range(500) t(i)",
                     /*rg_size=*/128, ignored);

  irs::ColReader r{dir, "vseg", Db()};
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
          col.ExtractShreddedLeafRun(state, rg, path, local_start, rlen, out,
                                     done, /*allow_entire=*/false);
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwA = w.OpenColumn(kA, duckdb::LogicalType::BIGINT,
                             /*skip_validity=*/false, kRgSize);
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
    // columns do); the caller pads the tail, exactly like MergeInto.
    cwA.PadNullsTo(kTotal);
    w.Commit(kTotal);
  }

  irs::ColReader r{dir, "seg", Db()};
  const auto* ca = r.Column(kA);
  ASSERT_NE(ca, nullptr);
  ASSERT_EQ(ca->RowCount(), kTotal);
  VerifyBigint(*ca, r.Ctx(), kTotal, is_valid, val);
}

// DENSE VARCHAR with FEW distinct short repeated values -> forces the dict_fsst
// codec (dictionary dedup + FSST). Isolates whether dict_fsst scan/point-read
// itself is broken (independent of sparsity). Mirrors the failing index tests'
// "abcd"/"abcde" prefix-column data.
TEST_F(ColumnReaderTest, DictFsstShortRepeatedStrings) {
  constexpr uint64_t kRows = 3000;
  constexpr uint32_t kRgSize = 1024;
  constexpr irs::field_id kS = 90;
  static const char* const kVals[] = {"abcd", "abcde", "abcdef", "xyz"};
  auto val = [](uint64_t g) -> std::string { return kVals[g % 4]; };

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             /*skip_validity=*/false, 1024);
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

  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::VARCHAR,
                             /*skip_validity=*/false, 1024);
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

  irs::ColReader r{dir, "seg", Db()};
  const auto* cs = r.Column(kS);
  ASSERT_NE(cs, nullptr);
  ASSERT_TRUE(cs->NullsInData());
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
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwS = w.OpenColumn(kS, duckdb::LogicalType::BLOB,
                             /*skip_validity=*/false, 1024);
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

  irs::ColReader r{dir, "seg", Db()};
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
  irs::MemoryDirectory dir{};
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
    irs::ColWriter w{dir, "seg", Db()};
    auto& cwName = w.OpenColumn(kName, duckdb::LogicalType::BLOB, false, 1024);
    const char* names[] = {"A", "B", "C", "D"};
    for (uint64_t i = 0; i < 4; ++i) {
      put(cwName, i, names[i]);
    }
    auto& cwPrefix =
      w.OpenColumn(kPrefix, duckdb::LogicalType::BLOB, false, 1024);
    put(cwPrefix, 0, "abcd");
    put_null(cwPrefix, 1);
    put_null(cwPrefix, 2);
    put(cwPrefix, 3, "abcde");
    w.Commit(4);
  }

  irs::ColReader r{dir, "seg", Db()};
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
  auto& edb = ::sdb::DuckDBEngine::Instance().instance();
  constexpr irs::field_id kPrefix = 1;
  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", edb};
    auto& cw0 = w.OpenColumn(0, duckdb::LogicalType::BLOB, false, 1024);
    const char* names[] = {"A", "B", "C", "D"};
    for (uint64_t i = 0; i < 4; ++i) {
      duckdb::Vector v{duckdb::LogicalType::BLOB, 1};
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v)[0] =
        duckdb::StringVector::AddStringOrBlob(v, names[i], 1);
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::SetSize(v, 1);
      cw0.Append(i, v, 1);
    }
    auto& cwPrefix =
      w.OpenColumn(kPrefix, duckdb::LogicalType::BLOB, false, 1024);
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

  irs::ColReader r{dir, "seg", edb};
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
// `compression = true` (COMPRESSION_AUTO). DuckDB's analyze tournament must
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    // Default compression arg == COMPRESSION_AUTO (the analyze tournament).
    auto& cw = w.OpenColumn(kV, vtype, /*skip_validity=*/false, kRgSize);
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

  irs::ColReader r{dir, "seg", Db()};
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
    EXPECT_TRUE(codec == duckdb::CompressionType::COMPRESSION_ALP ||
                codec == duckdb::CompressionType::COMPRESSION_ALPRD)
      << "row-group " << rg
      << " codec=" << duckdb::CompressionTypeToString(codec);
  }
}

// `compression = false` maps to COMPRESSION_UNCOMPRESSED: forcing that codec on
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

  irs::MemoryDirectory dir{};
  {
    irs::ColWriter w{dir, "seg", Db()};
    auto& cw = w.OpenColumn(kV, vtype, /*skip_validity=*/false, kRgSize,
                            duckdb::CompressionType::COMPRESSION_UNCOMPRESSED);
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

  irs::ColReader r{dir, "seg", Db()};
  const auto* cv = r.Column(kV);
  ASSERT_NE(cv, nullptr);
  ASSERT_EQ(cv->RowCount(), kRows);
  const auto* child = cv->Child();
  ASSERT_NE(child, nullptr);
  const auto blocks = child->DataBlocks();
  ASSERT_FALSE(blocks.empty());
  for (size_t rg = 0; rg < blocks.size(); ++rg) {
    ASSERT_NE(blocks[rg].codec, nullptr) << "row-group " << rg;
    EXPECT_EQ(blocks[rg].codec->type,
              duckdb::CompressionType::COMPRESSION_UNCOMPRESSED)
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

}  // namespace
