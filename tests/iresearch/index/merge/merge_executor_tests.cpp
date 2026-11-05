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
#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <atomic>
#include <duckdb/common/fault_points.hpp>
#include <iresearch/index/merge/merge_executor.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;
using irs::MergeUnitKind;

constexpr size_t kRangeThreads = 2;
constexpr irs::field_id kHnswColumn = kFirstColumn + 1;
constexpr irs::field_id kIvfColumn = kFirstColumn + 2;

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

irs::ColumnOptions VectorColumns(irs::field_id id) {
  irs::ColumnOptions options;
  if (id == kHnswColumn) {
    options.ann_info =
      irs::AnnInfo{.kind = irs::AnnKind::Hnsw, .index_id = kHnswColumn};
  } else if (id == kIvfColumn) {
    options.ann_info =
      irs::AnnInfo{.kind = irs::AnnKind::Ivf, .index_id = kIvfColumn};
  }
  return options;
}

irs::AnnFieldsByColumn VectorAnns() {
  irs::AnnFieldsByColumn anns;
  anns[kHnswColumn].push_back(kHnswColumn);
  anns[kIvfColumn].push_back(kIvfColumn);
  return anns;
}

class MergeExecutorTest : public ::testing::Test {
 protected:
  MergeExecutorTest() = default;
  explicit MergeExecutorTest(size_t docs) : _docs{docs} {}

  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = PlainColumns;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    Generator gen{57};
    size_t next_id = 0;
    for (const double delete_ratio : {0.0, 0.3, 0.0, 0.1}) {
      SegmentShape shape;
      shape.docs = _docs;
      shape.delete_ratio = delete_ratio;
      shape.null_ratio = 0.1;
      shape.columns = {0, 1, 2, 3, 4, 5, 6};
      shape.mixed = irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
      WriteSegment(*_writer, gen, shape, next_id);
    }
    _reader = _writer->GetSnapshot();
    _inputs = Segments(_reader);
    ASSERT_EQ(4, _inputs.size());
    ResetDocMap();
  }

  void ResetDocMap() {
    _meta = {};
    _doc_map.clear();
    for (const auto* input : _inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
  }

  std::unique_ptr<irs::MergePlan> MakePlan(
    irs::Directory& out, const irs::IndexFieldOptions& field_options,
    duckdb::shared_ptr<duckdb::DedicatedPool> pool = MergePool(1),
    const irs::MergeProgress& progress = {}) {
    auto plan = std::make_unique<irs::MergePlan>(
      out, _doc_map, _meta, nullptr, irs::DuckDBEngine::Instance().instance(),
      &field_options, progress, nullptr);
    EXPECT_TRUE(plan->Prepare(std::move(pool)));
    return plan;
  }

  std::string MergedDifference(irs::Directory& out) {
    const auto merged = irs::SegmentReaderImpl::Open(
      out, _meta, irs::tests::DefaultReaderOptions());
    if (merged == nullptr) {
      return "merged segment did not open";
    }
    return FirstDifference(ExpectedMerge(_inputs), Dump(*merged));
  }

  size_t _docs = 300;
  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<const irs::SubReader*> _inputs;
  irs::FunctionFieldOptions _plain{PlainColumns,
                                   irs::tests::MakeNormColumnIdProvider(),
                                   DEFAULT_ROW_GROUP_SIZE,
                                   {}};
  irs::FunctionFieldOptions _vectors{VectorColumns,
                                     irs::tests::MakeNormColumnIdProvider(),
                                     DEFAULT_ROW_GROUP_SIZE, VectorAnns()};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
};

class MergeExecutorRangesTest : public MergeExecutorTest {
 protected:
  MergeExecutorRangesTest() : MergeExecutorTest{2500} {}
};

size_t Find(const irs::MergePlan& plan, MergeUnitKind kind,
            irs::field_id id = irs::field_limits::invalid()) {
  const auto units = plan.Units();
  const auto it = std::ranges::find_if(units, [&](const irs::MergeUnit& unit) {
    return unit.kind == kind &&
           (!irs::field_limits::valid(id) || unit.id == id);
  });
  EXPECT_NE(units.end(), it);
  return static_cast<size_t>(it - units.begin());
}

std::vector<size_t> TermRangeParts(const irs::MergePlan& plan) {
  std::vector<size_t> parts;
  const auto units = plan.Units();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    if (units[unit].kind == MergeUnitKind::TermRange) {
      parts.push_back(unit);
    }
  }
  return parts;
}

std::vector<size_t> Roots(const irs::MergePlan& plan) {
  std::vector<size_t> roots;
  const auto units = plan.Units();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    if (units[unit].deps.empty()) {
      roots.push_back(unit);
    }
  }
  return roots;
}

TEST_F(MergeExecutorTest, NormsChainBeforeTheColumnsBesidePostings) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  const auto units = plan->Units();
  const auto norms = static_cast<size_t>(
    std::ranges::count(units, MergeUnitKind::Norms, &irs::MergeUnit::kind));
  ASSERT_GE(norms, 2);
  const auto postings = Find(*plan, MergeUnitKind::Postings);
  const auto footer = Find(*plan, MergeUnitKind::ColumnsFooter);
  const auto index = Find(*plan, MergeUnitKind::Index);
  EXPECT_EQ((std::vector<size_t>{0, postings}), Roots(*plan));
  for (size_t unit = 1; unit != norms; ++unit) {
    EXPECT_EQ(std::vector<size_t>{unit - 1}, units[unit].deps) << unit;
  }
  ASSERT_EQ(norms + 7, footer);
  for (size_t unit = norms; unit != footer; ++unit) {
    EXPECT_EQ(MergeUnitKind::Column, units[unit].kind) << unit;
    EXPECT_EQ(std::vector<size_t>{norms - 1}, units[unit].deps) << unit;
  }
  EXPECT_EQ(footer, units[footer].deps.size());
  EXPECT_EQ((std::vector<size_t>{footer, postings}), units[index].deps);
  ASSERT_TRUE(irs::ExecuteMerge(*plan));
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergeExecutorTest, ColumnsAndPostingsCarryTheirCost) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  const auto units = plan->Units();
  for (const auto& unit : units) {
    if (unit.kind == MergeUnitKind::Column) {
      EXPECT_NE(0, unit.cost) << unit.id;
    }
  }
  EXPECT_NE(0, units[Find(*plan, MergeUnitKind::Postings)].cost);
}

TEST_F(MergeExecutorTest, VectorUnitsRunOneAfterAnotherAfterTheFooter) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _vectors);
  const auto footer = Find(*plan, MergeUnitKind::ColumnsFooter);
  const auto hnsw = Find(*plan, MergeUnitKind::Ann, kHnswColumn);
  const auto ivf = Find(*plan, MergeUnitKind::Ann, kIvfColumn);
  const auto postings = Find(*plan, MergeUnitKind::Postings);
  const auto index = Find(*plan, MergeUnitKind::Index);
  ASSERT_EQ(footer + 1, hnsw);
  ASSERT_EQ(hnsw + 1, ivf);
  const auto units = plan->Units();
  EXPECT_TRUE(
    std::ranges::equal(std::vector<size_t>{footer}, units[hnsw].deps));
  EXPECT_TRUE(std::ranges::equal(std::vector<size_t>{hnsw}, units[ivf].deps));
  EXPECT_TRUE(
    std::ranges::equal(std::vector<size_t>{ivf}, units[postings].deps));
  EXPECT_TRUE(std::ranges::equal(
    std::vector<size_t>{footer, hnsw, ivf, postings}, units[index].deps));
  EXPECT_GT(units[hnsw].cost, 0);

  if (!duckdb::FaultPoints::Arm("irs::MergeUnit::Skip")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit::Skip");
  };
  EXPECT_TRUE(irs::ExecuteMerge(*plan));
}

TEST_F(MergeExecutorTest, VectorUnitsWithoutIvfLeavePostingsFree) {
  irs::AnnFieldsByColumn hnsw_anns;
  hnsw_anns[kHnswColumn].push_back(kHnswColumn);
  irs::FunctionFieldOptions hnsw_only{
    [](irs::field_id id) {
      return id == kIvfColumn ? irs::ColumnOptions{} : VectorColumns(id);
    },
    irs::tests::MakeNormColumnIdProvider(), DEFAULT_ROW_GROUP_SIZE,
    std::move(hnsw_anns)};
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, hnsw_only);
  EXPECT_TRUE(
    absl::c_linear_search(Roots(*plan), Find(*plan, MergeUnitKind::Postings)));
}

TEST_F(MergeExecutorTest, FailedUnitStopsTheOthers) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  std::atomic<size_t> calls{0};
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, MergePool(1), [&] {
    calls.fetch_add(1);
    return true;
  });
  const auto prepared = calls.load();
  EXPECT_THROW(std::ignore = irs::ExecuteMerge(*plan), irs::SqlException);
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  for (size_t unit = 0; unit != plan->Units().size(); ++unit) {
    EXPECT_FALSE(plan->RunUnit(unit)) << unit;
  }
  EXPECT_EQ(prepared, calls.load());
}

TEST_F(MergeExecutorTest, NoUnitStartsAfterAFailure) {
  const auto merge = [&](const irs::MergeProgress& progress) {
    ResetDocMap();
    irs::MemoryDirectory out;
    std::atomic_bool prepared{false};
    const auto plan = MakePlan(out, _plain, MergePool(1),
                               [&] { return !prepared.load() || progress(); });
    prepared.store(true);
    return irs::ExecuteMerge(*plan);
  };
  std::atomic<size_t> total{0};
  ASSERT_TRUE(merge([&] {
    total.fetch_add(1);
    return true;
  }));
  ASSERT_LT(2, total.load());
  for (const auto fail_at : {size_t{1}, total.load() / 2, total.load()}) {
    SCOPED_TRACE(fail_at);
    std::atomic<size_t> calls{0};
    EXPECT_FALSE(merge([&] { return calls.fetch_add(1) + 1 < fail_at; }));
    EXPECT_EQ(fail_at, calls.load());
  }
}

TEST_F(MergeExecutorTest, AnAbortedPlanRunsNoUnit) {
  std::atomic<size_t> calls{0};
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, MergePool(1), [&] {
    calls.fetch_add(1);
    return true;
  });
  const auto prepared = calls.load();
  plan->Abort();
  EXPECT_FALSE(irs::ExecuteMerge(*plan));
  EXPECT_EQ(prepared, calls.load());
}

TEST_F(MergeExecutorTest, StoppedMergeFails) {
  std::atomic<size_t> calls{0};
  irs::MemoryDirectory out;
  const auto plan =
    MakePlan(out, _plain, MergePool(1), [&] { return calls.fetch_add(1) < 3; });
  EXPECT_FALSE(irs::ExecuteMerge(*plan));
}

TEST_F(MergeExecutorRangesTest, PostingsWaitForEveryTermRangePart) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, MergePool(kRangeThreads));
  const auto units = plan->Units();
  const auto parts = TermRangeParts(*plan);
  ASSERT_LT(1, parts.size());
  EXPECT_EQ(parts, units[Find(*plan, MergeUnitKind::Postings)].deps);
  for (const auto part : parts) {
    EXPECT_TRUE(units[part].deps.empty());
    EXPECT_LT(0, units[part].cost);
  }
  ASSERT_TRUE(irs::ExecuteMerge(*plan));
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergeExecutorRangesTest, PostingsWaitForTheIvfBuildAfterTheirParts) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _vectors, MergePool(kRangeThreads));
  const auto units = plan->Units();
  auto deps = TermRangeParts(*plan);
  ASSERT_FALSE(deps.empty());
  deps.push_back(Find(*plan, MergeUnitKind::Ann, kIvfColumn));
  EXPECT_EQ(deps, units[Find(*plan, MergeUnitKind::Postings)].deps);
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit::Skip")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit::Skip");
  };
  EXPECT_TRUE(irs::ExecuteMerge(*plan));
}

TEST_F(MergeExecutorRangesTest, AFailedUnitStopsTheTermRangeParts) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, MergePool(kRangeThreads));
  const auto parts = TermRangeParts(*plan);
  ASSERT_LT(0, parts.size());
  EXPECT_THROW(std::ignore = irs::ExecuteMerge(*plan), irs::SqlException);
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  for (const auto part : parts) {
    EXPECT_FALSE(plan->RunUnit(part)) << part;
  }
}

TEST_F(MergeExecutorTest, ThreadedMergesMatchTheInputs) {
  for (const size_t threads : {1, 2, 3, 8, 64}) {
    SCOPED_TRACE(threads);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto plan = MakePlan(out, _plain, MergePool(threads));
    ASSERT_TRUE(irs::ExecuteMerge(*plan));
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST_F(MergeExecutorTest, ShuffledMergesMatchTheInputs) {
  for (const uint64_t seed : {1, 2, 3, 4, 5, 6, 7, 8}) {
    SCOPED_TRACE(seed);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto plan = MakePlan(out, _plain, MergePool(1 + seed % 4));
    const ShuffledUnits shuffle;
    ASSERT_TRUE(irs::ExecuteMerge(*plan));
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST_F(MergeExecutorTest, SerialShuffledMergesMatchTheInputs) {
  for (const uint64_t seed : {1, 2, 3, 4}) {
    SCOPED_TRACE(seed);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto pool = NewMergePool(seed);
    const auto plan = MakePlan(out, _plain, pool);
    pool->SetThreads(1);
    const ShuffledUnits shuffle;
    ASSERT_TRUE(irs::ExecuteMerge(*plan));
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST(MergeProfileTest, WallTimeReplacesTheSumOfSteps) {
  irs::MergeProfile profile;
  profile.Add(irs::MergeStep::Columns, 30);
  profile.Add(irs::MergeStep::Postings, 20);
  EXPECT_EQ(50, profile.TotalNanos());
  profile.SetWallNanos(35);
  EXPECT_EQ(35, profile.TotalNanos());
  EXPECT_EQ(30, profile.Nanos(irs::MergeStep::Columns));
  const auto copy = profile;
  EXPECT_EQ(35, copy.TotalNanos());
  irs::MergeProfile assigned;
  assigned = copy;
  EXPECT_EQ(20, assigned.Nanos(irs::MergeStep::Postings));
}

}  // namespace
