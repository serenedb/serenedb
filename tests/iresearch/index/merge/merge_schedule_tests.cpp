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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <duckdb/parallel/task.hpp>
#include <functional>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
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

constexpr size_t kNoUnit = irs::MergeSchedule::kNoUnit;
constexpr size_t kFinished = irs::MergeSchedule::kFinished;
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

class Waker final : public duckdb::Task {
 public:
  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) final {
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

  void Reschedule() final { _woken = true; }

  bool Woken() noexcept { return std::exchange(_woken, false); }

 private:
  bool _woken = false;
};

class MergeScheduleTest : public ::testing::Test {
 protected:
  MergeScheduleTest() = default;
  explicit MergeScheduleTest(size_t docs) : _docs{docs} {}

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
    size_t threads = 1) {
    auto plan = std::make_unique<irs::MergePlan>(
      out, _doc_map, _meta, nullptr, irs::DuckDBEngine::Instance().instance(),
      &field_options, irs::MergeProgress{}, nullptr);
    EXPECT_TRUE(plan->Prepare(threads));
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

  size_t Claim(irs::MergeSchedule& schedule) {
    return schedule.Claim(_interrupt);
  }

  std::vector<size_t> ClaimAll(irs::MergeSchedule& schedule) {
    std::vector<size_t> claimed;
    for (auto unit = Claim(schedule); unit != kNoUnit && unit != kFinished;
         unit = Claim(schedule)) {
      claimed.push_back(unit);
    }
    return claimed;
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
  duckdb::shared_ptr<Waker> _waker = duckdb::make_shared_ptr<Waker>();
  duckdb::InterruptState _interrupt{duckdb::weak_ptr<duckdb::Task>{_waker}};
};

class MergeScheduleRangesTest : public MergeScheduleTest {
 protected:
  MergeScheduleRangesTest() : MergeScheduleTest{2500} {}
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

TEST_F(MergeScheduleTest, NormsRunFirstThenColumnsBesidePostings) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  const auto units = plan->Units();
  const auto norms = static_cast<size_t>(
    std::ranges::count(units, MergeUnitKind::Norms, &irs::MergeUnit::kind));
  ASSERT_GE(norms, 2);
  irs::MergeSchedule schedule{*plan};
  const auto postings = Find(*plan, MergeUnitKind::Postings);
  auto first = ClaimAll(schedule);
  std::ranges::sort(first);
  ASSERT_EQ((std::vector<size_t>{0, postings}), first);
  for (size_t unit = 0; unit + 1 < norms; ++unit) {
    ASSERT_TRUE(schedule.Run(unit));
    ASSERT_EQ(unit + 1, Claim(schedule));
    EXPECT_EQ(kNoUnit, Claim(schedule));
  }
  ASSERT_TRUE(schedule.Run(norms - 1));
  ASSERT_TRUE(schedule.Run(postings));
  auto claimed = ClaimAll(schedule);
  EXPECT_EQ(7, claimed.size());
  for (const auto unit : claimed) {
    EXPECT_EQ(MergeUnitKind::Column, units[unit].kind);
  }
  EXPECT_EQ(kNoUnit, Claim(schedule));

  const auto footer = Find(*plan, MergeUnitKind::ColumnsFooter);
  const auto last_column = std::ranges::find(
    claimed, Find(*plan, MergeUnitKind::Column, kFirstColumn + 6));
  ASSERT_NE(claimed.end(), last_column);
  const auto held = *last_column;
  claimed.erase(last_column);
  for (const auto unit : claimed) {
    ASSERT_TRUE(schedule.Run(unit));
  }
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_TRUE(schedule.Run(held));
  ASSERT_EQ(footer, Claim(schedule));
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_TRUE(schedule.Run(footer));
  const auto index = Find(*plan, MergeUnitKind::Index);
  ASSERT_EQ(index, Claim(schedule));
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_TRUE(schedule.Run(index));
  EXPECT_EQ(kFinished, Claim(schedule));
  EXPECT_TRUE(schedule.Result());
}

TEST_F(MergeScheduleTest, LongestUnitsAreClaimedFirst) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  irs::MergeSchedule schedule{*plan};
  const auto units = plan->Units();
  while (true) {
    const auto unit = Claim(schedule);
    ASSERT_LT(unit, units.size());
    if (units[unit].kind == MergeUnitKind::Postings) {
      continue;
    }
    ASSERT_TRUE(schedule.Run(unit));
    if (unit + 1 == units.size() ||
        units[unit + 1].kind != MergeUnitKind::Norms) {
      break;
    }
  }
  const auto claimed = ClaimAll(schedule);
  ASSERT_EQ(7, claimed.size());
  for (size_t i = 1; i < claimed.size(); ++i) {
    const auto& prev = units[claimed[i - 1]];
    const auto& next = units[claimed[i]];
    EXPECT_TRUE(prev.cost > next.cost ||
                (prev.cost == next.cost && claimed[i - 1] < claimed[i]))
      << i;
  }
  for (const auto& unit : units) {
    if (unit.kind == MergeUnitKind::Column) {
      EXPECT_NE(0, unit.cost) << unit.id;
    }
  }
  EXPECT_NE(0, units[Find(*plan, MergeUnitKind::Postings)].cost);
}

TEST_F(MergeScheduleTest, VectorUnitsRunOneAfterAnotherAfterTheFooter) {
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
  EXPECT_GT(plan->Units()[hnsw].cost, 0);

  if (!duckdb::FaultPoints::Arm("irs::MergeUnit::Skip")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit::Skip");
  };
  irs::MergeSchedule schedule{*plan};
  while (true) {
    const auto unit = Claim(schedule);
    ASSERT_LT(unit, units.size());
    ASSERT_NE(postings, unit);
    ASSERT_TRUE(schedule.Run(unit));
    if (unit == footer) {
      break;
    }
  }
  ASSERT_EQ(hnsw, Claim(schedule));
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_TRUE(schedule.Run(hnsw));
  ASSERT_EQ(ivf, Claim(schedule));
  ASSERT_TRUE(schedule.Run(ivf));
  ASSERT_EQ(postings, Claim(schedule));
  ASSERT_TRUE(schedule.Run(postings));
  ASSERT_EQ(index, Claim(schedule));
  ASSERT_TRUE(schedule.Run(index));
  EXPECT_EQ(kFinished, Claim(schedule));
}

TEST_F(MergeScheduleTest, VectorUnitsWithoutIvfLeavePostingsFree) {
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
  EXPECT_TRUE(plan->Units()[Find(*plan, MergeUnitKind::Postings)].deps.empty());
  irs::MergeSchedule schedule{*plan};
  const auto claimed = ClaimAll(schedule);
  EXPECT_NE(std::ranges::end(claimed),
            std::ranges::find(claimed, Find(*plan, MergeUnitKind::Postings)));
}

TEST_F(MergeScheduleTest, FailedUnitStopsTheOthers) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  irs::MergeSchedule schedule{*plan};
  const auto first = Claim(schedule);
  const auto second = Claim(schedule);
  ASSERT_LT(first, plan->Units().size());
  ASSERT_LT(second, plan->Units().size());
  duckdb::FaultPoints::Arm("irs::MergeUnit");
  EXPECT_FALSE(schedule.Run(first));
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  EXPECT_THROW(schedule.Result(), irs::SqlException);
  EXPECT_EQ(kNoUnit, Claim(schedule));
  EXPECT_FALSE(schedule.Run(second));
  EXPECT_EQ(kFinished, Claim(schedule));
  EXPECT_THROW(schedule.Result(), irs::SqlException);
}

TEST_F(MergeScheduleTest, AFailedUnitWakesTheBlockedClaims) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  irs::MergeSchedule schedule{*plan};
  const auto claimed = ClaimAll(schedule);
  ASSERT_EQ(2, claimed.size());
  EXPECT_FALSE(_waker->Woken());
  duckdb::FaultPoints::Arm("irs::MergeUnit");
  EXPECT_FALSE(schedule.Run(claimed[0]));
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  EXPECT_TRUE(_waker->Woken());
  EXPECT_EQ(kNoUnit, Claim(schedule));
  EXPECT_FALSE(schedule.Run(claimed[1]));
  EXPECT_TRUE(_waker->Woken());
  EXPECT_EQ(kFinished, Claim(schedule));
  EXPECT_THROW(schedule.Result(), irs::SqlException);
}

TEST_F(MergeScheduleTest, StopWakesTheBlockedClaimsAndEndsThem) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  irs::MergeSchedule schedule{*plan};
  ASSERT_FALSE(ClaimAll(schedule).empty());
  EXPECT_FALSE(_waker->Woken());
  schedule.Stop();
  EXPECT_TRUE(_waker->Woken());
  EXPECT_EQ(kFinished, Claim(schedule));
}

TEST_F(MergeScheduleRangesTest, PostingsWaitForEveryTermRangePart) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, kRangeThreads);
  const auto units = plan->Units();
  const auto parts = TermRangeParts(*plan);
  ASSERT_LT(1, parts.size());
  const auto postings = Find(*plan, MergeUnitKind::Postings);
  const auto index = Find(*plan, MergeUnitKind::Index);
  EXPECT_EQ(parts, units[postings].deps);
  for (const auto part : parts) {
    EXPECT_TRUE(units[part].deps.empty());
    EXPECT_LT(0, units[part].cost);
  }
  irs::MergeSchedule schedule{*plan};
  size_t held = kNoUnit;
  for (auto unit = Claim(schedule); unit < units.size();
       unit = Claim(schedule)) {
    ASSERT_NE(postings, unit);
    ASSERT_NE(index, unit);
    if (held == kNoUnit && units[unit].kind == MergeUnitKind::TermRange) {
      held = unit;
      continue;
    }
    ASSERT_TRUE(schedule.Run(unit));
  }
  ASSERT_NE(kNoUnit, held);
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_TRUE(schedule.Run(held));
  ASSERT_EQ(postings, Claim(schedule));
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_TRUE(schedule.Run(postings));
  ASSERT_EQ(index, Claim(schedule));
  ASSERT_TRUE(schedule.Run(index));
  EXPECT_EQ(kFinished, Claim(schedule));
  EXPECT_TRUE(schedule.Result());
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergeScheduleRangesTest, PostingsWaitForTheIvfBuildAfterTheirParts) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _vectors, kRangeThreads);
  const auto units = plan->Units();
  auto deps = TermRangeParts(*plan);
  ASSERT_FALSE(deps.empty());
  const auto parts = deps.size();
  const auto postings = Find(*plan, MergeUnitKind::Postings);
  const auto ivf = Find(*plan, MergeUnitKind::Ann, kIvfColumn);
  deps.push_back(ivf);
  EXPECT_EQ(deps, units[postings].deps);
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit::Skip")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit::Skip");
  };
  irs::MergeSchedule schedule{*plan};
  bool ivf_claimed = false;
  size_t parts_run = 0;
  for (auto unit = Claim(schedule); unit < units.size();
       unit = Claim(schedule)) {
    ASSERT_NE(postings, unit);
    if (unit == ivf) {
      ivf_claimed = true;
      continue;
    }
    parts_run += units[unit].kind == MergeUnitKind::TermRange;
    ASSERT_TRUE(schedule.Run(unit));
  }
  ASSERT_TRUE(ivf_claimed);
  EXPECT_EQ(parts, parts_run);
  ASSERT_TRUE(schedule.Run(ivf));
  EXPECT_EQ(postings, Claim(schedule));
}

TEST_F(MergeScheduleRangesTest, AFailedUnitStopsTheTermRangeParts) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  const absl::Cleanup disarm = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, kRangeThreads);
  const auto units = plan->Units();
  irs::MergeSchedule schedule{*plan};
  auto claimed = ClaimAll(schedule);
  const auto failing = absl::c_find_if(claimed, [&](size_t unit) {
    return units[unit].kind != MergeUnitKind::TermRange;
  });
  ASSERT_NE(claimed.end(), failing);
  duckdb::FaultPoints::Arm("irs::MergeUnit");
  EXPECT_FALSE(schedule.Run(*failing));
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  claimed.erase(failing);
  EXPECT_THROW(schedule.Result(), irs::SqlException);
  EXPECT_EQ(kNoUnit, Claim(schedule));
  ASSERT_LT(0, absl::c_count_if(claimed, [&](size_t unit) {
              return units[unit].kind == MergeUnitKind::TermRange;
            }));
  for (const auto unit : claimed) {
    EXPECT_FALSE(schedule.Run(unit));
  }
  EXPECT_EQ(kFinished, Claim(schedule));
  EXPECT_THROW(schedule.Result(), irs::SqlException);
}

TEST_F(MergeScheduleRangesTest, TheLastPartWakesTheBlockedClaims) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain, kRangeThreads);
  const auto units = plan->Units();
  irs::MergeSchedule schedule{*plan};
  std::vector<size_t> parts;
  for (auto unit = Claim(schedule); unit < units.size();
       unit = Claim(schedule)) {
    if (units[unit].kind == MergeUnitKind::TermRange) {
      parts.push_back(unit);
      continue;
    }
    ASSERT_TRUE(schedule.Run(unit));
  }
  ASSERT_LT(1, parts.size());
  for (size_t i = 0; i + 1 < parts.size(); ++i) {
    ASSERT_TRUE(schedule.Run(parts[i]));
    EXPECT_FALSE(_waker->Woken());
  }
  ASSERT_TRUE(schedule.Run(parts.back()));
  EXPECT_TRUE(_waker->Woken());
  EXPECT_EQ(Find(*plan, MergeUnitKind::Postings), Claim(schedule));
}

TEST_F(MergeScheduleTest, ThreadedMergesMatchTheInputs) {
  for (const size_t threads : {1, 2, 3, 8, 64}) {
    SCOPED_TRACE(threads);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto plan = MakePlan(out, _plain);
    ASSERT_TRUE(RunPlan(*plan, threads));
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST_F(MergeScheduleTest, ShuffledMergesMatchTheInputs) {
  for (const uint64_t seed : {1, 2, 3, 4, 5, 6, 7, 8}) {
    SCOPED_TRACE(seed);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto plan = MakePlan(out, _plain);
    const ShuffledClaims shuffle;
    ASSERT_TRUE(RunPlan(*plan, 1 + seed % 4));
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST_F(MergeScheduleTest, StoppedMergeFailsEveryExecutor) {
  for (const size_t threads : {1, 4}) {
    SCOPED_TRACE(threads);
    ResetDocMap();
    irs::MemoryDirectory out;
    std::atomic<size_t> calls{0};
    irs::MergePlan plan{out,
                        _doc_map,
                        _meta,
                        nullptr,
                        irs::DuckDBEngine::Instance().instance(),
                        &_plain,
                        [&] { return calls.fetch_add(1) < 3; },
                        nullptr};
    ASSERT_TRUE(plan.Prepare());
    EXPECT_FALSE(RunPlan(plan, threads));
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
