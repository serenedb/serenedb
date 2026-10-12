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

#include <algorithm>
#include <functional>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <optional>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;
using irs::MergeUnitKind;

constexpr irs::field_id kHnswColumn = kFirstColumn + 1;
constexpr irs::field_id kIvfColumn = kFirstColumn + 2;

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

irs::ColumnOptions VectorColumns(irs::field_id id) {
  irs::ColumnOptions options;
  if (id == kHnswColumn) {
    options.ann_info = irs::AnnInfo{.kind = irs::AnnKind::Hnsw};
  } else if (id == kIvfColumn) {
    options.ann_info = irs::AnnInfo{.kind = irs::AnnKind::Ivf};
  }
  return options;
}

class MergeScheduleTest : public ::testing::Test {
 protected:
  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = PlainColumns;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    Generator gen{57};
    size_t next_id = 0;
    for (const double delete_ratio : {0.0, 0.3, 0.0, 0.1}) {
      SegmentShape shape;
      shape.docs = 300;
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
    irs::Directory& out, const irs::IndexFieldOptions& field_options) {
    auto plan = std::make_unique<irs::MergePlan>(
      out, _doc_map, _meta, nullptr, irs::DuckDBEngine::Instance().instance(),
      &field_options, irs::MergeProgress{}, nullptr);
    EXPECT_TRUE(plan->Prepare());
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

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<const irs::SubReader*> _inputs;
  irs::FunctionFieldOptions _plain{PlainColumns,
                                   irs::tests::MakeNormColumnIdProvider(),
                                   DEFAULT_ROW_GROUP_SIZE};
  irs::FunctionFieldOptions _vectors{VectorColumns,
                                     irs::tests::MakeNormColumnIdProvider(),
                                     DEFAULT_ROW_GROUP_SIZE};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
};

std::vector<size_t> ClaimAll(irs::MergeSchedule& schedule) {
  std::vector<size_t> claimed;
  while (const auto unit = schedule.Claim()) {
    claimed.push_back(*unit);
  }
  return claimed;
}

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

TEST_F(MergeScheduleTest, NormsRunFirstThenColumnsBesidePostings) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  const auto units = plan->Units();
  const auto norms = static_cast<size_t>(
    std::ranges::count(units, MergeUnitKind::Norms, &irs::MergeUnit::kind));
  ASSERT_GE(norms, 2);
  irs::MergeSchedule schedule{*plan};
  const auto postings = Find(*plan, MergeUnitKind::Postings);
  EXPECT_EQ(2, schedule.Ready());
  auto first = ClaimAll(schedule);
  std::ranges::sort(first);
  EXPECT_EQ((std::vector<size_t>{0, postings}), first);
  EXPECT_EQ(0, schedule.Ready());
  for (size_t unit = 0; unit + 1 < norms; ++unit) {
    schedule.Finish(unit, true);
    EXPECT_EQ(unit + 1, schedule.Claim());
    EXPECT_EQ(std::nullopt, schedule.Claim());
  }
  schedule.Finish(norms - 1, true);
  schedule.Finish(postings, true);
  auto claimed = ClaimAll(schedule);
  EXPECT_EQ(7, claimed.size());
  for (const auto unit : claimed) {
    EXPECT_EQ(MergeUnitKind::Column, units[unit].kind);
  }
  EXPECT_FALSE(schedule.Done());

  const auto footer = Find(*plan, MergeUnitKind::ColumnsFooter);
  const auto last_column = std::ranges::find(
    claimed, Find(*plan, MergeUnitKind::Column, kFirstColumn + 6));
  ASSERT_NE(claimed.end(), last_column);
  const auto held = *last_column;
  claimed.erase(last_column);
  for (const auto unit : claimed) {
    schedule.Finish(unit, true);
  }
  EXPECT_EQ(std::nullopt, schedule.Claim());
  schedule.Finish(held, true);
  EXPECT_EQ(footer, schedule.Claim());
  EXPECT_EQ(std::nullopt, schedule.Claim());
  schedule.Finish(footer, true);
  EXPECT_EQ(Find(*plan, MergeUnitKind::Index), schedule.Claim());
  EXPECT_FALSE(schedule.Done());
  schedule.Finish(Find(*plan, MergeUnitKind::Index), true);
  EXPECT_TRUE(schedule.Done());
  EXPECT_TRUE(schedule.Ok());
  EXPECT_EQ(std::nullopt, schedule.Claim());
}

TEST_F(MergeScheduleTest, LongestUnitsAreClaimedFirst) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  irs::MergeSchedule schedule{*plan};
  const auto units = plan->Units();
  while (true) {
    const auto unit = schedule.Claim();
    ASSERT_TRUE(unit.has_value());
    if (units[*unit].kind == MergeUnitKind::Postings) {
      continue;
    }
    schedule.Finish(*unit, true);
    if (*unit + 1 == units.size() ||
        units[*unit + 1].kind != MergeUnitKind::Norms) {
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
  EXPECT_TRUE(
    std::ranges::equal(std::vector<size_t>{footer}, plan->Dependencies(hnsw)));
  EXPECT_TRUE(
    std::ranges::equal(std::vector<size_t>{hnsw}, plan->Dependencies(ivf)));
  EXPECT_TRUE(
    std::ranges::equal(std::vector<size_t>{ivf}, plan->Dependencies(postings)));
  EXPECT_TRUE(
    std::ranges::equal(std::vector<size_t>{footer, hnsw, ivf, postings},
                       plan->Dependencies(index)));
  EXPECT_GT(plan->Units()[hnsw].cost, 0);

  irs::MergeSchedule schedule{*plan};
  while (true) {
    const auto unit = schedule.Claim();
    ASSERT_TRUE(unit.has_value());
    ASSERT_NE(postings, *unit);
    schedule.Finish(*unit, true);
    if (*unit == footer) {
      break;
    }
  }
  EXPECT_EQ(hnsw, schedule.Claim());
  EXPECT_EQ(std::nullopt, schedule.Claim());
  schedule.Finish(hnsw, true);
  EXPECT_EQ(ivf, schedule.Claim());
  schedule.Finish(ivf, true);
  EXPECT_EQ(postings, schedule.Claim());
  schedule.Finish(postings, true);
  EXPECT_EQ(index, schedule.Claim());
  schedule.Finish(index, true);
  EXPECT_TRUE(schedule.Done());
}

TEST_F(MergeScheduleTest, VectorUnitsWithoutIvfLeavePostingsFree) {
  irs::FunctionFieldOptions hnsw_only{
    [](irs::field_id id) {
      return id == kIvfColumn ? irs::ColumnOptions{} : VectorColumns(id);
    },
    irs::tests::MakeNormColumnIdProvider(), DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, hnsw_only);
  EXPECT_TRUE(plan->Dependencies(Find(*plan, MergeUnitKind::Postings)).empty());
  irs::MergeSchedule schedule{*plan};
  const auto claimed = ClaimAll(schedule);
  EXPECT_NE(std::ranges::end(claimed),
            std::ranges::find(claimed, Find(*plan, MergeUnitKind::Postings)));
}

TEST_F(MergeScheduleTest, FailedUnitStopsTheOthers) {
  irs::MemoryDirectory out;
  const auto plan = MakePlan(out, _plain);
  irs::MergeSchedule schedule{*plan};
  const auto first = schedule.Claim();
  const auto second = schedule.Claim();
  ASSERT_TRUE(first && second);
  schedule.Finish(*first, false);
  EXPECT_FALSE(schedule.Ok());
  EXPECT_EQ(0, schedule.Ready());
  EXPECT_EQ(std::nullopt, schedule.Claim());
  EXPECT_FALSE(schedule.Done());
  schedule.Finish(*second, true);
  EXPECT_TRUE(schedule.Done());
  EXPECT_FALSE(schedule.Ok());
  EXPECT_EQ(std::nullopt, schedule.Claim());
}

TEST_F(MergeScheduleTest, ThreadedMergesMatchTheInputs) {
  for (const size_t threads : {1, 2, 3, 8, 64}) {
    SCOPED_TRACE(threads);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto plan = MakePlan(out, _plain);
    ThreadedMergeExecutor executor{threads};
    ASSERT_TRUE(executor.Run(*plan));
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST_F(MergeScheduleTest, ShuffledMergesMatchTheInputs) {
  for (const uint64_t seed : {1, 2, 3, 4, 5, 6, 7, 8}) {
    SCOPED_TRACE(seed);
    ResetDocMap();
    irs::MemoryDirectory out;
    const auto plan = MakePlan(out, _plain);
    ShuffledMergeExecutor executor{1 + seed % 4, seed};
    ASSERT_TRUE(executor.Run(*plan));
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
    ThreadedMergeExecutor threaded{threads};
    EXPECT_FALSE(threaded.Run(plan));
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
