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

#include <absl/cleanup/cleanup.h>
#include <absl/time/clock.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <duckdb.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/logical_plan_verifier.hpp>
#include <exception>
#include <initializer_list>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/merge/doc_map.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/term_ranges.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <numeric>
#include <thread>
#include <tuple>
#include <vector>

#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "connector/segment_rewrite/logical_merge_order.h"
#include "connector/segment_rewrite/logical_merge_units.h"
#include "connector/segment_rewrite/logical_postings_merge.h"
#include "connector/segment_rewrite/physical_merge_order.h"
#include "connector/segment_rewrite/physical_merge_units.h"
#include "connector/segment_rewrite/physical_postings_merge.h"
#include "gtest/gtest.h"
#include "search/compaction/merge_unit_run.h"
#include "search/compaction/pipeline_merge_executor.h"

namespace {

using namespace sdb;
using namespace sdb::connector;

duckdb::DatabaseInstance& TestDb() {
  return ::irs::DuckDBEngine::Instance().instance();
}

duckdb::ClientContext& TestContext() {
  static auto* conn = new duckdb::Connection{TestDb()};
  return *conn->context;
}

catalog::ColumnTokenizer KeywordTokenizer(irs::field_id) {
  static auto gTokenizer = std::make_shared<catalog::Tokenizer>(
    search::Features{},
    irs::analysis::TokenizerConfig{.config = irs::KeywordTokenizer::Options{}});
  return {.analyzer = gTokenizer->Acquire(TestContext()),
          .features = irs::IndexFeatures::None};
}

constexpr ColumnId kValueColumn{1};

const irs::CompactionPolicy& FullMerge() {
  static const auto kPolicy = irs::index_utils::MakePolicy(
    irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});
  return kPolicy;
}

class NoColumnsDirectory final : public irs::Directory {
 public:
  explicit NoColumnsDirectory(irs::Directory& impl) noexcept
    : irs::Directory{impl.ResourceManager()}, _impl{impl} {}

  irs::DirectoryAttributes& attributes() noexcept final {
    return _impl.attributes();
  }
  irs::IndexOutput::ptr create(std::string_view name) final {
    if (name.ends_with(".col")) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("cannot create ", name));
    }
    return _impl.create(name);
  }
  bool exists(bool& result, std::string_view name) const noexcept final {
    return _impl.exists(result, name);
  }
  bool length(uint64_t& result, std::string_view name) const noexcept final {
    return _impl.length(result, name);
  }
  irs::IndexLock::ptr make_lock(std::string_view name) noexcept final {
    return _impl.make_lock(name);
  }
  bool mtime(std::time_t& result, std::string_view name) const noexcept final {
    return _impl.mtime(result, name);
  }
  irs::IndexInput::ptr open(std::string_view name,
                            irs::IOAdvice advice) const noexcept final {
    return _impl.open(name, advice);
  }
  bool remove(std::string_view name) noexcept final {
    return _impl.remove(name);
  }
  bool rename(std::string_view src, std::string_view dst) noexcept final {
    return _impl.rename(src, dst);
  }
  bool sync(std::span<const std::string_view> files) noexcept final {
    return _impl.sync(files);
  }
  bool visit(const visitor_f& visitor) const final {
    return _impl.visit(visitor);
  }

 private:
  irs::Directory& _impl;
};

class PipelineMergeExecutorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    irs::IndexWriterOptions options;
    options.db = &TestDb();
    options.reader_options.db = &TestDb();
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    for (int64_t segment = 0; segment != 4; ++segment) {
      std::vector<int64_t> rowids(3000 + segment * 700);
      std::iota(rowids.begin(), rowids.end(), segment * 100000);
      Insert(rowids);
    }
  }

  void TearDown() override { _writer.reset(); }

  void Reopen(irs::SortSpec sort) {
    _writer.reset();
    irs::IndexWriterOptions options;
    options.db = &TestDb();
    options.reader_options.db = &TestDb();
    options.sort = std::move(sort);
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  void Insert(std::span<const int64_t> rowids) {
    auto trx = _writer->GetBatch(/*exclusive_segment=*/true);
    DuckDBSearchSinkInsertWriter sink{
      trx, KeywordTokenizer, std::array<ColumnId, 1>{kValueColumn},
      NoEntryInfoProvider(),
      PkPolicy{.index_term = false, .column = catalog::PkColumnKind::Has}};
    for (size_t first = 0; first < rowids.size();
         first += STANDARD_VECTOR_SIZE) {
      const auto n =
        std::min<size_t>(STANDARD_VECTOR_SIZE, rowids.size() - first);
      duckdb::Vector ids{duckdb::LogicalType::BIGINT, n};
      duckdb::Vector values{duckdb::LogicalType::BIGINT, n};
      auto* id_data = duckdb::FlatVector::GetDataMutable<int64_t>(ids);
      auto* value_data = duckdb::FlatVector::GetDataMutable<int64_t>(values);
      for (size_t i = 0; i < n; ++i) {
        id_data[i] = rowids[first + i];
        value_data[i] = rowids[first + i] * 10;
      }
      sink.Init(n, PkChunk{.column = &ids});
      sink.SwitchColumn(
        ColumnDescriptor{kValueColumn, duckdb::LogicalType::BIGINT}, values, n);
      sink.Finish();
    }
    ASSERT_TRUE(trx.Commit());
    _writer->RefreshCommit();
  }

  static std::vector<int64_t> Read(const irs::SubReader& segment,
                                   irs::field_id id) {
    std::vector<int64_t> out;
    const auto* columns = segment.GetColReader();
    const auto* column = columns ? columns->Column(id) : nullptr;
    if (!column) {
      return out;
    }
    irs::ReadContext ctx{*columns};
    auto state = column->InitScan(ctx);
    irs::ColumnReader::VectorScratch scratch{column->Type()};
    uint64_t row = 0;
    while (row < column->RowCount()) {
      const auto take =
        std::min<uint64_t>(column->RowCount() - row, STANDARD_VECTOR_SIZE);
      auto& values = scratch.Reset();
      column->Scan(state, values, take);
      duckdb::UnifiedVectorFormat format;
      values.ToUnifiedFormat(take, format);
      const auto* data = duckdb::UnifiedVectorFormat::GetData<int64_t>(format);
      for (duckdb::idx_t i = 0; i < take; ++i) {
        out.push_back(data[format.sel->get_index(i)]);
      }
      row += take;
    }
    return out;
  }

  std::vector<int64_t> Rows() const {
    std::vector<int64_t> out;
    const auto reader = _writer->GetSnapshot();
    for (const auto& segment : reader) {
      const auto rowids = Read(segment, kGeneratedPKId);
      EXPECT_EQ(segment.docs_count(), rowids.size());
      auto mask = segment.MaskedDocs();
      for (size_t row = 0; row < rowids.size(); ++row) {
        const auto doc =
          static_cast<irs::doc_id_t>(row) + irs::doc_limits::min();
        if (!mask.Contains(doc)) {
          out.push_back(rowids[row]);
        }
      }
    }
    std::ranges::sort(out);
    return out;
  }

  std::vector<std::tuple<irs::field_id, irs::bstring, int64_t>> Postings()
    const {
    std::vector<std::tuple<irs::field_id, irs::bstring, int64_t>> out;
    const auto reader = _writer->GetSnapshot();
    for (const auto& segment : reader) {
      const auto rowids = Read(segment, kGeneratedPKId);
      for (const auto id : segment.field_ids()) {
        for (auto it = segment.field(id)->iterator(); it->next();) {
          const irs::bstring term{it->value()};
          auto postings = it->postings(irs::IndexFeatures::None);
          auto mask = segment.MaskedDocs();
          irs::doc_id_t docs[irs::doc_limits::kBlockSize];
          uint32_t freqs[irs::doc_limits::kBlockSize];
          while (const auto count = postings->NextDocs(docs, freqs)) {
            for (uint32_t i = 0; i < count; ++i) {
              if (!mask.Contains(docs[i])) {
                out.emplace_back(id, term,
                                 rowids[docs[i] - irs::doc_limits::min()]);
              }
            }
          }
        }
      }
    }
    std::ranges::sort(out);
    return out;
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
};

TEST_F(PipelineMergeExecutorTest, MergesKeepEveryRow) {
  const auto before = Rows();
  ASSERT_EQ(4, _writer->GetSnapshot().size());
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
  const auto result = irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor));
  ASSERT_TRUE(result);
  EXPECT_EQ(4, result.size);
  EXPECT_GT(result.stats.profile.TotalNanos(), 0);
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(PipelineMergeExecutorTest, TotalTimeCountsThePreparation) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(4));
  constexpr std::chrono::milliseconds kPrepared{50};
  std::this_thread::sleep_for(kPrepared);
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 4};
  ASSERT_TRUE(executor.Run(plan));
  EXPECT_GE(plan.Profile().TotalNanos(),
            static_cast<uint64_t>(std::chrono::nanoseconds{kPrepared}.count()));
}

TEST_F(PipelineMergeExecutorTest, MergedPostingsKeepEveryRowsTerms) {
  const auto before = Postings();
  ASSERT_LT(Rows().size(), before.size());
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
  ASSERT_TRUE(irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor)));
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Postings());
}

TEST_F(PipelineMergeExecutorTest, MergesKeepInsertionOrderOnOtherConnections) {
  duckdb::Connection other{TestDb()};
  ASSERT_TRUE(duckdb::Settings::Get<duckdb::PreserveInsertionOrderSetting>(
    *other.context));
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
  ASSERT_TRUE(irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor)));
  EXPECT_TRUE(duckdb::Settings::Get<duckdb::PreserveInsertionOrderSetting>(
    *other.context));
  duckdb::Connection fresh{TestDb()};
  EXPECT_TRUE(duckdb::Settings::Get<duckdb::PreserveInsertionOrderSetting>(
    *fresh.context));
}

TEST_F(PipelineMergeExecutorTest, OneThreadRunsEveryUnitItself) {
  const auto before = Rows();
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 1};
  EXPECT_EQ(1, executor.Threads());
  ASSERT_TRUE(irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor)));
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(PipelineMergeExecutorTest, MergesRunOnTheSchedulerTheyAreGiven) {
  const auto before = Rows();
  duckdb::TaskScheduler own{TestDb()};
  own.SetThreads(4, 0);
  own.RelaunchThreads();
  search::PipelineMergeExecutor executor{TestDb(), &own, 4};
  ASSERT_TRUE(irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor)));
  own.Join();
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(PipelineMergeExecutorTest, CallerRunsTheUnitsNoWorkerTook) {
  const auto before = Rows();
  duckdb::TaskScheduler lonely{TestDb()};
  lonely.SetThreads(1, 1);
  lonely.RelaunchThreads();
  search::PipelineMergeExecutor executor{TestDb(), &lonely, 4};
  ASSERT_TRUE(irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor)));
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(PipelineMergeExecutorTest, StoppedMergeKeepsTheInputs) {
  const auto before = Rows();
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 4};
  std::atomic<size_t> calls{0};
  const auto result = irs::GetReady(_writer->CompactAsync(
    FullMerge(), nullptr, [&] { return calls.fetch_add(1) < 2; }, nullptr,
    &executor));
  EXPECT_FALSE(result);
  _writer->RefreshCommit();
  EXPECT_EQ(4, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(PipelineMergeExecutorTest, StopAtAnyPointFailsTheMerge) {
  const auto reader = _writer->GetSnapshot();
  const auto merge = [&](const irs::MergeProgress& progress) {
    irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
    for (const auto& segment : reader) {
      doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
    }
    irs::MemoryDirectory out;
    irs::SegmentMeta meta;
    irs::MergePlan plan{out,      doc_map, meta,     nullptr,
                        TestDb(), nullptr, progress, nullptr};
    if (!plan.Prepare(8)) {
      return false;
    }
    EXPECT_TRUE(plan.PostingsPending());
    search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
    return executor.Run(plan);
  };
  std::atomic<size_t> calls{0};
  ASSERT_TRUE(merge([&] {
    calls.fetch_add(1);
    return true;
  }));
  const auto total = calls.load();
  ASSERT_LT(8, total);
  for (const auto stop : {size_t{0}, total / 8, total / 4, total / 2}) {
    std::atomic<size_t> seen{0};
    EXPECT_FALSE(merge([&] { return seen.fetch_add(1) < stop; })) << stop;
  }
}

TEST_F(PipelineMergeExecutorTest, SortedMergesInterleaveTheirInputs) {
  Reopen(irs::SortSpec{{{.column = kGeneratedPKId}}});
  for (int64_t segment = 0; segment != 4; ++segment) {
    std::vector<int64_t> rowids(3000);
    for (size_t i = 0; i != rowids.size(); ++i) {
      rowids[i] = static_cast<int64_t>(i) * 4 + segment;
    }
    Insert(rowids);
  }
  const auto before = Rows();
  const auto postings = Postings();
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 4};
  const auto result = irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor));
  ASSERT_TRUE(result);
  EXPECT_GT(result.stats.profile.Nanos(irs::MergeStep::Prepare), 0);
  _writer->RefreshCommit();
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(before, Read(*reader.begin(), kGeneratedPKId));
  EXPECT_EQ(postings, Postings());
}

TEST_F(PipelineMergeExecutorTest, SortedMergesPartitionOnTheirScheduler) {
  Reopen(irs::SortSpec{{{.column = kGeneratedPKId}}});
  for (int64_t segment = 0; segment != 6; ++segment) {
    std::vector<int64_t> rowids(3000);
    for (size_t i = 0; i != rowids.size(); ++i) {
      rowids[i] = static_cast<int64_t>(i) * 6 + segment;
    }
    Insert(rowids);
  }
  const auto before = Rows();
  const auto postings = Postings();
  duckdb::TaskScheduler own{TestDb()};
  own.SetThreads(4, 0);
  own.RelaunchThreads();
  search::PipelineMergeExecutor executor{TestDb(), &own, 4};
  const auto result = irs::GetReady(
    _writer->CompactAsync(FullMerge(), nullptr, {}, nullptr, &executor));
  own.Join();
  ASSERT_TRUE(result);
  EXPECT_GT(result.stats.profile.Nanos(irs::MergeStep::Postings), 0);
  _writer->RefreshCommit();
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(before, Read(*reader.begin(), kGeneratedPKId));
  EXPECT_EQ(postings, Postings());
}

TEST_F(PipelineMergeExecutorTest, AnUnsortedInputFailsTheSortedMerge) {
  const irs::SortSpec spec{{{.column = kGeneratedPKId}}};
  Reopen({});
  Insert(std::vector<int64_t>{1, 3, 5, 7});
  Insert(std::vector<int64_t>{6, 4, 2, 0});
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,     doc_map, meta,    nullptr, TestDb(),
                      nullptr, {},      nullptr, &spec};
  ASSERT_TRUE(plan.Prepare(4));
  ASSERT_TRUE(plan.OrderPending());
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 4};
  EXPECT_THROW(executor.Run(plan), duckdb::InvalidInputException);
  EXPECT_TRUE(plan.Units().empty());
}

TEST_F(PipelineMergeExecutorTest, AnIncompleteOrderFailsTheSortedMerge) {
  if (!irs::AddFailurePointDebugging("irs::KeyOrder::Finish")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup remove = [] {
    irs::RemoveFailurePointDebugging("irs::KeyOrder::Finish");
  };
  const irs::SortSpec spec{{{.column = kGeneratedPKId}}};
  Reopen(spec);
  Insert(std::vector<int64_t>{0, 2, 4, 6});
  Insert(std::vector<int64_t>{1, 3, 5, 7});
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,     doc_map, meta,    nullptr, TestDb(),
                      nullptr, {},      nullptr, &spec};
  ASSERT_TRUE(plan.Prepare(4));
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 4};
  EXPECT_THROW(executor.Run(plan), irs::SqlException);
  EXPECT_TRUE(plan.Units().empty());
}

TEST_F(PipelineMergeExecutorTest, OperatorsNameThemselves) {
  const irs::SortSpec spec{{{.column = kGeneratedPKId}}};
  Reopen(spec);
  Insert(std::vector<int64_t>{0, 2});
  Insert(std::vector<int64_t>{1, 3});
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,     doc_map, meta,    nullptr, TestDb(),
                      nullptr, {},      nullptr, &spec};
  ASSERT_TRUE(plan.Prepare(1));
  ASSERT_TRUE(plan.OrderPending());
  const auto run = std::make_shared<search::MergeUnitRun>(
    plan, std::chrono::steady_clock::now());
  const connector::LogicalMergeOrder order{plan};
  const connector::LogicalMergeUnits units{run, 1};
  EXPECT_EQ("MERGE_ORDER", order.GetName());
  EXPECT_EQ("MERGE_UNITS", units.GetName());
  for (const auto* op :
       std::initializer_list<const duckdb::LogicalExtensionOperator*>{&order,
                                                                      &units}) {
    EXPECT_FALSE(op->RequireOptimizer());
    EXPECT_FALSE(op->SupportSerialization());
  }
  duckdb::PhysicalPlan physical{duckdb::Allocator::DefaultAllocator()};
  EXPECT_EQ("MERGE_ORDER",
            physical.Make<connector::PhysicalMergeOrder>(plan).GetName());
  EXPECT_EQ("MERGE_UNITS",
            physical.Make<connector::PhysicalMergeUnits>(run, 1).GetName());
}

TEST_F(PipelineMergeExecutorTest, PostingsMergeInTermRanges) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(8));
  ASSERT_TRUE(plan.PostingsPending());
  ASSERT_FALSE(plan.TermRanges().empty());
  auto& range = *plan.TermRanges().front();
  EXPECT_LT(1, range.PartCount());
  const connector::LogicalPostingsMerge merge{range};
  EXPECT_EQ("POSTINGS_MERGE", merge.GetName());
  EXPECT_FALSE(merge.RequireOptimizer());
  EXPECT_FALSE(merge.SupportSerialization());
  duckdb::PhysicalPlan physical{duckdb::Allocator::DefaultAllocator()};
  const auto& postings = physical.Make<connector::PhysicalPostingsMerge>(range);
  EXPECT_EQ("POSTINGS_MERGE", postings.GetName());
  const auto params = postings.ParamsToString();
  EXPECT_EQ(std::to_string(range.PartCount()), params.at("Parts"));
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
  ASSERT_TRUE(executor.Run(plan));
  EXPECT_FALSE(plan.PostingsPending());
  EXPECT_GT(plan.Profile().Nanos(irs::MergeStep::Postings), 0);
}

TEST_F(PipelineMergeExecutorTest, PostingsPieceIsReleasedByTheLastRangePart) {
  if (!irs::AddFailurePointDebugging("pause_postings_merge_finalize")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup remove = [] {
    irs::RemoveFailurePointDebugging("pause_postings_merge_finalize");
    irs::RemoveFailurePointDebugging("pause_postings_merge_finalize:release");
  };
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(8));
  ASSERT_FALSE(plan.TermRanges().empty());
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
  bool ok = false;
  std::thread merge{[&] { ok = executor.Run(plan); }};
  while (
    !irs::RemoveFailurePointDebugging("pause_postings_merge_finalize:parked")) {
    absl::SleepFor(absl::Milliseconds(1));
  }
  EXPECT_FALSE(plan.PostingsPending());
  irs::AddFailurePointDebugging("pause_postings_merge_finalize:release");
  merge.join();
  EXPECT_TRUE(ok);
}

TEST_F(PipelineMergeExecutorTest, AFailedUnitStopsTheTermRanges) {
  if (!irs::AddFailurePointDebugging("pause_merge_unit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  irs::AddFailurePointDebugging("pause_postings_merge_sink");
  const absl::Cleanup remove = [] {
    for (const auto* point :
         {"pause_merge_unit", "pause_postings_merge_sink",
          "pause_postings_merge_sink:release", "pause_merge_unit_failed",
          "pause_merge_unit_failed:release", "irs::MergeUnit"}) {
      irs::RemoveFailurePointDebugging(point);
    }
  };
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(8));
  ASSERT_FALSE(plan.TermRanges().empty());
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 8};
  std::exception_ptr failure;
  std::thread merge{[&] {
    try {
      executor.Run(plan);
    } catch (...) {
      failure = std::current_exception();
    }
  }};
  const auto await = [](const char* parked) {
    while (!irs::RemoveFailurePointDebugging(parked)) {
      absl::SleepFor(absl::Milliseconds(1));
    }
  };
  await("pause_postings_merge_sink:parked");
  irs::AddFailurePointDebugging("irs::MergeUnit");
  irs::AddFailurePointDebugging("pause_merge_unit_failed");
  irs::RemoveFailurePointDebugging("pause_merge_unit");
  await("pause_merge_unit_failed:parked");
  irs::AddFailurePointDebugging("pause_postings_merge_sink:release");
  irs::AddFailurePointDebugging("pause_merge_unit_failed:release");
  merge.join();
  EXPECT_TRUE(failure);
  size_t left = 0;
  for (const auto& range : plan.TermRanges()) {
    left += range->PartsLeft();
  }
  EXPECT_LT(0, left);
}

TEST_F(PipelineMergeExecutorTest, MergeUnitsPlanPassesThePlanVerifier) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(8));
  ASSERT_FALSE(plan.TermRanges().empty());
  const auto run = std::make_shared<search::MergeUnitRun>(
    plan, std::chrono::steady_clock::now(), plan.PostingsPending());
  connector::LogicalMergeUnits units{run, 8, plan.TermRanges()};
  units.ResolveOperatorTypes();
  const auto result = duckdb::LogicalPlanVerifier::VerifyAlways(units);
  for (const auto& issue : result.GetIssues()) {
    ADD_FAILURE() << issue.message;
  }
  EXPECT_FALSE(result.HasError());
}

TEST_F(PipelineMergeExecutorTest, UnitsLeaveThreadsToUnfinishedTermRanges) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(8));
  ASSERT_EQ(1, plan.TermRanges().size());
  auto& range = *plan.TermRanges().front();
  const auto parts = range.PartCount();
  ASSERT_LT(1, parts);
  ASSERT_EQ(parts, range.PartsLeft());
  search::MergeUnitRun run{plan, std::chrono::steady_clock::now(),
                           plan.PostingsPending()};
  EXPECT_TRUE(run.MayClaim(1));
  EXPECT_TRUE(run.MayClaim(parts));
  const auto unit = run.schedule.Claim();
  ASSERT_TRUE(unit);
  EXPECT_FALSE(run.MayClaim(parts));
  EXPECT_FALSE(run.MayClaim(parts + 1));
  EXPECT_TRUE(run.MayClaim(parts + 2));
  ASSERT_TRUE(range.MergePart(0));
  EXPECT_EQ(parts - 1, range.PartsLeft());
  EXPECT_FALSE(run.MayClaim(parts));
  EXPECT_TRUE(run.MayClaim(parts + 1));
  for (size_t part = 1; part < parts; ++part) {
    ASSERT_TRUE(range.MergePart(part));
  }
  EXPECT_EQ(0, range.PartsLeft());
  EXPECT_FALSE(run.MayClaim(1));
  EXPECT_TRUE(run.MayClaim(2));
  run.ReleasePostings(plan.FinishPostings());
  EXPECT_TRUE(run.MayClaim(1));
  run.Run(*unit);
  while (const auto next = run.schedule.Claim()) {
    run.Run(*next);
  }
  EXPECT_TRUE(run.schedule.Done());
  EXPECT_TRUE(run.Finish());
}

TEST_F(PipelineMergeExecutorTest, UnitErrorReachesTheCaller) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  NoColumnsDirectory failing{out};
  irs::SegmentMeta meta;
  irs::MergePlan plan{failing,  doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare());
  search::PipelineMergeExecutor executor{TestDb(), nullptr, 4};
  EXPECT_THROW(executor.Run(plan), irs::SqlException);
}

}  // namespace
