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
#include <absl/synchronization/notification.h>
#include <absl/time/time.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/index/merge/doc_map.hpp>
#include <iresearch/index/merge/merge_executor.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <numeric>
#include <thread>
#include <tuple>
#include <vector>

#include "connector/column_id.h"
#include "connector/search_sink_writer.hpp"
#include "gtest/gtest.h"
#include "index/compaction/policy_compaction.hpp"

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

[[nodiscard]] auto PinWorkers(duckdb::idx_t total, duckdb::idx_t external) {
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(TestDb());
  scheduler.SetThreads(total, external);
  scheduler.RelaunchThreads();
  return absl::Cleanup{[&scheduler] {
    const auto& config = duckdb::DBConfig::GetConfig(TestDb());
    scheduler.SetThreads(
      config.options.maximum_threads,
      duckdb::Settings::Get<duckdb::ExternalThreadsSetting>(config));
    scheduler.RelaunchThreads();
  }};
}

catalog::ColumnTokenizer KeywordTokenizer(irs::field_id) {
  static auto gTokenizer = std::make_shared<catalog::Tokenizer>(
    search::Features{},
    irs::analysis::TokenizerConfig{.config = irs::KeywordTokenizer::Options{}});
  return {.analyzer = gTokenizer->Acquire(TestContext()),
          .features = irs::IndexFeatures::None};
}

constexpr ColumnId kValueColumn{1};

const tests::CompactionPolicy& FullMerge() {
  static const auto kPolicy = tests::MakePolicy(
    tests::CompactionCount{std::numeric_limits<size_t>::max()});
  return kPolicy;
}

class NoPostingsDirectory final : public irs::Directory {
 public:
  explicit NoPostingsDirectory(irs::Directory& impl) noexcept
    : irs::Directory{impl.ResourceManager()}, _impl{impl} {}

  irs::DirectoryAttributes& attributes() noexcept final {
    return _impl.attributes();
  }
  irs::IndexOutput::ptr create(std::string_view name) final {
    if (name.ends_with(".doc")) {
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

class MergeExecutorTest : public ::testing::Test {
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

  void Reopen(std::vector<irs::SortKey> sort) {
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
      NoEntryInfoProvider(), catalog::PkColumnKind::Has};
    for (size_t first = 0; first < rowids.size();
         first += STANDARD_VECTOR_SIZE) {
      const auto n =
        std::min<size_t>(STANDARD_VECTOR_SIZE, rowids.size() - first);
      duckdb::Vector ids{duckdb::LogicalType::BIGINT, n};
      duckdb::Vector values{duckdb::LogicalType::VARCHAR, n};
      auto* id_data = duckdb::FlatVector::GetDataMutable<int64_t>(ids);
      auto* value_data =
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(values);
      for (size_t i = 0; i < n; ++i) {
        id_data[i] = rowids[first + i];
        value_data[i] = duckdb::StringVector::AddString(
          values, absl::StrCat("v", rowids[first + i] * 10));
      }
      sink.Init(n, &ids);
      sink.SwitchColumn(
        ColumnDescriptor{kValueColumn, duckdb::LogicalType::VARCHAR}, values,
        n);
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
        for (const auto* field : segment.fields(id)) {
          for (auto it = field->iterator(); it->next();) {
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
    }
    std::ranges::sort(out);
    return out;
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
};

TEST_F(MergeExecutorTest, MergesKeepEveryRow) {
  const auto before = Rows();
  ASSERT_EQ(4, _writer->GetSnapshot().size());
  const auto result = tests::Compact(*_writer, FullMerge());
  ASSERT_TRUE(result);
  EXPECT_EQ(4, result.size);
  EXPECT_GT(result.stats.profile.TotalNanos(), 0);
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(MergeExecutorTest, MergedPostingsKeepEveryRowsTerms) {
  const auto before = Postings();
  ASSERT_EQ(Rows().size(), before.size());
  ASSERT_TRUE(tests::Compact(*_writer, FullMerge()));
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Postings());
}

TEST_F(MergeExecutorTest, MergesNeedNoSchedulerWorker) {
  const auto before = Rows();
  const auto restore = PinWorkers(1, 1);
  ASSERT_TRUE(tests::Compact(*_writer, FullMerge()));
  _writer->RefreshCommit();
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

class NotifyTask final : public duckdb::Task {
 public:
  explicit NotifyTask(absl::Notification& done) : _done{done} {}

  duckdb::TaskExecutionResult Execute(duckdb::TaskExecutionMode) final {
    _done.Notify();
    return duckdb::TaskExecutionResult::TASK_FINISHED;
  }

 private:
  absl::Notification& _done;
};

TEST_F(MergeExecutorTest, UnitsLeaveTheSchedulerWorkersToOtherTasks) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  std::atomic<bool> armed{false};
  std::atomic<std::thread::id> driver;
  std::atomic<bool> held{false};
  absl::Notification worker_inside;
  absl::Notification ran;
  const irs::MergeProgress progress = [&] {
    if (!armed.load()) {
      return true;
    }
    if (std::this_thread::get_id() == driver.load()) {
      worker_inside.WaitForNotificationWithTimeout(absl::Seconds(10));
    } else if (!held.exchange(true)) {
      worker_inside.Notify();
      ran.WaitForNotificationWithTimeout(absl::Seconds(10));
    }
    return true;
  };
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta,     nullptr,
                      TestDb(), nullptr, progress, nullptr};
  ASSERT_TRUE(plan.Prepare(tests::merge::MergePool(1)));
  const auto restore = PinWorkers(1, 0);
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(TestDb());
  armed = true;
  bool merged = false;
  std::thread merge{[&] {
    driver = std::this_thread::get_id();
    merged = irs::ExecuteMerge(plan);
  }};
  EXPECT_TRUE(worker_inside.WaitForNotificationWithTimeout(absl::Seconds(10)));
  const auto producer = scheduler.CreateProducer(
    scheduler.CreateAccount(duckdb::SchedulerClass::FOREGROUND));
  scheduler.ScheduleTask(*producer, duckdb::make_shared_ptr<NotifyTask>(ran));
  EXPECT_TRUE(ran.WaitForNotificationWithTimeout(absl::Seconds(5)));
  merge.join();
  EXPECT_TRUE(merged);
}

TEST_F(MergeExecutorTest, StoppedMergeKeepsTheInputs) {
  const auto before = Rows();
  std::atomic<size_t> calls{0};
  const auto result = tests::Compact(*_writer, FullMerge(), nullptr,
                                     [&] { return calls.fetch_add(1) < 2; });
  EXPECT_FALSE(result);
  _writer->RefreshCommit();
  EXPECT_EQ(4, _writer->GetSnapshot().size());
  EXPECT_EQ(before, Rows());
}

TEST_F(MergeExecutorTest, StopAtAnyPointFailsTheMerge) {
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
    if (!plan.Prepare(tests::merge::MergePool(8))) {
      return false;
    }
    EXPECT_TRUE(absl::c_any_of(plan.Units(), [](const irs::MergeUnit& unit) {
      return unit.kind == irs::MergeUnitKind::TermRange;
    }));
    return irs::ExecuteMerge(plan);
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

TEST_F(MergeExecutorTest, SortedMergesInterleaveTheirInputs) {
  Reopen(std::vector<irs::SortKey>{{{.column = kGeneratedPKId}}});
  for (int64_t segment = 0; segment != 4; ++segment) {
    std::vector<int64_t> rowids(3000);
    for (size_t i = 0; i != rowids.size(); ++i) {
      rowids[i] = static_cast<int64_t>(i) * 4 + segment;
    }
    Insert(rowids);
  }
  const auto before = Rows();
  const auto postings = Postings();
  const auto result = tests::Compact(*_writer, FullMerge());
  ASSERT_TRUE(result);
  EXPECT_GT(result.stats.profile.Nanos(irs::MergeStep::Prepare), 0);
  _writer->RefreshCommit();
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(before, Read(*reader.begin(), kGeneratedPKId));
  EXPECT_EQ(postings, Postings());
}

TEST_F(MergeExecutorTest, SortedMergesPartitionOnTheirPool) {
  Reopen(std::vector<irs::SortKey>{{{.column = kGeneratedPKId}}});
  for (int64_t segment = 0; segment != 6; ++segment) {
    std::vector<int64_t> rowids(3000);
    for (size_t i = 0; i != rowids.size(); ++i) {
      rowids[i] = static_cast<int64_t>(i) * 6 + segment;
    }
    Insert(rowids);
  }
  const auto before = Rows();
  const auto postings = Postings();
  const auto result = tests::Compact(*_writer, FullMerge(), nullptr, {},
                                     nullptr, tests::merge::MergePool(4));
  ASSERT_TRUE(result);
  EXPECT_GT(result.stats.profile.Nanos(irs::MergeStep::Postings), 0);
  _writer->RefreshCommit();
  const auto reader = _writer->GetSnapshot();
  ASSERT_EQ(1, reader.size());
  EXPECT_EQ(before, Read(*reader.begin(), kGeneratedPKId));
  EXPECT_EQ(postings, Postings());
}

TEST_F(MergeExecutorTest, AnUnsortedInputFailsTheSortedMerge) {
  const std::vector<irs::SortKey> spec{{{.column = kGeneratedPKId}}};
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
  EXPECT_THROW(plan.Prepare(tests::merge::MergePool(4)), irs::SqlException);
  EXPECT_TRUE(plan.Units().empty());
}

TEST_F(MergeExecutorTest, AnIncompleteOrderFailsTheSortedMerge) {
  if (!duckdb::FaultPoints::Arm("irs::KeyOrder::Finish")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup remove = [] {
    duckdb::FaultPoints::Disarm("irs::KeyOrder::Finish");
  };
  const std::vector<irs::SortKey> spec{{{.column = kGeneratedPKId}}};
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
  EXPECT_THROW(plan.Prepare(tests::merge::MergePool(4)), irs::SqlException);
  EXPECT_TRUE(plan.Units().empty());
}

TEST_F(MergeExecutorTest, PostingsMergeInTermRanges) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(tests::merge::MergePool(8)));
  const auto units = plan.Units();
  const auto part = absl::c_find_if(units, [](const irs::MergeUnit& unit) {
    return unit.kind == irs::MergeUnitKind::TermRange;
  });
  ASSERT_NE(part, units.end());
  EXPECT_LT(1, absl::c_count_if(units, [&](const irs::MergeUnit& unit) {
              return unit.kind == irs::MergeUnitKind::TermRange &&
                     unit.id == part->id;
            }));
  ASSERT_TRUE(irs::ExecuteMerge(plan));
  EXPECT_GT(plan.Profile().Nanos(irs::MergeStep::Postings), 0);
}

TEST_F(MergeExecutorTest, AFailedUnitStopsTheTermRangePartsStillRunning) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  const absl::Cleanup remove = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  std::atomic<std::thread::id> watched;
  size_t calls = 0;
  absl::Notification inside;
  absl::Notification failed;
  const irs::MergeProgress progress = [&] {
    if (std::this_thread::get_id() == watched.load() && ++calls == 1) {
      inside.Notify();
      failed.WaitForNotification();
    }
    return true;
  };
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta,     nullptr,
                      TestDb(), nullptr, progress, nullptr};
  ASSERT_TRUE(plan.Prepare(tests::merge::MergePool(8)));
  const auto units = plan.Units();
  const auto part = absl::c_find_if(units, [](const irs::MergeUnit& unit) {
    return unit.kind == irs::MergeUnitKind::TermRange;
  });
  ASSERT_NE(part, units.end());
  const auto running = static_cast<size_t>(part - units.begin());
  bool finished = true;
  std::thread unit{[&] {
    watched = std::this_thread::get_id();
    finished = plan.RunUnit(running);
  }};
  inside.WaitForNotification();
  duckdb::FaultPoints::Arm("irs::MergeUnit");
  EXPECT_THROW(std::ignore = irs::ExecuteMerge(plan), irs::SqlException);
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  failed.Notify();
  unit.join();
  EXPECT_FALSE(finished);
}

TEST_F(MergeExecutorTest, AFailedUnitStopsTheUnitsStillRunning) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  const absl::Cleanup remove = [] {
    duckdb::FaultPoints::Disarm("irs::MergeUnit");
  };
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  std::atomic<std::thread::id> watched;
  size_t calls = 0;
  absl::Notification inside;
  absl::Notification failed;
  const irs::MergeProgress progress = [&] {
    if (std::this_thread::get_id() == watched.load() && ++calls == 2) {
      inside.Notify();
      failed.WaitForNotification();
    }
    return true;
  };
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,      doc_map, meta,     nullptr,
                      TestDb(), nullptr, progress, nullptr};
  ASSERT_TRUE(plan.Prepare(tests::merge::MergePool(1)));
  const auto units = plan.Units();
  const auto column = absl::c_find_if(units, [](const irs::MergeUnit& unit) {
    return unit.deps.empty() && (unit.kind == irs::MergeUnitKind::Column ||
                                 unit.kind == irs::MergeUnitKind::ColumnCopy);
  });
  ASSERT_NE(column, units.end());
  const auto running = static_cast<size_t>(column - units.begin());
  bool finished = true;
  std::thread unit{[&] {
    watched = std::this_thread::get_id();
    finished = plan.RunUnit(running);
  }};
  inside.WaitForNotification();
  duckdb::FaultPoints::Arm("irs::MergeUnit");
  EXPECT_THROW(std::ignore = irs::ExecuteMerge(plan), irs::SqlException);
  duckdb::FaultPoints::Disarm("irs::MergeUnit");
  failed.Notify();
  unit.join();
  EXPECT_FALSE(finished);
}

TEST_F(MergeExecutorTest, UnitErrorReachesTheCaller) {
  const auto reader = _writer->GetSnapshot();
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto& segment : reader) {
    doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  NoPostingsDirectory failing{out};
  irs::SegmentMeta meta;
  irs::MergePlan plan{failing,  doc_map, meta, nullptr,
                      TestDb(), nullptr, {},   nullptr};
  ASSERT_TRUE(plan.Prepare(tests::merge::MergePool(1)));
  EXPECT_THROW(std::ignore = irs::ExecuteMerge(plan), irs::SqlException);
}

}  // namespace
