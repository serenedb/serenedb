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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstdint>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/key_merge.hpp>
#include <iresearch/index/merge/merge_order.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/detail/ann_index.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;
using irs::MergeUnitKind;

constexpr irs::field_id kBigint = kFirstColumn;
constexpr irs::field_id kDouble = kFirstColumn + 1;
constexpr irs::field_id kText = kFirstColumn + 2;
constexpr irs::field_id kFlag = kFirstColumn + 3;
constexpr irs::field_id kBlob = kFirstColumn + 4;
constexpr uint32_t kSmallRowGroup = 64;

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

irs::IndexWriter::ptr MakeWriter(irs::Directory& dir, uint32_t row_group_size,
                                 irs::ColumnOptionsProvider columns,
                                 irs::SortSpec sort = {}) {
  auto options = irs::tests::DefaultWriterOptions();
  options.column_options = std::move(columns);
  options.row_group_size = row_group_size;
  options.sort = std::move(sort);
  return irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
}

irs::SortSpec RandomSpec(Generator& gen) {
  constexpr irs::field_id kKeys[] = {kBigint, kDouble, kText, kFlag, kBlob};
  irs::SortSpec spec;
  const auto keys = gen.Uniform(1, 3);
  for (size_t k = 0; k != keys; ++k) {
    spec.keys.push_back({.column = kKeys[gen.Uniform(0, std::size(kKeys) - 1)],
                         .descending = gen.Coin(0.3),
                         .nulls_first = gen.Coin(0.5)});
  }
  return spec;
}

std::shared_ptr<const irs::SegmentReaderImpl> SortedMerge(
  irs::Directory& out, std::span<const irs::SubReader* const> inputs,
  const irs::SortSpec* spec, uint32_t row_group_size,
  irs::ColumnOptionsProvider columns, bool& flushed,
  irs::MergeExecutor* executor = nullptr, irs::ScorerPtr scorer = nullptr,
  irs::SegmentMeta* written = nullptr) {
  irs::FunctionFieldOptions field_options{
    std::move(columns), irs::tests::MakeNormColumnIdProvider(), row_group_size};
  irs::SegmentMeta local;
  auto& meta = written != nullptr ? *written : local;
  const irs::SegmentWriterOptions options{
    .scorer = scorer,
    .db = &irs::DuckDBEngine::Instance().instance(),
    .field_options = &field_options,
    .sort = spec,
  };
  irs::MergeWriter merger{out, options};
  merger.Reset(inputs.begin(), inputs.end());
  flushed = irs::GetReady(merger.Flush(meta, {}, nullptr, executor));
  if (!flushed) {
    return nullptr;
  }
  return irs::SegmentReaderImpl::Open(out, meta,
                                      irs::tests::DefaultReaderOptions());
}

class SortedMergeOracleTest
  : public ::testing::TestWithParam<std::tuple<uint64_t, size_t>> {};

TEST_P(SortedMergeOracleTest, RandomSortedSegmentSets) {
  const auto [seed, threads] = GetParam();
  Generator gen{seed};
  const auto spec = RandomSpec(gen);
  const uint32_t row_group_size =
    gen.Coin(0.5) ? kSmallRowGroup : DEFAULT_ROW_GROUP_SIZE;
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, row_group_size, SketchedColumns);
  size_t next_id = 0;
  const auto segments = gen.Uniform(1, 6);
  for (size_t s = 0; s != segments; ++s) {
    WriteSegment(*writer, gen, RandomShape(gen), next_id, &spec);
  }
  const auto reader = writer->GetSnapshot();
  auto inputs = Segments(reader);
  if (inputs.empty()) {
    GTEST_SKIP() << "every document was deleted";
  }
  std::shuffle(inputs.begin(), inputs.end(), std::mt19937_64{seed});
  SCOPED_TRACE(absl::StrCat("seed ", seed, ", ", threads, " threads, ",
                            inputs.size(), " input segments, ",
                            spec.keys.size(), " keys"));

  const auto expected = ExpectedSortedMerge(inputs, spec);
  for (const bool shuffled : {false, true}) {
    if (shuffled && threads == 1) {
      continue;
    }
    SCOPED_TRACE(shuffled ? "shuffled" : "scheduled");
    ThreadedMergeExecutor scheduled{threads};
    ShuffledMergeExecutor random{threads, seed};
    irs::MergeExecutor* executor = nullptr;
    if (threads != 1) {
      executor = shuffled ? static_cast<irs::MergeExecutor*>(&random)
                          : static_cast<irs::MergeExecutor*>(&scheduled);
    }
    irs::MemoryDirectory out;
    bool flushed = false;
    const auto merged = SortedMerge(out, inputs, &spec, row_group_size,
                                    SketchedColumns, flushed, executor);
    ASSERT_EQ(expected.docs != 0, flushed);
    if (!flushed) {
      continue;
    }
    ASSERT_NE(nullptr, merged);
    EXPECT_EQ(merged->docs_count(), merged->live_docs_count());
    EXPECT_EQ("", FirstDifference(expected, Dump(*merged)));
  }
}

INSTANTIATE_TEST_SUITE_P(seeds, SortedMergeOracleTest,
                         ::testing::Combine(::testing::Range<uint64_t>(0, 32),
                                            ::testing::Values(1, 2, 8)));

class SortedMergeTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _writer = MakeWriter(_dir, kSmallRowGroup, PlainColumns);
  }

  void AddSegment(std::span<const int64_t> keys, bool normed = false,
                  bool with_double = true, bool with_id = true) {
    auto batch = _writer->GetBatch();
    for (const auto key : keys) {
      auto doc = batch.Insert();
      tests::StringField id{"id", absl::StrCat("doc-", _next++)};
      id.id = kIdField;
      if (with_id) {
        tests::InsertField(doc, id);
      }
      tests::TextField<std::string> text{
        "text", absl::StrCat("all word", key % 7, " word", key % 3)};
      text.id = kTextField;
      tests::InsertField(doc, text);
      if (normed) {
        tests::TextField<std::string> norm{
          "normed",
          absl::StrCat("w", key % 5, std::string(key % 13, 'x'), " w", key % 11,
                       key % 17 == 0 ? std::string(70, 'y') : ""),
          false, irs::IndexFeatures::Norm};
        norm.id = kNormField;
        tests::InsertField(doc, norm);
      }
      WriteValue(doc, kBigint, duckdb::LogicalType::BIGINT,
                 duckdb::Value::BIGINT(key));
      if (with_double) {
        WriteValue(doc, kDouble, duckdb::LogicalType::DOUBLE,
                   duckdb::Value::DOUBLE(static_cast<double>(key) / 7.0));
      }
      WriteValue(doc, kText, duckdb::LogicalType::VARCHAR,
                 duckdb::Value{absl::StrCat("value-", key % 300)});
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  std::vector<const irs::SubReader*> Snapshot() {
    _reader = _writer->GetSnapshot();
    return Segments(_reader);
  }

  irs::MergePlan MakePlan(irs::Directory& out,
                          std::span<const irs::SubReader* const> inputs,
                          const irs::SortSpec& spec,
                          const irs::MergeProgress& progress = {}) {
    _doc_map.clear();
    for (const auto* input : inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
    _meta = {};
    return irs::MergePlan{out,
                          _doc_map,
                          _meta,
                          nullptr,
                          irs::DuckDBEngine::Instance().instance(),
                          &_field_options,
                          progress,
                          nullptr,
                          &spec};
  }

  std::string MergedDifference(irs::Directory& out,
                               std::span<const irs::SubReader* const> inputs,
                               const irs::SortSpec& spec) {
    const auto merged = irs::SegmentReaderImpl::Open(
      out, _meta, irs::tests::DefaultReaderOptions());
    if (merged == nullptr) {
      return "merged segment did not open";
    }
    return FirstDifference(ExpectedSortedMerge(inputs, spec), Dump(*merged));
  }

  static size_t CountUnits(std::span<const irs::MergeUnit> units,
                           MergeUnitKind kind, irs::field_id id) {
    return absl::c_count_if(units, [&](const irs::MergeUnit& unit) {
      return unit.kind == kind && unit.id == id;
    });
  }

  static std::vector<int64_t> Keys(int64_t first, int64_t step, size_t count) {
    std::vector<int64_t> keys(count);
    for (size_t i = 0; i != count; ++i) {
      keys[i] = first + static_cast<int64_t>(i) * step;
    }
    return keys;
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  irs::FunctionFieldOptions _field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(), kSmallRowGroup};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
  size_t _next = 0;
};

TEST_F(SortedMergeTest, InputsWithDisjointKeysCopyTheirBlocks) {
  AddSegment(Keys(3000, 1, 3000));
  AddSegment(Keys(0, 1, 3000));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  ASSERT_TRUE(plan.OrderPending());
  EXPECT_TRUE(plan.Units().empty());
  ASSERT_TRUE(plan.RunOrder());
  EXPECT_FALSE(plan.OrderPending());
  EXPECT_FALSE(plan.KeyMerge());
  EXPECT_EQ(2, CountUnits(plan.Units(), MergeUnitKind::ColumnCopy, kBigint));
  EXPECT_EQ(2, CountUnits(plan.Units(), MergeUnitKind::ColumnCopy, kDouble));
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, inputs, spec));
}

TEST_F(SortedMergeTest, InterleavedInputsAreGathered) {
  AddSegment(Keys(2046, -2, 1024));
  AddSegment(Keys(2047, -2, 1024));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint, .descending = true}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(4));
  ASSERT_TRUE(plan.RunOrder());
  EXPECT_EQ(0, CountUnits(plan.Units(), MergeUnitKind::ColumnCopy, kBigint));
  EXPECT_LT(1, CountUnits(plan.Units(), MergeUnitKind::Column, kText));
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, inputs, spec));
}

TEST_F(SortedMergeTest, UnsortedInputsFailTheMerge) {
  auto first = Keys(0, 2, 300);
  std::swap(first[5], first[250]);
  AddSegment(first);
  AddSegment(Keys(1, 2, 300));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(4));
  EXPECT_FALSE(irs::RunSerially(plan));
}

TEST_F(SortedMergeTest, AnUnsortedInputThatLooksApartFailsTheMerge) {
  auto unsorted = Keys(3000, 1, 3000);
  std::swap(unsorted[10], unsorted[20]);
  AddSegment(unsorted);
  AddSegment(Keys(0, 1, 3000));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  EXPECT_FALSE(irs::RunSerially(plan));
  EXPECT_TRUE(plan.Units().empty());
}

TEST_F(SortedMergeTest, RunsThatMissOrRepeatDocumentsFailTheOrder) {
  AddSegment(Keys(3000, 1, 3000));
  AddSegment(Keys(0, 1, 3000));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  const std::vector<std::vector<duckdb::PresortedRun>> broken{
    {},
    {{.input = 1, .begin = 0, .count = 3000}},
    {{.input = 2, .begin = 0, .count = 1}},
    {{.input = 1, .begin = 3001, .count = 0}},
    {{.input = 1, .begin = 1, .count = 3000}},
    {{.input = 1, .begin = 0, .count = 3000},
     {.input = 1, .begin = 2999, .count = 1}},
  };
  for (size_t i = 0; i != broken.size(); ++i) {
    SCOPED_TRACE(i);
    irs::MemoryDirectory out;
    auto plan = MakePlan(out, inputs, spec);
    ASSERT_TRUE(plan.Prepare(1));
    plan.ExpandOrder(0, broken[i]);
    EXPECT_FALSE(plan.FinishOrder());
    EXPECT_TRUE(plan.Units().empty());
  }
  const std::vector<duckdb::PresortedRun> runs{
    {.input = 1, .begin = 0, .count = 3000},
    {.input = 0, .begin = 0, .count = 3000}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  plan.ExpandOrder(0, runs);
  ASSERT_TRUE(plan.FinishOrder());
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, inputs, spec));
}

TEST_F(SortedMergeTest, PartitionsExpandInAnyOrder) {
  AddSegment(Keys(0, 3, 3000));
  AddSegment(Keys(1, 3, 3000));
  AddSegment(Keys(2, 3, 3000));
  std::vector<std::string> removed;
  for (size_t id = 0; id < 9000; id += 5) {
    removed.push_back(absl::StrCat("doc-", id));
  }
  RemoveTerms(*_writer, kIdField, removed);
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MergeDocMap serial_map{{irs::IResourceManager::gNoop}};
  irs::MergeDocMap split_map{{irs::IResourceManager::gNoop}};
  irs::MergeDocMap broken_map{{irs::IResourceManager::gNoop}};
  for (const auto* input : inputs) {
    serial_map.emplace_back(input, irs::IResourceManager::gNoop);
    split_map.emplace_back(input, irs::IResourceManager::gNoop);
    broken_map.emplace_back(input, irs::IResourceManager::gNoop);
  }
  irs::KeyOrder serial{serial_map, spec};
  ASSERT_TRUE(serial.Prepare(1));
  ASSERT_TRUE(serial.Merge([] { return true; }));
  ASSERT_TRUE(serial.Finish());

  const auto expand_backwards = [](irs::KeyOrder& order) {
    const auto merge = order.KeyMerge();
    for (auto partition = merge->PartitionCount(); partition-- != 0;) {
      const auto state = merge->BeginMerge(partition);
      while (!state->Done()) {
        duckdb::vector<duckdb::PresortedRun> runs;
        duckdb::optional_idx unsorted;
        ASSERT_TRUE(
          merge->Merge(*state, runs, 7, [] { return true; }, unsorted));
        order.Expand(partition, runs);
      }
    }
  };
  irs::KeyOrder split{split_map, spec};
  ASSERT_TRUE(split.Prepare(4));
  ASSERT_LT(1, split.KeyMerge()->PartitionCount());
  expand_backwards(split);
  ASSERT_TRUE(split.Finish());
  EXPECT_TRUE(
    std::ranges::equal(serial.Order().Inputs(), split.Order().Inputs()));
  for (size_t i = 0; i != inputs.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_TRUE(std::ranges::equal(serial_map[i].remap.id_map,
                                   split_map[i].remap.id_map));
    EXPECT_EQ(serial.IsolatedBegin(i), split.IsolatedBegin(i));
  }

  irs::KeyOrder broken{broken_map, spec};
  ASSERT_TRUE(broken.Prepare(4));
  const auto first = broken.KeyMerge()->PartitionBegin(1, 0);
  ASSERT_LT(0, first);
  expand_backwards(broken);
  const std::vector<duckdb::PresortedRun> before_partition{
    {.input = 0, .begin = first - 1, .count = 1}};
  broken.Expand(1, before_partition);
  EXPECT_FALSE(broken.Finish());
}

TEST_F(SortedMergeTest, KeysMissingOrNullAtTheEndSortAsNulls) {
  const auto add = [&](int64_t first, bool null_tail) {
    auto batch = _writer->GetBatch();
    for (int64_t i = 0; i != 5100; ++i) {
      auto doc = batch.Insert();
      tests::StringField id{"id", absl::StrCat("doc-", _next++)};
      id.id = kIdField;
      tests::InsertField(doc, id);
      if (i < 100) {
        WriteValue(doc, kBigint, duckdb::LogicalType::BIGINT,
                   duckdb::Value::BIGINT(first + i * 3));
      } else if (null_tail) {
        WriteValue(doc, kBigint, duckdb::LogicalType::BIGINT,
                   duckdb::Value{duckdb::LogicalType::BIGINT});
      }
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  };
  add(0, true);
  add(1, false);
  add(2, true);
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, inputs, spec));
}

TEST_F(SortedMergeTest, AnIncompleteOrderFailsTheMerge) {
  if (!irs::AddFailurePointDebugging("irs::KeyOrder::Finish")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const absl::Cleanup remove = [] {
    irs::RemoveFailurePointDebugging("irs::KeyOrder::Finish");
  };
  AddSegment(Keys(0, 2, 300));
  AddSegment(Keys(1, 2, 300));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  EXPECT_FALSE(irs::RunSerially(plan));
  EXPECT_TRUE(plan.Units().empty());
}

TEST_F(SortedMergeTest, EqualKeysKeepTheInputOrder) {
  AddSegment(std::vector<int64_t>(300, 5));
  AddSegment(std::vector<int64_t>(300, 5));
  AddSegment(Keys(4, 1, 3));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, inputs, spec));
}

TEST_F(SortedMergeTest, KeysNoInputHasKeepTheInputOrder) {
  AddSegment(Keys(10, -1, 10));
  AddSegment(Keys(0, 1, 10));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kFirstColumn + 50}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, spec);
  ASSERT_TRUE(plan.Prepare(1));
  EXPECT_FALSE(plan.OrderPending());
  EXPECT_FALSE(plan.KeyMerge());
  EXPECT_FALSE(plan.Units().empty());
  ASSERT_TRUE(irs::RunSerially(plan));
  const auto merged = irs::SegmentReaderImpl::Open(
    out, _meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, merged);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged)));
}

TEST_F(SortedMergeTest, FullyDeletedInputsAddNoRows) {
  AddSegment(Keys(0, 2, 100), true);
  AddSegment(Keys(1, 2, 100));
  const auto inputs = Snapshot();
  auto meta = _reader.Meta().index_meta.segments.back().meta;
  irs::DocumentMask all;
  for (auto doc = irs::doc_limits::min();
       doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
    all.Add(doc);
  }
  meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
  meta.live_docs_count = 0;
  const auto deleted = irs::SegmentReaderImpl::Open(
    _dir, meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, deleted);
  const std::vector<const irs::SubReader*> with_deleted{deleted.get(),
                                                        inputs.front()};
  for (const auto column : {kBigint, irs::field_id{kFirstColumn + 50}}) {
    SCOPED_TRACE(column);
    const irs::SortSpec spec{{{.column = column}}};
    irs::MemoryDirectory out;
    auto plan = MakePlan(out, with_deleted, spec);
    ASSERT_TRUE(plan.Prepare(1));
    ASSERT_TRUE(irs::RunSerially(plan));
    EXPECT_EQ("", MergedDifference(out, with_deleted, spec));
  }
  const std::vector<const irs::SubReader*> only_deleted{deleted.get()};
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, only_deleted, spec);
  ASSERT_TRUE(plan.Prepare(1));
  EXPECT_FALSE(plan.OrderPending());
  EXPECT_EQ(0, _meta.docs_count);
  EXPECT_FALSE(irs::RunSerially(plan));
}

TEST_F(SortedMergeTest, LargeInputsSpanWindowsAndCheckpoints) {
  AddSegment(Keys(0, 3, 45000), true);
  AddSegment(Keys(1, 3, 45000), true);
  AddSegment(Keys(2, 3, 45000), false, false);
  std::vector<std::string> removed;
  for (size_t id = 2048; id != 4096; ++id) {
    removed.push_back(absl::StrCat("doc-", id));
  }
  for (size_t id = 45000; id < 90000; id += 7) {
    removed.push_back(absl::StrCat("doc-", id));
  }
  RemoveTerms(*_writer, kIdField, removed);
  const auto inputs = Snapshot();
  ASSERT_EQ(3, inputs.size());
  uint64_t live = 0;
  for (const auto* input : inputs) {
    live += input->live_docs_count();
  }
  ASSERT_LT(DEFAULT_ROW_GROUP_SIZE, live);
  const irs::SortSpec spec{{{.column = kBigint}}};
  for (const size_t threads : {1, 4}) {
    SCOPED_TRACE(threads);
    irs::MemoryDirectory out;
    auto plan = MakePlan(out, inputs, spec);
    ASSERT_TRUE(plan.Prepare(threads));
    ASSERT_TRUE(irs::RunSerially(plan));
    EXPECT_EQ("", MergedDifference(out, inputs, spec));
  }
}

TEST_F(SortedMergeTest, EveryCancellationStopsTheMerge) {
  AddSegment(Keys(0, 2, 9000), true, true, false);
  AddSegment(Keys(1, 2, 9000), true, true, false);
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  size_t refused = 0;
  size_t stopped = 0;
  for (size_t fail_at = 1;; ++fail_at) {
    SCOPED_TRACE(fail_at);
    irs::MemoryDirectory out;
    size_t calls = 0;
    auto plan = MakePlan(out, inputs, spec, [&] { return ++calls < fail_at; });
    if (!plan.Prepare(4)) {
      ++refused;
      continue;
    }
    if (irs::RunSerially(plan)) {
      break;
    }
    ++stopped;
  }
  EXPECT_LE(1, refused);
  EXPECT_LT(2, stopped);
}

TEST_F(SortedMergeTest, DocMapsThatCannotBeChargedRefuseTheMerge) {
  AddSegment(Keys(0, 2, 10));
  AddSegment(Keys(1, 2, 10));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  SimpleMemoryAccounter refusing;
  refusing.result = false;
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto* input : inputs) {
    doc_map.emplace_back(input, refusing);
  }
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,
                      doc_map,
                      meta,
                      nullptr,
                      irs::DuckDBEngine::Instance().instance(),
                      &_field_options,
                      {},
                      nullptr,
                      &spec};
  EXPECT_FALSE(plan.Prepare(1));
}

TEST_F(SortedMergeTest, AnOrderThatCannotBeChargedRefusesTheMerge) {
  AddSegment(Keys(0, 2, 10));
  AddSegment(Keys(1, 2, 10));
  const auto inputs = Snapshot();
  const irs::SortSpec spec{{{.column = kBigint}}};
  SimpleMemoryAccounter refusing;
  irs::MergeDocMap doc_map{{refusing}};
  for (const auto* input : inputs) {
    doc_map.emplace_back(input, irs::IResourceManager::gNoop);
  }
  refusing.result = false;
  irs::MemoryDirectory out;
  irs::SegmentMeta meta;
  irs::MergePlan plan{out,
                      doc_map,
                      meta,
                      nullptr,
                      irs::DuckDBEngine::Instance().instance(),
                      &_field_options,
                      {},
                      nullptr,
                      &spec};
  EXPECT_FALSE(plan.Prepare(1));
}

TEST_F(SortedMergeTest, InputsWithoutAColumnStoreSortAsNulls) {
  AddSegment(Keys(0, 2, 300), true);
  AddSegment(Keys(1, 2, 300), true);
  const auto inputs = Snapshot();
  const WithoutColumnStore storeless{*inputs[1]};
  const irs::SortSpec spec{{{.column = kBigint, .nulls_first = true}}};
  for (const auto& mixed :
       {std::vector<const irs::SubReader*>{inputs[0], &storeless},
        std::vector<const irs::SubReader*>{&storeless, inputs[0]}}) {
    irs::MemoryDirectory out;
    auto plan = MakePlan(out, mixed, spec);
    ASSERT_TRUE(plan.Prepare(1));
    ASSERT_TRUE(irs::RunSerially(plan));
    EXPECT_EQ("", MergedDifference(out, mixed, spec));
  }
}

TEST_F(SortedMergeTest, MoreInputsThanTheFanInAreRefused) {
  AddSegment(Keys(0, 1, 3));
  const auto inputs = Snapshot();
  const std::vector<const irs::SubReader*> many(irs::kSortedMergeInputsMax + 1,
                                                inputs.front());
  const irs::SortSpec spec{{{.column = kBigint}}};
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, many, spec);
  EXPECT_FALSE(plan.Prepare(1));
}

constexpr irs::field_id kVector = 10;
constexpr int kVectorDim = 8;

irs::ColumnOptions IvfColumns(irs::field_id id) {
  irs::ColumnOptions out;
  if (id == kVector) {
    out.ann_info = irs::AnnInfo{
      .kind = irs::AnnKind::Ivf,
      .centroids_id = id,
      .postings_id = id,
      .d = kVectorDim,
      .metric = irs::VectorMetric::L2Sqr,
      .quant = {.kind = irs::VectorQuantization::SQ8},
      .sample_factor = 0.5f,
      .posting_size = 64,
    };
  }
  return out;
}

TEST(SortedMergeAnnTest, IvfClustersCoverTheSortedDocs) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, DEFAULT_ROW_GROUP_SIZE, IvfColumns);
  const auto type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kVectorDim);
  std::mt19937_64 rng{17};
  std::uniform_real_distribution<float> coord{-1.f, 1.f};
  size_t next = 0;
  constexpr int64_t kDocs = 600;
  for (const int64_t parity : {0, 1}) {
    auto trx = writer->GetBatch();
    for (int64_t i = 0; i != kDocs; ++i) {
      auto doc = trx.Insert();
      tests::StringField id{"id", absl::StrCat("doc-", next++)};
      id.id = kIdField;
      tests::InsertField(doc, id);
      WriteValue(doc, kBigint, duckdb::LogicalType::BIGINT,
                 duckdb::Value::BIGINT((kDocs - i) * 2 + parity));
      duckdb::vector<duckdb::Value> coords;
      for (int j = 0; j != kVectorDim; ++j) {
        coords.push_back(duckdb::Value::FLOAT(coord(rng)));
      }
      WriteValue(
        doc, kVector, type,
        duckdb::Value::ARRAY(duckdb::LogicalType::FLOAT, std::move(coords)));
    }
    trx.Commit();
    ASSERT_TRUE(writer->RefreshCommit());
  }
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());

  const irs::SortSpec spec{{{.column = kBigint, .descending = true}}};
  irs::MemoryDirectory out;
  bool flushed = false;
  const auto merged = SortedMerge(out, inputs, &spec, DEFAULT_ROW_GROUP_SIZE,
                                  IvfColumns, flushed);
  ASSERT_TRUE(flushed);
  ASSERT_NE(nullptr, merged);
  const auto* ann = merged->Ann(kVector);
  ASSERT_NE(nullptr, ann);
  EXPECT_EQ(irs::AnnKind::Ivf, ann->Kind());

  auto expected = ExpectedSortedMerge(inputs, spec);
  auto actual = Dump(*merged);
  std::vector<irs::doc_id_t> clustered;
  for (const auto& field : actual.fields) {
    if (field.id != kVector) {
      continue;
    }
    for (const auto& term : field.terms) {
      for (const auto& posting : term.postings) {
        clustered.push_back(posting.doc);
      }
    }
  }
  std::ranges::sort(clustered);
  std::vector<irs::doc_id_t> all(2 * kDocs);
  std::iota(all.begin(), all.end(), irs::doc_limits::min());
  EXPECT_EQ(all, clustered);
  for (auto* dump : {&expected, &actual}) {
    std::erase_if(dump->fields,
                  [](const DumpField& field) { return field.id == kVector; });
  }
  EXPECT_EQ("", FirstDifference(expected, actual));
}

TEST(SortedMergeBytesTest, PostingsAndScoreBoundsMatchASortedRebuild) {
  const irs::BM25 bm25;
  for (const uint64_t seed : {11, 12, 13}) {
    SCOPED_TRACE(seed);
    Generator gen{seed};
    const irs::SortSpec spec{
      {{.column = kText}, {.column = kBigint, .descending = true}}};
    SegmentShape shape;
    shape.docs = 300;
    shape.null_ratio = 0.2;
    shape.columns = {0, 1, 2, 3, 4};
    shape.mixed = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                  irs::IndexFeatures::Offs;
    irs::MemoryDirectory dir;
    auto writer = MakeWriter(dir, DEFAULT_ROW_GROUP_SIZE, PlainColumns);
    std::vector<GeneratedDoc> all;
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      auto docs = GenerateDocs(gen, shape, next_id);
      SortDocs(docs, shape, spec);
      InsertDocs(*writer, shape, docs);
      all.insert(all.end(), docs.begin(), docs.end());
    }
    SortDocs(all, shape, spec);
    irs::MemoryDirectory rebuilt_dir;
    auto rebuilt_writer =
      MakeWriter(rebuilt_dir, DEFAULT_ROW_GROUP_SIZE, PlainColumns);
    InsertDocs(*rebuilt_writer, shape, all);

    const auto reader = writer->GetSnapshot();
    const auto inputs = Segments(reader);
    ASSERT_EQ(4, inputs.size());
    const auto rebuilt_reader = rebuilt_writer->GetSnapshot();
    const auto rebuilt = Segments(rebuilt_reader);
    ASSERT_EQ(1, rebuilt.size());

    const auto bytes = [&](std::span<const irs::SubReader* const> segments,
                           const irs::SortSpec* sort) {
      irs::MemoryDirectory out;
      bool flushed = false;
      irs::SegmentMeta meta;
      SortedMerge(out, segments, sort, DEFAULT_ROW_GROUP_SIZE, PlainColumns,
                  flushed, nullptr, &bm25, &meta);
      EXPECT_TRUE(flushed);
      std::vector<std::string> files;
      for (const auto& file : meta.files) {
        if (!file.ends_with(".col")) {
          files.push_back(file);
        }
      }
      std::ranges::sort(files);
      std::string content;
      for (const auto& file : files) {
        auto in = out.open(file, irs::IOAdvice::NORMAL);
        EXPECT_NE(nullptr, in) << file;
        const auto at = content.size();
        content.resize(at + in->Length());
        in->ReadData(reinterpret_cast<duckdb::data_ptr_t>(content.data() + at),
                     in->Length());
      }
      return content;
    };
    EXPECT_EQ(bytes(rebuilt, nullptr), bytes(inputs, &spec));
  }
}

}  // namespace
