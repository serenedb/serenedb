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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <numeric>
#include <random>
#include <span>
#include <string>
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

constexpr uint32_t kSmallRowGroup = 32;
constexpr irs::field_id kBigintColumn = kFirstColumn;
constexpr irs::field_id kTextColumn = kFirstColumn + 2;
constexpr irs::field_id kListColumn = kFirstColumn + 5;
constexpr irs::field_id kStructColumn = kFirstColumn + 6;

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

irs::ColumnOptions UncompressedText(irs::field_id id) {
  return {.compression = id == kTextColumn
                           ? duckdb::CompressionType::COMPRESSION_UNCOMPRESSED
                           : duckdb::CompressionType::COMPRESSION_AUTO};
}

std::vector<size_t> UnitsOf(std::span<const irs::MergeUnit> units,
                            MergeUnitKind kind, irs::field_id id) {
  std::vector<size_t> out;
  for (size_t unit = 0; unit != units.size(); ++unit) {
    if (units[unit].kind == kind && units[unit].id == id) {
      out.push_back(unit);
    }
  }
  return out;
}

class MergePlanTest : public ::testing::Test {
 protected:
  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = PlainColumns;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    Generator gen{91};
    size_t next_id = 0;
    for (const double delete_ratio : {0.0, 0.25, 0.0}) {
      SegmentShape shape;
      shape.docs = 250;
      shape.delete_ratio = delete_ratio;
      shape.null_ratio = 0.1;
      shape.columns = {0, 1, 2, 3, 4, 5, 6};
      WriteSegment(*_writer, gen, shape, next_id);
    }
    _reader = _writer->GetSnapshot();
    _inputs = Segments(_reader);
    ASSERT_EQ(3, _inputs.size());
    for (const auto* input : _inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
  }

  irs::MergePlan MakePlan(irs::Directory& out,
                          const irs::IndexFieldOptions* options = nullptr,
                          const irs::MergeProgress& progress = {}) {
    return irs::MergePlan{out,
                          _doc_map,
                          _meta,
                          nullptr,
                          irs::DuckDBEngine::Instance().instance(),
                          options != nullptr ? options : &_field_options,
                          progress,
                          nullptr};
  }

  void ResetMerge() {
    _meta = {};
    _doc_map.clear();
    for (const auto* input : _inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
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
  irs::FunctionFieldOptions _field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::FunctionFieldOptions _small_row_groups{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(), kSmallRowGroup};
  irs::FunctionFieldOptions _uncompressed_text{
    UncompressedText, irs::tests::MakeNormColumnIdProvider(), kSmallRowGroup};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
};

TEST_F(MergePlanTest, UnitsComeInDependencyOrder) {
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  ASSERT_TRUE(plan.Prepare());
  std::vector<MergeUnitKind> kinds;
  std::vector<irs::field_id> norms;
  for (const auto& unit : plan.Units()) {
    kinds.push_back(unit.kind);
    if (unit.kind == MergeUnitKind::Norms) {
      norms.push_back(unit.id);
    }
  }
  EXPECT_TRUE(std::ranges::is_sorted(kinds));
  EXPECT_FALSE(norms.empty());
  EXPECT_TRUE(std::ranges::is_sorted(norms));
  EXPECT_EQ(7, std::ranges::count(kinds, MergeUnitKind::Column));
  EXPECT_EQ(1, std::ranges::count(kinds, MergeUnitKind::ColumnsFooter));
  EXPECT_EQ(0, std::ranges::count(kinds, MergeUnitKind::Ann));
  EXPECT_EQ(1, std::ranges::count(kinds, MergeUnitKind::Postings));
  EXPECT_EQ(1, std::ranges::count(kinds, MergeUnitKind::Index));
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergePlanTest, OnlyDeletedDocsGiveNoSegment) {
  std::vector<std::shared_ptr<const irs::SegmentReaderImpl>> deleted;
  _doc_map.clear();
  for (size_t i = 0; i < 2; ++i) {
    auto meta = _reader.Meta().index_meta.segments[i].meta;
    irs::DocumentMask all;
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
      all.Add(doc);
    }
    meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
    meta.live_docs_count = 0;
    deleted.push_back(irs::SegmentReaderImpl::Open(
      _dir, meta, irs::tests::DefaultReaderOptions()));
    ASSERT_NE(nullptr, deleted.back());
    _doc_map.emplace_back(deleted.back().get(), irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  ASSERT_TRUE(plan.Prepare());
  EXPECT_EQ(0, _meta.docs_count);
  EXPECT_FALSE(irs::RunSerially(plan));
  EXPECT_GT(plan.Profile().Nanos(irs::MergeStep::Postings), 0);
  EXPECT_GT(plan.Profile().Nanos(irs::MergeStep::Commit), 0);
}

class OversizedSegment final : public irs::SubReader {
 public:
  explicit OversizedSegment(uint64_t docs) noexcept {
    _meta.docs_count = docs;
    _meta.live_docs_count = docs;
  }

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  irs::lead::Node::ptr docs_iterator() const final { return {}; }

  std::span<const irs::field_id> field_ids() const final { return {}; }

  const irs::TermReader* field(irs::field_id) const final { return nullptr; }

  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

TEST_F(MergePlanTest, MergeUpToTheDocIdLimitIsPlanned) {
  const OversizedSegment most{irs::kMergeDocsMax - 1};
  const OversizedSegment one{1};
  _doc_map.clear();
  _doc_map.emplace_back(&most, irs::IResourceManager::gNoop);
  _doc_map.emplace_back(&one, irs::IResourceManager::gNoop);
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  ASSERT_TRUE(plan.Prepare());
  EXPECT_EQ(irs::kMergeDocsMax, _meta.docs_count);
  EXPECT_FALSE(plan.Units().empty());
}

TEST_F(MergePlanTest, MergePastTheDocIdLimitIsRefused) {
  const OversizedSegment most{irs::kMergeDocsMax};
  const OversizedSegment one{1};
  _doc_map.clear();
  _doc_map.emplace_back(&most, irs::IResourceManager::gNoop);
  _doc_map.emplace_back(&one, irs::IResourceManager::gNoop);
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  EXPECT_FALSE(plan.Prepare());
  EXPECT_TRUE(plan.Units().empty());
  EXPECT_EQ(0, _meta.docs_count);
  EXPECT_LT(irs::kMergeDocsMax + irs::doc_limits::min(),
            irs::doc_limits::eof());
}

TEST_F(MergePlanTest, NormAndColumnUnitsMergeInAnyOrder) {
  for (const uint64_t seed : {1, 2, 3, 4}) {
    SCOPED_TRACE(seed);
    ResetMerge();
    irs::MemoryDirectory out;
    auto plan = MakePlan(out);
    ASSERT_TRUE(plan.Prepare());
    const auto units = plan.Units();
    const auto footer = std::ranges::find(units, MergeUnitKind::ColumnsFooter,
                                          &irs::MergeUnit::kind);
    ASSERT_NE(units.end(), footer);
    std::vector<size_t> order(units.size());
    std::iota(order.begin(), order.end(), 0);
    std::mt19937_64 rng{seed};
    std::shuffle(order.begin(), order.begin() + (footer - units.begin()), rng);
    for (const auto unit : order) {
      ASSERT_TRUE(plan.RunUnit(unit)) << "unit " << unit;
    }
    EXPECT_EQ("", MergedDifference(out));
  }
}

TEST_F(MergePlanTest, OneThreadKeepsEveryColumnWhole) {
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, &_small_row_groups);
  ASSERT_TRUE(plan.Prepare());
  const auto units = plan.Units();
  EXPECT_EQ(
    7, std::ranges::count(units, MergeUnitKind::Column, &irs::MergeUnit::kind));
  EXPECT_EQ(0, std::ranges::count(units, MergeUnitKind::ColumnTrain,
                                  &irs::MergeUnit::kind));
}

TEST_F(MergePlanTest, BigFlatColumnsSplitIntoRowRanges) {
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, &_small_row_groups);
  ASSERT_TRUE(plan.Prepare(64));
  const auto units = plan.Units();
  const auto trains = UnitsOf(units, MergeUnitKind::ColumnTrain, kTextColumn);
  ASSERT_EQ(1, trains.size());
  const auto text = UnitsOf(units, MergeUnitKind::Column, kTextColumn);
  ASSERT_LT(1, text.size());
  for (uint32_t part = 0; part != text.size(); ++part) {
    EXPECT_EQ(part, units[text[part]].part);
    EXPECT_TRUE(std::ranges::equal(std::vector{trains[0]},
                                   plan.Dependencies(text[part])));
  }
  const auto numbers = UnitsOf(units, MergeUnitKind::Column, kBigintColumn);
  ASSERT_LT(1, numbers.size());
  EXPECT_TRUE(
    UnitsOf(units, MergeUnitKind::ColumnTrain, kBigintColumn).empty());
  for (const auto unit : numbers) {
    EXPECT_TRUE(std::ranges::equal(plan.Dependencies(trains[0]),
                                   plan.Dependencies(unit)));
  }
  for (const auto id : {kListColumn, kStructColumn}) {
    const auto whole = UnitsOf(units, MergeUnitKind::Column, id);
    ASSERT_EQ(1, whole.size());
    EXPECT_EQ(0, units[whole[0]].part);
  }
  const auto footer =
    static_cast<size_t>(std::ranges::find(units, MergeUnitKind::ColumnsFooter,
                                          &irs::MergeUnit::kind) -
                        units.begin());
  EXPECT_EQ(footer, plan.Dependencies(footer).size());
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergePlanTest, SplitUnitsMergeInAnyOrder) {
  for (const size_t threads : {2, 4, 64}) {
    for (const uint64_t seed : {1, 2}) {
      SCOPED_TRACE(absl::StrCat(threads, " threads, seed ", seed));
      ResetMerge();
      irs::MemoryDirectory out;
      auto plan = MakePlan(out, &_small_row_groups);
      ASSERT_TRUE(plan.Prepare(threads));
      ShuffledMergeExecutor executor{4, seed};
      ASSERT_TRUE(executor.Run(plan));
      EXPECT_EQ("", MergedDifference(out));
    }
  }
}

TEST_F(MergePlanTest, SplitTextWithoutATrainedCodecMerges) {
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, &_uncompressed_text);
  ASSERT_TRUE(plan.Prepare(64));
  const auto units = plan.Units();
  ASSERT_EQ(1, UnitsOf(units, MergeUnitKind::ColumnTrain, kTextColumn).size());
  ASSERT_LT(1, UnitsOf(units, MergeUnitKind::Column, kTextColumn).size());
  ShuffledMergeExecutor executor{4, 3};
  ASSERT_TRUE(executor.Run(plan));
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergePlanTest, CancelledSplitUnitsStopTheMerge) {
  for (size_t fail_at = 1;; ++fail_at) {
    SCOPED_TRACE(fail_at);
    ResetMerge();
    irs::MemoryDirectory out;
    size_t calls = 0;
    auto plan =
      MakePlan(out, &_small_row_groups, [&] { return ++calls < fail_at; });
    if (!plan.Prepare(64)) {
      continue;
    }
    ASSERT_FALSE(irs::RunSerially(plan));
    if (plan.Profile().Nanos(irs::MergeStep::ColumnsCommit) != 0) {
      break;
    }
  }
}

constexpr std::string_view kAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789";

std::string RandomId(std::mt19937_64& rng, size_t size) {
  std::string id;
  for (size_t i = 0; i != size; ++i) {
    id.push_back(kAlphabet[rng() % kAlphabet.size()]);
  }
  return id;
}

std::string LogLine(size_t row) {
  static constexpr std::string_view kVerbs[] = {
    "handled request", "rejected payment", "retried connection",
    "flushed cache",   "opened session",   "closed stream",
    "queued job",      "scheduled task"};
  static constexpr std::string_view kServices[] = {
    "checkout", "frontend", "cart", "shipping", "currency", "ads", "email"};
  std::mt19937_64 rng{row * 0x9E3779B97F4A7C15ULL + 7};
  const auto id = RandomId(rng, 16);
  return absl::StrCat("2026-10-05T12:", rng() % 60, " INFO ",
                      kServices[rng() % std::size(kServices)], " ",
                      kVerbs[rng() % std::size(kVerbs)], " id=", id,
                      " user=", rng() % 100000, " took ", rng() % 997, "ms");
}

std::vector<std::string> DictionariesByBlock(const irs::SubReader& segment,
                                             irs::field_id id) {
  std::vector<std::string> out;
  const auto* cs = segment.GetColReader();
  const auto* col = cs != nullptr ? cs->Column(id) : nullptr;
  if (col == nullptr) {
    return out;
  }
  irs::ReadContext ctx{*cs};
  for (size_t block = 0; block != col->DataBlocks().size(); ++block) {
    auto seg = col->OpenSegment(block, ctx);
    auto info = seg->GetCompressionFunction().get_segment_info(
      duckdb::QueryContext{}, *seg);
    out.push_back(info["dictionary"]);
  }
  return out;
}

class ColumnSplitTest : public ::testing::Test {
 protected:
  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = PlainColumns;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  template<typename Fill>
  void AddSegment(size_t docs, Fill&& fill) {
    auto batch = _writer->GetBatch();
    for (size_t i = 0; i != docs; ++i) {
      auto doc = batch.Insert();
      tests::StringField id{"id", absl::StrCat("doc-", _next++)};
      id.id = kIdField;
      tests::InsertField(doc, id);
      fill(doc, _next - 1);
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  std::shared_ptr<const irs::SegmentReaderImpl> Merge(
    irs::Directory& out, uint32_t row_group, size_t threads,
    std::span<const irs::MergeUnit>* units = nullptr) {
    TakeSnapshot();
    return MergeInputs(out, row_group, threads, units);
  }

  void TakeSnapshot() {
    _reader = _writer->GetSnapshot();
    _inputs = Segments(_reader);
  }

  std::shared_ptr<const irs::SegmentReaderImpl> MergeInputs(
    irs::Directory& out, uint32_t row_group, size_t threads,
    std::span<const irs::MergeUnit>* units) {
    _doc_map.clear();
    for (const auto* input : _inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
    _options = std::make_unique<irs::FunctionFieldOptions>(
      PlainColumns, irs::tests::MakeNormColumnIdProvider(), row_group);
    _plan = std::make_unique<irs::MergePlan>(
      out, _doc_map, _meta, nullptr, irs::DuckDBEngine::Instance().instance(),
      _options.get(), irs::MergeProgress{}, nullptr);
    EXPECT_TRUE(_plan->Prepare(threads));
    if (units != nullptr) {
      *units = _plan->Units();
    }
    ShuffledMergeExecutor executor{threads, 11};
    EXPECT_TRUE(executor.Run(*_plan));
    return irs::SegmentReaderImpl::Open(out, _meta,
                                        irs::tests::DefaultReaderOptions());
  }

  void ExpectOneSharedDictionary(size_t docs_per_segment, bool deletes) {
    constexpr irs::field_id kText = kFirstColumn;
    constexpr uint32_t kRowGroup = 4096;
    for (size_t s = 0; s != 2; ++s) {
      AddSegment(docs_per_segment, [&](auto& doc, size_t row) {
        WriteValue(doc, kText, duckdb::LogicalType::VARCHAR,
                   duckdb::Value{LogLine(row)});
      });
    }
    std::vector<std::string> removed;
    for (size_t row = 0; deletes && row < 2 * docs_per_segment; row += 10) {
      removed.emplace_back(absl::StrCat("doc-", row));
    }
    RemoveTerms(*_writer, kIdField, removed);
    irs::MemoryDirectory out;
    std::span<const irs::MergeUnit> units;
    const auto merged = Merge(out, kRowGroup, 4, &units);
    ASSERT_NE(nullptr, merged);
    ASSERT_EQ(1, UnitsOf(units, MergeUnitKind::ColumnTrain, kText).size());
    const auto parts = UnitsOf(units, MergeUnitKind::Column, kText).size();
    ASSERT_LT(1, parts);
    EXPECT_EQ("", FirstDifference(ExpectedMerge(_inputs), Dump(*merged)));
    const auto* col = merged->GetColReader()->Column(kText);
    ASSERT_NE(nullptr, col);
    const auto dictionaries = DictionariesByBlock(*merged, kText);
    ASSERT_EQ(col->DataBlocks().size(), dictionaries.size());
    const auto last_part = col->RowCount() - col->RowCount() / parts;
    bool first = false;
    bool last = false;
    for (size_t block = 0; block != dictionaries.size(); ++block) {
      const bool trained = dictionaries[block] == "trained:1";
      first |= trained && col->DataBlockFirstRow(block) < kRowGroup;
      last |= trained && col->DataBlockFirstRow(block) >= last_part;
    }
    EXPECT_TRUE(first);
    EXPECT_TRUE(last);
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  size_t _next = 0;
  irs::DirectoryReader _reader;
  std::vector<const irs::SubReader*> _inputs;
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
  std::unique_ptr<irs::FunctionFieldOptions> _options;
  std::unique_ptr<irs::MergePlan> _plan;
};

TEST_F(ColumnSplitTest, SplitTextColumnSharesOneTrainedDictionary) {
  ExpectOneSharedDictionary(30000, true);
}

TEST_F(ColumnSplitTest, SplitTextSamplesRowGroupsWhenWindowsFallShort) {
  ExpectOneSharedDictionary(17000, false);
}

TEST_F(ColumnSplitTest, SplitTextSamplesRowGroupsPastDeletedRows) {
  ExpectOneSharedDictionary(17000, true);
}

TEST_F(ColumnSplitTest, RangesPadInputsWithoutTheColumn) {
  constexpr irs::field_id kNumber = kFirstColumn;
  constexpr irs::field_id kText = kFirstColumn + 1;
  const auto number = [](auto& doc, size_t row) {
    WriteValue(doc, kNumber, duckdb::LogicalType::BIGINT,
               duckdb::Value::BIGINT(static_cast<int64_t>(row * 7)));
  };
  const auto text = [](auto& doc, size_t row) {
    WriteValue(doc, kText, duckdb::LogicalType::VARCHAR,
               duckdb::Value{absl::StrCat("value-", row)});
  };
  AddSegment(300, [&](auto& doc, size_t row) {
    number(doc, row);
    text(doc, row);
  });
  AddSegment(300, [](auto&, size_t) {});
  AddSegment(300, text);
  AddSegment(300, number);
  AddSegment(300, [&](auto& doc, size_t row) {
    number(doc, row);
    text(doc, row);
  });
  TakeSnapshot();
  const WithoutColumnStore storeless{*_inputs[4]};
  _inputs[4] = &storeless;
  irs::MemoryDirectory out;
  std::span<const irs::MergeUnit> units;
  const auto merged = MergeInputs(out, kSmallRowGroup, 16, &units);
  ASSERT_NE(nullptr, merged);
  EXPECT_LT(1, UnitsOf(units, MergeUnitKind::Column, kNumber).size());
  EXPECT_LT(1, UnitsOf(units, MergeUnitKind::Column, kText).size());
  EXPECT_EQ("", FirstDifference(ExpectedMerge(_inputs), Dump(*merged)));
}

TEST_F(ColumnSplitTest, ArrayColumnsStayWhole) {
  constexpr irs::field_id kVector = kFirstColumn;
  constexpr irs::field_id kNumber = kFirstColumn + 1;
  const auto vector_type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, 4);
  for (size_t s = 0; s != 2; ++s) {
    AddSegment(300, [&](auto& doc, size_t row) {
      const auto x = static_cast<float>(row);
      WriteValue(doc, kVector, vector_type,
                 duckdb::Value::ARRAY(
                   duckdb::LogicalType::FLOAT,
                   {duckdb::Value::FLOAT(x), duckdb::Value::FLOAT(x + 1),
                    duckdb::Value::FLOAT(x * 2), duckdb::Value::FLOAT(-x)}));
      WriteValue(doc, kNumber, duckdb::LogicalType::BIGINT,
                 duckdb::Value::BIGINT(static_cast<int64_t>(row * row)));
    });
  }
  irs::MemoryDirectory out;
  std::span<const irs::MergeUnit> units;
  const auto merged = Merge(out, kSmallRowGroup, 16, &units);
  ASSERT_NE(nullptr, merged);
  EXPECT_EQ(1, UnitsOf(units, MergeUnitKind::Column, kVector).size());
  EXPECT_LT(1, UnitsOf(units, MergeUnitKind::Column, kNumber).size());
  EXPECT_EQ("", FirstDifference(ExpectedMerge(_inputs), Dump(*merged)));
}

}  // namespace
