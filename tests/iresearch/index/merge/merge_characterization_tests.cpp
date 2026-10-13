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
#include <duckdb/common/types/value.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/detail/ann_index.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <new>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using irs::IndexFeatures;
using namespace tests::merge;

struct NthIncreaseFails final : irs::IResourceManager {
  explicit NthIncreaseFails(size_t n) noexcept : fail_at{n} {}

  void Increase(size_t) final {
    if (++calls == fail_at) {
      throw std::bad_alloc{};
    }
  }

  void Decrease(size_t) noexcept final {}

  size_t fail_at;
  size_t calls = 0;
};

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

struct MergeResult {
  bool flushed = false;
  irs::SegmentMeta meta;
  std::shared_ptr<const irs::SegmentReaderImpl> reader;
};

MergeResult RunMerge(
  irs::Directory& out, std::span<const irs::SubReader* const> inputs,
  const irs::IndexFieldOptions* field_options,
  const irs::MergeWriter::FlushProgress& progress = {},
  irs::IResourceManager& memory = irs::IResourceManager::gNoop,
  irs::ScorerPtr scorer = nullptr, irs::MergeExecutor* executor = nullptr) {
  MergeResult result;
  const irs::SegmentWriterOptions options{
    .scorer = scorer,
    .resource_manager = memory,
    .db = &irs::DuckDBEngine::Instance().instance(),
    .field_options = field_options,
  };
  irs::MergeWriter merger{out, options};
  merger.Reset(inputs.begin(), inputs.end());
  result.flushed =
    irs::GetReady(merger.Flush(result.meta, progress, nullptr, executor));
  if (result.flushed) {
    result.reader = irs::SegmentReaderImpl::Open(
      out, result.meta, irs::tests::DefaultReaderOptions());
  }
  return result;
}

class MergeCharacterizationTest : public ::testing::Test {
 protected:
  static constexpr size_t kBigDocs = 16500;
  static constexpr size_t kSmallDocs = 300;
  static constexpr size_t kWordsTerms = 150;

  void SetUp() final {
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate,
                                     irs::tests::DefaultWriterOptions());
    Write(kBigDocs, 0);
    Write(kSmallDocs, kBigDocs);
    const std::vector<std::string> doomed{"w7"};
    RemoveTerms(*_writer, kTagField, doomed);
    _reader = _writer->GetSnapshot();
    ASSERT_EQ(2, _reader.size());
    _inputs = Segments(_reader);
  }

  void Write(size_t docs, size_t first) {
    auto trx = _writer->GetBatch();
    for (size_t i = 0; i != docs; ++i) {
      auto doc = trx.Insert();
      tests::StringField common{"common", "x"};
      common.id = kIdField;
      tests::InsertField(doc, common);
      tests::StringField words{"words",
                               absl::StrCat("w", (first + i) % kWordsTerms)};
      words.id = kTagField;
      tests::InsertField(doc, words);
      tests::TextField<std::string> text{
        "text", std::string{Words()[(first + i) % Words().size()]}, false,
        IndexFeatures::Norm};
      text.id = kNormField;
      tests::InsertField(doc, text);
      WriteValue(doc, kFirstColumn, duckdb::LogicalType::BIGINT,
                 duckdb::Value::BIGINT(static_cast<int64_t>(first + i)));
    }
    trx.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }

  bool Merge(irs::SegmentMeta& meta,
             const irs::MergeWriter::FlushProgress& progress,
             irs::IResourceManager& memory = irs::IResourceManager::gNoop,
             const irs::IndexFieldOptions* field_options = nullptr) {
    const irs::SegmentWriterOptions options{
      .resource_manager = memory,
      .db = &irs::DuckDBEngine::Instance().instance(),
      .field_options =
        field_options != nullptr ? field_options : &_field_options,
    };
    irs::MergeWriter merger{_out, options};
    merger.Reset(_inputs.begin(), _inputs.end());
    return irs::GetReady(merger.Flush(meta, progress));
  }

  irs::MemoryDirectory _dir;
  irs::MemoryDirectory _out;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<const irs::SubReader*> _inputs;
  irs::FunctionFieldOptions _field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
};

TEST_F(MergeCharacterizationTest, CompleteMergeMatchesTheInputs) {
  size_t calls = 0;
  irs::SegmentMeta meta;
  meta.name = "merged";
  ASSERT_TRUE(Merge(meta, [&] { return ++calls != 0; }));
  EXPECT_GT(calls, 6U);
  auto merged = irs::SegmentReaderImpl::Open(
    _out, meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, merged);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(_inputs), Dump(*merged)));
}

TEST_F(MergeCharacterizationTest, AbortsAtEveryProgressCall) {
  size_t total = 0;
  {
    irs::SegmentMeta meta;
    meta.name = "counted";
    ASSERT_TRUE(Merge(meta, [&] { return ++total != 0; }));
  }
  for (size_t stop_at = 1; stop_at <= total; ++stop_at) {
    SCOPED_TRACE(stop_at);
    size_t calls = 0;
    irs::SegmentMeta meta;
    meta.name = absl::StrCat("aborted", stop_at);
    EXPECT_FALSE(Merge(meta, [&] { return ++calls < stop_at; }));
    EXPECT_GE(calls, stop_at);
    EXPECT_TRUE(meta.name.empty());
    EXPECT_TRUE(meta.files.empty());
    EXPECT_EQ(0, meta.docs_count);
  }
}

TEST_F(MergeCharacterizationTest, MergeWithoutFieldOptionsDropsNorms) {
  irs::MemoryDirectory dir;
  const auto merged = RunMerge(dir, _inputs, nullptr);
  ASSERT_TRUE(merged.flushed);
  ASSERT_NE(nullptr, merged.reader);
  const auto* text = merged.reader->field(kNormField);
  ASSERT_NE(nullptr, text);
  EXPECT_TRUE(
    irs::IsSubsetOf(IndexFeatures::Norm, text->meta().index_features));
  EXPECT_FALSE(irs::field_limits::valid(text->meta().norm));

  auto expected = ExpectedMerge(_inputs);
  for (auto& field : expected.fields) {
    field.norms.clear();
  }
  EXPECT_EQ("", FirstDifference(expected, Dump(*merged.reader)));
}

TEST_F(MergeCharacterizationTest, ScoredMergeReadsTheMergedNorms) {
  const irs::BM25 bm25;
  irs::MemoryDirectory scored_dir;
  const auto scored = RunMerge(scored_dir, _inputs, &_field_options, {},
                               irs::IResourceManager::gNoop, &bm25);
  ASSERT_TRUE(scored.flushed);
  irs::MemoryDirectory unnormed_dir;
  const auto unnormed = RunMerge(unnormed_dir, _inputs, nullptr, {},
                                 irs::IResourceManager::gNoop, &bm25);
  ASSERT_TRUE(unnormed.flushed);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(_inputs), Dump(*scored.reader)));
  EXPECT_TRUE(scored.reader->field(kNormField)->HasScoreBounds());
}

TEST_F(MergeCharacterizationTest, InvalidNormColumnIdIsRefused) {
  irs::FunctionFieldOptions no_norm_ids{
    PlainColumns, [](irs::field_id) { return irs::field_limits::invalid(); },
    DEFAULT_ROW_GROUP_SIZE};
  irs::SegmentMeta meta;
  const auto merge = [&] {
    Merge(meta, {}, irs::IResourceManager::gNoop, &no_norm_ids);
  };
#ifdef SDB_DEV
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(merge(), "GetNormColumnId did not mint a valid id");
#else
  EXPECT_ANY_THROW(merge());
#endif
}

TEST_F(MergeCharacterizationTest, DocMapAllocationFailureFailsTheMerge) {
  bool doc_map_failed = false;
  for (size_t fail_at = 1; fail_at <= 8; ++fail_at) {
    SCOPED_TRACE(fail_at);
    NthIncreaseFails memory{fail_at};
    irs::SegmentMeta meta;
    meta.name = absl::StrCat("oom", fail_at);
    bool merged = false;
    try {
      merged = Merge(meta, {}, memory);
    } catch (const std::bad_alloc&) {
      continue;
    }
    if (!merged) {
      doc_map_failed = true;
      EXPECT_TRUE(meta.files.empty());
    }
  }
  EXPECT_TRUE(doc_map_failed);
}

TEST_F(MergeCharacterizationTest, FullyDeletedInputYieldsNoSegment) {
  auto meta = _reader.Meta().index_meta.segments.back().meta;
  irs::DocumentMask all;
  for (irs::doc_id_t doc = irs::doc_limits::min();
       doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
    all.Add(doc);
  }
  meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
  meta.live_docs_count = 0;
  auto deleted = irs::SegmentReaderImpl::Open(
    _dir, meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, deleted);
  ASSERT_EQ(0, deleted->live_docs_count());
  const irs::SubReader* inputs[] = {deleted.get()};
  irs::MemoryDirectory dir;
  const auto merged = RunMerge(dir, inputs, &_field_options);
  EXPECT_FALSE(merged.flushed);
  EXPECT_TRUE(merged.meta.files.empty());
}

irs::IndexWriter::ptr MakeWriter(irs::Directory& dir,
                                 irs::ColumnOptionsProvider columns) {
  auto options = irs::tests::DefaultWriterOptions();
  options.column_options = std::move(columns);
  return irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
}

TEST(MergeColumnsTest, InputWithoutColumnsIsPaddedWithNulls) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, SketchedColumns);
  Generator gen{77};
  size_t next_id = 0;
  SegmentShape bare;
  bare.docs = 120;
  bare.has_norm_field = false;
  WriteSegment(*writer, gen, bare, next_id);
  SegmentShape full = bare;
  full.docs = 300;
  full.null_ratio = 0.2;
  full.columns = {0, 1, 2, 3, 4, 5, 6};
  WriteSegment(*writer, gen, full, next_id);
  const auto reader = writer->GetSnapshot();
  ASSERT_EQ(2, reader.size());
  ASSERT_EQ(nullptr, reader[0].GetColReader()->Column(kFirstColumn));
  ASSERT_NE(nullptr, reader[1].GetColReader()->Column(kFirstColumn));
  const WithoutColumnStore storeless{reader[0]};

  irs::FunctionFieldOptions field_options{
    SketchedColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  const irs::SubReader* firsts[] = {&reader[0], &storeless};
  for (const auto* first : firsts) {
    const irs::SubReader* inputs[] = {first, &reader[1]};
    irs::MemoryDirectory out;
    const auto merged = RunMerge(out, inputs, &field_options);
    ASSERT_TRUE(merged.flushed);
    EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged.reader)));
  }
}

TEST(MergeNormsTest, InputsWithoutNormColumnsGiveZeroNorms) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  Generator gen{91};
  size_t next_id = 0;
  SegmentShape shape;
  shape.docs = 200;
  WriteSegment(*writer, gen, shape, next_id);
  WriteSegment(*writer, gen, shape, next_id);
  const auto reader = writer->GetSnapshot();
  ASSERT_EQ(2, reader.size());

  irs::MemoryDirectory first_dir;
  const irs::SubReader* first[] = {&reader[0]};
  const auto first_merged = RunMerge(first_dir, first, nullptr);
  ASSERT_TRUE(first_merged.flushed);
  irs::MemoryDirectory second_dir;
  const irs::SubReader* second[] = {&reader[1]};
  const auto second_merged = RunMerge(second_dir, second, nullptr);
  ASSERT_TRUE(second_merged.flushed);

  auto meta = reader.Meta().index_meta.segments.front().meta;
  irs::DocumentMask all;
  for (irs::doc_id_t doc = irs::doc_limits::min();
       doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
    all.Add(doc);
  }
  meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
  meta.live_docs_count = 0;
  const auto deleted =
    irs::SegmentReaderImpl::Open(dir, meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, deleted);
  const WithoutColumnStore storeless{reader[0]};
  const WithoutColumnStore storeless_deleted{*deleted};

  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  const std::vector<std::vector<const irs::SubReader*>> cases{
    {first_merged.reader.get(), &reader[1]},
    {first_merged.reader.get(), second_merged.reader.get()},
    {&storeless, &reader[1]},
    {&storeless_deleted, &reader[1]},
  };
  for (size_t c = 0; c != cases.size(); ++c) {
    irs::MemoryDirectory out;
    const auto merged = RunMerge(out, cases[c], &field_options);
    ASSERT_TRUE(merged.flushed) << "case " << c;
    EXPECT_EQ("",
              FirstDifference(ExpectedMerge(cases[c]), Dump(*merged.reader)))
      << "case " << c;
  }
}

TEST(MergeColumnsTest, SketchesStartAtTheMerge) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  Generator gen{78};
  size_t next_id = 0;
  for (const double delete_ratio : {0.0, 0.3}) {
    SegmentShape shape;
    shape.docs = 400;
    shape.delete_ratio = delete_ratio;
    shape.columns = {0, 2};
    WriteSegment(*writer, gen, shape, next_id);
  }
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());
  for (const auto* input : inputs) {
    ASSERT_EQ(nullptr,
              input->GetColReader()->Column(kFirstColumn)->HyperLogLog());
  }

  irs::FunctionFieldOptions field_options{
    SketchedColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options);
  ASSERT_TRUE(merged.flushed);
  const auto* cs = merged.reader->GetColReader();
  ASSERT_NE(nullptr, cs->Column(kFirstColumn)->HyperLogLog());
  ASSERT_NE(nullptr, cs->Column(kFirstColumn + 2)->HyperLogLog());
  EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged.reader)));
}

TEST(MergeColumnsTest, EachColumnEndsBeforeTheNextOneStarts) {
  constexpr uint32_t kRowGroup = 128;
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  Generator gen{79};
  size_t next_id = 0;
  SegmentShape shape;
  shape.docs = 150;
  shape.null_ratio = 0.1;
  shape.columns = {0, 1, 2, 3, 4};
  WriteSegment(*writer, gen, shape, next_id);
  WriteSegment(*writer, gen, shape, next_id);
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());

  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(), kRowGroup};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options);
  ASSERT_TRUE(merged.flushed);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged.reader)));

  std::vector<std::pair<uint64_t, irs::field_id>> blocks;
  for (const auto& column : merged.reader->GetColReader()->Columns()) {
    EXPECT_GE(column->DataBlocks().size(), 3U) << "column " << column->Id();
    for (const auto& block : column->DataBlocks()) {
      if (block.byte_size != 0) {
        blocks.emplace_back(block.file_offset, column->Id());
      }
    }
  }
  std::ranges::sort(blocks);
  std::vector<irs::field_id> finished;
  for (size_t i = 1; i < blocks.size(); ++i) {
    const auto previous = blocks[i - 1].second;
    const auto current = blocks[i].second;
    if (current == previous) {
      continue;
    }
    finished.emplace_back(previous);
    EXPECT_EQ(0, std::ranges::count(finished, current))
      << "column " << current << " has blocks after other columns";
  }
}

std::string LogLine(size_t g) {
  static constexpr std::string_view kVerbs[] = {
    "handled request", "rejected payment", "retried connection",
    "flushed cache",   "opened session",   "closed stream"};
  static constexpr std::string_view kServices[] = {
    "checkout", "frontend", "cart", "shipping", "currency", "ads"};
  std::mt19937_64 rng{g * 0x9E3779B97F4A7C15ULL + 7};
  return absl::StrCat("2026-10-05T12:", rng() % 60, " INFO ",
                      kServices[rng() % std::size(kServices)], " ",
                      kVerbs[rng() % std::size(kVerbs)], " id=", rng() % 99991,
                      " user=", rng() % 100000, " took ", rng() % 997, "ms");
}

TEST(MergeColumnsTest, DictionarySamplingSkipsInputsWithoutTheColumn) {
  constexpr irs::field_id kLog = kFirstColumn + 2;
  constexpr size_t kRows = 30000;
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  Generator gen{79};
  size_t next_id = 0;
  SegmentShape bare;
  bare.docs = 50;
  bare.has_norm_field = false;
  WriteSegment(*writer, gen, bare, next_id);
  for (size_t s = 0; s != 2; ++s) {
    auto trx = writer->GetBatch();
    for (size_t i = 0; i != kRows; ++i) {
      auto doc = trx.Insert();
      tests::StringField id_field{"id", absl::StrCat("log-", s, "-", i)};
      id_field.id = kIdField;
      tests::InsertField(doc, id_field);
      WriteValue(doc, kLog, duckdb::LogicalType::VARCHAR,
                 duckdb::Value{LogLine(s * kRows + i)});
    }
    trx.Commit();
    ASSERT_TRUE(writer->RefreshCommit());
  }
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(3, inputs.size());

  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options);
  ASSERT_TRUE(merged.flushed);
  const auto* cs = merged.reader->GetColReader();
  const auto* col = cs->Column(kLog);
  ASSERT_NE(nullptr, col);
  irs::ReadContext ctx{*cs};
  bool trained = false;
  for (size_t b = 0; b < col->DataBlocks().size() && !trained; ++b) {
    auto seg = col->OpenSegment(b, ctx);
    auto info = seg->GetCompressionFunction().get_segment_info(
      duckdb::QueryContext{}, *seg);
    trained = info["dictionary"] == "trained:1";
  }
  EXPECT_TRUE(trained);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged.reader)));
}

template<typename Value>
void AppendStringRows(irs::IndexWriter& writer, irs::field_id column,
                      size_t rows, Value&& value) {
  auto trx = writer.GetBatch();
  for (size_t begin = 0; begin < rows; begin += STANDARD_VECTOR_SIZE) {
    const auto count = std::min<size_t>(STANDARD_VECTOR_SIZE, rows - begin);
    duckdb::Vector chunk{duckdb::LogicalType::VARCHAR, count};
    auto* data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(chunk);
    uint64_t first_row = 0;
    irs::ColWriter* cols = nullptr;
    for (size_t i = 0; i != count; ++i) {
      auto doc = trx.Insert();
      if (i == 0) {
        first_row = doc.DocId() - irs::doc_limits::min();
        cols = doc.GetColWriter();
      }
      const auto text = value(begin + i);
      data[i] =
        duckdb::StringVector::AddString(chunk, text.data(), text.size());
    }
    duckdb::FlatVector::SetSize(chunk, count);
    cols->OpenColumn(column, duckdb::LogicalType::VARCHAR)
      .Append(first_row, chunk, count);
  }
  trx.Commit();
  ASSERT_TRUE(writer.RefreshCommit());
}

TEST(MergeColumnsTest, DictionarySamplingStopsAtTheWindowBytes) {
  constexpr irs::field_id kLog = kFirstColumn + 2;
  constexpr size_t kRows = 80000;
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  AppendStringRows(*writer, kLog, kRows, LogLine);
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(1, inputs.size());
  ASSERT_EQ(kRows, inputs.front()->docs_count());

  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options);
  ASSERT_TRUE(merged.flushed);
  EXPECT_EQ("", FirstDifference(ExpectedMerge(inputs), Dump(*merged.reader)));
}

TEST(MergeColumnsTest, DictionarySamplingStopsAtTheWindowVectors) {
  constexpr irs::field_id kLevel = kFirstColumn + 2;
  constexpr size_t kRows = 32 * 66 * STANDARD_VECTOR_SIZE;
  static constexpr std::string_view kLevels[] = {"debug", "info", "warn",
                                                 "error"};
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  AppendStringRows(*writer, kLevel, kRows, [](size_t row) {
    return kLevels[row % std::size(kLevels)];
  });
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(1, inputs.size());
  ASSERT_EQ(kRows, inputs.front()->docs_count());

  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options);
  ASSERT_TRUE(merged.flushed);
  ASSERT_EQ(kRows, merged.reader->docs_count());
  const auto* col = merged.reader->GetColReader()->Column(kLevel);
  ASSERT_NE(nullptr, col);
  EXPECT_EQ(kRows, col->RowCount());
}

constexpr irs::field_id kLowVector = 10;
constexpr irs::field_id kHighVector = 1000;
constexpr irs::field_id kWordField = 50;
constexpr int kVectorDim = 8;

irs::ColumnOptions IvfColumns(irs::field_id id) {
  irs::ColumnOptions out;
  if (id == kLowVector || id == kHighVector) {
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

void WriteVectors(irs::IndexWriter& writer, size_t docs, uint64_t seed) {
  std::mt19937_64 rng{seed};
  std::uniform_real_distribution<float> coord{-1.f, 1.f};
  const auto type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kVectorDim);
  auto trx = writer.GetBatch();
  for (size_t i = 0; i != docs; ++i) {
    auto doc = trx.Insert();
    tests::StringField word{"word", std::string{Words()[i % Words().size()]}};
    word.id = kWordField;
    tests::InsertField(doc, word);
    for (const auto id : {kLowVector, kHighVector}) {
      duckdb::vector<duckdb::Value> coords;
      for (int j = 0; j != kVectorDim; ++j) {
        coords.push_back(duckdb::Value::FLOAT(coord(rng)));
      }
      WriteValue(
        doc, id, type,
        duckdb::Value::ARRAY(duckdb::LogicalType::FLOAT, std::move(coords)));
    }
  }
  trx.Commit();
  ASSERT_TRUE(writer.RefreshCommit());
}

void ExpectIvfMerge(std::span<const irs::SubReader* const> inputs,
                    irs::MergeExecutor* executor) {
  irs::FunctionFieldOptions field_options{
    IvfColumns, irs::tests::MakeNormColumnIdProvider(), DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options, {},
                               irs::IResourceManager::gNoop, nullptr, executor);
  ASSERT_TRUE(merged.flushed);
  for (const auto id : {kLowVector, kHighVector}) {
    SCOPED_TRACE(id);
    ASSERT_NE(nullptr, merged.reader->field(id));
    const auto* ann = merged.reader->Ann(id);
    ASSERT_NE(nullptr, ann);
    EXPECT_EQ(irs::AnnKind::Ivf, ann->Kind());
  }
  auto expected = ExpectedMerge(inputs);
  auto actual = Dump(*merged.reader);
  const auto without_clusters = [](SegmentDump& dump) {
    std::erase_if(dump.fields, [](const DumpField& field) {
      return field.id == kLowVector || field.id == kHighVector;
    });
  };
  without_clusters(expected);
  without_clusters(actual);
  EXPECT_EQ("", FirstDifference(expected, actual));
}

void ExpectIvfMerge(std::span<const irs::SubReader* const> inputs) {
  {
    SCOPED_TRACE("serial");
    ExpectIvfMerge(inputs, nullptr);
  }
  {
    SCOPED_TRACE("scheduled");
    ThreadedMergeExecutor executor{4};
    ExpectIvfMerge(inputs, &executor);
  }
  for (const uint64_t seed : {1, 2}) {
    SCOPED_TRACE(absl::StrCat("shuffled ", seed));
    ShuffledMergeExecutor executor{4, seed};
    ExpectIvfMerge(inputs, &executor);
  }
}

TEST(MergeIvfTest, ClusterPostingsReplaceTheInputs) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, IvfColumns);
  WriteVectors(*writer, 700, 1);
  WriteVectors(*writer, 500, 2);
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());
  ASSERT_NE(nullptr, inputs.front()->field(kLowVector));
  ExpectIvfMerge(inputs);
}

TEST(MergeIvfTest, ClusterPostingsAreWrittenAroundTheOtherFields) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, PlainColumns);
  WriteVectors(*writer, 700, 3);
  WriteVectors(*writer, 500, 4);
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());
  ASSERT_EQ(nullptr, inputs.front()->field(kLowVector));
  ExpectIvfMerge(inputs);
}

TEST(MergeIvfTest, OnlyDeletedVectorsGiveNoSegment) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, IvfColumns);
  WriteVectors(*writer, 300, 7);
  WriteVectors(*writer, 200, 8);
  const auto reader = writer->GetSnapshot();
  std::vector<std::shared_ptr<const irs::SegmentReaderImpl>> deleted;
  std::vector<const irs::SubReader*> inputs;
  for (const auto& segment : reader.Meta().index_meta.segments) {
    auto meta = segment.meta;
    irs::DocumentMask all;
    for (auto doc = irs::doc_limits::min();
         doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
      all.Add(doc);
    }
    meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
    meta.live_docs_count = 0;
    deleted.push_back(irs::SegmentReaderImpl::Open(
      dir, meta, irs::tests::DefaultReaderOptions()));
    ASSERT_NE(nullptr, deleted.back());
    inputs.push_back(deleted.back().get());
  }
  ASSERT_EQ(2, inputs.size());
  irs::FunctionFieldOptions field_options{
    IvfColumns, irs::tests::MakeNormColumnIdProvider(), DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  EXPECT_FALSE(RunMerge(out, inputs, &field_options).flushed);
}

TEST(MergeIvfTest, ClusterFieldsWithoutIvfOptionsAreDropped) {
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir, IvfColumns);
  WriteVectors(*writer, 700, 5);
  WriteVectors(*writer, 500, 6);
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());
  ASSERT_NE(nullptr, inputs.front()->field(kLowVector));

  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MemoryDirectory out;
  const auto merged = RunMerge(out, inputs, &field_options);
  ASSERT_TRUE(merged.flushed);
  for (const auto id : {kLowVector, kHighVector}) {
    SCOPED_TRACE(id);
    EXPECT_EQ(nullptr, merged.reader->field(id));
    EXPECT_EQ(nullptr, merged.reader->Ann(id));
    EXPECT_NE(nullptr, merged.reader->Column(id));
  }
  auto expected = ExpectedMerge(inputs);
  std::erase_if(expected.fields, [](const DumpField& field) {
    return field.id == kLowVector || field.id == kHighVector;
  });
  EXPECT_EQ("", FirstDifference(expected, Dump(*merged.reader)));
}

}  // namespace
