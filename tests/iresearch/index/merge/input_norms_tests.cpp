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

#include <array>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/norm_column_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/input_norms.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/norms_unit.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/scorers/score_function.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

struct InputShape {
  double delete_ratio;
  bool has_norm_field;
};

void WriteNormedDocs(irs::IndexWriter& writer,
                     std::span<const std::string> texts) {
  auto trx = writer.GetBatch();
  for (const auto& text : texts) {
    auto doc = trx.Insert();
    tests::TextField<std::string> normed{"normed", text, false,
                                         irs::IndexFeatures::Norm};
    normed.id = kNormField;
    tests::InsertField(doc, normed);
  }
  trx.Commit();
  ASSERT_TRUE(writer.RefreshCommit());
}

std::vector<std::string> RandomTexts(Generator& gen, size_t count,
                                     size_t max_words) {
  std::vector<std::string> texts;
  texts.reserve(count);
  for (size_t i = 0; i != count; ++i) {
    texts.push_back(gen.Text(max_words));
  }
  return texts;
}

irs::field_id NormColumnOf(const irs::SubReader& segment) {
  const auto* field = segment.field(kNormField);
  return field == nullptr ? irs::field_limits::invalid() : field->meta().norm;
}

class InputNormsTest : public ::testing::Test {
 protected:
  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = PlainColumns;
    options.row_group_size = kRegionRows;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    Generator gen{17};
    size_t next_id = 0;
    for (const auto& [delete_ratio, has_norm_field] :
         {InputShape{0.0, true}, InputShape{0.3, true}, InputShape{0.2, false},
          InputShape{0.0, true}, InputShape{0.5, true}}) {
      SegmentShape shape;
      shape.docs = 700;
      shape.delete_ratio = delete_ratio;
      shape.columns = {0};
      shape.has_norm_field = has_norm_field;
      WriteSegment(*_writer, gen, shape, next_id);
    }
    WriteNormedDocs(*_writer,
                    std::vector<std::string>(300, "alpha beta gamma"));
    auto escaped = RandomTexts(gen, 300, 5);
    escaped[10].clear();
    for (size_t i = 0; i != 300; ++i) {
      escaped[10] += i == 0 ? "alpha" : " alpha";
    }
    WriteNormedDocs(*_writer, escaped);
    _reader = _writer->GetSnapshot();
    _inputs = Segments(_reader);
    ASSERT_EQ(7, _inputs.size());

    auto meta = _reader.Meta().index_meta.segments[3].meta;
    irs::DocumentMask all;
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
      all.Add(doc);
    }
    meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
    meta.live_docs_count = 0;
    _deleted = irs::SegmentReaderImpl::Open(_dir, meta,
                                            irs::tests::DefaultReaderOptions());
    ASSERT_NE(nullptr, _deleted);

    for (size_t i = 0; i != _inputs.size(); ++i) {
      if (i == 2) {
        _doc_map.emplace_back(_deleted.get(), irs::IResourceManager::gNoop);
      }
      _doc_map.emplace_back(_inputs[i], irs::IResourceManager::gNoop);
    }

    irs::MergePlan plan{_out,
                        _doc_map,
                        _meta,
                        nullptr,
                        irs::DuckDBEngine::Instance().instance(),
                        &_field_options,
                        {},
                        nullptr};
    ASSERT_TRUE(plan.Prepare());
    ASSERT_TRUE(irs::RunSerially(plan));
    _merged = irs::SegmentReaderImpl::Open(_out, _meta,
                                           irs::tests::DefaultReaderOptions());
    ASSERT_NE(nullptr, _merged);
    const auto* field = _merged->field(kNormField);
    ASSERT_NE(nullptr, field);
    _column = field->meta().norm;
    ASSERT_TRUE(irs::field_limits::valid(_column));

    for (const auto& input : _doc_map) {
      _sources.push_back(irs::MergeSource{
        .reader = input.reader,
        .col_reader = input.reader->GetColReader(),
        .alive_count = input.reader->live_docs_count(),
        .remap = &input.remap,
      });
    }
  }

  const irs::NormColumnReader& MergedColumn() const {
    const auto* column = _merged->GetColReader()->NormColumn(_column);
    SDB_ASSERT(column != nullptr);
    return *column;
  }

  std::unique_ptr<irs::InputNormProvider> MakeProvider(
    irs::IResourceManager& memory = irs::IResourceManager::gNoop) const {
    auto provider = std::make_unique<irs::InputNormProvider>(_sources, memory);
    const auto& column = MergedColumn();
    provider->AddField(_column, kNormField, column.Sum(),
                       column.NonZeroCount());
    return provider;
  }

  irs::doc_id_t Docs() const {
    return static_cast<irs::doc_id_t>(_merged->docs_count());
  }

  static constexpr uint32_t kRegionRows = 256;

  irs::MemoryDirectory _dir;
  irs::MemoryDirectory _out;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<const irs::SubReader*> _inputs;
  std::shared_ptr<const irs::SegmentReaderImpl> _deleted;
  irs::FunctionFieldOptions _field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
  std::shared_ptr<const irs::SegmentReaderImpl> _merged;
  irs::field_id _column = irs::field_limits::invalid();
  std::vector<irs::MergeSource> _sources;
};

TEST_F(InputNormsTest, LiveStatsMatchTheMergedColumn) {
  const auto stats = irs::ComputeLiveNormStats(kNormField, _sources);
  EXPECT_TRUE(stats.present);
  EXPECT_EQ(MergedColumn().Sum(), stats.sum);
  EXPECT_EQ(MergedColumn().NonZeroCount(), stats.non_zero);
  EXPECT_FALSE(irs::ComputeLiveNormStats(kIdField, _sources).present);
}

TEST_F(InputNormsTest, EveryDocReadsTheMergedValue) {
  const auto provider = MakeProvider();
  auto expected = _merged->norms(_column);
  auto actual = provider->norms(_column);
  ASSERT_NE(nullptr, expected);
  ASSERT_NE(nullptr, actual);
  EXPECT_EQ(expected->GetAvg(), actual->GetAvg());
  size_t zeros = 0;
  for (auto doc = irs::doc_limits::min(); doc <= Docs(); ++doc) {
    const auto value = actual->Get(doc);
    EXPECT_EQ(expected->Get(doc), value) << doc;
    zeros += value == 0;
  }
  EXPECT_NE(0, zeros);
}

TEST_F(InputNormsTest, BlocksReadTheMergedValues) {
  const auto provider = MakeProvider();
  auto expected = _merged->norms(_column);
  auto actual = provider->norms(_column);
  std::array<irs::doc_id_t, irs::kPostingBlock> docs;
  std::array<uint32_t, irs::kPostingBlock> want;
  std::array<uint32_t, irs::kPostingBlock> got;
  for (auto first = irs::doc_limits::min();
       first + irs::kPostingBlock <= Docs() + 1; first += 61) {
    for (size_t i = 0; i != docs.size(); ++i) {
      docs[i] = static_cast<irs::doc_id_t>(first + i);
    }
    expected->GetPostingBlock(docs, want);
    actual->GetPostingBlock(docs, got);
    EXPECT_EQ(want, got) << first;
    const auto score_docs = std::span<const irs::doc_id_t, irs::kScoreBlock>{
      docs.data(), irs::kScoreBlock};
    expected->GetScoreBlock(score_docs, std::span<uint32_t, irs::kScoreBlock>{
                                          want.data(), irs::kScoreBlock});
    actual->GetScoreBlock(score_docs, std::span<uint32_t, irs::kScoreBlock>{
                                        got.data(), irs::kScoreBlock});
    EXPECT_EQ(want, got) << first;
  }
}

TEST_F(InputNormsTest, SparseRunsReadTheMergedValues) {
  const auto provider = MakeProvider();
  auto expected = _merged->norms(_column);
  auto actual = provider->norms(_column);
  std::mt19937_64 rng{5};
  std::vector<irs::doc_id_t> docs;
  std::vector<uint32_t> want;
  std::vector<uint32_t> got;
  for (size_t round = 0; round != 200; ++round) {
    docs.clear();
    const auto step = 1 + rng() % 97;
    for (auto doc = static_cast<irs::doc_id_t>(1 + rng() % Docs());
         doc <= Docs() && docs.size() < irs::kPostingBlock;
         doc += static_cast<irs::doc_id_t>(1 + rng() % step)) {
      docs.push_back(doc);
    }
    want.assign(docs.size(), 1);
    got.assign(docs.size(), 1);
    expected->Get(docs, want);
    actual->Get(docs, got);
    EXPECT_EQ(want, got) << round;
  }
  actual->Get(std::span<const irs::doc_id_t>{}, std::span<uint32_t>{});
}

TEST_F(InputNormsTest, RunsJumpingOverInputsReadTheMergedValues) {
  const auto provider = MakeProvider();
  auto expected = _merged->norms(_column);
  auto actual = provider->norms(_column);
  const std::array<irs::doc_id_t, 4> docs{irs::doc_limits::min(), Docs() / 3,
                                          2 * Docs() / 3, Docs()};
  std::array<uint32_t, 4> want{};
  std::array<uint32_t, 4> got{};
  expected->Get(docs, want);
  actual->Get(docs, got);
  EXPECT_EQ(want, got);
  EXPECT_EQ(expected->Get(irs::doc_limits::min()),
            actual->Get(irs::doc_limits::min()));
}

TEST_F(InputNormsTest, InputsCoverEveryRegionShape) {
  size_t multi_region = 0;
  bool constant = false;
  bool escaped = false;
  for (const auto* input : _inputs) {
    const auto id = NormColumnOf(*input);
    if (!irs::field_limits::valid(id)) {
      continue;
    }
    const auto* column = input->GetColReader()->NormColumn(id);
    ASSERT_NE(nullptr, column);
    multi_region += column->RegionCount() > 1;
    for (size_t r = 0; r != column->RegionCount(); ++r) {
      constant |= column->Region(r).bits == 0;
      escaped |= column->Region(r).exceptions;
    }
  }
  EXPECT_GE(multi_region, 4);
  EXPECT_TRUE(constant);
  EXPECT_TRUE(escaped);
}

TEST(InputNormsWideTest, FarApartPostingBlocksReadTheMergedValues) {
  irs::MemoryDirectory dir;
  auto options = irs::tests::DefaultWriterOptions();
  options.column_options = PlainColumns;
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
  Generator gen{29};
  constexpr size_t kDocs = 20000;
  for (size_t s = 0; s != 2; ++s) {
    WriteNormedDocs(*writer, RandomTexts(gen, kDocs, 20));
  }
  const auto reader = writer->GetSnapshot();
  const auto inputs = Segments(reader);
  ASSERT_EQ(2, inputs.size());
  irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
  for (const auto* input : inputs) {
    doc_map.emplace_back(input, irs::IResourceManager::gNoop);
  }
  irs::FunctionFieldOptions field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::SegmentMeta meta;
  irs::MemoryDirectory out;
  irs::MergePlan plan{out,
                      doc_map,
                      meta,
                      nullptr,
                      irs::DuckDBEngine::Instance().instance(),
                      &field_options,
                      {},
                      nullptr};
  ASSERT_TRUE(plan.Prepare());
  ASSERT_TRUE(irs::RunSerially(plan));
  const auto merged =
    irs::SegmentReaderImpl::Open(out, meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, merged);
  const auto column = NormColumnOf(*merged);
  ASSERT_TRUE(irs::field_limits::valid(column));

  std::vector<irs::MergeSource> sources;
  for (const auto& input : doc_map) {
    sources.push_back(irs::MergeSource{
      .reader = input.reader,
      .col_reader = input.reader->GetColReader(),
      .alive_count = input.reader->live_docs_count(),
      .remap = &input.remap,
    });
  }
  irs::InputNormProvider provider{sources, irs::IResourceManager::gNoop};
  const auto* merged_column = merged->GetColReader()->NormColumn(column);
  ASSERT_NE(nullptr, merged_column);
  provider.AddField(column, kNormField, merged_column->Sum(),
                    merged_column->NonZeroCount());
  auto expected = merged->norms(column);
  auto actual = provider.norms(column);
  std::array<irs::doc_id_t, irs::kPostingBlock> docs;
  std::array<uint32_t, irs::kPostingBlock> want;
  std::array<uint32_t, irs::kPostingBlock> got;
  for (const irs::doc_id_t step : {1, 70, 150}) {
    for (size_t k = 0; k != docs.size(); ++k) {
      docs[k] = static_cast<irs::doc_id_t>(irs::doc_limits::min() + k * step);
    }
    ASSERT_LE(docs.back(), merged->docs_count());
    expected->GetPostingBlock(docs, want);
    actual->GetPostingBlock(docs, got);
    EXPECT_EQ(want, got) << step;
  }
}

TEST_F(InputNormsTest, UnknownColumnHasNoNorms) {
  const auto provider = MakeProvider();
  EXPECT_EQ(nullptr, provider->norms(_column + 1000));
}

TEST_F(InputNormsTest, AverageOfOnlyZerosIsZero) {
  irs::InputNormProvider provider{_sources, irs::IResourceManager::gNoop};
  provider.AddField(_column, kNormField, 0, 0);
  auto reader = provider.norms(_column);
  ASSERT_NE(nullptr, reader);
  EXPECT_EQ(irs::score_t{}, reader->GetAvg());
}

TEST_F(InputNormsTest, DocMapsAreChargedOnlyOnceNormsAreRead) {
  MaxMemoryCounter memory;
  {
    const auto provider = MakeProvider(memory);
    EXPECT_EQ(0, memory.current);
    auto first = provider->norms(_column);
    const auto charged = memory.current;
    EXPECT_GE(charged,
              (_inputs[1]->live_docs_count() + _inputs[2]->live_docs_count() +
               _inputs[4]->live_docs_count()) *
                sizeof(irs::doc_id_t));
    auto second = provider->norms(_column);
    EXPECT_EQ(charged, memory.current);
  }
  EXPECT_EQ(0, memory.current);
}

}  // namespace
