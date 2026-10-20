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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <iresearch/formats/flush_state.hpp>
#include <iresearch/formats/index/term_parts.hpp>
#include <iresearch/formats/posting/block_index.hpp>
#include <iresearch/formats/posting/writer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/index/merge/term_ranges.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "formats/formats_test_case_base.hpp"
#include "index/doc_generator.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

constexpr irs::field_id kKey = kFirstColumn;

std::string Key(std::string_view prefix, size_t n) {
  return absl::StrCat(prefix, absl::Dec(n, absl::kZeroPad6));
}

size_t ExpectSeeksFindEveryDoc(const irs::SubReader& segment,
                               irs::field_id field) {
  const auto* terms = segment.field(field);
  EXPECT_NE(nullptr, terms);
  if (terms == nullptr) {
    return 0;
  }
  const auto features = terms->meta().index_features;
  size_t checked = 0;
  for (auto it = terms->iterator(); it->next();) {
    const auto meta = it->cookie();
    if (meta.docs_count <= irs::doc_limits::kBlockSize) {
      continue;
    }
    SCOPED_TRACE(irs::ViewCast<char>(it->value()));
    std::vector<irs::doc_id_t> docs;
    auto all = tests::MakeSeekPostings(meta, terms->Handles(), features,
                                       irs::IndexFeatures::None,
                                       terms->HasScoreBounds());
    for (auto doc = all->Next(); !irs::doc_limits::eof(doc);
         doc = all->Next()) {
      docs.push_back(doc);
    }
    EXPECT_EQ(meta.docs_count, docs.size());
    irs::BlockCursor cursor;
    cursor.Arm(meta, {.pos = irs::IndexFeatures::None !=
                             (features & irs::IndexFeatures::Pos),
                      .offs = irs::IndexFeatures::None !=
                              (features & irs::IndexFeatures::Offs),
                      .bounds = terms->HasScoreBounds()});
    auto in = terms->Handles().doc->Reopen();
    const auto& index = cursor.Loaded(*in);
    EXPECT_EQ(irs::BlockIndex::Blocks(meta.docs_count), index.Size());
    for (uint32_t k = 0; k < index.Size(); ++k) {
      const auto last = std::min<size_t>(
        (k + 1) * size_t{irs::doc_limits::kBlockSize}, docs.size());
      EXPECT_EQ(docs[last - 1], index.Last(k)) << "block " << k;
    }
    auto seeker = tests::MakeSeekPostings(meta, terms->Handles(), features,
                                          irs::IndexFeatures::None,
                                          terms->HasScoreBounds());
    for (size_t i = 0; i < docs.size(); i += 97) {
      EXPECT_EQ(docs[i], seeker->Seek(docs[i]));
    }
    EXPECT_TRUE(irs::doc_limits::eof(seeker->Seek(docs.back() + 1)));
    ++checked;
  }
  return checked;
}

class VectorPostings final : public irs::TermPostings {
 public:
  explicit VectorPostings(std::span<const irs::doc_id_t> docs) noexcept
    : _docs{docs} {}

  uint32_t NextDocs(irs::doc_id_t* docs, uint32_t* freqs) final {
    const auto n = static_cast<uint32_t>(
      std::min<size_t>(irs::doc_limits::kBlockSize, _docs.size() - _at));
    for (uint32_t i = 0; i != n; ++i) {
      docs[i] = _docs[_at + i];
      freqs[i] = 1;
    }
    _at += n;
    return n;
  }

 private:
  std::span<const irs::doc_id_t> _docs;
  size_t _at = 0;
};

std::vector<irs::doc_id_t> Docs(size_t count, irs::doc_id_t step) {
  std::vector<irs::doc_id_t> docs;
  for (auto doc = irs::doc_limits::min(); docs.size() != count; doc += step) {
    docs.push_back(doc);
  }
  return docs;
}

TEST(PostingsAppendTest, AppendedSkipIndexesReadBack) {
  constexpr auto kFeatures = irs::IndexFeatures::Freq;
  irs::FieldProperties props;
  props.index_features = kFeatures;
  const auto long_docs = Docs(1000, 3);
  irs::MemoryDirectory part_dir;
  irs::PostingMeta part_meta;
  {
    irs::PostingsWriter writer;
    const irs::FlushState state{.dir = &part_dir,
                                .name = "part",
                                .doc_count = 4000,
                                .index_features = kFeatures};
    writer.Prepare(state);
    writer.BeginField(props);
    VectorPostings postings{long_docs};
    writer.Write(postings, part_meta);
    writer.EndField();
    writer.FinishPart();
  }
  ASSERT_LT(irs::doc_limits::kBlockSize, part_meta.docs_count);
  const auto part_doc = part_dir.open("part.doc", irs::IOAdvice::NORMAL);
  ASSERT_NE(nullptr, part_doc);
  for (size_t lead = 20; lead != 140; ++lead) {
    SCOPED_TRACE(lead);
    irs::MemoryDirectory dir;
    uint64_t base = 0;
    {
      irs::PostingsWriter writer;
      const irs::FlushState state{.dir = &dir,
                                  .name = "final",
                                  .doc_count = 4000,
                                  .index_features = kFeatures};
      writer.Prepare(state);
      writer.BeginField(props);
      const auto lead_docs = Docs(lead, 7);
      VectorPostings lead_postings{lead_docs};
      irs::PostingMeta lead_meta;
      writer.Write(lead_postings, lead_meta);
      base = writer.Append(part_doc.get(), nullptr, nullptr).doc;
      writer.EndField();
      writer.End();
    }
    EXPECT_EQ(0, irs::BlockIndex::Pad(base));
    auto meta = part_meta;
    meta.doc_start += base;
    auto in = dir.open("final.doc", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, in);
    irs::BlockCursor cursor;
    cursor.Arm(meta, irs::BlockIndexShapeOf(kFeatures, false));
    const auto& index = cursor.Loaded(*in);
    ASSERT_EQ(irs::BlockIndex::Blocks(meta.docs_count), index.Size());
    for (uint32_t k = 0; k != index.Size(); ++k) {
      const auto last = std::min<size_t>(
        (k + 1) * size_t{irs::doc_limits::kBlockSize}, long_docs.size());
      EXPECT_EQ(long_docs[last - 1], index.Last(k)) << "block " << k;
    }
  }
}

class TermPartsTest : public ::testing::TestWithParam<irs::IndexFeatures> {};

TEST_P(TermPartsTest, StreamsReadBackWhatTheyWrote) {
  const auto features = GetParam();
  const bool has_freq =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Freq);
  const bool has_pos =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Pos);
  const bool has_offs =
    irs::IndexFeatures::None != (features & irs::IndexFeatures::Offs);
  std::mt19937_64 rng{7};
  std::vector<std::string> terms;
  for (size_t n = 0; n != 3000; ++n) {
    std::string term(rng() % 20, '\x00');
    for (auto& c : term) {
      c = static_cast<char>(rng() % 4 == 0 ? rng() % 3 : 'a' + rng() % 26);
    }
    terms.emplace_back(std::move(term));
  }
  absl::c_sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

  std::vector<irs::PostingMeta> metas;
  std::vector<irs::bstring> inlines;
  inlines.reserve(terms.size());
  irs::TermPartWriter writer{features};
  uint64_t doc = 0;
  uint64_t pos = 0;
  uint64_t pay = 0;
  uint64_t doc_freq = 0;
  uint64_t term_freq = 0;
  for (const auto& term : terms) {
    auto& meta = metas.emplace_back();
    meta.docs_count = 1 + static_cast<uint32_t>(rng() % 500);
    meta.freq =
      has_freq ? meta.docs_count + static_cast<uint32_t>(rng() % 9) : 0;
    meta.doc_delta = static_cast<uint32_t>(rng());
    auto& bytes =
      inlines.emplace_back(rng() % 3 == 0 ? rng() % 33 : 0, irs::byte_type{0});
    for (auto& b : bytes) {
      b = static_cast<irs::byte_type>(rng());
    }
    meta.inline_size = static_cast<uint8_t>(bytes.size());
    meta.inline_data = bytes.empty() ? nullptr : bytes.data();
    meta.doc_start = doc += rng() % 1000;
    if (has_pos) {
      meta.pos_start = pos += rng() % 1000;
      meta.pos_offset = static_cast<uint16_t>(rng() % 4096);
      meta.pos_extent = static_cast<uint32_t>(rng() % 100000);
      if (has_offs) {
        meta.pay_start = pay += rng() % 1000;
        meta.pay_extent = static_cast<uint32_t>(rng() % 100000);
      }
    }
    doc_freq += meta.docs_count;
    term_freq += meta.freq;
    writer.Add(irs::ViewCast<irs::byte_type>(std::string_view{term}), meta);
  }
  EXPECT_EQ(terms.size(), writer.Count());
  EXPECT_EQ(doc_freq, writer.TotalDocFreq());
  EXPECT_EQ(term_freq, writer.TotalTermFreq());

  irs::TermPartReader reader{writer.Terms(), features};
  for (size_t i = 0; i != terms.size(); ++i) {
    SCOPED_TRACE(i);
    ASSERT_TRUE(reader.Next());
    EXPECT_EQ(terms[i], irs::ViewCast<char>(reader.Term()));
    const auto& expected = metas[i];
    const auto& actual = reader.Meta();
    EXPECT_EQ(expected.docs_count, actual.docs_count);
    EXPECT_EQ(expected.freq, actual.freq);
    EXPECT_EQ(expected.doc_delta, actual.doc_delta);
    EXPECT_EQ(expected.doc_start, actual.doc_start);
    EXPECT_EQ(expected.Inline(), actual.Inline());
    EXPECT_EQ(expected.pos_start, actual.pos_start);
    EXPECT_EQ(expected.pos_offset, actual.pos_offset);
    EXPECT_EQ(expected.pos_extent, actual.pos_extent);
    EXPECT_EQ(expected.pay_start, actual.pay_start);
    EXPECT_EQ(expected.pay_extent, actual.pay_extent);
  }
  EXPECT_FALSE(reader.Next());
}

INSTANTIATE_TEST_SUITE_P(
  features, TermPartsTest,
  ::testing::Values(irs::IndexFeatures::None, irs::IndexFeatures::Freq,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos,
                    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                      irs::IndexFeatures::Offs));

class TermRangeMergeTest : public ::testing::Test {
 protected:
  void Open(irs::SortSpec sort = {}) {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = SketchedColumns;
    options.sort = std::move(sort);
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  void AddSegments(size_t segments) {
    for (size_t s = 0; s != segments; ++s) {
      auto batch = _writer->GetBatch();
      for (size_t n = 0; n != 2500; ++n) {
        auto doc = batch.Insert();
        tests::StringField id{"id", Key(absl::StrCat("s", s, "-"), n)};
        id.id = kIdField;
        tests::InsertField(doc, id);
        tests::StringField tag{"tag", Key("tag", s * 700 + n * (s % 3 + 1))};
        tag.id = kTagField;
        tests::InsertField(doc, tag);
        const auto rare = (n * 31 + s) % 4000;
        tests::TextField<std::string> text{
          "text",
          absl::StrCat(
            "all word", n % 7, " word", n % 3, " ",
            std::string(1, static_cast<char>('a' + rare % 26)), rare, " ",
            std::string(1, static_cast<char>('a' + n % 26)), "x", n % 3, " ",
            std::string(1, static_cast<char>('a' + n % 26)), "m", n % 300 / 26),
          false, irs::IndexFeatures::Norm};
        text.id = kTextField;
        tests::InsertField(doc, text);
        WriteValue(doc, kKey, duckdb::LogicalType::BIGINT,
                   duckdb::Value::BIGINT(
                     static_cast<int64_t>((n * 7919 + s * 104729) % 1000003)));
      }
      batch.Commit();
      ASSERT_TRUE(_writer->RefreshCommit());
    }
    std::vector<std::string> removed;
    for (size_t n = 0; n < 2500; n += 3) {
      removed.emplace_back(Key("s1-", n));
    }
    RemoveTerms(*_writer, kIdField, removed);
  }

  std::vector<const irs::SubReader*> Snapshot() {
    _reader = _writer->GetSnapshot();
    return Segments(_reader);
  }

  irs::MergePlan MakePlan(irs::Directory& out,
                          std::span<const irs::SubReader* const> inputs,
                          const irs::SortSpec* spec,
                          const irs::MergeProgress& progress = {},
                          irs::ScorerPtr scorer = nullptr) {
    _doc_map.clear();
    for (const auto* input : inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
    _meta = {};
    return irs::MergePlan{out,
                          _doc_map,
                          _meta,
                          scorer,
                          irs::DuckDBEngine::Instance().instance(),
                          &_field_options,
                          progress,
                          nullptr,
                          spec};
  }

  std::string MergedDifference(irs::Directory& out,
                               const tests::merge::SegmentDump& expected) {
    const auto merged = irs::SegmentReaderImpl::Open(
      out, _meta, irs::tests::DefaultReaderOptions());
    if (merged == nullptr) {
      return "merged segment did not open";
    }
    return FirstDifference(expected, Dump(*merged));
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  irs::FunctionFieldOptions _field_options{
    SketchedColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
};

class TermRangeMergeOracleTest
  : public TermRangeMergeTest,
    public ::testing::WithParamInterface<std::tuple<bool, size_t, bool>> {};

TEST_P(TermRangeMergeOracleTest, PartsMatchTheOracle) {
  const auto [sorted, threads, shuffled] = GetParam();
  const irs::SortSpec spec{{{.column = kKey}}};
  Open(sorted ? spec : irs::SortSpec{});
  AddSegments(12);
  const auto inputs = Snapshot();
  ASSERT_EQ(12, inputs.size());
  SCOPED_TRACE(absl::StrCat(sorted ? "sorted" : "unsorted", ", ", threads,
                            " threads, ", shuffled ? "shuffled" : "scheduled"));
  const auto expected =
    sorted ? ExpectedSortedMerge(inputs, spec) : ExpectedMerge(inputs);
  irs::MemoryDirectory out;
  const irs::BM25 bm25;
  auto plan = MakePlan(out, inputs, sorted ? &spec : nullptr, {}, &bm25);
  ASSERT_TRUE(plan.Prepare(threads));
  ASSERT_TRUE(plan.PostingsPending());
  ASSERT_FALSE(plan.TermRanges().empty());
  for (const auto& range : plan.TermRanges()) {
    EXPECT_LT(1, range->PartCount());
  }
  ThreadedMergeExecutor scheduled{threads};
  ShuffledMergeExecutor random{threads, 42};
  ASSERT_TRUE(shuffled ? random.Run(plan) : scheduled.Run(plan));
  EXPECT_FALSE(plan.PostingsPending());
  EXPECT_EQ("", MergedDifference(out, expected));
  const auto merged = irs::SegmentReaderImpl::Open(
    out, _meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, merged);
  const auto* text = merged->field(kTextField);
  ASSERT_NE(nullptr, text);
  EXPECT_TRUE(text->HasScoreBounds());
  EXPECT_LT(0, ExpectSeeksFindEveryDoc(*merged, kTextField));
}

INSTANTIATE_TEST_SUITE_P(executors, TermRangeMergeOracleTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(2, 8),
                                            ::testing::Bool()));

TEST_F(TermRangeMergeTest, OneThreadMergesTermsWithoutParts) {
  Open();
  AddSegments(3);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, nullptr);
  ASSERT_TRUE(plan.Prepare(1));
  EXPECT_FALSE(plan.PostingsPending());
  EXPECT_TRUE(plan.TermRanges().empty());
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, ExpectedMerge(inputs)));
}

TEST_F(TermRangeMergeTest, SerialRunsMergeEveryPart) {
  Open();
  AddSegments(4);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, nullptr);
  ASSERT_TRUE(plan.Prepare(4));
  ASSERT_TRUE(plan.PostingsPending());
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out, ExpectedMerge(inputs)));
}

TEST_F(TermRangeMergeTest, StoppedPartsFailTheMerge) {
  Open();
  AddSegments(4);
  const auto inputs = Snapshot();
  for (const size_t stop_after : {1, 3, 10, 40}) {
    SCOPED_TRACE(stop_after);
    std::atomic<size_t> calls{0};
    irs::MemoryDirectory out;
    auto plan = MakePlan(out, inputs, nullptr,
                         [&] { return calls.fetch_add(1) < stop_after; });
    if (!plan.Prepare(4)) {
      continue;
    }
    ThreadedMergeExecutor executor{4};
    const bool ok = executor.Run(plan);
    EXPECT_EQ(calls.load() <= stop_after, ok);
  }
}

}  // namespace
