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
#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <iresearch/formats/flush_state.hpp>
#include <iresearch/formats/index/term_dict_reader.hpp>
#include <iresearch/formats/index/term_parts.hpp>
#include <iresearch/formats/posting/block_index.hpp>
#include <iresearch/formats/posting/writer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <string_view>
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

absl::flat_hash_map<irs::field_id, size_t> TermRangeParts(
  const irs::MergePlan& plan) {
  absl::flat_hash_map<irs::field_id, size_t> parts;
  for (const auto& unit : plan.Units()) {
    if (unit.kind == irs::MergeUnitKind::TermRange) {
      ++parts[unit.id];
    }
  }
  return parts;
}

std::vector<irs::doc_id_t> Docs(size_t count, irs::doc_id_t step) {
  std::vector<irs::doc_id_t> docs;
  for (auto doc = irs::doc_limits::min(); docs.size() != count; doc += step) {
    docs.push_back(doc);
  }
  return docs;
}

std::vector<std::string> PostingsFiles(const irs::Directory& dir) {
  std::vector<std::string> files;
  dir.visit([&](std::string_view name) {
    if (name.ends_with(".doc") || name.ends_with(".pos") ||
        name.ends_with(".pay")) {
      files.emplace_back(name);
    }
    return true;
  });
  return files;
}

class NoPositionsDirectory final : public tests::DirectoryMock {
 public:
  using tests::DirectoryMock::DirectoryMock;

  irs::IndexOutput::ptr create(std::string_view name) final {
    if (name.ends_with(".pos")) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("cannot create ", name));
    }
    return tests::DirectoryMock::create(name);
  }
};

TEST(PostingsAppendTest, AppendedSkipIndexesReadBack) {
  constexpr auto kFeatures = irs::IndexFeatures::Freq;
  irs::FieldProperties props;
  props.index_features = kFeatures;
  const auto write_part = [&](irs::Directory& dir,
                              std::span<const irs::doc_id_t> docs) {
    irs::PostingsWriter writer;
    const irs::FlushState state{.dir = &dir,
                                .name = "part",
                                .doc_count = 4000,
                                .index_features = kFeatures};
    writer.Prepare(state);
    writer.BeginField(props);
    VectorPostings postings{docs};
    irs::PostingMeta meta;
    writer.Write(postings, meta);
    writer.EndField();
    writer.FinishPart();
    return meta;
  };
  const auto long_docs = Docs(1000, 3);
  irs::MemoryDirectory part_dir;
  const auto part_meta = write_part(part_dir, long_docs);
  ASSERT_LT(irs::doc_limits::kBlockSize, part_meta.docs_count);
  const auto part_doc = part_dir.open("part.doc", irs::IOAdvice::SEQUENTIAL);
  ASSERT_NE(nullptr, part_doc);
  for (size_t lead = 20; lead != 140; ++lead) {
    SCOPED_TRACE(lead);
    irs::MemoryDirectory lead_dir;
    write_part(lead_dir, Docs(lead, 7));
    const auto lead_doc = lead_dir.open("part.doc", irs::IOAdvice::SEQUENTIAL);
    ASSERT_NE(nullptr, lead_doc);
    irs::MemoryDirectory dir;
    uint64_t base = 0;
    {
      const irs::FlushState created{.dir = &dir,
                                    .name = "final",
                                    .doc_count = 4000,
                                    .index_features = kFeatures};
      irs::PostingsOutputs outputs;
      irs::PostingsWriter::PrepareOutputs(outputs, created);
      irs::AppendPostings(outputs, lead_doc.get(), nullptr, nullptr);
      base = irs::AppendPostings(outputs, part_doc.get(), nullptr, nullptr).doc;
      irs::PostingsWriter writer;
      const irs::FlushState adopted{.dir = &dir,
                                    .name = "final",
                                    .doc_count = 4000,
                                    .index_features = kFeatures};
      writer.Prepare(adopted, std::move(outputs));
      EXPECT_EQ(nullptr, outputs.doc);
      writer.BeginField(props);
      writer.EndField();
      writer.End();
    }
    EXPECT_EQ(0, irs::BlockIndex::Pad(base));
    auto meta = part_meta;
    meta.doc_start += base;
    auto in = dir.open("final.doc", irs::IOAdvice::RANDOM);
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

class TailPayloads final : public irs::TermPayloadWriter {
 public:
  void WriteTermPayload(irs::IndexOutput&,
                        std::span<const irs::doc_id_t>) final {}

  void Finish(irs::IndexOutput& out) final {
    constexpr irs::byte_type kTail[3]{};
    out.WriteData(kTail, std::size(kTail));
  }

  uint32_t Alignment() const noexcept final { return 8; }
};

TEST(PostingsAppendTest, AFieldWithoutPayloadWriterKeepsThePayStreamUnpadded) {
  const auto pay_length = [](bool then_terms) {
    irs::MemoryDirectory dir;
    irs::PostingsWriter writer;
    const irs::FlushState state{
      .dir = &dir,
      .name = "final",
      .doc_count = 16,
      .index_features = irs::IndexFeatures::Freq | irs::IndexFeatures::Vec,
    };
    writer.Prepare(state);
    TailPayloads payloads;
    irs::FieldProperties vectors;
    vectors.index_features = irs::IndexFeatures::Vec;
    writer.BeginField(vectors, &payloads);
    writer.EndField();
    if (then_terms) {
      irs::FieldProperties terms;
      terms.index_features = irs::IndexFeatures::Freq;
      writer.BeginField(terms);
      writer.EndField();
    }
    writer.End();
    uint64_t length = 0;
    EXPECT_TRUE(dir.length(length, "final.pay"));
    return length;
  };
  EXPECT_EQ(pay_length(false), pay_length(true));
}

TEST(PostingsAppendTest, PrepareCreatesOnlyTheOutputsItIsNotHanded) {
  irs::MemoryDirectory dir;
  const auto features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
                        irs::IndexFeatures::Offs;
  irs::PostingsOutputs outputs{.doc = dir.create("final.doc")};
  constexpr irs::byte_type kLead[]{1, 2, 3};
  outputs.doc->WriteData(kLead, std::size(kLead));
  {
    irs::PostingsWriter writer;
    writer.Prepare(
      {
        .dir = &dir,
        .name = "final",
        .doc_count = 16,
        .index_features = features,
      },
      std::move(outputs));
    EXPECT_EQ(nullptr, outputs.doc);
    irs::FieldProperties props;
    props.index_features = features;
    writer.BeginField(props);
    writer.EndField();
    writer.End();
  }
  for (const std::string_view name : {"final.pos", "final.pay"}) {
    bool exists = false;
    EXPECT_TRUE(dir.exists(exists, name));
    EXPECT_TRUE(exists) << name;
  }
  const auto doc = dir.open("final.doc", irs::IOAdvice::SEQUENTIAL);
  ASSERT_NE(nullptr, doc);
  irs::byte_type lead[std::size(kLead)];
  doc->ReadData(lead, std::size(lead));
  EXPECT_TRUE(absl::c_equal(kLead, lead));
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
  void Open(std::vector<irs::SortKey> sort = {}) {
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
                          const std::vector<irs::SortKey>* spec,
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
    SketchedColumns,
    irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE,
    {}};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
};

class TermRangeMergeOracleTest
  : public TermRangeMergeTest,
    public ::testing::WithParamInterface<std::tuple<bool, size_t, bool>> {};

TEST_P(TermRangeMergeOracleTest, PartsMatchTheOracle) {
  const auto [sorted, threads, shuffled] = GetParam();
  const std::vector<irs::SortKey> spec{{{.column = kKey}}};
  Open(sorted ? spec : std::vector<irs::SortKey>{});
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
  const auto parts = TermRangeParts(plan);
  ASSERT_FALSE(parts.empty());
  for (const auto& [field, count] : parts) {
    EXPECT_LT(1, count) << field;
  }
  const ShuffledClaims shuffle{shuffled};
  ASSERT_TRUE(RunPlan(plan, threads));
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
  EXPECT_TRUE(TermRangeParts(plan).empty());
  ASSERT_TRUE(RunPlan(plan));
  EXPECT_EQ("", MergedDifference(out, ExpectedMerge(inputs)));
}

TEST_F(TermRangeMergeTest, SerialRunsMergeEveryPart) {
  Open();
  AddSegments(4);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, nullptr);
  ASSERT_TRUE(plan.Prepare(4));
  ASSERT_FALSE(TermRangeParts(plan).empty());
  ASSERT_TRUE(RunPlan(plan));
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
    const bool ok = RunPlan(plan, 4);
    EXPECT_EQ(calls.load() <= stop_after, ok);
  }
}

TEST_F(TermRangeMergeTest, PartsAppendInPartOrderWhicheverFinishesFirst) {
  Open();
  AddSegments(12);
  const auto inputs = Snapshot();
  const auto expected = ExpectedMerge(inputs);
  auto doc_map = irs::MakeMergeDocMap(inputs, irs::IResourceManager::gNoop);
  irs::MemoryDirectory out;
  irs::MergePlan plan{out,
                      doc_map,
                      _meta,
                      nullptr,
                      irs::DuckDBEngine::Instance().instance(),
                      &_field_options,
                      {},
                      nullptr};
  ASSERT_TRUE(plan.Prepare(8));
  const auto units = plan.Units();
  std::vector<size_t> firsts;
  std::vector<size_t> rest;
  for (size_t unit = units.size(); unit-- != 0;) {
    if (units[unit].kind == irs::MergeUnitKind::TermRange) {
      (units[unit].part == 0 ? firsts : rest).push_back(unit);
    }
  }
  ASSERT_FALSE(rest.empty());
  for (const auto unit : rest) {
    ASSERT_TRUE(plan.RunUnit(unit));
  }
  for (const auto unit : firsts) {
    ASSERT_TRUE(plan.RunUnit(unit));
  }
  for (size_t unit = 0; unit != units.size(); ++unit) {
    if (units[unit].kind != irs::MergeUnitKind::TermRange) {
      ASSERT_TRUE(plan.RunUnit(unit));
    }
  }
  EXPECT_EQ("", MergedDifference(out, expected));
}

TEST_F(TermRangeMergeTest, AnAbortedMergeLeavesNoPostingsFiles) {
  Open();
  AddSegments(4);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  {
    auto plan = MakePlan(out, inputs, nullptr);
    ASSERT_TRUE(plan.Prepare(4));
    EXPECT_FALSE(PostingsFiles(out).empty());
    const auto units = plan.Units();
    std::vector<size_t> later;
    for (size_t unit = 0; unit != units.size(); ++unit) {
      if (units[unit].kind != irs::MergeUnitKind::TermRange) {
        continue;
      }
      if (units[unit].part == 0) {
        ASSERT_TRUE(plan.RunUnit(unit));
      } else {
        later.push_back(unit);
      }
    }
    ASSERT_FALSE(later.empty());
    plan.Abort();
    EXPECT_FALSE(plan.RunUnit(later.front()));
  }
  EXPECT_EQ(std::vector<std::string>{}, PostingsFiles(out));
}

TEST_F(TermRangeMergeTest, EmptyThinAndLongTermPartsMatchTheOracle) {
  constexpr size_t kSegments = 4;
  constexpr size_t kDocs = 12500;
  constexpr size_t kKeys = kSegments * kDocs;
  const auto value = [](size_t key) {
    auto value = Key("k", key);
    if (key % 997 == 0) {
      value.append(5000, static_cast<char>('a' + key % 26));
    }
    return value;
  };
  Open();
  for (size_t s = 0; s != kSegments; ++s) {
    auto batch = _writer->GetBatch();
    for (size_t n = 0; n != kDocs; ++n) {
      auto doc = batch.Insert();
      tests::StringField id{"id", value(n * kSegments + s)};
      id.id = kIdField;
      tests::InsertField(doc, id);
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  std::vector<std::string> removed;
  for (size_t key = kKeys / 4; key != kKeys * 3 / 4; ++key) {
    if (key < kKeys / 2 || key % 500 != 0) {
      removed.emplace_back(value(key));
    }
  }
  RemoveTerms(*_writer, kIdField, removed);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, nullptr);
  ASSERT_TRUE(plan.Prepare(8));
  EXPECT_LE(8, TermRangeParts(plan)[kIdField]);
  ASSERT_TRUE(RunPlan(plan, 8));
  EXPECT_EQ("", MergedDifference(out, ExpectedMerge(inputs)));
}

TEST_F(TermRangeMergeTest, ARangedFieldLeftWithOneTermMatchesTheOracle) {
  Open();
  for (size_t s = 0; s != 2; ++s) {
    auto batch = _writer->GetBatch();
    for (size_t n = 0; n != 5000; ++n) {
      auto doc = batch.Insert();
      tests::StringField id{"id", Key("k", n)};
      id.id = kIdField;
      tests::InsertField(doc, id);
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  std::vector<std::string> removed;
  for (size_t n = 0; n != 5000; ++n) {
    if (n != 2500) {
      removed.push_back(Key("k", n));
    }
  }
  RemoveTerms(*_writer, kIdField, removed);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  auto plan = MakePlan(out, inputs, nullptr);
  ASSERT_TRUE(plan.Prepare(4));
  EXPECT_LE(2, TermRangeParts(plan)[kIdField]);
  ASSERT_TRUE(RunPlan(plan, 4));
  EXPECT_EQ("", MergedDifference(out, ExpectedMerge(inputs)));
}

TEST_F(TermRangeMergeTest, RangedNumbersMatchASerialMerge) {
  constexpr irs::field_id kNumberField = 6;
  constexpr size_t kSegments = 4;
  constexpr size_t kDocs = 12500;
  const auto number = [](size_t s, size_t n) -> int64_t {
    if (n % 2 == 0) {
      return 0;
    }
    return static_cast<int64_t>((n * kSegments + s) * 7919 % 1000003) - 500000;
  };
  Open();
  for (size_t s = 0; s != kSegments; ++s) {
    auto batch = _writer->GetBatch();
    for (size_t n = 0; n != kDocs; ++n) {
      auto doc = batch.Insert();
      tests::StringField id{"id", Key(absl::StrCat("s", s, "-"), n)};
      id.id = kIdField;
      tests::InsertField(doc, id);
      tests::LongField value;
      value.name = "number";
      value.id = kNumberField;
      value.value(number(s, n));
      tests::InsertField(doc, value);
    }
    batch.Commit();
    ASSERT_TRUE(_writer->RefreshCommit());
  }
  std::vector<std::string> removed;
  for (size_t n = 0; n < kDocs; n += 5) {
    removed.emplace_back(Key("s2-", n));
  }
  RemoveTerms(*_writer, kIdField, removed);
  const auto inputs = Snapshot();
  const auto merge = [&](irs::Directory& out, size_t threads) {
    auto plan = MakePlan(out, inputs, nullptr);
    EXPECT_TRUE(plan.Prepare(threads));
    const auto parts = TermRangeParts(plan)[kNumberField];
    EXPECT_TRUE(RunPlan(plan, threads));
    return std::pair{parts, _meta};
  };
  irs::MemoryDirectory serial_dir;
  irs::MemoryDirectory parallel_dir;
  const auto [serial_parts, serial_meta] = merge(serial_dir, 1);
  const auto [parallel_parts, parallel_meta] = merge(parallel_dir, 8);
  EXPECT_EQ(0, serial_parts);
  EXPECT_LE(2, parallel_parts);
  const auto serial = irs::SegmentReaderImpl::Open(
    serial_dir, serial_meta, irs::tests::DefaultReaderOptions());
  const auto parallel = irs::SegmentReaderImpl::Open(
    parallel_dir, parallel_meta, irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, serial);
  ASSERT_NE(nullptr, parallel);
  EXPECT_EQ("", FirstDifference(Dump(*serial), Dump(*parallel)));
  const auto numbers = [](const irs::SubReader& segment) {
    const irs::TermReader* found = nullptr;
    for (const auto* terms : segment.fields(kNumberField)) {
      if (terms->meta().dict.member == irs::DictMember::Numbers) {
        found = terms;
      }
    }
    return found;
  };
  const auto* serial_numbers = numbers(*serial);
  const auto* parallel_numbers = numbers(*parallel);
  ASSERT_NE(nullptr, serial_numbers);
  ASSERT_NE(nullptr, parallel_numbers);
  EXPECT_FALSE(
    static_cast<const irs::term_dict::TermReaderImpl&>(*parallel_numbers)
      .RangeLevels()
      .empty());
  const auto range_docs = [](const irs::TermReader& terms, irs::bytes_view from,
                             irs::bytes_view to) {
    std::vector<irs::doc_id_t> docs;
    terms.VisitRange(from, to,
                     [&](irs::bytes_view, const irs::PostingMeta& meta) {
                       auto it = tests::MakeSeekPostings(
                         meta, terms.Handles(), terms.meta().index_features,
                         irs::IndexFeatures::None, terms.HasScoreBounds());
                       for (auto doc = it->Next(); !irs::doc_limits::eof(doc);
                            doc = it->Next()) {
                         docs.push_back(doc);
                       }
                     });
    absl::c_sort(docs);
    return docs;
  };
  std::mt19937 rng{7};
  irs::byte_type lower[irs::numeric_utils::kNumericTermMaxSize];
  irs::byte_type upper[irs::numeric_utils::kNumericTermMaxSize];
  for (size_t i = 0; i != 200; ++i) {
    const auto lo = static_cast<int64_t>(rng() % 1200000) - 600000;
    const auto hi = lo + static_cast<int64_t>(rng() % 400000);
    const auto from = i % 10 == 0
                        ? irs::bytes_view{}
                        : irs::numeric_utils::EncodeNumericTerm(lower, lo);
    const auto to = i % 10 == 1
                      ? irs::bytes_view{}
                      : irs::numeric_utils::EncodeNumericTerm(upper, hi);
    SCOPED_TRACE(absl::StrCat(lo, " ", hi));
    EXPECT_EQ(range_docs(*serial_numbers, from, to),
              range_docs(*parallel_numbers, from, to));
  }
}

TEST_F(TermRangeMergeTest, PostingsFilesThatCannotBeCreatedFailThePlan) {
  Open();
  AddSegments(4);
  const auto inputs = Snapshot();
  irs::MemoryDirectory out;
  NoPositionsDirectory failing{out};
  {
    auto plan = MakePlan(failing, inputs, nullptr);
    EXPECT_THROW(plan.Prepare(4), irs::SqlException);
  }
  EXPECT_EQ(std::vector<std::string>{}, PostingsFiles(out));
}

}  // namespace
