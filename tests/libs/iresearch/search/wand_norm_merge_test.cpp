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

// Norm + BM25 wand round-trip tests across all the corner cases the
// search-benchmark-game flagged. The minimal scenario passed; this file
// adds the shapes the bench is likely actually hitting:
//   * multi-row-group norm columns (small `norm_row_group_size`),
//   * RGs with mixed byte_size on the same column,
//   * mismatched per-source byte widths during compaction,
//   * removals before compaction (mask-filter on the norm merge),
//   * multiple norm-bearing fields in one segment.
//
// Each test exercises: write -> read-norm -> BM25 wand vs no-wand -> ...
// optional compact -> read-norm -> BM25 wand vs no-wand.

#include <gtest/gtest.h>

#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/utils/index_utils.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/search/cost.hpp"

namespace {

constexpr std::string_view kBody = "body";
constexpr std::string_view kBody2 = "body2";
constexpr std::string_view kId = "id";
constexpr std::string_view kTerm = "x";

// Emits `target_count` copies of "x" (the query term) followed by
// `filler_count` copies of "f" (filler). Tokenizer side: tf("x") =
// target_count. Norm side: stats.len = target_count + filler_count, so
// dl != tf and BM25 length normalisation has signal across the corpus.
class MixedAnalyzer : public irs::analysis::TypedAnalyzer<MixedAnalyzer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "WandNormMergeAnalyzer";
  }

  explicit MixedAnalyzer(size_t target_count, size_t filler_count = 0,
                         std::string_view target_term = kTerm,
                         std::string_view filler_term = "f")
    : _target_count{target_count},
      _filler_count{filler_count},
      _target_term{target_term},
      _filler_term{filler_term} {}

  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return irs::GetMutable(_attrs, id);
  }

  bool reset(std::string_view /*value*/) noexcept final {
    _i = 0;
    return true;
  }

  bool next() final {
    if (_i >= _target_count + _filler_count) {
      return false;
    }
    const auto t = (_i < _target_count) ? _target_term : _filler_term;
    std::get<irs::TermAttr>(_attrs).value = irs::ViewCast<irs::byte_type>(t);
    ++_i;
    return true;
  }

 private:
  std::tuple<irs::TermAttr, irs::IncAttr> _attrs;
  size_t _target_count;
  size_t _filler_count;
  std::string_view _target_term;
  std::string_view _filler_term;
  size_t _i{};
};

// Back-compat alias used by older tests where tf == dl.
using CountAnalyzer = MixedAnalyzer;

// Norm-bearing text field. Index features = Freq | Norm so BM25 has both
// signals.
class NormField final : public tests::Ifield {
 public:
  // Single-term form: tf == dl == count (legacy callers).
  NormField(std::string_view name, size_t count)
    : _name{name}, _value{kTerm}, _analyzer{count, 0} {}

  // Two-term form: tf("x") = target_count, dl = target_count + filler_count.
  NormField(std::string_view name, size_t target_count, size_t filler_count)
    : _name{name}, _value{kTerm}, _analyzer{target_count, filler_count} {}

  std::string_view Name() const final { return _name; }
  irs::Tokenizer& GetTokens() const final {
    _analyzer.reset(_value);
    return _analyzer;
  }
  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
  }
  bool Write(irs::DataOutput& out) const final {
    irs::WriteStr(out, _value);
    return true;
  }

 private:
  std::string _name;
  std::string _value;
  mutable MixedAnalyzer _analyzer;
};

// One-token-per-doc field used as a removal handle ("id" = "doc_N").
class IdField final : public tests::Ifield {
 public:
  explicit IdField(std::string value)
    : _value{std::move(value)}, _analyzer{1, 0, _value, ""} {}

  std::string_view Name() const final { return kId; }
  irs::Tokenizer& GetTokens() const final {
    _analyzer.reset(_value);
    return _analyzer;
  }
  irs::IndexFeatures GetIndexFeatures() const noexcept final {
    return irs::IndexFeatures::None;
  }
  bool Write(irs::DataOutput& out) const final {
    irs::WriteStr(out, _value);
    return true;
  }

 private:
  std::string _value;
  mutable MixedAnalyzer _analyzer;
};

auto MakeByTerm(std::string_view field, std::string_view value) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = field;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

class WandNormMergeCase : public tests::IndexTestBase {
 protected:
  // Customise `norm_row_group_size` per test so we can force multi-RG
  // shapes without needing 100K docs.
  irs::IndexWriterOptions MakeOpts(irs::Scorer* scorer,
                                   uint32_t norm_rgs = 122880) {
    auto opts = irs::tests::DefaultWriterOptions();
    opts.reader_options.scorer = scorer;
    opts.norm_column_options =
      [norm_rgs, next = std::make_shared<std::atomic<irs::field_id>>(0)](
        std::string_view) -> irs::NormColumnOptions {
      return {.id = next->fetch_add(1, std::memory_order_relaxed),
              .row_group_size = norm_rgs};
    };
    return opts;
  }

  irs::IndexReaderOptions MakeReaderOpts(irs::Scorer* scorer) {
    return irs::IndexReaderOptions{.scorer = scorer, .db = &::sdb::query::DuckDBEngine::Instance().instance()};
  }

  // doc with one NormField "body" of `count` tokens + a unique id.
  // tf("x") == dl == count.
  bool InsertNormDoc(irs::IndexWriter& writer, std::string_view id_value,
                     size_t count) {
    tests::Particle p;
    p.push_back(std::make_shared<IdField>(std::string{id_value}));
    p.push_back(std::make_shared<NormField>(kBody, count));
    return tests::Insert(writer, p.begin(), p.end());
  }

  // Realistic: tf("x") = `target` but dl = `target + filler`. BM25 length
  // normalisation now varies independently of tf, which is the shape real
  // corpora hit.
  bool InsertMixedDoc(irs::IndexWriter& writer, std::string_view id_value,
                      size_t target, size_t filler) {
    tests::Particle p;
    p.push_back(std::make_shared<IdField>(std::string{id_value}));
    p.push_back(std::make_shared<NormField>(kBody, target, filler));
    return tests::Insert(writer, p.begin(), p.end());
  }

  // doc with TWO NormFields ("body" count1, "body2" count2) + id.
  bool InsertDualNormDoc(irs::IndexWriter& writer, std::string_view id_value,
                         size_t count1, size_t count2) {
    tests::Particle p;
    p.push_back(std::make_shared<IdField>(std::string{id_value}));
    p.push_back(std::make_shared<NormField>(kBody, count1));
    p.push_back(std::make_shared<NormField>(kBody2, count2));
    return tests::Insert(writer, p.begin(), p.end());
  }

  // Read the NormColumn for `field_name` on `segment` and assert each
  // (doc, expected) pair. doc_id is 1-based per segment
  // (irs::doc_limits::min()).
  void AssertNorms(const irs::SubReader& segment, std::string_view field_name,
                   const std::vector<std::pair<irs::doc_id_t, uint32_t>>& exp) {
    const auto* field = segment.field(field_name);
    ASSERT_NE(nullptr, field) << "field missing: " << field_name;
    const auto norm_id = field->meta().norm;
    ASSERT_TRUE(irs::field_limits::valid(norm_id))
      << "no norm id on " << field_name;

    const auto* cs = segment.CsReader();
    ASSERT_NE(nullptr, cs);
    const auto* column = cs->NormColumn(norm_id);
    ASSERT_NE(nullptr, column) << "norm column missing for " << field_name;
    ASSERT_EQ(norm_id, column->Id());

    for (const auto& [doc, value] : exp) {
      const auto row = static_cast<uint64_t>(doc) - irs::doc_limits::min();
      ASSERT_EQ(value, column->Get(row))
        << field_name << " norm mismatch doc=" << doc;
    }
  }

  // Run BM25 ExecuteTopK twice and assert wand == no-wand. Returns the
  // wand-off baseline so callers can diff across snapshots.
  std::vector<irs::ScoreDoc> RunBM25(const irs::DirectoryReader& reader,
                                     const irs::Scorer& scorer,
                                     std::string_view field, size_t k) {
    irs::ByTerm filter;
    *filter.mutable_field() = field;
    filter.mutable_options()->term = irs::ViewCast<irs::byte_type>(kTerm);

    std::vector<irs::ScoreDoc> hits_no_wand(irs::BlockSize(k));
    std::vector<irs::ScoreDoc> hits_wand(irs::BlockSize(k));

    irs::ExecuteTopK(reader, filter, scorer, k,
                     irs::WandContext{.wand_enabled = false}, hits_no_wand);
    irs::ExecuteTopK(reader, filter, scorer, k,
                     irs::WandContext{.wand_enabled = true}, hits_wand);

    auto canon = [](std::vector<irs::ScoreDoc>& v) {
      std::sort(v.begin(), v.end(),
                [](const irs::ScoreDoc& a, const irs::ScoreDoc& b) {
                  if (a.score != b.score) {
                    return a.score > b.score;
                  }
                  if (a.segment_idx != b.segment_idx) {
                    return a.segment_idx < b.segment_idx;
                  }
                  return a.doc < b.doc;
                });
    };
    canon(hits_wand);
    canon(hits_no_wand);

    EXPECT_EQ(hits_no_wand, hits_wand)
      << "wand returned different top-K than non-wand on field=" << field;

    return hits_no_wand;
  }

  // Sort-and-extract just the scores (segment-id-independent) so we can
  // compare pre- vs post-merge BM25 results across changing layouts.
  static std::vector<irs::score_t> Scores(
    const std::vector<irs::ScoreDoc>& hits) {
    std::vector<irs::score_t> out;
    out.reserve(hits.size());
    for (const auto& h : hits) {
      if (h.doc != irs::doc_limits::eof()) {
        out.push_back(h.score);
      }
    }
    std::sort(out.begin(), out.end());
    return out;
  }
};

// -------------------------------------------------------------------------
// Baseline that already passed before this round of expansion. Kept to
// catch regressions on the simplest end of the surface.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, BasicBM25WandRoundTripAcrossCompact) {
  static constexpr uint32_t kCountsA[] = {1, 2, 3, 4};
  static constexpr uint32_t kCountsB[] = {5, 6, 7, 8};

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get());

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  for (auto c : kCountsA) {
    ASSERT_TRUE(InsertNormDoc(*writer, "a", c));
  }
  writer->RefreshCommit();
  for (auto c : kCountsB) {
    ASSERT_TRUE(InsertNormDoc(*writer, "b", c));
  }
  writer->RefreshCommit();

  std::vector<irs::score_t> pre_scores;
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(2, reader.size());
    AssertNorms(
      reader[0], kBody,
      {{1, kCountsA[0]}, {2, kCountsA[1]}, {3, kCountsA[2]}, {4, kCountsA[3]}});
    AssertNorms(
      reader[1], kBody,
      {{1, kCountsB[0]}, {2, kCountsB[1]}, {3, kCountsB[2]}, {4, kCountsB[3]}});
    pre_scores = Scores(RunBM25(reader, *bm25, kBody, 8));
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(1, reader.size());
    AssertNorms(reader[0], kBody,
                {{1, kCountsA[0]},
                 {2, kCountsA[1]},
                 {3, kCountsA[2]},
                 {4, kCountsA[3]},
                 {5, kCountsB[0]},
                 {6, kCountsB[1]},
                 {7, kCountsB[2]},
                 {8, kCountsB[3]}});
    auto post_scores = Scores(RunBM25(reader, *bm25, kBody, 8));
    ASSERT_EQ(pre_scores, post_scores);
  }
}

// -------------------------------------------------------------------------
// Multi-RG norm column inside ONE segment. Forces several FlushRowGroup
// calls in the norm writer; each RG may pick a different byte_size based
// on its local max. Reader's Get must walk to the right RG.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormMultiRgInOneSegment) {
  static constexpr size_t kRgSize = 4;
  static constexpr uint32_t kCounts[] = {1, 2, 3, 4,  // RG 0
                                         5, 6, 7, 8,  // RG 1
                                         9, 10};      // RG 2 (partial)

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get(), kRgSize);

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);
  for (size_t i = 0; i < std::size(kCounts); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("doc_", i), kCounts[i]));
  }
  writer->RefreshCommit();

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  const auto& seg = reader[0];
  ASSERT_EQ(std::size(kCounts), seg.docs_count());

  std::vector<std::pair<irs::doc_id_t, uint32_t>> exp;
  for (size_t i = 0; i < std::size(kCounts); ++i) {
    exp.emplace_back(static_cast<irs::doc_id_t>(i + 1), kCounts[i]);
  }
  AssertNorms(seg, kBody, exp);

  // Reader RG metadata: 3 RGs (4 + 4 + 2 docs).
  const auto* field = seg.field(kBody);
  ASSERT_NE(nullptr, field);
  const auto norm_id = field->meta().norm;
  const auto* col = seg.CsReader()->NormColumn(norm_id);
  ASSERT_NE(nullptr, col);
  ASSERT_EQ(3u, col->RowGroupCount())
    << "expected 4+4+2 layout, got " << col->RowGroupCount();
  EXPECT_EQ(4u, col->RowGroupRowCount(0));
  EXPECT_EQ(4u, col->RowGroupRowCount(1));
  EXPECT_EQ(2u, col->RowGroupRowCount(2));

  RunBM25(reader, *bm25, kBody, 10);
}

// -------------------------------------------------------------------------
// Multi-RG per segment + compaction. The merge writes a single output
// norm column built from per-source byte spans; if the merge mishandles
// RG boundaries or row offsets, per-doc Get(row) returns stale data.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormMultiRgAcrossMerge) {
  static constexpr size_t kRgSize = 3;
  static constexpr uint32_t kA[] = {1, 2, 3, 4, 5};       // 5 docs => 2 RGs
  static constexpr uint32_t kB[] = {6, 7, 8, 9, 10, 11};  // 6 docs => 2 RGs

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get(), kRgSize);

  auto writer = open_writer(irs::kOmCreate, opts);
  for (size_t i = 0; i < std::size(kA); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("a_", i), kA[i]));
  }
  writer->RefreshCommit();
  for (size_t i = 0; i < std::size(kB); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("b_", i), kB[i]));
  }
  writer->RefreshCommit();

  std::vector<irs::score_t> pre_scores;
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(2, reader.size());
    pre_scores = Scores(RunBM25(reader, *bm25, kBody, 16));
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  const auto& seg = reader[0];
  ASSERT_EQ(std::size(kA) + std::size(kB), seg.docs_count());

  std::vector<std::pair<irs::doc_id_t, uint32_t>> exp;
  irs::doc_id_t doc = irs::doc_limits::min();
  for (auto v : kA) {
    exp.emplace_back(doc++, v);
  }
  for (auto v : kB) {
    exp.emplace_back(doc++, v);
  }
  AssertNorms(seg, kBody, exp);

  auto post_scores = Scores(RunBM25(reader, *bm25, kBody, 16));
  ASSERT_EQ(pre_scores, post_scores)
    << "BM25 score set diverged across multi-RG compaction";
}

// -------------------------------------------------------------------------
// Source segments use DIFFERENT byte widths per RG (one with values that
// fit in uint8, the other with values requiring uint16). The merged
// column should still read back each doc's original value.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormMixedByteWidthsMerge) {
  // segment A: all <= 255 -> byte_size=1 per RG.
  static constexpr uint32_t kA[] = {10, 50, 100, 200};
  // segment B: max > 255 -> byte_size=2.
  static constexpr uint32_t kB[] = {300, 1000, 65000};

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get());  // default large RG

  auto writer = open_writer(irs::kOmCreate, opts);
  for (size_t i = 0; i < std::size(kA); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("a_", i), kA[i]));
  }
  writer->RefreshCommit();
  for (size_t i = 0; i < std::size(kB); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("b_", i), kB[i]));
  }
  writer->RefreshCommit();

  // Read pre-merge norms.
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(2, reader.size());
    AssertNorms(reader[0], kBody,
                {{1, kA[0]}, {2, kA[1]}, {3, kA[2]}, {4, kA[3]}});
    AssertNorms(reader[1], kBody, {{1, kB[0]}, {2, kB[1]}, {3, kB[2]}});
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  AssertNorms(reader[0], kBody,
              {{1, kA[0]},
               {2, kA[1]},
               {3, kA[2]},
               {4, kA[3]},
               {5, kB[0]},
               {6, kB[1]},
               {7, kB[2]}});

  RunBM25(reader, *bm25, kBody, 7);
}

// -------------------------------------------------------------------------
// Deletes before compaction: MergeNormColumnFromSources's mask filter
// must skip the per-row bytes of removed docs and produce a compacted
// merged column. If the mask path mis-counts bytes, post-merge norms
// shift by the wrong offset.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormMergeWithMask) {
  static constexpr uint32_t kA[] = {1, 2, 3, 4, 5};
  static constexpr uint32_t kB[] = {6, 7, 8};

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get());

  auto writer = open_writer(irs::kOmCreate, opts);
  for (size_t i = 0; i < std::size(kA); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("a_", i), kA[i]));
  }
  writer->RefreshCommit();

  // Remove "a_1" (count=2) and "a_3" (count=4) from segment A. The
  // filter must outlive Commit() per IndexWriter::Transaction::Remove
  // contract, so keep them as named locals.
  auto rm1 = MakeByTerm(kId, "a_1");
  auto rm2 = MakeByTerm(kId, "a_3");
  writer->GetBatch().Remove(*rm1);
  writer->GetBatch().Remove(*rm2);
  writer->RefreshCommit();

  for (size_t i = 0; i < std::size(kB); ++i) {
    ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("b_", i), kB[i]));
  }
  writer->RefreshCommit();

  // Pre-merge: segment A still has 5 docs but live_count=3.
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(2, reader.size());
    ASSERT_EQ(5, reader[0].docs_count());
    ASSERT_EQ(3, reader[0].live_docs_count());
    ASSERT_EQ(3, reader[1].docs_count());
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  ASSERT_EQ(6, reader[0].docs_count()) << "merged dropped masked docs";

  // After mask + merge, surviving docs in source-order are:
  //   a_0 (count=1), a_2 (count=3), a_4 (count=5), b_0..b_2.
  AssertNorms(
    reader[0], kBody,
    {{1, kA[0]}, {2, kA[2]}, {3, kA[4]}, {4, kB[0]}, {5, kB[1]}, {6, kB[2]}});

  RunBM25(reader, *bm25, kBody, 6);
}

// -------------------------------------------------------------------------
// Two norm-bearing fields ("body" and "body2") in the same segment. The
// per-field norm_id allocator inside `norm_column_options` must produce
// disjoint ids; norm reads must be by id, not by field name.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormTwoFieldsAcrossMerge) {
  static constexpr uint32_t kBodyA[] = {1, 2, 3};
  static constexpr uint32_t kBody2A[] = {10, 20, 30};
  static constexpr uint32_t kBodyB[] = {4, 5};
  static constexpr uint32_t kBody2B[] = {40, 50};

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get());

  auto writer = open_writer(irs::kOmCreate, opts);
  for (size_t i = 0; i < std::size(kBodyA); ++i) {
    ASSERT_TRUE(
      InsertDualNormDoc(*writer, absl::StrCat("a_", i), kBodyA[i], kBody2A[i]));
  }
  writer->RefreshCommit();
  for (size_t i = 0; i < std::size(kBodyB); ++i) {
    ASSERT_TRUE(
      InsertDualNormDoc(*writer, absl::StrCat("b_", i), kBodyB[i], kBody2B[i]));
  }
  writer->RefreshCommit();

  // Pre-merge: each field's norm column is independent per segment.
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(2, reader.size());
    AssertNorms(reader[0], kBody,
                {{1, kBodyA[0]}, {2, kBodyA[1]}, {3, kBodyA[2]}});
    AssertNorms(reader[0], kBody2,
                {{1, kBody2A[0]}, {2, kBody2A[1]}, {3, kBody2A[2]}});
    AssertNorms(reader[1], kBody, {{1, kBodyB[0]}, {2, kBodyB[1]}});
    AssertNorms(reader[1], kBody2, {{1, kBody2B[0]}, {2, kBody2B[1]}});
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  AssertNorms(reader[0], kBody,
              {{1, kBodyA[0]},
               {2, kBodyA[1]},
               {3, kBodyA[2]},
               {4, kBodyB[0]},
               {5, kBodyB[1]}});
  AssertNorms(reader[0], kBody2,
              {{1, kBody2A[0]},
               {2, kBody2A[1]},
               {3, kBody2A[2]},
               {4, kBody2B[0]},
               {5, kBody2B[1]}});

  RunBM25(reader, *bm25, kBody, 5);
  RunBM25(reader, *bm25, kBody2, 5);
}

// -------------------------------------------------------------------------
// 16 source segments compacted in one shot. Mirrors the
// search-benchmark-game ingest pattern: ~15-17 commit-per-batch segments,
// then a single `CompactionCount` merges them all. If any cross-source
// state in the norm merge (running merged_row, byte_size promotion,
// per-source NormColumnReader cache) gets confused with more than 2
// sources, this is where it surfaces.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormMultiSegmentCompact) {
  static constexpr size_t kSegments = 16;
  static constexpr size_t kDocsPerSeg = 5;

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get());

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);

  // Build N segments. Per-doc tf grows monotonically across the whole
  // corpus so each (segment, doc) cell gets a unique norm; that turns
  // any mis-aligned merge into a per-doc value mismatch instead of a
  // silently-equivalent value.
  std::vector<uint32_t> expected_counts;
  expected_counts.reserve(kSegments * kDocsPerSeg);
  for (size_t s = 0; s < kSegments; ++s) {
    for (size_t d = 0; d < kDocsPerSeg; ++d) {
      const uint32_t count =
        static_cast<uint32_t>(s * kDocsPerSeg + d + 1);  // 1..80
      expected_counts.push_back(count);
      ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("s", s, "_d", d), count));
    }
    ASSERT_TRUE(writer->RefreshCommit());
  }

  // Pre-merge: kSegments separate segments, each with its own NormColumn.
  std::vector<irs::score_t> pre_scores;
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(kSegments, reader.size());
    for (size_t s = 0; s < kSegments; ++s) {
      std::vector<std::pair<irs::doc_id_t, uint32_t>> exp;
      for (size_t d = 0; d < kDocsPerSeg; ++d) {
        exp.emplace_back(static_cast<irs::doc_id_t>(d + 1),
                         expected_counts[s * kDocsPerSeg + d]);
      }
      AssertNorms(reader[s], kBody, exp);
    }
    pre_scores = Scores(RunBM25(reader, *bm25, kBody, kSegments * kDocsPerSeg));
  }

  // Single-shot CompactionCount with default max merges everything.
  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  // Post-merge: ONE segment, per-doc norms equal to expected_counts in
  // source-iteration order (no deletes -> identity remap, doc ids
  // contiguous 1..N).
  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size())
    << "compact-all did not produce a single merged segment";
  const auto& seg = reader[0];
  ASSERT_EQ(expected_counts.size(), seg.docs_count());

  std::vector<std::pair<irs::doc_id_t, uint32_t>> exp_all;
  for (size_t i = 0; i < expected_counts.size(); ++i) {
    exp_all.emplace_back(static_cast<irs::doc_id_t>(i + 1), expected_counts[i]);
  }
  AssertNorms(seg, kBody, exp_all);

  auto post_scores =
    Scores(RunBM25(reader, *bm25, kBody, kSegments * kDocsPerSeg));
  ASSERT_EQ(pre_scores, post_scores)
    << "BM25 score multiset diverged after " << kSegments << "-way compact-all";
}

// -------------------------------------------------------------------------
// 16 sources, multi-RG per source, AND cross-byte-width: the combination
// the bench actually drives.
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, NormMultiSegmentMultiRgMixedWidthsCompact) {
  static constexpr size_t kSegments = 16;
  static constexpr size_t kDocsPerSeg = 7;  // multi-RG with RG=3
  static constexpr size_t kRgSize = 3;

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get(), kRgSize);

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);

  // Counts ramp into the uint16 range half-way through so some sources
  // pick byte_size=1 and some byte_size=2 per RG.
  std::vector<uint32_t> expected_counts;
  expected_counts.reserve(kSegments * kDocsPerSeg);
  for (size_t s = 0; s < kSegments; ++s) {
    const uint32_t base = (s < kSegments / 2) ? 1u : 300u;
    for (size_t d = 0; d < kDocsPerSeg; ++d) {
      const uint32_t count = base + static_cast<uint32_t>(s * kDocsPerSeg + d);
      expected_counts.push_back(count);
      ASSERT_TRUE(InsertNormDoc(*writer, absl::StrCat("s", s, "_d", d), count));
    }
    ASSERT_TRUE(writer->RefreshCommit());
  }

  std::vector<irs::score_t> pre_scores;
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(kSegments, reader.size());
    for (size_t s = 0; s < kSegments; ++s) {
      std::vector<std::pair<irs::doc_id_t, uint32_t>> exp;
      for (size_t d = 0; d < kDocsPerSeg; ++d) {
        exp.emplace_back(static_cast<irs::doc_id_t>(d + 1),
                         expected_counts[s * kDocsPerSeg + d]);
      }
      AssertNorms(reader[s], kBody, exp);
    }
    pre_scores = Scores(RunBM25(reader, *bm25, kBody, kSegments * kDocsPerSeg));
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  const auto& seg = reader[0];
  ASSERT_EQ(expected_counts.size(), seg.docs_count());

  std::vector<std::pair<irs::doc_id_t, uint32_t>> exp_all;
  for (size_t i = 0; i < expected_counts.size(); ++i) {
    exp_all.emplace_back(static_cast<irs::doc_id_t>(i + 1), expected_counts[i]);
  }
  AssertNorms(seg, kBody, exp_all);

  auto post_scores =
    Scores(RunBM25(reader, *bm25, kBody, kSegments * kDocsPerSeg));
  ASSERT_EQ(pre_scores, post_scores)
    << "BM25 diverged on multi-RG + mixed-width " << kSegments << "-way merge";
}

// -------------------------------------------------------------------------
// search-benchmark-game-shaped scenario, with tf != dl (independent BM25
// signal axes). Bench uses mmap; this is the workload most likely to
// actually be hit. Per-doc tf("x") cycles 1..5 (so docs that match the
// query have varying freq), and dl cycles independently (5..35 tokens).
// -------------------------------------------------------------------------
TEST_P(WandNormMergeCase, BenchShape16SegmentsRealisticTfDl) {
  static constexpr size_t kSegments = 16;
  static constexpr size_t kDocsPerSeg = 5;
  static constexpr size_t kTfMod = 5;  // tf cycles 1..5
  static constexpr size_t kFillerMod =
    7;  // filler cycles 0..6 -> dl = tf+filler

  auto bm25 = std::make_unique<irs::BM25>();
  auto opts = MakeOpts(bm25.get());

  auto writer = open_writer(irs::kOmCreate, opts);
  ASSERT_NE(nullptr, writer);

  // Record per-doc expected norm value (dl = tf + filler). Per-segment
  // doc_id is 1-based.
  struct Expected {
    size_t segment;
    irs::doc_id_t doc;
    uint32_t norm;  // = dl
  };
  std::vector<Expected> expected;
  expected.reserve(kSegments * kDocsPerSeg);

  for (size_t s = 0; s < kSegments; ++s) {
    for (size_t d = 0; d < kDocsPerSeg; ++d) {
      const size_t tf = 1 + (s * kDocsPerSeg + d) % kTfMod;
      const size_t filler = ((s * 31 + d * 7) % kFillerMod);
      const uint32_t dl = static_cast<uint32_t>(tf + filler);
      expected.push_back({s, static_cast<irs::doc_id_t>(d + 1), dl});
      ASSERT_TRUE(
        InsertMixedDoc(*writer, absl::StrCat("s", s, "_d", d), tf, filler));
    }
    ASSERT_TRUE(writer->RefreshCommit());
  }

  // Pre-merge: norms match (tf, dl) per doc on each segment.
  std::vector<irs::score_t> pre_scores;
  {
    auto reader = open_reader(MakeReaderOpts(bm25.get()));
    ASSERT_EQ(kSegments, reader.size());
    for (size_t s = 0; s < kSegments; ++s) {
      std::vector<std::pair<irs::doc_id_t, uint32_t>> exp;
      for (const auto& e : expected) {
        if (e.segment == s) {
          exp.emplace_back(e.doc, e.norm);
        }
      }
      AssertNorms(reader[s], kBody, exp);
    }
    pre_scores = Scores(RunBM25(reader, *bm25, kBody, 50));
  }

  const irs::index_utils::CompactionCount all;
  ASSERT_TRUE(writer->Compact(irs::index_utils::MakePolicy(all)));
  ASSERT_TRUE(writer->RefreshCommit());

  auto reader = open_reader(MakeReaderOpts(bm25.get()));
  ASSERT_EQ(1, reader.size());
  const auto& seg = reader[0];
  ASSERT_EQ(expected.size(), seg.docs_count());

  // After merge, source-iteration order: segments 0..15 each contribute
  // 5 docs in order, so global doc id i+1 maps to expected[i].
  std::vector<std::pair<irs::doc_id_t, uint32_t>> exp_all;
  for (size_t i = 0; i < expected.size(); ++i) {
    exp_all.emplace_back(static_cast<irs::doc_id_t>(i + 1), expected[i].norm);
  }
  AssertNorms(seg, kBody, exp_all);

  auto post_scores = Scores(RunBM25(reader, *bm25, kBody, 50));
  ASSERT_EQ(pre_scores, post_scores)
    << "BM25 score multiset diverged on bench-shaped compaction with"
    << " independent tf/dl";
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();
static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5simd"}));
INSTANTIATE_TEST_SUITE_P(WandNormMergeTest, WandNormMergeCase, kTestValues,
                         WandNormMergeCase::to_string);

}  // namespace
