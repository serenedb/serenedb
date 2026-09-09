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

#include <absl/strings/str_join.h>

#include <array>
#include <string>
#include <vector>

#include "filter_test_case_base.hpp"
#include "index/index_tests.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/constant_score.hpp"
#include "iresearch/search/dfi.hpp"
#include "iresearch/search/idf.hpp"
#include "iresearch/search/indri_dirichlet.hpp"
#include "iresearch/search/lm_dirichlet.hpp"
#include "iresearch/search/lm_jelinek_mercer.hpp"
#include "iresearch/search/raw_boost.hpp"
#include "iresearch/search/raw_dl.hpp"
#include "iresearch/search/raw_tf.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/tfidf.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests;

inline constexpr irs::field_id kNone = 1;
inline constexpr irs::field_id kFreq = 2;
inline constexpr irs::field_id kFreqNorm = 3;

irs::IndexFeatures FeaturesOf(irs::field_id id) noexcept {
  switch (id) {
    case kFreq:
      return irs::IndexFeatures::Freq;
    case kFreqNorm:
      return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
    default:
      return irs::IndexFeatures::None;
  }
}

std::string_view NameOf(irs::field_id id) noexcept {
  switch (id) {
    case kFreq:
      return "freq";
    case kFreqNorm:
      return "freq_norm";
    default:
      return "none";
  }
}

class WordsField : public tests::FieldBase {
 public:
  WordsField(irs::field_id field_id, const std::vector<std::string>& words)
    : _value{absl::StrJoin(words, " ")} {
    this->Name(std::string{NameOf(field_id)});
    this->id = field_id;
    this->index_features = FeaturesOf(field_id);
  }

  irs::analysis::Tokenizer& GetTokens() const final { return _stream; }

  std::string_view Value() const final { return _value; }

 private:
  bool Write(irs::DataOutput&) const final { return false; }

  std::string _value;
  mutable irs::analysis::DelimitedTokenizer _stream{" "};
};

struct Case {
  std::string_view name;
  irs::Scorer::ptr (*make)();
  std::array<bool, 3> constant;
  std::array<bool, 3> varies;
};

const std::array kCases{
  Case{"bm25",
       [] -> irs::Scorer::ptr { return irs::BM25::Make(irs::BM25::Options{}); },
       {true, false, false},
       {false, true, true}},
  Case{"bm15",
       [] -> irs::Scorer::ptr {
         return irs::BM25::Make(irs::BM25::Options{.b = 0.f});
       },
       {true, false, false},
       {false, true, true}},
  Case{"bm11",
       [] -> irs::Scorer::ptr {
         return irs::BM25::Make(irs::BM25::Options{.b = 1.f});
       },
       {true, false, false},
       {false, true, true}},
  Case{"bm1",
       [] -> irs::Scorer::ptr {
         return irs::BM25::Make(irs::BM25::Options{.k1 = 0.f});
       },
       {true, true, true},
       {false, false, false}},
  Case{"tfidf",
       [] -> irs::Scorer::ptr {
         return irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = false});
       },
       {true, false, false},
       {false, true, true}},
  Case{"tfidf_norm",
       [] -> irs::Scorer::ptr {
         return irs::TFIDF::Make(irs::TFIDF::Options{.with_norms = true});
       },
       {true, false, false},
       {false, true, true}},
  Case{
    "raw_tf",
    [] -> irs::Scorer::ptr { return irs::RawTF::Make(irs::RawTF::Options{}); },
    {true, false, false},
    {false, true, true}},
  Case{
    "raw_dl",
    [] -> irs::Scorer::ptr { return irs::RawDL::Make(irs::RawDL::Options{}); },
    {true, false, false},
    {false, false, true}},
  Case{"dfi",
       [] -> irs::Scorer::ptr { return irs::DFI::Make(irs::DFI::Options{}); },
       {true, false, false},
       {false, true, true}},
  Case{"lm_dirichlet",
       [] -> irs::Scorer::ptr {
         return irs::LMDirichlet::Make(irs::LMDirichlet::Options{});
       },
       {true, false, false},
       {false, true, true}},
  Case{"lm_jelinek_mercer",
       [] -> irs::Scorer::ptr {
         return irs::LMJelinekMercer::Make(irs::LMJelinekMercer::Options{});
       },
       {true, false, false},
       {false, true, true}},
  Case{"indri_dirichlet",
       [] -> irs::Scorer::ptr {
         return irs::IndriDirichlet::Make(irs::IndriDirichlet::Options{});
       },
       {true, false, false},
       {false, true, true}},
  Case{"constant",
       [] -> irs::Scorer::ptr {
         return irs::ConstantScore::Make(irs::ConstantScore::Options{});
       },
       {true, true, true},
       {false, false, false}},
  Case{"idf",
       [] -> irs::Scorer::ptr { return irs::IDF::Make(irs::IDF::Options{}); },
       {true, true, true},
       {false, false, false}},
  Case{"raw_boost",
       [] -> irs::Scorer::ptr {
         return irs::RawBoost::Make(irs::RawBoost::Options{});
       },
       {true, true, true},
       {false, false, false}},
};

size_t SlotOf(irs::field_id id) noexcept { return static_cast<size_t>(id) - 1; }

class ScoreConstantTest : public tests::FilterTestCaseBase {
 protected:
  void BuildIndex() {
    auto writer = open_writer(irs::kOmCreate);
    ASSERT_NE(nullptr, writer);
    for (const auto& words : {std::vector<std::string>{"fox", "fox", "dog"},
                              std::vector<std::string>{"fox", "cat"},
                              std::vector<std::string>{"dog", "cat", "bird"},
                              std::vector<std::string>{"dog"}}) {
      tests::Document doc;
      for (const auto id : {kNone, kFreq, kFreqNorm}) {
        doc.insert(std::make_shared<WordsField>(id, words), true, false);
      }
      ASSERT_TRUE(
        tests::Insert(*writer, doc.indexed.begin(), doc.indexed.end()));
    }
    writer->RefreshCommit();
  }
};

TEST_P(ScoreConstantTest, folds_only_what_reads_nothing_per_document) {
  BuildIndex();
  auto index = open_reader();
  ASSERT_EQ(1, index->size());
  const auto& segment = *index->begin();

  for (const auto& c : kCases) {
    auto scorer = c.make();
    ASSERT_NE(nullptr, scorer);
    for (const auto id : {kNone, kFreq, kFreqNorm}) {
      SCOPED_TRACE(testing::Message()
                   << c.name << " over field " << NameOf(id));
      irs::ByTerm filter;
      *filter.mutable_field_id() = id;
      filter.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view{"fox"});

      tests::PreparedFilter prepared{filter, *index, scorer.get()};
      const auto* reader = segment.field(id);
      ASSERT_NE(nullptr, reader);
      ASSERT_NE(nullptr, irs::search::DocOf(*reader));

      irs::ColumnArgsFetcher fetcher;
      const irs::search::ScoreArgs args{.scorer = scorer.get(),
                                        .stats = prepared.Stats().stats,
                                        .fetcher = &fetcher,
                                        .boost = irs::kNoBoost};

      const auto folded = irs::search::ConstantOf(segment, *reader, args);
      EXPECT_EQ(c.constant[SlotOf(id)], folded.has_value());
    }
  }
}

TEST_P(ScoreConstantTest, every_combination_executes) {
  BuildIndex();
  auto index = open_reader();
  ASSERT_EQ(1, index->size());

  for (const auto& c : kCases) {
    auto scorer = c.make();
    ASSERT_NE(nullptr, scorer);
    for (const auto id : {kNone, kFreq, kFreqNorm}) {
      SCOPED_TRACE(testing::Message()
                   << c.name << " over field " << NameOf(id));
      irs::ByTerm filter;
      *filter.mutable_field_id() = id;
      filter.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view{"fox"});

      tests::PreparedFilter prepared{filter, *index, scorer.get()};
      irs::ColumnArgsFetcher fetcher;
      auto docs = prepared.ExecuteScored(0, fetcher);
      ASSERT_NE(nullptr, docs);
      auto score = docs->PrepareScore();

      std::vector<irs::score_t> values;
      while (!irs::doc_limits::eof(docs->Advance())) {
        docs->FetchScoreArgs(0);
        fetcher.Fetch(docs->Value());
        irs::score_t value = 0;
        score.Score(&value, 1);
        values.emplace_back(value);
      }
      ASSERT_EQ(2, values.size());
      if (c.constant[SlotOf(id)]) {
        EXPECT_FLOAT_EQ(values[0], values[1]);
        EXPECT_FALSE(c.varies[SlotOf(id)]);
      }
      if (c.varies[SlotOf(id)]) {
        EXPECT_NE(values[0], values[1]);
      } else {
        EXPECT_FLOAT_EQ(values[0], values[1]);
      }
    }
  }
}

TEST_P(ScoreConstantTest, every_root_answers_every_combination) {
  BuildIndex();
  auto index = open_reader();
  ASSERT_EQ(1, index->size());

  for (const auto& c : kCases) {
    auto scorer = c.make();
    ASSERT_NE(nullptr, scorer);
    for (const auto id : {kNone, kFreq, kFreqNorm}) {
      SCOPED_TRACE(testing::Message()
                   << c.name << " over field " << NameOf(id));
      irs::ByTerm filter;
      *filter.mutable_field_id() = id;
      filter.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view{"fox"});

      tests::PreparedFilter prepared{filter, *index, scorer.get()};
      const auto* query = prepared.Query(0);
      ASSERT_NE(nullptr, query);

      irs::SlackBuf<irs::doc_id_t, irs::doc_limits::kMinCapacity,
                    irs::doc_limits::kDocsSlack>
        docs;
      irs::SlackBuf<irs::score_t, irs::doc_limits::kMinCapacity,
                    irs::doc_limits::kScoresSlack>
        scores;
      {
        irs::ColumnArgsFetcher fetcher;
        auto plan = query->PlanScored({.scorer = *scorer, .fetcher = fetcher});
        ASSERT_NE(nullptr, plan);
        size_t seen = 0;
        for (;;) {
          const auto n = plan->Run(docs.data(), scores.data(),
                                   irs::doc_limits::kMinCapacity);
          if (n == 0) {
            break;
          }
          fetcher.Fetch(docs[0]);
          seen += n;
        }
        EXPECT_EQ(2, seen);
      }
      {
        irs::ColumnArgsFetcher fetcher;
        auto plan =
          query->PlanTop({.scorer = *scorer, .fetcher = fetcher, .k = 10});
        ASSERT_NE(nullptr, plan);
      }
    }
  }
}

// A constant scorer over a field that stores frequencies takes the constant
// leaf, and that leaf still has to step over the frequency blocks the stream
// holds. Two terms so the plan fills a window, and more documents than a block
// so the blocks are full: a short block is stepped over by neither spelling,
// which is why the four-document fixture above cannot see this.
TEST_P(ScoreConstantTest, constant_over_freq_field_steps_over_full_blocks) {
  constexpr size_t kDocs = 400;
  {
    auto writer = open_writer(irs::kOmCreate);
    ASSERT_NE(nullptr, writer);
    for (size_t i = 0; i != kDocs; ++i) {
      std::vector<std::string> words{"common", "common"};
      if (i % 3 == 0) {
        words.emplace_back("rare");
      }
      tests::Document doc;
      doc.insert(std::make_shared<WordsField>(kFreqNorm, words), true, false);
      ASSERT_TRUE(
        tests::Insert(*writer, doc.indexed.begin(), doc.indexed.end()));
    }
    writer->RefreshCommit();
  }
  auto index = open_reader();
  ASSERT_EQ(1, index->size());

  auto scorer = irs::BM25::Make(irs::BM25::Options{.k1 = 0.f});
  ASSERT_NE(nullptr, scorer);

  irs::BooleanFilter filter;
  for (const auto* term : {"common", "rare"}) {
    filter.Add(
      irs::TermClause{.field = kFreqNorm,
                      .term = irs::bstring{irs::ViewCast<irs::byte_type>(
                        std::string_view{term})}},
      irs::Occur::Should);
  }
  filter.SetMinShouldMatch(1);

  tests::PreparedFilter prepared{filter, *index, scorer.get()};
  irs::ColumnArgsFetcher fetcher;
  auto docs = prepared.ExecuteScored(0, fetcher);
  ASSERT_NE(nullptr, docs);

  std::vector<irs::doc_id_t> got;
  for (auto doc = docs->Advance(); !irs::doc_limits::eof(doc);
       doc = docs->Advance()) {
    got.emplace_back(doc);
  }
  ASSERT_EQ(kDocs, got.size());
  for (size_t i = 0; i != kDocs; ++i) {
    EXPECT_EQ(irs::doc_limits::min() + i, got[i]);
  }
}

TEST_P(ScoreConstantTest, bm25_degrades_to_freq_one_norm_one) {
  BuildIndex();
  auto index = open_reader();
  ASSERT_EQ(1, index->size());
  const auto& segment = *index->begin();

  auto scorer = irs::BM25::Make(irs::BM25::Options{});
  irs::ByTerm filter;
  *filter.mutable_field_id() = kNone;
  filter.mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"fox"});

  tests::PreparedFilter prepared{filter, *index, scorer.get()};
  const auto* reader = segment.field(kNone);
  ASSERT_NE(nullptr, reader);
  ASSERT_NE(nullptr, irs::search::DocOf(*reader));

  irs::ColumnArgsFetcher fetcher;
  const irs::search::ScoreArgs args{.scorer = scorer.get(),
                                    .stats = prepared.Stats().stats,
                                    .fetcher = &fetcher,
                                    .boost = irs::kNoBoost};

  const auto folded = irs::search::ConstantOf(segment, *reader, args);
  ASSERT_TRUE(folded.has_value());

  struct Provider final : irs::AttributeProvider {
    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      return type == irs::Type<irs::FreqBlockAttr>::id() ? &freq : nullptr;
    }

    irs::FreqBlockAttr freq;
  } provider;
  uint32_t one = 1;
  provider.freq.value = &one;
  auto expected = scorer->PrepareScorer({
    .segment = segment,
    .field = reader->meta(),
    .doc_attrs = provider,
    .fetcher = &fetcher,
    .stats = prepared.Stats().stats,
    .boost = irs::kNoBoost,
  });
  EXPECT_FLOAT_EQ(expected.Score(), *folded);

  auto docs = prepared.ExecuteScored(0, fetcher);
  ASSERT_NE(nullptr, docs);
  auto score = docs->PrepareScore();
  while (!irs::doc_limits::eof(docs->Advance())) {
    docs->FetchScoreArgs(0);
    fetcher.Fetch(docs->Value());
    irs::score_t value = 0;
    score.Score(&value, 1);
    EXPECT_FLOAT_EQ(*folded, value);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(score_constant_test, ScoreConstantTest,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         ScoreConstantTest::to_string);

}  // namespace
