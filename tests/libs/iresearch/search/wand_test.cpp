////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/index/index_reader_options.hpp>
#include <iresearch/search/scorer.hpp>

#include "index/index_tests.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/tfidf.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

struct Doc {
  Doc(size_t segment, irs::doc_id_t doc) noexcept
    : segment{segment}, doc{doc} {}

  bool operator==(const Doc&) const = default;

  size_t segment;
  irs::doc_id_t doc;
};

struct ScoredDoc : Doc {
  ScoredDoc(size_t segment, irs::doc_id_t doc, float score) noexcept
    : Doc{segment, doc}, score{score} {}

  bool operator<(const ScoredDoc& rhs) const noexcept {
    if (score > rhs.score) {
      return true;
    }
    if (score < rhs.score) {
      return false;
    }
    if (segment < rhs.segment) {
      return true;
    }
    if (segment > rhs.segment) {
      return false;
    }
    return doc < rhs.doc;
  }

  float score;
};

class WandTestCase : public tests::IndexTestBase {
 public:
  static irs::IndexWriterOptions GetWriterOptions(irs::ScorerPtr scorer,
                                                  bool write_norms);

  std::vector<Doc> Collect(const irs::DirectoryReader& index,
                           const irs::Filter& filter, irs::ScorerPtr scorers,
                           bool can_use_wand, size_t limit);

  void AssertResults(const irs::DirectoryReader& index,
                     const irs::Filter& filter, irs::ScorerPtr scorers,
                     bool can_use_wand, size_t limit);

  void GenerateSegment(irs::ScorerPtr scorers, bool write_norms,
                       bool append_data = false);
  void GenerateSegmentMinNorm(irs::ScorerPtr scorer);

  void AssertFilters(irs::ScorerPtr scorer, bool disjunction = true) {
    auto apply = [&](auto assert_filter) {
      ASSERT_TRUE(scorer);
      std::invoke(assert_filter, *this, *scorer);
      // Invalid scorer
      // std::invoke(assert_filter, *this, scorers, *scorers[0],
      // scorers.size());
    };
    apply(&WandTestCase::AssertTermFilter);
    apply(&WandTestCase::AssertConjunctionFilter);
    if (disjunction) {
      apply(&WandTestCase::AssertDisjunctionFilter);
    }
  }

  void AssertTermFilter(const irs::Scorer& scorer);

  void AssertConjunctionFilter(const irs::Scorer& scorer);

  void AssertDisjunctionFilter(const irs::Scorer& scorer);

  bool CanUseWand(const irs::Scorer& scorer, const irs::TermReader& field) {
    return false;
    // TODO(mbkkt) Enable this back?
    // const auto& field_meta = field.meta();
    // const auto index_features = scorer.GetIndexFeatures();
    // if (!irs::IsSubsetOf(index_features, field_meta.index_features)) {
    //   return false;
    // }

    // if (irs::IsSubsetOf(irs::IndexFeatures::Norm, index_features) &&
    //     !irs::field_limits::valid(field_meta.norm)) {
    //   return false;
    // }

    // return wand_index < scorers.size();
  }
};

std::vector<Doc> WandTestCase::Collect(const irs::DirectoryReader& index,
                                       const irs::Filter& filter,
                                       irs::ScorerPtr scorer, bool can_use_wand,
                                       size_t limit) {
  auto query = filter.prepare({.index = index, .scorer = scorer});
  EXPECT_NE(nullptr, query);

  const irs::WandContext mode{.index = 0};

  std::vector<ScoredDoc> sorted;
  sorted.reserve(limit);

  for (size_t left = limit, segment_id = 0; const auto& segment : index) {
    irs::ColumnArgsFetcher fetcher;
    auto docs = query->execute(irs::ExecutionContext{
      .segment = segment,
      .scorer = scorer,
      .wand = mode,
    });
    EXPECT_NE(nullptr, docs);

    irs::ScoreFunction score;
    if (can_use_wand) {
      // EXPECT_NE(std::numeric_limits<irs::score_t>::max(), score.max.tail);
      score = docs->PrepareScore({
        .scorer = scorer,
        .segment = &segment,
        .fetcher = &fetcher,
      });
    } else {
      // EXPECT_EQ(std::numeric_limits<irs::score_t>::max(), score.max.tail);
    }

    if (!left) {
      EXPECT_TRUE(!sorted.empty());
      EXPECT_TRUE(std::is_heap(std::begin(sorted), std::end(sorted)));
    }
    irs::score_t score_value = 0;
    while (docs->next()) {
      auto doc = docs->value();
      fetcher.Fetch(doc);
      docs->FetchScoreArgs(0);
      score.Score(&score_value, 1);

      if (left) {
        sorted.emplace_back(segment_id, doc, score_value);

        if (0 == --left) {
          std::make_heap(std::begin(sorted), std::end(sorted));
        }
      } else if (sorted.front().score < score_value) {
        std::pop_heap(std::begin(sorted), std::end(sorted));

        auto& min_doc = sorted.back();
        min_doc.segment = segment_id;
        min_doc.doc = doc;
        min_doc.score = score_value;

        std::push_heap(std::begin(sorted), std::end(sorted));
      }
    }

    ++segment_id;
  }

  std::sort(std::begin(sorted), std::end(sorted));

  return {std::begin(sorted), std::end(sorted)};
}

void WandTestCase::AssertResults(const irs::DirectoryReader& index,
                                 const irs::Filter& filter,
                                 irs::ScorerPtr scorer, bool can_use_wand,
                                 size_t limit) {
  auto wand_result = Collect(index, filter, scorer, can_use_wand, limit);
  auto result = Collect(index, filter, scorer, false, limit);
  ASSERT_EQ(result, wand_result);
}

irs::IndexWriterOptions WandTestCase::GetWriterOptions(irs::ScorerPtr scorer,
                                                       bool write_norms) {
  irs::IndexWriterOptions writer_options;
  writer_options.reader_options.scorer = scorer;
  writer_options.features = [write_norms](irs::IndexFeatures id) {
    if (write_norms && irs::IndexFeatures::Norm == id) {
      return std::make_pair(
        irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
        &irs::Norm::MakeWriter);
    }

    return std::make_pair(
      irs::ColumnInfo{irs::Type<irs::compression::None>::get(), {}, false},
      irs::FeatureWriterFactory{});
  };

  return writer_options;
}

void WandTestCase::GenerateSegment(irs::ScorerPtr scorer, bool write_norms,
                                   bool append_data) {
  tests::JsonDocGenerator gen(
    resource("simple_single_column_multi_term.json"),
    [](tests::Document& doc, std::string_view name,
       const tests::JsonDocGenerator::JsonValue& data) {
      using TextField = tests::TextField<std::string>;

      if (tests::JsonDocGenerator::ValueType::STRING == data.vt) {
        auto f =
          std::make_shared<TextField>(std::string{name}, data.str, false);
        f->index_features |= irs::IndexFeatures::Norm;
        doc.indexed.push_back(f);
      }
    });

  auto open_mode = irs::kOmCreate;
  if (append_data) {
    open_mode |= irs::kOmAppend;
  }

  add_segment(gen, open_mode, GetWriterOptions(scorer, write_norms));
}

void WandTestCase::GenerateSegmentMinNorm(irs::ScorerPtr scorer) {
  tests::JsonDocGenerator gen(
    resource("simple_single_column_multi_term_norm.json"),
    [](tests::Document& doc, std::string_view name,
       const tests::JsonDocGenerator::JsonValue& data) {
      using TextField = tests::TextField<std::string>;

      if (tests::JsonDocGenerator::ValueType::STRING == data.vt) {
        auto f =
          std::make_shared<TextField>(std::string{name}, data.str, false);
        f->index_features |= irs::IndexFeatures::Norm;
        doc.indexed.push_back(f);
      }
    });

  auto open_mode = irs::kOmCreate;
  // if (append_data) {
  //   open_mode |= irs::OM_APPEND;
  // }

  add_segment(gen, open_mode, GetWriterOptions(scorer, true));
}

void WandTestCase::AssertTermFilter(const irs::Scorer& scorer) {
  static constexpr std::string_view kFieldName = "name";

  irs::ByTerm filter;
  *filter.mutable_field() = kFieldName;

  auto reader = irs::DirectoryReader{
    dir(), codec(), irs::IndexReaderOptions{.scorer = &scorer}};
  ASSERT_NE(nullptr, reader);

  for (const auto& segment : reader) {
    const auto* field = segment.field(kFieldName);
    ASSERT_NE(nullptr, field);

    const auto can_use_wand = CanUseWand(scorer, *field);
    // TODO(mbkkt) enable this!
    // ASSERT_EQ(can_use_wand, field->has_scorer(wand_index));

    for (auto terms = field->iterator(irs::SeekMode::NORMAL); terms->next();) {
      filter.mutable_options()->term = terms->value();

      AssertResults(reader, filter, &scorer, can_use_wand, 10);
      AssertResults(reader, filter, &scorer, can_use_wand, 100);
    }
  }
}

void WandTestCase::AssertConjunctionFilter(const irs::Scorer& scorer) {
  static constexpr std::string_view kFieldName = "name";

  irs::And conjunction;
  irs::ByTerm& filter1 = conjunction.add<irs::ByTerm>();
  *filter1.mutable_field() = kFieldName;
  irs::ByTerm& filter2 = conjunction.add<irs::ByTerm>();
  *filter2.mutable_field() = kFieldName;

  auto reader = irs::DirectoryReader{
    dir(), codec(), irs::IndexReaderOptions{.scorer = &scorer}};
  ASSERT_NE(nullptr, reader);

  for (const auto& segment : reader) {
    const auto* field = segment.field(kFieldName);
    ASSERT_NE(nullptr, field);

    const auto can_use_wand = CanUseWand(scorer, *field);
    // TODO(mbkkt) enable this!
    // ASSERT_EQ(can_use_wand, field->has_scorer(wand_index));

    auto terms = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(terms->next());
    filter1.mutable_options()->term = terms->value();
    ASSERT_TRUE(terms->next());
    filter2.mutable_options()->term = terms->value();

    AssertResults(reader, conjunction, &scorer, can_use_wand, 10);
    AssertResults(reader, conjunction, &scorer, can_use_wand, 100);
  }
}

void WandTestCase::AssertDisjunctionFilter(const irs::Scorer& scorer) {
  static constexpr std::string_view kFieldName = "name";

  irs::Or disjunction;
  irs::ByTerm& filter1 = disjunction.add<irs::ByTerm>();
  *filter1.mutable_field() = kFieldName;
  irs::ByTerm& filter2 = disjunction.add<irs::ByTerm>();
  *filter2.mutable_field() = kFieldName;
  irs::ByTerm& filter3 = disjunction.add<irs::ByTerm>();
  *filter3.mutable_field() = kFieldName;

  auto reader = irs::DirectoryReader{
    dir(), codec(), irs::IndexReaderOptions{.scorer = &scorer}};
  ASSERT_NE(nullptr, reader);

  for (const auto& segment : reader) {
    const auto* field = segment.field(kFieldName);
    ASSERT_NE(nullptr, field);

    const auto can_use_wand = CanUseWand(scorer, *field);
    // TODO(mbkkt) enable this!
    // ASSERT_EQ(can_use_wand, field->has_scorer(wand_index));

    auto terms = field->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(terms->next());
    filter1.mutable_options()->term = terms->value();
    ASSERT_TRUE(terms->next());
    filter2.mutable_options()->term = terms->value();
    ASSERT_TRUE(terms->next());
    filter3.mutable_options()->term = terms->value();

    AssertResults(reader, disjunction, &scorer, can_use_wand, 10);
    AssertResults(reader, disjunction, &scorer, can_use_wand, 100);
  }
}

TEST_P(WandTestCase, TermFilterTFIDF) {
  auto scorer = std::make_unique<irs::TFIDF>(false);

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterTFIDFWithNorms) {
  auto scorer = std::make_unique<irs::TFIDF>(true);

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterBM25) {
  auto scorer = std::make_unique<irs::BM25>();
  ASSERT_FALSE(scorer->IsBM15());
  ASSERT_FALSE(scorer->IsBM11());

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);

  GenerateSegmentMinNorm(scorer.get());
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterBM15) {
  auto scorer = std::make_unique<irs::BM25>(irs::BM25::K(), 0.f);
  ASSERT_TRUE(scorer->IsBM15());

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterBM11) {
  auto scorer = std::make_unique<irs::BM25>(irs::BM25::K(), 1.f);
  ASSERT_TRUE(scorer->IsBM11());

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterBM01) {
  auto scorer = std::make_unique<irs::BM25>(irs::BM25::K(), 0.1f);

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterBM02) {
  auto scorer = std::make_unique<irs::BM25>(irs::BM25::K(), 0.2f);

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

TEST_P(WandTestCase, TermFilterBM04) {
  auto scorer = std::make_unique<irs::BM25>(irs::BM25::K(), 0.4f);

  GenerateSegment(scorer.get(), true);
  AssertFilters(scorer.get(), false);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5simd"}));

INSTANTIATE_TEST_SUITE_P(WandTest, WandTestCase, kTestValues,
                         WandTestCase::to_string);

}  // namespace
