////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include <functional>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/tfidf.hpp>
#include <iresearch/utils/ngram_match_utils.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace tests {
namespace {

irs::ByNGramSimilarity MakeFilter(const std::string_view& field,
                                  const std::vector<std::string_view>& ngrams,
                                  float_t threshold = 1.f,
                                  bool allow_phrase = true) {
  irs::ByNGramSimilarity filter;
  *filter.mutable_field() = field;
  auto* opts = filter.mutable_options();
  for (auto& ngram : ngrams) {
    opts->ngrams.emplace_back(irs::ViewCast<irs::byte_type>(ngram));
  }
  opts->threshold = threshold;
  opts->allow_phrase = allow_phrase;
  return filter;
}

class CustomNGramScorer : public sort::CustomSort {
 public:
  using CustomSort::CustomSort;

  irs::IndexFeatures GetIndexFeatures() const final {
    return irs::IndexFeatures::Freq;
  }
};

irs::score_t GetFilterBoost(const irs::DocIterator::ptr& doc) {
  const auto* filter_boost = irs::get<irs::FilterBoost>(*doc);
  return filter_boost != nullptr ? filter_boost->value : 1.F;
}

}  // namespace

TEST(ngram_similarity_base_test, options) {
  irs::ByNGramSimilarityOptions opts;
  ASSERT_TRUE(opts.ngrams.empty());
  ASSERT_EQ(1.f, opts.threshold);
}

TEST(ngram_similarity_base_test, ctor) {
  irs::ByNGramSimilarity q;
  ASSERT_EQ(irs::Type<irs::ByNGramSimilarity>::id(), q.type());
  ASSERT_EQ(irs::ByNGramSimilarityOptions{}, q.options());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
  ASSERT_EQ("", q.field());

  static_assert((irs::IndexFeatures::Freq | irs::IndexFeatures::Pos) ==
                irs::NGramSimilarityQuery::kRequiredFeatures);
}

TEST(ngram_similarity_base_test, equal) {
  ASSERT_EQ(irs::ByNGramSimilarity(), irs::ByNGramSimilarity());

  {
    irs::ByNGramSimilarity q0 = MakeFilter("a", {"1", "2"}, 0.5f);
    irs::ByNGramSimilarity q1 = MakeFilter("a", {"1", "2"}, 0.5f);
    ASSERT_EQ(q0, q1);

    // different threshold
    irs::ByNGramSimilarity q2 = MakeFilter("a", {"1", "2"}, 0.25f);
    ASSERT_NE(q0, q2);

    // different terms
    irs::ByNGramSimilarity q3 = MakeFilter("a", {"1", "3"}, 0.5f);
    ASSERT_NE(q0, q3);

    // different terms count (less)
    irs::ByNGramSimilarity q4 = MakeFilter("a", {"1"}, 0.5f);
    ASSERT_NE(q0, q4);

    // different terms count (more)
    irs::ByNGramSimilarity q5 = MakeFilter("a", {"1", "2", "2"}, 0.5f);
    ASSERT_NE(q0, q5);

    // different field
    irs::ByNGramSimilarity q6 = MakeFilter("b", {"1", "2"}, 0.5f);
    ASSERT_NE(q0, q6);
  }
}

class NGramSimilarityFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  static irs::FeatureInfoProvider FeaturesWithNorms() {
    return [](irs::IndexFeatures id) {
      const irs::ColumnInfo info{
        irs::Type<irs::compression::Lz4>::get(), {}, false};

      if (irs::IndexFeatures::Norm == id) {
        return std::make_pair(info, &irs::Norm::MakeWriter);
      }

      return std::make_pair(info, irs::FeatureWriterFactory{});
    };
  }
};

TEST_P(NGramSimilarityFilterTestCase, boost) {
  // no boost
  {
    tests::JsonDocGenerator gen(
      R"([{ "field": [ "1", "3", "4", "5", "6", "7", "2"] }])",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());
  auto& segment = rdr[0];

  {
    MaxMemoryCounter counter;

    // no terms no field
    {
      irs::ByNGramSimilarity q;

      auto prepared = q.prepare({
        .index = segment,
        .memory = counter,
      });
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_EQ(counter.max, 0);
    counter.Reset();

    // simple disjunction
    {
      irs::ByNGramSimilarity q = MakeFilter("field", {"1", "2"}, 0.5f);

      auto prepared = q.prepare({
        .index = segment,
        .memory = counter,
      });
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // multiple terms
    {
      irs::ByNGramSimilarity q =
        MakeFilter("field", {"1", "2", "3", "4"}, 0.5f);

      auto prepared = q.prepare({
        .index = segment,
        .memory = counter,
      });
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }

  // with boost
  {
    MaxMemoryCounter counter;

    irs::score_t boost = 1.5f;

    // no terms, return empty query
    {
      irs::ByNGramSimilarity q;
      q.boost(boost);

      auto prepared = q.prepare({
        .index = segment,
        .memory = counter,
      });
      ASSERT_EQ(irs::kNoBoost, prepared->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_EQ(counter.max, 0);
    counter.Reset();

    // simple disjunction
    {
      irs::ByNGramSimilarity q = MakeFilter("field", {"1", "2"}, 0.5f);
      q.boost(boost);

      auto prepared = q.prepare({
        .index = segment,
        .memory = counter,
      });
      ASSERT_EQ(boost, prepared->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();

    // multiple terms
    {
      irs::ByNGramSimilarity q =
        MakeFilter("field", {"1", "2", "3", "4"}, 0.5f);
      q.boost(boost);

      auto prepared = q.prepare({
        .index = segment,
        .memory = counter,
      });
      ASSERT_EQ(boost, prepared->Boost());
    }
    EXPECT_EQ(counter.current, 0);
    EXPECT_GT(counter.max, 0);
    counter.Reset();
  }
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_1) {
  // sequence 1 3 4 ______ 2 -> longest is 134 not 12
  // add segment
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"3\", \"4\", \"5\", \"6\", "
      "\"7\", \"2\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"1", "2", "3", "4"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(0.75, GetFilterBoost(docs));
    const std::string_view rhs = "134";
    const std::string_view lhs = "1234";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(1, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_2) {
  // sequence 1 1 2 2 3 3 4 4 -> longest is 1234  and freq should be 1 not 2 as
  // this sequence could not be built twice one after another but only
  // intereaved
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"1\", \"2\", \"2\", \"3\", "
      "\"3\", \"4\", \"4\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"1", "2", "3", "4"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(1, GetFilterBoost(docs));
    const std::string_view rhs = "11223344";
    const std::string_view lhs = "1234";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(1, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_3) {
  // sequence 1 2 1 1 3 4 -> longest is 1234  not 134!
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"2\", \"1\", \"1\", \"3\", "
      "\"4\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"1", "2", "3", "4"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(1, GetFilterBoost(docs));
    const std::string_view rhs = "121134";
    const std::string_view lhs = "1234";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(1, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_4) {
  // sequence 1 2 1 1 1 1 pattern 1 1 -> longest is 1 1 and frequency is 2
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"2\", \"1\", \"1\", \"1\", "
      "\"1\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter = MakeFilter("field", {"1", "1"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(1, GetFilterBoost(docs));
    const std::string_view rhs = "121111";
    const std::string_view lhs = "11";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(2, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_5) {
  // sequence 1 2 1 2 1 2 1 2 1 2 1 2 1 2 1 pattern 1 2 1 -> longest is 1 2 1
  // and frequency is 4
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"2\", \"1\", \"2\", \"1\", "
      " \"2\", \"1\", \"2\", \"1\", \"2\", \"1\", \"2\", \"1\", \"2\", "
      "\"1\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter = MakeFilter("field", {"1", "2", "1"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(1, GetFilterBoost(docs));
    const std::string_view rhs = "121212121212121";
    const std::string_view lhs = "121";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(4, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_6) {
  // sequence 1 1 pattern 1 1 -> longest is 1 1 and frequency is 1
  // checks seek for second term does not  skips it at all
  {
    tests::JsonDocGenerator gen("[{ \"seq\" : 1, \"field\": [ \"1\", \"1\"] }]",
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter = MakeFilter("field", {"1", "1"}, 1.0f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(1, GetFilterBoost(docs));
    const std::string_view rhs = "11";
    const std::string_view lhs = "11";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(1, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_7) {
  // sequence 2 4 2 4 1 3 1 3 pattern 1 2 3 4-> longest is 1 3  and 2 4 but
  // frequency is 2 as only first longest counted
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"2\", \"4\", \"2\", \"4\", \"1\", "
      "\"3\", \"1\", \"3\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"1", "2", "3", "4"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(0.5, GetFilterBoost(docs));
    const std::string_view rhs = "24241313";
    const std::string_view lhs = "1234";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(2, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_8) {
  // sequence 1 2 3 4 pattern 1 5 6 2 -> longest is 1 2  and  boost 0.5
  // as only first longest counted
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"2\", \"3\", \"4\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"1", "5", "6", "2"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(0.5, GetFilterBoost(docs));
    const std::string_view lhs = "1234";
    const std::string_view rhs = "1562";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(1, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_9) {
  // bulk pos read check (for future optimization)
  // sequence 1 1 2 3 4 5 1  pattern 1 2 3 4 5 1 -> longest is 1 2 3 4 5 1   and
  // boost 1 and frequency 1
  {
    tests::JsonDocGenerator gen(
      "[{ \"seq\" : 1, \"field\": [ \"1\", \"1\", \"2\", \"3\", \"4\", "
      "\"5\", \"1\"] }]",
      &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"1", "2", "3", "4", "5", "1"}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    ASSERT_TRUE(bool(doc));
    ASSERT_TRUE(bool(frequency));
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_FALSE(irs::doc_limits::eof(doc->value));
    ASSERT_DOUBLE_EQ(1., GetFilterBoost(docs));
    const std::string_view rhs = "1123451";
    const std::string_view lhs = "123451";
    ASSERT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    ASSERT_EQ(1, frequency->value);
    ASSERT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, check_matcher_10) {
  // bulk pos read check (for future optimization)
  // sequence '' pattern '' -> longest is ''   and  boost 1 and frequency 1
  {
    tests::JsonDocGenerator gen("[{ \"seq\" : 1, \"field\": [ \"\"] }]",
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter = MakeFilter("field", {""}, 0.5f);

  CustomNGramScorer sort;
  auto prepared_order = irs::Scorers::Prepare(sort);
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({
      .segment = sub,
      .scorers = prepared_order,
    });
    auto* doc = irs::get<irs::DocAttr>(*docs);
    auto* frequency = irs::get<irs::FreqAttr>(*docs);
    // ensure all iterators contain  attributes
    EXPECT_TRUE(bool(doc));
    EXPECT_TRUE(bool(frequency));
    EXPECT_TRUE(docs->next());
    EXPECT_EQ(docs->value(), doc->value);
    EXPECT_FALSE(irs::doc_limits::eof(doc->value));
    EXPECT_DOUBLE_EQ(1., GetFilterBoost(docs));
    const std::string_view rhs = "";
    const std::string_view lhs = "";
    EXPECT_DOUBLE_EQ(GetFilterBoost(docs),
                     (irs::ngram_similarity<char, true>(
                       lhs.data(), lhs.size(), rhs.data(), rhs.size(), 1)));
    EXPECT_EQ(1, frequency->value);
    EXPECT_FALSE(docs->next());
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, no_match_case) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"ee", "we", "qq", "rr", "ff", "never_match"}, 0.1f);

  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, no_serial_match_case) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"ee", "ss", "pa", "rr"}, 0.5f);

  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    ASSERT_FALSE(docs->next());
    ASSERT_EQ(docs->value(), doc->value);
    ASSERT_TRUE(irs::doc_limits::eof(doc->value));
  }
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, one_match_case) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"ee", "ss", "qq", "rr", "ff", "never_match"}, 0.1f);

  Docs expected{1, 3, 5, 6, 7, 8, 9, 10, 12};
  const size_t expected_size = expected.size();
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, missed_last_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"at", "tl", "la", "as", "ll", "never_match"}, 0.5f);

  Docs expected{1, 2, 5, 8, 11, 12, 13};
  const size_t expected_size = expected.size();
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, missed_first_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);

  Docs expected{1, 2, 5, 8, 11, 12, 13};
  const size_t expected_size = expected.size();
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, not_miss_match_for_tail) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"at", "tl", "la", "as", "ll", "never_match"}, 0.33f);

  Docs expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
  const size_t expected_size = expected.size();
  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, missed_middle_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"at", "never_match", "la", "as", "ll"}, 0.333f);

  Docs expected{1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13, 14};
  const size_t expected_size = expected.size();

  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, missed_middle2_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter = MakeFilter(
    "field", {"at", "never_match", "never_match2", "la", "as", "ll"}, 0.5f);

  Docs expected{1, 2, 5, 8, 11, 12, 13};
  const size_t expected_size = expected.size();

  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, missed_middle3_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter = MakeFilter(
    "field", {"at", "never_match", "tl", "never_match2", "la", "as", "ll"},
    0.28f);

  Docs expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
  const size_t expected_size = expected.size();

  auto prepared = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  size_t count = 0;
  for (const auto& sub : rdr) {
    auto docs = prepared->execute({.segment = sub});

    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    while (docs->next()) {
      ASSERT_EQ(docs->value(), doc->value);
      expected.erase(
        std::remove(expected.begin(), expected.end(), docs->value()),
        expected.end());
      ++count;
    }
  }
  ASSERT_EQ(expected_size, count);
  ASSERT_EQ(0, expected.size());
  prepared.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

struct TestScoreCtx final : public irs::ScoreCtx {
  TestScoreCtx(std::vector<size_t>* f, const irs::FreqAttr* p,
               std::vector<irs::score_t>* b,
               const irs::FilterBoost* fb) noexcept
    : freq(f), filter_boost(b), freq_from_filter(p), boost_from_filter(fb) {}

  std::vector<size_t>* freq;
  std::vector<irs::score_t>* filter_boost;
  const irs::FreqAttr* freq_from_filter;
  const irs::FilterBoost* boost_from_filter;
};

TEST_P(NGramSimilarityFilterTestCase, missed_last_scored_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"at", "tl", "la", "as", "ll", "never_match"}, 0.5f);

  Docs expected{1, 2, 5, 8, 11, 12, 13};
  size_t collect_field_count = 0;
  size_t collect_term_count = 0;
  size_t finish_count = 0;
  std::vector<size_t> frequency;
  std::vector<irs::score_t> filter_boost;

  irs::Scorer::ptr order{std::make_unique<CustomNGramScorer>()};
  auto& scorer = static_cast<CustomNGramScorer&>(*order);

  scorer.collector_collect_field = [&collect_field_count](
                                     const irs::SubReader&,
                                     const irs::TermReader&) -> void {
    ++collect_field_count;
  };
  scorer.collector_collect_term =
    [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                          const irs::AttributeProvider&) -> void {
    ++collect_term_count;
  };
  scorer.collectors_collect =
    [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                    const irs::TermCollector*) -> void { ++finish_count; };
  scorer.prepare_field_collector = [&scorer]() -> irs::FieldCollector::ptr {
    return std::make_unique<CustomNGramScorer::FieldCollector>(scorer);
  };
  scorer.prepare_term_collector = [&scorer]() -> irs::TermCollector::ptr {
    return std::make_unique<CustomNGramScorer::TermCollector>(scorer);
  };
  scorer.prepare_scorer =
    [&frequency, &filter_boost](
      const irs::ColumnProvider& /*segment*/,
      const irs::FieldProperties& /*term*/, const irs::byte_type* /*stats_buf*/,
      const irs::AttributeProvider& attr, irs::score_t) -> irs::ScoreFunction {
    auto* freq = irs::get<irs::FreqAttr>(attr);
    auto* boost = irs::get<irs::FilterBoost>(attr);
    return irs::ScoreFunction::Make<TestScoreCtx>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& freq = *reinterpret_cast<TestScoreCtx*>(ctx);
        freq.freq->push_back(freq.freq_from_filter->value);
        freq.filter_boost->push_back(freq.boost_from_filter->value);
        *res = {};
      },
      irs::ScoreFunction::DefaultMin, &frequency, freq, &filter_boost, boost);
  };
  std::vector<size_t> expected_frequency{1, 1, 2, 1, 1, 1, 1};
  std::vector<irs::score_t> expected_filter_boost{
    4.f / 6.f, 4.f / 6.f, 4.f / 6.f, 4.f / 6.f, 0.5, 0.5, 0.5};
  CheckQuery(filter, std::span{&order, 1}, expected, rdr);
  ASSERT_EQ(expected_frequency, frequency);
  ASSERT_EQ(expected_filter_boost.size(), filter_boost.size());
  for (size_t i = 0; i < expected_filter_boost.size(); ++i) {
    SCOPED_TRACE(testing::Message("i=") << i);
    ASSERT_DOUBLE_EQ(expected_filter_boost[i], filter_boost[i]);
  }
  ASSERT_EQ(1, collect_field_count);
  ASSERT_EQ(5, collect_term_count);
  ASSERT_EQ(collect_field_count + collect_term_count, finish_count);
}

TEST_P(NGramSimilarityFilterTestCase, missed_frequency_test) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);

  Docs expected{1, 2, 5, 8, 11, 12, 13};
  size_t collect_field_count = 0;
  size_t collect_term_count = 0;
  size_t finish_count = 0;
  std::vector<size_t> frequency;
  std::vector<irs::score_t> filter_boost;

  irs::Scorer::ptr order{std::make_unique<CustomNGramScorer>()};
  auto& scorer = static_cast<CustomNGramScorer&>(*order);

  scorer.collector_collect_field = [&collect_field_count](
                                     const irs::SubReader&,
                                     const irs::TermReader&) -> void {
    ++collect_field_count;
  };
  scorer.collector_collect_term =
    [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                          const irs::AttributeProvider&) -> void {
    ++collect_term_count;
  };
  scorer.collectors_collect =
    [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                    const irs::TermCollector*) -> void { ++finish_count; };
  scorer.prepare_field_collector = [&scorer]() -> irs::FieldCollector::ptr {
    return std::make_unique<CustomNGramScorer::FieldCollector>(scorer);
  };
  scorer.prepare_term_collector = [&scorer]() -> irs::TermCollector::ptr {
    return std::make_unique<CustomNGramScorer::TermCollector>(scorer);
  };
  scorer.prepare_scorer =
    [&frequency, &filter_boost](
      const irs::ColumnProvider& /*segment*/,
      const irs::FieldProperties& /*term*/, const irs::byte_type* /*stats_buf*/,
      const irs::AttributeProvider& attr, irs::score_t) -> irs::ScoreFunction {
    auto* freq = irs::get<irs::FreqAttr>(attr);
    auto* boost = irs::get<irs::FilterBoost>(attr);
    return irs::ScoreFunction::Make<TestScoreCtx>(
      [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
        const auto& freq = *static_cast<TestScoreCtx*>(ctx);
        freq.freq->push_back(freq.freq_from_filter->value);
        freq.filter_boost->push_back(freq.boost_from_filter->value);
        *res = {};
      },
      irs::ScoreFunction::DefaultMin, &frequency, freq, &filter_boost, boost);
  };
  std::vector<size_t> expected_frequency{1, 1, 2, 1, 1, 1, 1};
  std::vector<irs::score_t> expected_filter_boost{
    4.f / 6.f, 4.f / 6.f, 4.f / 6.f, 4.f / 6.f, 0.5, 0.5, 0.5};
  CheckQuery(filter, std::span{&order, 1}, expected, rdr);
  ASSERT_EQ(expected_frequency, frequency);
  ASSERT_EQ(expected_filter_boost.size(), filter_boost.size());
  for (size_t i = 0; i < expected_filter_boost.size(); ++i) {
    SCOPED_TRACE(testing::Message("i=") << i);
    ASSERT_DOUBLE_EQ(expected_filter_boost[i], filter_boost[i]);
  }
  ASSERT_EQ(1, collect_field_count);
  ASSERT_EQ(5, collect_term_count);
  ASSERT_EQ(collect_field_count + collect_term_count, finish_count);
}

TEST_P(NGramSimilarityFilterTestCase, missed_first_tfidf_norm_test) {
  {
    irs::IndexWriterOptions opts;
    opts.features = FeaturesWithNorms();

    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::NormalizedStringJsonFieldFactory);

    add_segment(gen, irs::kOmCreate, opts);
  }

  auto rdr = open_reader();

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);

  Docs expected{11, 12, 8, 13, 5, 1, 2};

  irs::Scorer::ptr scorer{std::make_unique<irs::TFIDF>(true)};

  CheckQuery(filter, std::span{&scorer, 1}, expected, rdr);
}

TEST_P(NGramSimilarityFilterTestCase, all_match_ngram_score_test) {
  {
    irs::IndexWriterOptions opts;
    opts.features = FeaturesWithNorms();

    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::NormalizedStringJsonFieldFactory);

    add_segment(gen, irs::kOmCreate, opts);
  }

  auto rdr = open_reader();

  irs::Scorer::ptr scorers[] = {
    std::make_unique<irs::TFIDF>(false),
    std::make_unique<irs::TFIDF>(true),
    std::make_unique<irs::BM25>(),
    std::make_unique<irs::BM25>(0.F),                  // BM1
    std::make_unique<irs::BM25>(irs::BM25::K(), 1.F),  // BM11
    std::make_unique<irs::BM25>(irs::BM25::K(), 0.F),  // BM15
  };

  std::vector<irs::doc_id_t> ngram;
  std::vector<irs::doc_id_t> phrase;
  for (auto& scorer : scorers) {
    irs::ByNGramSimilarity ngram_filter =
      MakeFilter("field", {"at", "tl", "la", "as"}, 1.F, false);
    irs::ByNGramSimilarity phrase_filter =
      MakeFilter("field", {"at", "tl", "la", "as"}, 1.F, true);

    MakeResult(ngram_filter, std::span{&scorer, 1}, rdr, ngram);
    MakeResult(phrase_filter, std::span{&scorer, 1}, rdr, phrase);
    EXPECT_EQ(ngram, phrase);
  }
}

TEST_P(NGramSimilarityFilterTestCase, missed_first_tfidf_test) {
  {
    irs::IndexWriterOptions opts;
    opts.features = FeaturesWithNorms();

    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::NormalizedStringJsonFieldFactory);

    add_segment(gen, irs::kOmCreate, opts);
  }

  auto rdr = open_reader();

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);

  Docs expected{11, 12, 13, 1, 2, 8, 5};

  irs::Scorer::ptr scorer{std::make_unique<irs::TFIDF>(false)};

  CheckQuery(filter, std::span{&scorer, 1}, expected, rdr);
}

TEST_P(NGramSimilarityFilterTestCase, missed_first_bm25_test) {
  {
    irs::IndexWriterOptions opts;
    opts.features = FeaturesWithNorms();

    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::NormalizedStringJsonFieldFactory);

    add_segment(gen, irs::kOmCreate, opts);
  }

  auto rdr = open_reader();

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);

  Docs expected{11, 12, 8, 13, 1, 5, 2};

  irs::Scorer::ptr scorer{std::make_unique<irs::BM25>()};

  CheckQuery(filter, std::span{&scorer, 1}, expected, rdr);
}

TEST_P(NGramSimilarityFilterTestCase, missed_first_bm15_test) {
  {
    irs::IndexWriterOptions opts;
    opts.features = FeaturesWithNorms();

    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::NormalizedStringJsonFieldFactory);

    add_segment(gen, irs::kOmCreate, opts);
  }

  auto rdr = open_reader();

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);

  Docs expected{11, 12, 13, 1, 2, 8, 5};

  irs::Scorer::ptr bm15{std::make_unique<irs::BM25>(irs::BM25::K(), 0.f)};

  CheckQuery(filter, std::span{&bm15, 1}, expected, rdr);
}

TEST_P(NGramSimilarityFilterTestCase, seek_next) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);
  Docs expected{1, 2, 5, 8, 11, 12, 13};
  auto expected_it = std::begin(expected);
  auto prepared_filter = filter.prepare({
    .index = rdr,
    .memory = counter,
  });
  for (const auto& sub : rdr) {
    auto docs = prepared_filter->execute({.segment = sub});
    auto* doc = irs::get<irs::DocAttr>(*docs);
    ASSERT_TRUE(
      bool(doc));  // ensure all iterators contain "document" attribute
    ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
    while (docs->next()) {
      ASSERT_EQ(docs->value(), *expected_it);
      ASSERT_EQ(doc->value, docs->value());
      // seek same
      ASSERT_EQ(*expected_it, docs->seek(*expected_it));
      // seek backward
      ASSERT_EQ(*expected_it, docs->seek((*expected_it) - 1));
      ++expected_it;
      if (expected_it != std::end(expected)) {
        // seek forward
        ASSERT_EQ(*expected_it, docs->seek(*expected_it));
        ++expected_it;
      }
    }
    ASSERT_EQ(irs::doc_limits::eof(), docs->value());
    ASSERT_FALSE(docs->next());
    ASSERT_EQ(irs::doc_limits::eof(), docs->value());
  }
  ASSERT_EQ(expected_it, std::end(expected));
  prepared_filter.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(NGramSimilarityFilterTestCase, seek) {
  {
    tests::JsonDocGenerator gen(resource("ngram_similarity.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  MaxMemoryCounter counter;

  irs::ByNGramSimilarity filter =
    MakeFilter("field", {"never_match", "at", "tl", "la", "as", "ll"}, 0.5f);
  Docs seek_tagrets{2, 5, 8, 13};
  auto seek_it = std::begin(seek_tagrets);
  auto& prepared_order = irs::Scorers::kUnordered;
  auto prepared_filter = filter.prepare({
    .index = rdr,
    .memory = counter,
    .scorers = prepared_order,
  });
  for (const auto& sub : rdr) {
    while (std::end(seek_tagrets) != seek_it) {
      auto docs = prepared_filter->execute({
        .segment = sub,
        .scorers = prepared_order,
      });
      auto* doc = irs::get<irs::DocAttr>(*docs);
      ASSERT_TRUE(
        bool(doc));  // ensure all iterators contain "document" attribute
      ASSERT_EQ(irs::doc_limits::invalid(), docs->value());
      ASSERT_EQ(doc->value, docs->value());
      auto actual_seeked = docs->seek(*seek_it);
      ASSERT_EQ(doc->value, docs->value());
      if (actual_seeked == *seek_it) {
        ASSERT_EQ(docs->seek(*seek_it), *seek_it);
        ASSERT_EQ(docs->seek((*seek_it) - 1), *seek_it);
        ASSERT_EQ(doc->value, docs->value());
        ++seek_it;
      }
      if (actual_seeked == irs::doc_limits::eof()) {
        // go try next subreader
        break;
      }
    }
  }
  ASSERT_EQ(std::end(seek_tagrets), seek_it);
  prepared_filter.reset();
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  ngram_similarity_test, NGramSimilarityFilterTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  NGramSimilarityFilterTestCase::to_string);

}  // namespace tests
