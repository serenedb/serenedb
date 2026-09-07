////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "basics/down_cast.h"
#include "basics/misc.hpp"
#include "filter_test_case_base.hpp"
#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/bm25.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"
#include "tests_shared.hpp"

namespace {

// Stable per-name field ids for the Levenshtein tests. The on-disk index
// keys fields by id; tests still address them by JSON key from the
// resource and translate through this helper.
// Stable per-name field ids, sourced from `tests::FieldIdFor` so the
// shared JSON factories and these tests agree on which id a name maps to.
[[maybe_unused]] inline constexpr irs::field_id kFieldId =
  tests::FieldIdFor("field");
[[maybe_unused]] inline constexpr irs::field_id kField1Id =
  tests::FieldIdFor("field1");
[[maybe_unused]] inline constexpr irs::field_id kFooId =
  tests::FieldIdFor("foo");
[[maybe_unused]] inline constexpr irs::field_id kIdId = tests::FieldIdFor("id");
[[maybe_unused]] inline constexpr irs::field_id kTitleId =
  tests::FieldIdFor("title");
[[maybe_unused]] inline constexpr irs::field_id kPrefixId =
  tests::FieldIdFor("prefix");

irs::field_id FieldIdFor(std::string_view name) {
  return tests::FieldIdFor(name);
}

irs::ByTerm MakeTermFilter(const std::string_view& field,
                           const std::string_view term) {
  irs::ByTerm q;
  *q.mutable_field_id() = FieldIdFor(field);
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

irs::ByEditDistance MakeFilter(const std::string_view& field,
                               const std::string_view term,
                               irs::byte_type max_distance = 0,
                               size_t max_terms = 0,
                               bool with_transpositions = false,
                               const std::string_view prefix = "") {
  irs::ByEditDistance q;
  *q.mutable_field_id() = FieldIdFor(field);
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  q.mutable_options()->max_distance = max_distance;
  q.mutable_options()->max_terms = max_terms;
  q.mutable_options()->with_transpositions = with_transpositions;
  q.mutable_options()->prefix = irs::ViewCast<irs::byte_type>(prefix);
  return q;
}

irs::Filter::ptr Lower(irs::ByEditDistance filter) {
  irs::Filter::ptr ptr =
    std::make_unique<irs::ByEditDistance>(std::move(filter));
  irs::Optimize(ptr);
  return ptr;
}

irs::Filter::ptr MakeLevenshtein(const std::string_view& field,
                                 const std::string_view term,
                                 irs::byte_type max_distance = 0,
                                 size_t max_terms = 0,
                                 bool with_transpositions = false,
                                 const std::string_view prefix = "") {
  return Lower(MakeFilter(field, term, max_distance, max_terms,
                          with_transpositions, prefix));
}

}  // namespace

class LevenshteinFilterTestCase : public tests::FilterTestCaseBase {};

TEST(by_edit_distance_test, options) {
  irs::ByEditDistanceOptions opts;
  ASSERT_EQ(0, opts.max_distance);
  ASSERT_EQ(0, opts.max_terms);
  ASSERT_FALSE(opts.with_transpositions);
  ASSERT_TRUE(opts.term.empty());
}

TEST(by_edit_distance_test, ctor) {
  irs::ByEditDistance q;
  ASSERT_EQ(irs::Type<irs::ByEditDistance>::id(), q.type());
  ASSERT_EQ(irs::ByEditDistanceOptions{}, q.options());
  ASSERT_FALSE(irs::field_limits::valid(q.field_id()));
  ASSERT_EQ(irs::kNoBoost, q.GetBoost());
}

TEST(by_edit_distance_test, equal) {
  const irs::ByEditDistance q = MakeFilter("field", "bar", 1, 0, true);

  ASSERT_EQ(q, MakeFilter("field", "bar", 1, 0, true));
  ASSERT_NE(q, MakeFilter("field1", "bar", 1, 0, true));
  ASSERT_NE(q, MakeFilter("field", "baz", 1, 0, true));
  ASSERT_NE(q, MakeFilter("field", "bar", 2, 0, true));
  ASSERT_NE(q, MakeFilter("field", "bar", 1, 1024, true));
  ASSERT_NE(q, MakeFilter("field", "bar", 1, 0, false));
  {
    irs::ByPrefix rhs;
    *rhs.mutable_field_id() = kFieldId;
    rhs.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("bar"));
    ASSERT_NE(q, rhs);
  }
}

TEST(by_edit_distance_test, boost) {
  MaxMemoryCounter counter;

  // no boost
  {
    irs::ByEditDistance q;
    *q.mutable_field_id() = kFieldId;
    q.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("bar*"));

    irs::Filter::ptr lowered = Lower(std::move(q));
    ASSERT_EQ(irs::kNoBoost, lowered->GetBoost());
  }

  // with boost
  {
    irs::score_t boost = 1.5f;

    irs::ByEditDistance q;
    *q.mutable_field_id() = kFieldId;
    q.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("bar*"));
    q.SetBoost(boost);

    irs::Filter::ptr lowered = Lower(std::move(q));
    ASSERT_EQ(boost, lowered->GetBoost());

    // a segment without the field matches nothing, and nothing carries no
    // boost -- so the boost is only observable where the field exists
    tests::PreparedFilter prepared{*lowered, irs::SubReader::empty(), nullptr,
                                   counter};
    ASSERT_TRUE(irs::QueryBuilder::IsEmpty(*prepared.Query(0)));
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  counter.Reset();
}

TEST(by_edit_distance_test, type_of_lowered_filter) {
  const auto lowered = Lower(MakeFilter("foo", "bar"));
  ASSERT_NE(nullptr, lowered);
  ASSERT_EQ(MakeTermFilter("foo", "bar").type(), lowered->type());
}

class ByEditDistanceTestCase : public tests::FilterTestCaseBase {};

TEST_P(ByEditDistanceTestCase, test_order) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("levenshtein_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // empty query
  CheckQuery(*Lower({}), Docs{}, Costs{0}, rdr);

  {
    Docs docs{28, 29};
    Costs costs{docs.size()};

    size_t finish_count = 0;
    uint64_t finish_docs_with_field = 0;
    uint64_t finish_docs_with_term = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

    scorer.collectors_collect = [&](irs::byte_type*,
                                    const irs::FieldCollector* field,
                                    const irs::TermCollector* term) -> void {
      ++finish_count;
      ASSERT_NE(nullptr, field);
      ASSERT_NE(nullptr, term);
      finish_docs_with_field += field->docs_with_field;
      finish_docs_with_term += term->docs_with_term;
    };

    CheckQuery(*MakeLevenshtein("title", "", 1, 0, false), order, docs, rdr);
    ASSERT_EQ(1, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
  }

  {
    Docs docs{28, 29};
    Costs costs{docs.size()};

    size_t finish_count = 0;
    uint64_t finish_docs_with_field = 0;
    uint64_t finish_docs_with_term = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

    scorer.collectors_collect = [&](irs::byte_type*,
                                    const irs::FieldCollector* field,
                                    const irs::TermCollector* term) -> void {
      ++finish_count;
      ASSERT_NE(nullptr, field);
      ASSERT_NE(nullptr, term);
      finish_docs_with_field += field->docs_with_field;
      finish_docs_with_term += term->docs_with_term;
    };

    CheckQuery(*MakeLevenshtein("title", "", 1, 10, false), order, docs, rdr);
    ASSERT_EQ(1, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
  }

  {
    Docs docs{29};
    Costs costs{docs.size()};

    size_t finish_count = 0;
    uint64_t finish_docs_with_field = 0;
    uint64_t finish_docs_with_term = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

    scorer.collectors_collect = [&](irs::byte_type*,
                                    const irs::FieldCollector* field,
                                    const irs::TermCollector* term) -> void {
      ++finish_count;
      ASSERT_NE(nullptr, field);
      ASSERT_NE(nullptr, term);
      finish_docs_with_field += field->docs_with_field;
      finish_docs_with_term += term->docs_with_term;
    };

    CheckQuery(*MakeLevenshtein("title", "", 1, 1, false), order, docs, rdr);
    ASSERT_EQ(1, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
  }
}

TEST_P(ByEditDistanceTestCase, test_filter) {
  // add data
  {
    tests::JsonDocGenerator gen(resource("levenshtein_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  // empty query
  CheckQuery(*Lower({}), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 0, 0, false), Docs{}, Costs{0}, rdr);

  //////////////////////////////////////////////////////////////////////////////
  /// Levenshtein and Damerau-Levenshtein with prefix
  //////////////////////////////////////////////////////////////////////////////
  // distance 0 (term query)
  CheckQuery(*MakeLevenshtein("title", "", 0, 1024, false, "aaaw"), Docs{32},
             Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "w", 0, 1024, false, "aaa"), Docs{32},
             Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "w", 0, 1024, true, "aaa"), Docs{32},
             Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 0, 1024, false, ""), Docs{},
             Costs{0}, rdr);
  // distance 1
  CheckQuery(*MakeLevenshtein("title", "aa", 1, 1024, false, "aaabbba"),
             Docs{9, 10}, Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 1, 1024, false, ""), Docs{28, 29},
             Costs{2}, rdr);
  // distance 2
  CheckQuery(*MakeLevenshtein("title", "ca", 2, 1024, false, "b"), Docs{29, 30},
             Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 2, 1024, false, "aa"),
             Docs{5, 7, 13, 16, 19, 27, 32}, Costs{7}, rdr);
  // distance 3
  CheckQuery(*MakeLevenshtein("title", "", 3, 1024, false, "aaa"),
             Docs{5, 7, 13, 16, 19, 32}, Costs{6}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 3, 1024, true, "aaa"),
             Docs{5, 7, 13, 16, 19, 32}, Costs{6}, rdr);

  //////////////////////////////////////////////////////////////////////////////
  /// Levenshtein
  //////////////////////////////////////////////////////////////////////////////

  // distance 0 (term query)
  CheckQuery(*MakeLevenshtein("title", "aa", 0, 1024), Docs{27}, Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 0, 0), Docs{27}, Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 0, 10), Docs{27}, Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 0, 0), Docs{27}, Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 0, 10), Docs{17}, Costs{1},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 0, 0), Docs{17}, Costs{1},
             rdr);

  // distance 1
  CheckQuery(*MakeLevenshtein("title", "", 1, 1024), Docs{28, 29}, Costs{2},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "", 1, 0), Docs{28, 29}, Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 1, 1), Docs{29}, Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 1, 1024), Docs{27, 28}, Costs{2},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 1, 0), Docs{27, 28}, Costs{2},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 1, 1024), Docs{17}, Costs{1},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 0, 1024), Docs{17}, Costs{1},
             rdr);

  // distance 2
  CheckQuery(*MakeLevenshtein("title", "", 2, 1024), Docs{27, 28, 29}, Costs{3},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "", 2, 0), Docs{27, 28, 29}, Costs{3},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "", 2, 1), Docs{29}, Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 2, 2), Docs{28, 29}, Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 2, 1024), Docs{27, 28, 29, 30, 32},
             Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 2, 0), Docs{27, 28, 29, 30, 32},
             Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 2, 1024), Docs{17}, Costs{1},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 2, 0), Docs{17}, Costs{1},
             rdr);

  // distance 3
  CheckQuery(*MakeLevenshtein("title", "", 3, 1024), Docs{27, 28, 29, 30, 31},
             Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 3, 0), Docs{27, 28, 29, 30, 31},
             Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aaaa", 3, 10),
             Docs{
               5,
               7,
               13,
               16,
               17,
               18,
               19,
               21,
               27,
               28,
               30,
               32,
             },
             Costs{12}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aaaa", 3, 0),
             Docs{
               5,
               7,
               13,
               16,
               17,
               18,
               19,
               21,
               27,
               28,
               30,
               32,
             },
             Costs{12}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 3, 1024),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 32}, Costs{9}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 3, 0),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 32}, Costs{9}, rdr);

  // distance 4
  CheckQuery(*MakeLevenshtein("title", "", 4, 1024),
             Docs{27, 28, 29, 30, 31, 32}, Costs{6}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 4, 0), Docs{27, 28, 29, 30, 31, 32},
             Costs{6}, rdr);
  CheckQuery(
    *MakeLevenshtein("title", "ababab", 4, 1024),
    Docs{3, 4, 5, 6, 7, 10, 13, 14, 15, 16, 17, 18, 19, 21, 27, 30, 32, 34},
    Costs{18}, rdr);
  CheckQuery(
    *MakeLevenshtein("title", "ababab", 4, 0),
    Docs{3, 4, 5, 6, 7, 10, 13, 14, 15, 16, 17, 18, 19, 21, 27, 30, 32, 34},
    Costs{18}, rdr);

  // default provider doesn't support Levenshtein distances > 4
  CheckQuery(*MakeLevenshtein("title", "", 5, 1024), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 5, 0), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 6, 1024), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 6, 0), Docs{}, Costs{0}, rdr);

  //////////////////////////////////////////////////////////////////////////////
  /// Damerau-Levenshtein
  //////////////////////////////////////////////////////////////////////////////

  // distance 0 (term query)
  CheckQuery(*MakeLevenshtein("title", "aa", 0, 1024, true), Docs{27}, Costs{1},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 0, 0, true), Docs{27}, Costs{1},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 0, 1024, true), Docs{17},
             Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 0, 0, true), Docs{17},
             Costs{1}, rdr);

  // distance 1
  CheckQuery(*MakeLevenshtein("title", "", 1, 1024, true), Docs{28, 29},
             Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 1, 0, true), Docs{28, 29}, Costs{2},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 1, 1024, true), Docs{27, 28},
             Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 1, 0, true), Docs{27, 28},
             Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 1, 1024, true), Docs{17},
             Costs{1}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 1, 0, true), Docs{17},
             Costs{1}, rdr);

  // distance 2
  CheckQuery(*MakeLevenshtein("title", "aa", 2, 1024, true),
             Docs{27, 28, 29, 30, 32}, Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "aa", 2, 0, true),
             Docs{27, 28, 29, 30, 32}, Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 2, 1024, true), Docs{17, 18},
             Costs{2}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 2, 0, true), Docs{17, 18},
             Costs{2}, rdr);

  // distance 3
  CheckQuery(*MakeLevenshtein("title", "", 3, 1024, true),
             Docs{27, 28, 29, 30, 31}, Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 3, 0, true),
             Docs{27, 28, 29, 30, 31}, Costs{5}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 3, 1024, true),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 18, 32}, Costs{10}, rdr);
  CheckQuery(*MakeLevenshtein("title", "ababab", 3, 0, true),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 18, 32}, Costs{10}, rdr);

  // default provider doesn't support Damerau-Levenshtein distances > 3
  CheckQuery(*MakeLevenshtein("title", "", 4, 1024, true), Docs{}, Costs{0},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "", 4, 0, true), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeLevenshtein("title", "", 5, 1024, true), Docs{}, Costs{0},
             rdr);
  CheckQuery(*MakeLevenshtein("title", "", 5, 0, true), Docs{}, Costs{0}, rdr);
}

TEST_P(ByEditDistanceTestCase, bm25) {
  using tests::FieldBase;
  using tests::JsonDocGenerator;

  irs::analysis::TextTokenizer::Options opts{
    .locale = icu::Locale::createFromName("en"),
  };
  opts.case_convert = irs::Case::Lower;
  opts.explicit_stopwords_set = true;
  opts.stemming = false;
  auto analyzer = irs::analysis::TextTokenizer::Make(std::move(opts));
  ASSERT_NE(nullptr, analyzer);

  struct TextField : FieldBase {
   public:
    TextField(irs::analysis::Analyzer& analyzer, std::string value)
      : _value(std::move(value)), _analyzer(&analyzer) {
      this->Name("id");
      this->id = kIdId;
      this->index_features =
        irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
    }

    bool Write(irs::DataOutput&) const noexcept final { return true; }

    irs::Tokenizer& GetTokens() const final {
      const bool res = _analyzer->reset(_value);
      EXPECT_TRUE(res);
      return *_analyzer;
    }

   private:
    std::string _value;
    irs::analysis::Analyzer* _analyzer;
  };

  {
    JsonDocGenerator gen(
      resource("v_DSS_Entity_id.json"),
      [&analyzer](tests::Document& doc, const std::string& name,
                  const JsonDocGenerator::JsonValue& data) {
        if (JsonDocGenerator::ValueType::STRING == data.vt && name == "id") {
          auto field = std::make_shared<TextField>(
            *analyzer, std::string{data.str.data, data.str.size});
          doc.insert(field);
        }
      });

    auto opts = irs::tests::DefaultWriterOptions();

    add_segment(gen, irs::kOmCreate, opts);
  }

  std::array<irs::Scorer::ptr, 1> order{irs::BM25::Make(irs::BM25::Options{})};
  ASSERT_NE(nullptr, order.front());

  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_NE(nullptr, index);
  ASSERT_EQ(1, index->size());

  MaxMemoryCounter counter;
  irs::ColumnArgsFetcher fetcher;

  {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = kIdId;
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("end202"));
    opts.max_distance = 2;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    irs::Filter::ptr lowered =
      std::make_unique<irs::ByEditDistance>(std::move(filter));
    irs::Optimize(lowered, {.scored = true});
    tests::PreparedFilter prepared{*lowered, *index, order.front().get(),
                                   counter};
    ASSERT_NE(nullptr, prepared.Query(0));

    auto docs = prepared.ExecuteScored(0, fetcher);
    ASSERT_NE(nullptr, docs);

    auto score = docs->PrepareScore();
    ASSERT_FALSE(score.IsDefault());

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {2.8243692f, 261},
      {4.2365546f, 272},
      {3.5304620f, 273},
      {2.8243692f, 289},
    };

    auto expected_doc = std::begin(kExpectedDocs);
    while (!irs::doc_limits::eof(docs->Advance())) {
      fetcher.Fetch(docs->Value());
      docs->FetchScoreArgs(0);
      irs::score_t value;
      score.Score(&value, 1);
      ASSERT_FLOAT_EQ(expected_doc->first, value);
      ASSERT_EQ(expected_doc->second, docs->Value());
      ++expected_doc;
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = kIdId;
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("end202"));
    opts.max_distance = 1;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    irs::Filter::ptr lowered =
      std::make_unique<irs::ByEditDistance>(std::move(filter));
    irs::Optimize(lowered, {.scored = true});
    tests::PreparedFilter prepared{*lowered, *index, order.front().get(),
                                   counter};
    ASSERT_NE(nullptr, prepared.Query(0));

    fetcher.Clear();
    auto docs = prepared.ExecuteScored(0, fetcher);
    ASSERT_NE(nullptr, docs);

    auto score = docs->PrepareScore();

    ASSERT_FALSE(score.IsDefault());

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {4.5050912f, 272},
      {3.7542424f, 273},
    };

    auto expected_doc = std::begin(kExpectedDocs);
    while (!irs::doc_limits::eof(docs->Advance())) {
      fetcher.Fetch(docs->Value());
      irs::score_t value;
      docs->FetchScoreArgs(0);
      score.Score(&value, 1);
      ASSERT_FLOAT_EQ(expected_doc->first, value);
      ASSERT_EQ(expected_doc->second, docs->Value());
      ++expected_doc;
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with prefix
  {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = kIdId;
    auto& opts = *filter.mutable_options();
    opts.prefix = irs::ViewCast<irs::byte_type>(std::string_view("end"));
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("202"));
    opts.max_distance = 1;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    irs::Filter::ptr lowered =
      std::make_unique<irs::ByEditDistance>(std::move(filter));
    irs::Optimize(lowered, {.scored = true});
    tests::PreparedFilter prepared{*lowered, *index, order.front().get(),
                                   counter};
    ASSERT_NE(nullptr, prepared.Query(0));

    fetcher.Clear();
    auto docs = prepared.ExecuteScored(0, fetcher);
    ASSERT_NE(nullptr, docs);

    auto score = docs->PrepareScore();

    ASSERT_FALSE(score.IsDefault());

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {4.5050912f, 272},
      {3.7542424f, 273},
    };

    auto expected_doc = std::begin(kExpectedDocs);
    while (!irs::doc_limits::eof(docs->Advance())) {
      fetcher.Fetch(docs->Value());
      irs::score_t value;
      docs->FetchScoreArgs(0);
      score.Score(&value, 1);

      ASSERT_FLOAT_EQ(expected_doc->first, value);
      ASSERT_EQ(expected_doc->second, docs->Value());
      ++expected_doc;
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = kIdId;
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("asm212"));
    opts.max_distance = 2;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    irs::Filter::ptr lowered =
      std::make_unique<irs::ByEditDistance>(std::move(filter));
    irs::Optimize(lowered, {.scored = true});
    tests::PreparedFilter prepared{*lowered, *index, order.front().get(),
                                   counter};
    ASSERT_NE(nullptr, prepared.Query(0));

    fetcher.Clear();
    auto docs = prepared.ExecuteScored(0, fetcher);
    ASSERT_NE(nullptr, docs);

    auto score = docs->PrepareScore();

    ASSERT_FALSE(score.IsDefault());

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {3.7019949f, 265},   {3.0849960f, 264},   {3.0849960f, 3054},
      {3.0849960f, 3069},  {2.6328459f, 46355}, {2.6328459f, 46356},
      {2.6328459f, 46357}, {2.4679966f, 263},   {2.4679966f, 3062},
      {2.1940382f, 46353}, {2.1940382f, 46354}, {1.7552302f, 46350},
      {1.7552302f, 46351}, {1.7552302f, 46352},
    };

    std::vector<std::pair<float_t, irs::doc_id_t>> actual_docs;
    while (!irs::doc_limits::eof(docs->Advance())) {
      fetcher.Fetch(docs->Value());
      irs::score_t value;
      docs->FetchScoreArgs(0);
      score.Score(&value, 1);
      actual_docs.emplace_back(value, docs->Value());
    }
    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(std::size(kExpectedDocs), actual_docs.size());

    std::sort(std::begin(actual_docs), std::end(actual_docs),
              [](const auto& lhs, const auto& rhs) {
                if (lhs.first < rhs.first) {
                  return false;
                }

                if (lhs.first > rhs.first) {
                  return true;
                }

                return lhs.second < rhs.second;
              });

    auto expected_doc = std::begin(kExpectedDocs);
    for (auto& actual_doc : actual_docs) {
      EXPECT_FLOAT_EQ(expected_doc->first, actual_doc.first);
      EXPECT_EQ(expected_doc->second, actual_doc.second);
      ++expected_doc;
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = kIdId;
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("et038-pm"));
    opts.max_distance = 3;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    irs::Filter::ptr lowered =
      std::make_unique<irs::ByEditDistance>(std::move(filter));
    irs::Optimize(lowered, {.scored = true});
    tests::PreparedFilter prepared{*lowered, *index, order.front().get(),
                                   counter};
    ASSERT_NE(nullptr, prepared.Query(0));

    fetcher.Clear();
    auto docs = prepared.ExecuteScored(0, fetcher);
    ASSERT_NE(nullptr, docs);

    auto score = docs->PrepareScore();

    ASSERT_FALSE(score.IsDefault());

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {1.7405479f, 275},
      {1.2378716f, 46376},
      {1.2378716f, 46377},
    };

    std::vector<std::pair<float_t, irs::doc_id_t>> actual_docs;
    while (!irs::doc_limits::eof(docs->Advance())) {
      fetcher.Fetch(docs->Value());
      irs::score_t value;
      docs->FetchScoreArgs(0);
      score.Score(&value, 1);
      actual_docs.emplace_back(value, docs->Value());
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(std::size(kExpectedDocs), actual_docs.size());

    std::sort(std::begin(actual_docs), std::end(actual_docs),
              [](const auto& lhs, const auto& rhs) {
                if (lhs.first < rhs.first) {
                  return false;
                }

                if (lhs.first > rhs.first) {
                  return true;
                }

                return lhs.second < rhs.second;
              });

    auto expected_doc = std::begin(kExpectedDocs);
    for (auto& actual_doc : actual_docs) {
      EXPECT_FLOAT_EQ(expected_doc->first, actual_doc.first);
      EXPECT_EQ(expected_doc->second, actual_doc.second);
      ++expected_doc;
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with prefix
  {
    irs::ByEditDistance filter;
    *filter.mutable_field_id() = kIdId;
    auto& opts = *filter.mutable_options();
    opts.prefix = irs::ViewCast<irs::byte_type>(std::string_view("et038"));
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("-pm"));
    opts.max_distance = 3;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    irs::Filter::ptr lowered =
      std::make_unique<irs::ByEditDistance>(std::move(filter));
    irs::Optimize(lowered, {.scored = true});
    tests::PreparedFilter prepared{*lowered, *index, order.front().get(),
                                   counter};
    ASSERT_NE(nullptr, prepared.Query(0));

    fetcher.Clear();
    auto docs = prepared.ExecuteScored(0, fetcher);
    ASSERT_NE(nullptr, docs);

    auto score = docs->PrepareScore();

    ASSERT_FALSE(score.IsDefault());

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {1.7405479f, 275},
      {1.2378716f, 46376},
      {1.2378716f, 46377},
    };

    std::vector<std::pair<float_t, irs::doc_id_t>> actual_docs;
    while (!irs::doc_limits::eof(docs->Advance())) {
      fetcher.Fetch(docs->Value());
      irs::score_t value;
      docs->FetchScoreArgs(0);
      score.Score(&value, 1);
      actual_docs.emplace_back(value, docs->Value());
    }

    ASSERT_FALSE(!irs::doc_limits::eof(docs->Advance()));
    ASSERT_EQ(std::size(kExpectedDocs), actual_docs.size());

    std::sort(std::begin(actual_docs), std::end(actual_docs),
              [](const auto& lhs, const auto& rhs) {
                if (lhs.first < rhs.first) {
                  return false;
                }

                if (lhs.first > rhs.first) {
                  return true;
                }

                return lhs.second < rhs.second;
              });

    auto expected_doc = std::begin(kExpectedDocs);
    for (auto& actual_doc : actual_docs) {
      EXPECT_FLOAT_EQ(expected_doc->first, actual_doc.first);
      EXPECT_EQ(expected_doc->second, actual_doc.second);
      ++expected_doc;
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(ByEditDistanceTestCase, visit) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  const irs::field_id field = kPrefixId;
  // read segment
  auto index = open_reader(irs::tests::DefaultReaderOptions());
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];
  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);

  {
    auto lowered = Lower(MakeFilter("prefix", "abc", 0));
    ASSERT_EQ(irs::Type<irs::ByTerm>::id(), lowered->type());
    const auto& filter = sdb::basics::downCast<irs::ByTerm>(*lowered);

    tests::EmptyFilterVisitor visitor;
    irs::ByTerm::Visit(segment, *reader, filter.options(), visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(1, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost}}),
              visitor.term_refs<char>());
    visitor.reset();
  }

  {
    auto lowered = Lower(MakeFilter("prefix", "abc", 1));
    ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(),
              lowered->type());
    const auto& filter =
      sdb::basics::downCast<irs::LevenshteinAutomatonFilter>(*lowered);

    tests::EmptyFilterVisitor visitor;
    auto field_visitor =
      irs::LevenshteinAutomatonFilter::visitor(filter.options());
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(3, visitor.visit_calls_counter());

    const auto actual_terms = visitor.term_refs<char>();
    std::vector<std::pair<std::string_view, irs::score_t>> expected_terms{
      {"abc", irs::kNoBoost},
      {"abcd", 2.f / 3},
      {"abcy", 2.f / 3},
    };
    ASSERT_EQ(expected_terms.size(), actual_terms.size());

    auto actual_term = actual_terms.begin();
    for (auto& expected_term : expected_terms) {
      ASSERT_EQ(expected_term.first, actual_term->first);
      ASSERT_FLOAT_EQ(expected_term.second, actual_term->second);
      ++actual_term;
    }

    visitor.reset();
  }

  // with prefix
  {
    auto lowered = Lower(MakeFilter("prefix", "c", 2, 0, false, "ab"));
    ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(),
              lowered->type());
    const auto& filter =
      sdb::basics::downCast<irs::LevenshteinAutomatonFilter>(*lowered);

    tests::EmptyFilterVisitor visitor;
    auto field_visitor =
      irs::LevenshteinAutomatonFilter::visitor(filter.options());
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(5, visitor.visit_calls_counter());

    const auto actual_terms = visitor.term_refs<char>();
    std::vector<std::pair<std::string_view, irs::score_t>> expected_terms{
      {"abc", irs::kNoBoost}, {"abcd", 2.f / 3}, {"abcde", 1.f / 3},
      {"abcy", 2.f / 3},      {"abde", 1.f / 3},
    };
    ASSERT_EQ(expected_terms.size(), actual_terms.size());

    auto actual_term = actual_terms.begin();
    for (auto& expected_term : expected_terms) {
      ASSERT_EQ(expected_term.first, actual_term->first);
      ASSERT_FLOAT_EQ(expected_term.second, actual_term->second);
      ++actual_term;
    }

    visitor.reset();
  }
}

static void AppendPrefix(irs::BooleanFilter& root, std::string_view field,
                         std::string_view term) {
  auto prefix = std::make_unique<irs::ByPrefix>();
  *prefix->mutable_field_id() = FieldIdFor(field);
  prefix->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  root.Add(std::move(prefix), irs::Occur::Must);
}

static void AppendEditDistance(irs::BooleanFilter& root,
                               irs::ByEditDistance filter) {
  root.Add(std::make_unique<irs::ByEditDistance>(std::move(filter)),
           irs::Occur::Must);
}

TEST(by_edit_distance_test, fuse_prefix_into_levenshtein) {
  {
    irs::BooleanFilter root;
    AppendPrefix(root, "title", "aa");
    AppendEditDistance(root, MakeFilter("title", "aaaa", 2, 0));
    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(),
              optimized->type());
    ASSERT_EQ(*optimized, *MakeLevenshtein("title", "aa", 2, 0, false, "aa"));
  }
  {
    irs::BooleanFilter root;
    AppendPrefix(root, "title", "aaa");
    AppendEditDistance(root, MakeFilter("title", "ab", 1, 0, false, "aa"));
    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(),
              optimized->type());
    ASSERT_EQ(*optimized, *MakeLevenshtein("title", "b", 1, 0, false, "aaa"));
  }
  {
    irs::BooleanFilter root;
    AppendPrefix(root, "title", "aa");
    AppendEditDistance(root, MakeFilter("title", "b", 1, 0, false, "aaa"));
    auto optimized = tests::Optimized(std::move(root));
    ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(),
              optimized->type());
    ASSERT_EQ(*optimized, *MakeLevenshtein("title", "b", 1, 0, false, "aaa"));
  }
}

TEST(by_edit_distance_test, fuse_prefix_multiple) {
  irs::BooleanFilter root;
  AppendPrefix(root, "title", "aa");
  AppendEditDistance(root, MakeFilter("title", "aaaa", 2, 0));
  AppendEditDistance(root, MakeFilter("title", "aab", 1, 0));
  auto optimized = tests::Optimized(std::move(root));
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), optimized->type());
  auto& node = sdb::basics::downCast<irs::BooleanFilter>(*optimized);
  ASSERT_EQ(2, node.Size(irs::Occur::Must));
  auto must = node.Filters(irs::Occur::Must);
  ASSERT_EQ(2, must.size());
  ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(), must[0]->type());
  ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(), must[1]->type());
}

TEST(by_edit_distance_test, fuse_prefix_non_matching) {
  irs::BooleanFilter root;
  AppendPrefix(root, "title", "zz");
  AppendEditDistance(root, MakeFilter("title", "aaaa", 2, 0));
  auto optimized = tests::Optimized(std::move(root));
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), optimized->type());
  auto& node = sdb::basics::downCast<irs::BooleanFilter>(*optimized);
  ASSERT_EQ(2, node.Size(irs::Occur::Must));
  auto must = node.Filters(irs::Occur::Must);
  ASSERT_EQ(2, must.size());
  ASSERT_EQ(irs::Type<irs::ByPrefix>::id(), must[0]->type());
  ASSERT_EQ(irs::Type<irs::LevenshteinAutomatonFilter>::id(), must[1]->type());
}

TEST(by_edit_distance_test, fuse_prefix_different_field) {
  irs::BooleanFilter root;
  AppendPrefix(root, "title", "aa");
  AppendEditDistance(root, MakeFilter("body", "aaaa", 2, 0));
  auto optimized = tests::Optimized(std::move(root));
  ASSERT_EQ(irs::Type<irs::BooleanFilter>::id(), optimized->type());
  ASSERT_EQ(2, sdb::basics::downCast<irs::BooleanFilter>(*optimized)
                 .Size(irs::Occur::Must));
}

TEST_P(ByEditDistanceTestCase, fuse_prefix) {
  {
    tests::JsonDocGenerator gen(resource("levenshtein_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader(irs::tests::DefaultReaderOptions());

  {
    irs::BooleanFilter root;
    AppendPrefix(root, "title", "aa");
    AppendEditDistance(root, MakeFilter("title", "aaaa", 2, 1024));
    CheckQuery(*tests::Optimized(std::move(root)),
               Docs{5, 7, 13, 16, 19, 27, 32}, rdr);
  }
  {
    irs::BooleanFilter root;
    AppendPrefix(root, "title", "aaa");
    AppendEditDistance(root, MakeFilter("title", "aaaw", 0, 1024));
    CheckQuery(*tests::Optimized(std::move(root)), Docs{32}, rdr);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(by_edit_distance_test, ByEditDistanceTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         ByEditDistanceTestCase::to_string);
