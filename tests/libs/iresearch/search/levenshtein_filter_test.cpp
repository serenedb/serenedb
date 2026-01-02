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

#include <iresearch/index/index_features.hpp>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/utils/levenshtein_default_pdp.hpp>

#include "basics/misc.hpp"
#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

irs::ByTerm MakeTermFilter(const std::string_view& field,
                           const std::string_view term) {
  irs::ByTerm q;
  *q.mutable_field() = field;
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
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  q.mutable_options()->max_distance = max_distance;
  q.mutable_options()->max_terms = max_terms;
  q.mutable_options()->with_transpositions = with_transpositions;
  q.mutable_options()->prefix = irs::ViewCast<irs::byte_type>(prefix);
  return q;
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
  ASSERT_TRUE(q.field().empty());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
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
    *rhs.mutable_field() = "field";
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
    *q.mutable_field() = "field";
    q.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("bar*"));

    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with boost
  {
    irs::score_t boost = 1.5f;

    irs::ByEditDistance q;
    *q.mutable_field() = "field";
    q.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("bar*"));
    q.boost(boost);

    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_EQ(boost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST(by_edit_distance_test, test_type_of_prepared_query) {
  MaxMemoryCounter counter;
  // term query
  {
    auto lhs = MakeTermFilter("foo", "bar")
                 .prepare({
                   .index = irs::SubReader::empty(),
                   .memory = counter,
                 });
    auto rhs = MakeFilter("foo", "bar")
                 .prepare({
                   .index = irs::SubReader::empty(),
                   .memory = counter,
                 });
    auto& lhs_ref = *lhs;
    auto& rhs_ref = *rhs;
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

class ByEditDistanceTestCase : public tests::FilterTestCaseBase {};

TEST_P(ByEditDistanceTestCase, test_order) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("levenshtein_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  CheckQuery(irs::ByEditDistance(), Docs{}, Costs{0}, rdr);

  {
    Docs docs{28, 29};
    Costs costs{docs.size()};

    size_t term_collectors_count = 0;
    size_t field_collectors_count = 0;
    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

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
    scorer.prepare_field_collector =
      [&scorer, &field_collectors_count]() -> irs::FieldCollector::ptr {
      ++field_collectors_count;
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(scorer);
    };
    scorer.prepare_term_collector =
      [&scorer, &term_collectors_count]() -> irs::TermCollector::ptr {
      ++term_collectors_count;
      return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
    };

    CheckQuery(MakeFilter("title", "", 1, 0, false), order, docs, rdr);
    ASSERT_EQ(1, field_collectors_count);  // 1 field, 1 field collector
    ASSERT_EQ(1, term_collectors_count);  // need only 1 term collector since we
                                          // distribute stats across terms
    ASSERT_EQ(1, collect_field_count);    // 1 fields
    ASSERT_EQ(2, collect_term_count);     // 2 different terms
    ASSERT_EQ(1, finish_count);  // we distribute idf across all matched terms
  }

  {
    Docs docs{28, 29};
    Costs costs{docs.size()};

    size_t term_collectors_count = 0;
    size_t field_collectors_count = 0;
    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

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
    scorer.prepare_field_collector =
      [&scorer, &field_collectors_count]() -> irs::FieldCollector::ptr {
      ++field_collectors_count;
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(scorer);
    };
    scorer.prepare_term_collector =
      [&scorer, &term_collectors_count]() -> irs::TermCollector::ptr {
      ++term_collectors_count;
      return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
    };

    CheckQuery(MakeFilter("title", "", 1, 10, false), order, docs, rdr);
    ASSERT_EQ(1, field_collectors_count);  // 1 field, 1 field collector
    ASSERT_EQ(1, term_collectors_count);  // need only 1 term collector since we
                                          // distribute stats across terms
    ASSERT_EQ(1, collect_field_count);    // 1 fields
    ASSERT_EQ(2, collect_term_count);     // 2 different terms
    ASSERT_EQ(1, finish_count);  // we distribute idf across all matched terms
  }

  {
    Docs docs{29};
    Costs costs{docs.size()};

    size_t term_collectors_count = 0;
    size_t field_collectors_count = 0;
    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());

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
    scorer.prepare_field_collector =
      [&scorer, &field_collectors_count]() -> irs::FieldCollector::ptr {
      ++field_collectors_count;
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(scorer);
    };
    scorer.prepare_term_collector =
      [&scorer, &term_collectors_count]() -> irs::TermCollector::ptr {
      ++term_collectors_count;
      return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
    };

    CheckQuery(MakeFilter("title", "", 1, 1, false), order, docs, rdr);
    ASSERT_EQ(1, field_collectors_count);  // 1 field, 1 field collector
    ASSERT_EQ(1, term_collectors_count);  // need only 1 term collector since we
                                          // distribute stats across terms
    ASSERT_EQ(1, collect_field_count);    // 1 fields
    ASSERT_EQ(1, collect_term_count);     // 1 term
    ASSERT_EQ(1, finish_count);  // we distribute idf across all matched terms
  }
}

TEST_P(ByEditDistanceTestCase, test_filter) {
  // add data
  {
    tests::JsonDocGenerator gen(resource("levenshtein_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  CheckQuery(irs::ByEditDistance(), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 0, 0, false), Docs{}, Costs{0}, rdr);

  //////////////////////////////////////////////////////////////////////////////
  /// Levenshtein and Damerau-Levenshtein with prefix
  //////////////////////////////////////////////////////////////////////////////
  // distance 0 (term query)
  CheckQuery(MakeFilter("title", "", 0, 1024, false, "aaaw"), Docs{32},
             Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "w", 0, 1024, false, "aaa"), Docs{32},
             Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "w", 0, 1024, true, "aaa"), Docs{32}, Costs{1},
             rdr);
  CheckQuery(MakeFilter("title", "", 0, 1024, false, ""), Docs{}, Costs{0},
             rdr);
  // distance 1
  CheckQuery(MakeFilter("title", "aa", 1, 1024, false, "aaabbba"), Docs{9, 10},
             Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "", 1, 1024, false, ""), Docs{28, 29},
             Costs{2}, rdr);
  // distance 2
  CheckQuery(MakeFilter("title", "ca", 2, 1024, false, "b"), Docs{29, 30},
             Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "aa", 2, 1024, false, "aa"),
             Docs{5, 7, 13, 16, 19, 27, 32}, Costs{7}, rdr);
  // distance 3
  CheckQuery(MakeFilter("title", "", 3, 1024, false, "aaa"),
             Docs{5, 7, 13, 16, 19, 32}, Costs{6}, rdr);
  CheckQuery(MakeFilter("title", "", 3, 1024, true, "aaa"),
             Docs{5, 7, 13, 16, 19, 32}, Costs{6}, rdr);

  //////////////////////////////////////////////////////////////////////////////
  /// Levenshtein
  //////////////////////////////////////////////////////////////////////////////

  // distance 0 (term query)
  CheckQuery(MakeFilter("title", "aa", 0, 1024), Docs{27}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "aa", 0, 0), Docs{27}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "aa", 0, 10), Docs{27}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "aa", 0, 0), Docs{27}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 0, 10), Docs{17}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 0, 0), Docs{17}, Costs{1}, rdr);

  // distance 1
  CheckQuery(MakeFilter("title", "", 1, 1024), Docs{28, 29}, Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "", 1, 0), Docs{28, 29}, Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "", 1, 1), Docs{29}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "aa", 1, 1024), Docs{27, 28}, Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "aa", 1, 0), Docs{27, 28}, Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 1, 1024), Docs{17}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 0, 1024), Docs{17}, Costs{1}, rdr);

  // distance 2
  CheckQuery(MakeFilter("title", "", 2, 1024), Docs{27, 28, 29}, Costs{3}, rdr);
  CheckQuery(MakeFilter("title", "", 2, 0), Docs{27, 28, 29}, Costs{3}, rdr);
  CheckQuery(MakeFilter("title", "", 2, 1), Docs{29}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "", 2, 2), Docs{28, 29}, Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "aa", 2, 1024), Docs{27, 28, 29, 30, 32},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "aa", 2, 0), Docs{27, 28, 29, 30, 32},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 2, 1024), Docs{17}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 2, 0), Docs{17}, Costs{1}, rdr);

  // distance 3
  CheckQuery(MakeFilter("title", "", 3, 1024), Docs{27, 28, 29, 30, 31},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "", 3, 0), Docs{27, 28, 29, 30, 31}, Costs{5},
             rdr);
  CheckQuery(MakeFilter("title", "aaaa", 3, 10),
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
  CheckQuery(MakeFilter("title", "aaaa", 3, 0),
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
  CheckQuery(MakeFilter("title", "ababab", 3, 1024),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 32}, Costs{9}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 3, 0),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 32}, Costs{9}, rdr);

  // distance 4
  CheckQuery(MakeFilter("title", "", 4, 1024), Docs{27, 28, 29, 30, 31, 32},
             Costs{6}, rdr);
  CheckQuery(MakeFilter("title", "", 4, 0), Docs{27, 28, 29, 30, 31, 32},
             Costs{6}, rdr);
  CheckQuery(
    MakeFilter("title", "ababab", 4, 1024),
    Docs{3, 4, 5, 6, 7, 10, 13, 14, 15, 16, 17, 18, 19, 21, 27, 30, 32, 34},
    Costs{18}, rdr);
  CheckQuery(
    MakeFilter("title", "ababab", 4, 0),
    Docs{3, 4, 5, 6, 7, 10, 13, 14, 15, 16, 17, 18, 19, 21, 27, 30, 32, 34},
    Costs{18}, rdr);

  // default provider doesn't support Levenshtein distances > 4
  CheckQuery(MakeFilter("title", "", 5, 1024), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 5, 0), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 6, 1024), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 6, 0), Docs{}, Costs{0}, rdr);

  //////////////////////////////////////////////////////////////////////////////
  /// Damerau-Levenshtein
  //////////////////////////////////////////////////////////////////////////////

  // distance 0 (term query)
  CheckQuery(MakeFilter("title", "aa", 0, 1024, true), Docs{27}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "aa", 0, 0, true), Docs{27}, Costs{1}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 0, 1024, true), Docs{17}, Costs{1},
             rdr);
  CheckQuery(MakeFilter("title", "ababab", 0, 0, true), Docs{17}, Costs{1},
             rdr);

  // distance 1
  CheckQuery(MakeFilter("title", "", 1, 1024, true), Docs{28, 29}, Costs{2},
             rdr);
  CheckQuery(MakeFilter("title", "", 1, 0, true), Docs{28, 29}, Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "aa", 1, 1024, true), Docs{27, 28}, Costs{2},
             rdr);
  CheckQuery(MakeFilter("title", "aa", 1, 0, true), Docs{27, 28}, Costs{2},
             rdr);
  CheckQuery(MakeFilter("title", "ababab", 1, 1024, true), Docs{17}, Costs{1},
             rdr);
  CheckQuery(MakeFilter("title", "ababab", 1, 0, true), Docs{17}, Costs{1},
             rdr);

  // distance 2
  CheckQuery(MakeFilter("title", "aa", 2, 1024, true), Docs{27, 28, 29, 30, 32},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "aa", 2, 0, true), Docs{27, 28, 29, 30, 32},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 2, 1024, true), Docs{17, 18},
             Costs{2}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 2, 0, true), Docs{17, 18}, Costs{2},
             rdr);

  // distance 3
  CheckQuery(MakeFilter("title", "", 3, 1024, true), Docs{27, 28, 29, 30, 31},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "", 3, 0, true), Docs{27, 28, 29, 30, 31},
             Costs{5}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 3, 1024, true),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 18, 32}, Costs{10}, rdr);
  CheckQuery(MakeFilter("title", "ababab", 3, 0, true),
             Docs{3, 5, 7, 13, 14, 15, 16, 17, 18, 32}, Costs{10}, rdr);

  // default provider doesn't support Damerau-Levenshtein distances > 3
  CheckQuery(MakeFilter("title", "", 4, 1024, true), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 4, 0, true), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 5, 1024, true), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("title", "", 5, 0, true), Docs{}, Costs{0}, rdr);
}

TEST_P(ByEditDistanceTestCase, bm25) {
  using tests::FieldBase;
  using tests::JsonDocGenerator;

  auto analyzer = irs::analysis::analyzers::Get(
    "text", irs::Type<irs::text_format::Json>::get(),
    R"({"locale":"en.UTF-8", "stem":false, "accent":false, "case":"lower", "stopwords":[]})");
  ASSERT_NE(nullptr, analyzer);

  struct TextField : FieldBase {
   public:
    TextField(irs::analysis::Analyzer& analyzer, std::string value)
      : _value(std::move(value)), _analyzer(&analyzer) {
      this->Name("id");
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

    irs::IndexWriterOptions opts;
    opts.features = [](irs::IndexFeatures id) {
      const irs::ColumnInfo info{
        irs::Type<irs::compression::Lz4>::get(), {}, false};

      if (irs::IndexFeatures::Norm == id) {
        return std::make_pair(info, &irs::Norm::MakeWriter);
      }

      return std::make_pair(info, irs::FeatureWriterFactory{});
    };

    add_segment(gen, irs::kOmCreate, opts);
  }

  std::array<irs::Scorer::ptr, 1> order{irs::scorers::Get(
    "bm25", irs::Type<irs::text_format::Json>::get(), std::string_view{})};
  ASSERT_NE(nullptr, order.front());

  auto prepared_order = irs::Scorers::Prepare(order);

  auto index = open_reader();
  ASSERT_NE(nullptr, index);
  ASSERT_EQ(1, index->size());

  MaxMemoryCounter counter;

  {
    irs::ByEditDistance filter;
    *filter.mutable_field() = "id";
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("end202"));
    opts.max_distance = 2;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    auto prepared = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorers = prepared_order,
    });
    ASSERT_NE(nullptr, prepared);

    auto docs =
      prepared->execute({.segment = index[0], .scorers = prepared_order});
    ASSERT_NE(nullptr, docs);

    auto* score = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {6.21361256f, 261},
      {9.32042027f, 272},
      {7.76701689f, 273},
      {6.21361256f, 289},
    };

    auto expected_doc = std::begin(kExpectedDocs);
    while (docs->next()) {
      irs::score_t value;
      (*score)(&value);
      ASSERT_FLOAT_EQ(expected_doc->first, value);
      ASSERT_EQ(expected_doc->second, docs->value());
      ++expected_doc;
    }

    ASSERT_FALSE(docs->next());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  {
    irs::ByEditDistance filter;
    *filter.mutable_field() = "id";
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("end202"));
    opts.max_distance = 1;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    auto prepared = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorers = prepared_order,
    });
    ASSERT_NE(nullptr, prepared);

    auto docs =
      prepared->execute({.segment = index[0], .scorers = prepared_order});
    ASSERT_NE(nullptr, docs);

    auto* score = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {9.9112005f, 272},
      {8.2593336f, 273},
    };

    auto expected_doc = std::begin(kExpectedDocs);
    while (docs->next()) {
      irs::score_t value;
      (*score)(&value);
      ASSERT_FLOAT_EQ(expected_doc->first, value);
      ASSERT_EQ(expected_doc->second, docs->value());
      ++expected_doc;
    }
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with prefix
  {
    irs::ByEditDistance filter;
    *filter.mutable_field() = "id";
    auto& opts = *filter.mutable_options();
    opts.prefix = irs::ViewCast<irs::byte_type>(std::string_view("end"));
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("202"));
    opts.max_distance = 1;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    auto prepared = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorers = prepared_order,
    });
    ASSERT_NE(nullptr, prepared);

    auto docs =
      prepared->execute({.segment = index[0], .scorers = prepared_order});
    ASSERT_NE(nullptr, docs);

    auto* score = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {9.9112005f, 272},
      {8.2593336f, 273},
    };

    auto expected_doc = std::begin(kExpectedDocs);
    while (docs->next()) {
      irs::score_t value;
      (*score)(&value);

      ASSERT_FLOAT_EQ(expected_doc->first, value);
      ASSERT_EQ(expected_doc->second, docs->value());
      ++expected_doc;
    }

    ASSERT_FALSE(docs->next());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  {
    irs::ByEditDistance filter;
    *filter.mutable_field() = "id";
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("asm212"));
    opts.max_distance = 2;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    auto prepared = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorers = prepared_order,
    });
    ASSERT_NE(nullptr, prepared);

    auto docs =
      prepared->execute({.segment = index[0], .scorers = prepared_order});
    ASSERT_NE(nullptr, docs);

    auto* score = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {8.1443892f, 265},   {6.7869911f, 264},   {6.7869911f, 3054},
      {6.7869911f, 3069},  {5.7922611f, 46355}, {5.7922611f, 46356},
      {5.7922611f, 46357}, {5.4295926f, 263},   {5.4295926f, 3062},
      {4.8268843f, 46353}, {4.8268843f, 46354}, {3.8615065f, 46350},
      {3.8615065f, 46351}, {3.8615065f, 46352},
    };

    std::vector<std::pair<float_t, irs::doc_id_t>> actual_docs;
    while (docs->next()) {
      irs::score_t value;
      (*score)(&value);
      actual_docs.emplace_back(value, docs->value());
    }
    ASSERT_FALSE(docs->next());
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
    *filter.mutable_field() = "id";
    auto& opts = *filter.mutable_options();
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("et038-pm"));
    opts.max_distance = 3;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    auto prepared = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorers = prepared_order,
    });
    ASSERT_NE(nullptr, prepared);

    auto docs =
      prepared->execute({.segment = index[0], .scorers = prepared_order});
    ASSERT_NE(nullptr, docs);

    auto* score = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {3.8292055f, 275},
      {2.7233176f, 46376},
      {2.7233176f, 46377},
    };

    std::vector<std::pair<float_t, irs::doc_id_t>> actual_docs;
    while (docs->next()) {
      irs::score_t value;
      (*score)(&value);
      actual_docs.emplace_back(value, docs->value());
    }

    ASSERT_FALSE(docs->next());
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
    *filter.mutable_field() = "id";
    auto& opts = *filter.mutable_options();
    opts.prefix = irs::ViewCast<irs::byte_type>(std::string_view("et038"));
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("-pm"));
    opts.max_distance = 3;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = true;

    auto prepared = filter.prepare({
      .index = *index,
      .memory = counter,
      .scorers = prepared_order,
    });
    ASSERT_NE(nullptr, prepared);

    auto docs =
      prepared->execute({.segment = index[0], .scorers = prepared_order});
    ASSERT_NE(nullptr, docs);

    auto* score = irs::get<irs::ScoreAttr>(*docs);
    ASSERT_NE(nullptr, score);
    ASSERT_FALSE(score->Func() == &irs::ScoreFunction::DefaultScore);

    constexpr std::pair<float_t, irs::doc_id_t> kExpectedDocs[]{
      {3.8292055f, 275},
      {2.7233176f, 46376},
      {2.7233176f, 46377},
    };

    std::vector<std::pair<float_t, irs::doc_id_t>> actual_docs;
    while (docs->next()) {
      irs::score_t value;
      (*score)(&value);
      actual_docs.emplace_back(value, docs->value());
    }

    ASSERT_FALSE(docs->next());
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
  const std::string_view field = "prefix";
  const auto term = irs::ViewCast<irs::byte_type>(std::string_view("abc"));
  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];
  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);

  {
    irs::ByEditDistanceOptions opts;
    opts.term = term;
    opts.max_distance = 0;
    opts.provider = nullptr;
    opts.with_transpositions = false;

    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByEditDistance::visitor(opts);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(1, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost}}),
              visitor.term_refs<char>());
    visitor.reset();
  }

  {
    irs::ByEditDistanceOptions opts;
    opts.term = term;
    opts.max_distance = 1;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = false;

    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByEditDistance::visitor(opts);
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
    irs::ByEditDistanceOptions opts;
    opts.term = irs::ViewCast<irs::byte_type>(std::string_view("c"));
    opts.max_distance = 2;
    opts.provider = irs::DefaultPDP;
    opts.with_transpositions = false;
    opts.prefix = irs::ViewCast<irs::byte_type>(std::string_view("ab"));

    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByEditDistance::visitor(opts);
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

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  by_edit_distance_test, ByEditDistanceTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  ByEditDistanceTestCase::to_string);
