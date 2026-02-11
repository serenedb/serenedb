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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/index/field_meta.hpp>
#include <iresearch/search/boost_scorer.hpp>
#include <iresearch/search/terms_filter.hpp>

#include "filter_test_case_base.hpp"
#include "index/doc_generator.hpp"
#include "tests_shared.hpp"

namespace {

irs::ByTerms MakeFilter(
  const std::string_view& field,
  const std::vector<std::pair<std::string_view, irs::score_t>>& terms,
  size_t min_match = 1) {
  irs::ByTerms q;
  *q.mutable_field() = field;
  q.mutable_options()->min_match = min_match;
  for (auto& term : terms) {
    q.mutable_options()->terms.emplace(
      irs::ViewCast<irs::byte_type>(term.first), term.second);
  }
  return q;
}

}  // namespace

TEST(by_terms_test, options) {
  irs::ByTermsOptions opts;
  ASSERT_TRUE(opts.terms.empty());
}

TEST(by_terms_test, ctor) {
  irs::ByTerms q;
  ASSERT_EQ(irs::Type<irs::ByTerms>::id(), q.type());
  ASSERT_EQ(irs::ByTermsOptions{}, q.options());
  ASSERT_TRUE(q.field().empty());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_terms_test, equal) {
  const irs::ByTerms q0 = MakeFilter("field", {{"bar", 0.5f}, {"baz", 0.25f}});
  const irs::ByTerms q1 = MakeFilter("field", {{"bar", 0.5f}, {"baz", 0.25f}});
  ASSERT_EQ(q0, q1);

  const irs::ByTerms q2 = MakeFilter("field1", {{"bar", 0.5f}, {"baz", 0.25f}});
  ASSERT_NE(q0, q2);

  const irs::ByTerms q3 = MakeFilter("field", {{"bar1", 0.5f}, {"baz", 0.25f}});
  ASSERT_NE(q0, q3);

  const irs::ByTerms q4 = MakeFilter("field", {{"bar", 0.5f}, {"baz", 0.5f}});
  ASSERT_NE(q0, q4);

  irs::ByTerms q5 = MakeFilter("field", {{"bar", 0.5f}, {"baz", 0.25f}});
  q5.mutable_options()->min_match = 2;
  ASSERT_NE(q0, q5);
}

class TermsFilterTestCase : public tests::FilterTestCaseBase {};

TEST_P(TermsFilterTestCase, boost) {
  MaxMemoryCounter counter;

  // no boost
  {
    irs::ByTerms q = MakeFilter("field", {{"bar", 0.5f}, {"baz", 0.25f}});

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

    irs::ByTerms q = MakeFilter("field", {{"bar", 0.5f}, {"baz", 0.25f}});
    q.boost(boost);

    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_EQ(irs::kNoBoost,
              prepared->Boost());  // no boost because index is empty
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with boost
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);

    auto rdr = open_reader();
    ASSERT_EQ(1, rdr.size());

    irs::score_t boost = 1.5f;

    irs::ByTerms q =
      MakeFilter("duplicated", {{"abcd", 0.5f}, {"vczc", 0.25f}});
    q.boost(boost);

    auto prepared = q.prepare({
      .index = *rdr,
      .memory = counter,
    });
    ASSERT_EQ(boost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST_P(TermsFilterTestCase, simple_sequential_order) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());

  // empty prefix test collector call count for field/term/finish
  {
    const Docs docs{1, 21, 31, 32};
    Costs costs{docs.size()};

    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    irs::Scorer::ptr impl{std::make_unique<tests::sort::CustomSort>()};
    auto* scorer = static_cast<tests::sort::CustomSort*>(impl.get());

    scorer->collector_collect_field = [&collect_field_count](
                                        const irs::SubReader&,
                                        const irs::TermReader&) -> void {
      ++collect_field_count;
    };
    scorer->collector_collect_term =
      [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                            const irs::AttributeProvider&) -> void {
      ++collect_term_count;
    };
    scorer->collectors_collect =
      [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                      const irs::TermCollector*) -> void { ++finish_count; };
    scorer->prepare_field_collector = [&scorer]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(*scorer);
    };
    scorer->prepare_term_collector = [&scorer]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(*scorer);
    };

    const auto filter = MakeFilter(
      "prefix", {{"abcd", 1.f}, {"abcd", 1.f}, {"abc", 1.f}, {"abcy", 1.f}});

    CheckQuery(filter, std::span{&impl, 1}, docs, rdr);
    ASSERT_EQ(1, collect_field_count);  // 1 fields in 1 segment
    ASSERT_EQ(3, collect_term_count);   // 3 different terms
    ASSERT_EQ(3, finish_count);         // 3 unque terms
  }

  // check boost
  {
    const Docs docs{21, 31, 32, 1};
    const Costs costs{docs.size()};
    const auto filter = MakeFilter(
      "prefix", {{"abcd", 0.5f}, {"abcd", 1.f}, {"abc", 1.f}, {"abcy", 1.f}});

    irs::Scorer::ptr impl{std::make_unique<irs::BoostScore>()};
    CheckQuery(filter, std::span{&impl, 1}, docs, rdr, true, true);
  }

  // check negative boost
  {
    const Docs docs{21, 31, 32, 1};
    const Costs costs{docs.size()};

    const auto filter = MakeFilter(
      "prefix",
      {{"abcd", -1.f}, {"abcd", 0.5f}, {"abc", 0.65}, {"abcy", 0.5f}});

    irs::Scorer::ptr impl{std::make_unique<irs::BoostScore>()};
    CheckQuery(filter, std::span{&impl, 1}, docs, rdr, true, true);
  }
}

TEST_P(TermsFilterTestCase, simple_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());
  auto& segment = rdr[0];

  // empty query
  CheckQuery(irs::ByTerms(), Docs{}, Costs{0}, rdr);

  // empty field
  CheckQuery(MakeFilter("", {{"xyz", 0.5f}}), Docs{}, Costs{0}, rdr);

  // invalid field
  CheckQuery(MakeFilter("same1", {{"xyz", 0.5f}}), Docs{}, Costs{0}, rdr);

  // invalid term
  CheckQuery(MakeFilter("same", {{"invalid_term", 0.5f}}), Docs{}, Costs{0},
             rdr);

  // no value requested to match
  CheckQuery(MakeFilter("duplicated", {}), Docs{}, Costs{0}, rdr);

  // match all
  {
    Docs result(32);
    std::iota(std::begin(result), std::end(result), irs::doc_limits::min());
    Costs costs{result.size()};
    const auto filter = MakeFilter("same", {{"xyz", 1.f}});
    CheckQuery(filter, result, costs, rdr);

    // test visit
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("same");
    ASSERT_NE(nullptr, reader);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(1, visitor.visit_calls_counter());
    ASSERT_EQ(
      (std::vector<std::pair<std::string_view, irs::score_t>>{{"xyz", 1.f}}),
      visitor.term_refs<char>());
  }

  // match all
  {
    Docs result(32);
    std::iota(std::begin(result), std::end(result), irs::doc_limits::min());
    Costs costs{result.size()};
    const auto filter = MakeFilter("same", {{"invalid", 1.f}}, 0);
    CheckQuery(filter, result, costs, rdr);

    // test visit
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("same");
    ASSERT_NE(nullptr, reader);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(0, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{}),
              visitor.term_refs<char>());
  }

  // match all
  {
    Docs result(32);
    std::iota(std::begin(result), std::end(result), irs::doc_limits::min());
    Costs costs{result.size()};
    const auto filter =
      MakeFilter("same", {{"xyz", 1.f}, {"invalid_term", 0.5f}});
    CheckQuery(filter, result, costs, rdr);

    // test visit
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("same");
    ASSERT_NE(nullptr, reader);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(1, visitor.visit_calls_counter());
    ASSERT_EQ(
      (std::vector<std::pair<std::string_view, irs::score_t>>{{"xyz", 1.f}}),
      visitor.term_refs<char>());
  }

  // match something
  {
    const Docs result{1, 21, 31, 32};
    const Costs costs{result.size()};
    const auto filter =
      MakeFilter("prefix", {{"abcd", 1.f}, {"abc", 0.5f}, {"abcy", 0.5f}});
    CheckQuery(filter, result, costs, rdr);

    // test visit
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("prefix");
    ASSERT_NE(nullptr, reader);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(3, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", 0.5f}, {"abcd", 1.f}, {"abcy", 0.5f}}),
              visitor.term_refs<char>());
  }

  // duplicate terms are not allowed
  {
    const Docs result{1, 21, 31, 32};
    const Costs costs{result.size()};
    const auto filter = MakeFilter(
      "prefix", {{"abcd", 1.f}, {"abcd", 0.f}, {"abc", 0.5f}, {"abcy", 0.5f}});
    CheckQuery(filter, result, costs, rdr);

    // test visit
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("prefix");
    ASSERT_NE(nullptr, reader);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(3, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", 0.5f}, {"abcd", 1.f}, {"abcy", 0.5f}}),
              visitor.term_refs<char>());
  }

  // test non existing term
  {
    const Docs result{1, 21, 31, 32};
    const Costs costs{result.size()};
    const auto filter = MakeFilter(
      "prefix",
      {{"abcd", 1.f}, {"invalid_term", 0.f}, {"abc", 0.5f}, {"abcy", 0.5f}});
    CheckQuery(filter, result, costs, rdr);

    // test visit
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("prefix");
    ASSERT_NE(nullptr, reader);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(3, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", 0.5f}, {"abcd", 1.f}, {"abcy", 0.5f}}),
              visitor.term_refs<char>());
  }
}

TEST_P(TermsFilterTestCase, min_match) {
  // write segments
  auto writer = open_writer(irs::kOmCreate);

  {
    tests::JsonDocGenerator gen{resource("AdventureWorks2014.json"),
                                &tests::GenericJsonFieldFactory};
    add_segment(*writer, gen);
  }
  {
    tests::JsonDocGenerator gen{resource("AdventureWorks2014Edges.json"),
                                &tests::GenericJsonFieldFactory};
    add_segment(*writer, gen);
  }
  {
    tests::JsonDocGenerator gen{resource("Northwnd.json"),
                                &tests::GenericJsonFieldFactory};
    add_segment(*writer, gen);
  }
  {
    tests::JsonDocGenerator gen{resource("NorthwndEdges.json"),
                                &tests::GenericJsonFieldFactory};
    add_segment(*writer, gen);
  }

  auto rdr = open_reader();
  ASSERT_EQ(4, rdr.size());

  {
    const auto& segment = rdr[0];
    tests::EmptyFilterVisitor visitor;
    const auto* reader = segment.field("Fields");
    ASSERT_NE(nullptr, reader);
    const auto filter =
      MakeFilter("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 1);
    irs::ByTerms::visit(segment, *reader, filter.options().terms, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(2, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"BusinessEntityID", 1.f}, {"StartDate", 1.f}}),
              visitor.term_refs<char>());
  }

  {
    const Docs result{4,  5,  6,  7,  19, 20, 21, 22, 25, 27, 28, 29,
                      30, 34, 38, 46, 52, 53, 57, 62, 65, 69, 70};
    const Costs costs{25, 0, 0, 0};
    const auto filter =
      MakeFilter("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 1);
    CheckQuery(filter, result, costs, rdr);
  }

  {
    const Docs result{21, 57};
    // FIXME(gnusi): fix estimation, it's not accurate
    const Costs costs{7, 0, 0, 0};
    const auto filter =
      MakeFilter("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 2);
    CheckQuery(filter, result, costs, rdr);
  }

  {
    const Docs result{21, 57};
    // FIXME(gnusi): fix estimation, it's not accurate
    const Costs costs{7, 0, 0, 0};
    const auto filter = MakeFilter(
      "Fields",
      {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}, {"InvalidValue", 1.f}},
      2);
    CheckQuery(filter, result, costs, rdr);
  }

  {
    const Docs result{};
    const Costs costs{0, 0, 0, 0};
    const auto filter =
      MakeFilter("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 3);
    CheckQuery(filter, result, costs, rdr);
  }

  {
    const Docs result{};
    const Costs costs{0, 0, 0, 0};
    const auto filter = MakeFilter("Fields",
                                   {{"BusinessEntityID", 1.f},
                                    {"StartDate", 1.f},
                                    {"InvalidValue0", 1.f},
                                    {"InvalidValue0", 1.f}},
                                   3);
    CheckQuery(filter, result, costs, rdr);
  }

  // empty prefix test collector call count for field/term/finish
  {
    ScoredDocs result(71);
    for (irs::doc_id_t i = 0; auto& [doc, scores] : result) {
      doc = ++i;
      scores = {0.f};
    }
    for (const auto doc : {4,  5,  6,  7,  19, 20, 21, 22, 25, 27, 28, 29,
                           30, 34, 38, 46, 52, 53, 57, 62, 65, 69, 70}) {
      result[doc - 1].second = {static_cast<float_t>(doc)};
    }
    for (const auto doc : {21, 57}) {
      result[doc - 1].second = {static_cast<float_t>(2 * doc)};
    }

    const Costs costs{25, 0, 0, 0};

    size_t collect_field_count = 0;
    size_t collect_term_count = 0;
    size_t finish_count = 0;

    irs::Scorer::ptr impl{std::make_unique<tests::sort::CustomSort>()};
    auto* scorer = static_cast<tests::sort::CustomSort*>(impl.get());

    scorer->collector_collect_field = [&collect_field_count](
                                        const irs::SubReader&,
                                        const irs::TermReader&) -> void {
      ++collect_field_count;
    };
    scorer->collector_collect_term =
      [&collect_term_count](const irs::SubReader&, const irs::TermReader&,
                            const irs::AttributeProvider&) -> void {
      ++collect_term_count;
    };
    scorer->collectors_collect =
      [&finish_count](irs::byte_type*, const irs::FieldCollector*,
                      const irs::TermCollector*) -> void { ++finish_count; };
    scorer->prepare_field_collector = [&scorer]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(*scorer);
    };
    scorer->prepare_term_collector = [&scorer]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(*scorer);
    };
    scorer->prepare_scorer =
      [](const irs::ColumnProvider&, const irs::FieldProperties&,
         const irs::byte_type*, const irs::AttributeProvider& attrs,
         irs::score_t boost) -> irs::ScoreFunction {
      struct ScoreCtx final : public irs::ScoreCtx {
        ScoreCtx(const irs::DocAttr* doc, irs::score_t boost) noexcept
          : doc{doc}, boost{boost} {}
        const irs::DocAttr* doc;
        irs::score_t boost;
      };

      auto* doc = irs::get<irs::DocAttr>(attrs);
      EXPECT_NE(nullptr, doc);

      return irs::ScoreFunction::Make<ScoreCtx>(
        [](irs::ScoreCtx* ctx, irs::score_t* res) noexcept {
          auto* state = static_cast<ScoreCtx*>(ctx);
          *res = static_cast<irs::score_t>(state->doc->value) * state->boost;
        },
        irs::ScoreFunction::DefaultMin, doc, boost);
    };

    const auto filter =
      MakeFilter("Fields", {{"BusinessEntityID", 1.f}, {"StartDate", 1.f}}, 0);

    CheckQuery(filter, std::span{&impl, 1}, result, rdr[0]);
    ASSERT_EQ(1, collect_field_count);  // 1 fields in 1 segment
    ASSERT_EQ(2, collect_term_count);   // 2 different terms
    ASSERT_EQ(3, finish_count);         // 3 unque terms
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  terms_filter_test, TermsFilterTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  TermsFilterTestCase::to_string);
