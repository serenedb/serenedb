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

#include "filter_test_case_base.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "tests_shared.hpp"

namespace {

// Stable field ids for the wildcard fixtures. Sourced from
// `tests::FieldIdFor` so the shared JSON factories and these tests agree
// on the id-per-name.
[[maybe_unused]] inline constexpr irs::field_id kFooId =
  tests::FieldIdFor("foo");
[[maybe_unused]] inline constexpr irs::field_id kFieldId =
  tests::FieldIdFor("field");
[[maybe_unused]] inline constexpr irs::field_id kField1Id =
  tests::FieldIdFor("field1");
[[maybe_unused]] inline constexpr irs::field_id kPrefixId =
  tests::FieldIdFor("prefix");
[[maybe_unused]] inline constexpr irs::field_id kSameId =
  tests::FieldIdFor("same");
[[maybe_unused]] inline constexpr irs::field_id kDuplicatedId =
  tests::FieldIdFor("duplicated");
[[maybe_unused]] inline constexpr irs::field_id kNameId =
  tests::FieldIdFor("name");
[[maybe_unused]] inline constexpr irs::field_id kUtf8Id =
  tests::FieldIdFor("utf8");
[[maybe_unused]] inline constexpr irs::field_id kInvalidFieldId =
  tests::FieldIdFor("invalid_field");
[[maybe_unused]] inline constexpr irs::field_id kEmptyFieldId =
  irs::field_limits::invalid();

template<typename Filter = irs::ByWildcard>
Filter MakeFilter(irs::field_id field, std::string_view term) {
  Filter q;
  *q.mutable_field_id() = field;
  if constexpr (std::is_same_v<Filter, irs::ByWildcard>) {
    *q.mutable_options() =
      irs::ByWildcardOptions{irs::ViewCast<irs::byte_type>(term)};
  } else {
    q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  }
  return q;
}

// Resolves a wildcard pattern into its concrete executable filter
// (ByTerm / ByPrefix / AutomatonFilter), mirroring how callers build and
// optimize wildcard filters in production.
irs::Filter::ptr MakeWildcard(irs::field_id field, std::string_view term) {
  auto filter =
    irs::CreateByWildcard(field, irs::ViewCast<irs::byte_type>(term));
  irs::Optimize(filter);
  return filter;
}

}  // namespace

TEST(by_wildcard_test, options) {
  irs::ByWildcardOptions opts;
  ASSERT_TRUE(opts.term.empty());
  ASSERT_EQ(1024, opts.scored_terms_limit);
}

TEST(by_wildcard_test, ctor) {
  irs::ByWildcard q;
  ASSERT_EQ(irs::Type<irs::ByWildcard>::id(), q.type());
  ASSERT_EQ(irs::ByWildcardOptions{}, q.options());
  ASSERT_EQ(irs::field_limits::invalid(), q.field_id());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_wildcard_test, equal) {
  const irs::ByWildcard q = MakeFilter(kFieldId, "bar*");

  ASSERT_EQ(q, MakeFilter(kFieldId, "bar*"));
  ASSERT_NE(q, MakeFilter(kField1Id, "bar*"));
  ASSERT_NE(q, MakeFilter(kFieldId, "bar"));

  irs::ByWildcard q1 = MakeFilter(kFieldId, "bar*");
  q1.mutable_options()->scored_terms_limit = 100;
  ASSERT_NE(q, q1);
}

TEST(by_wildcard_test, boost) {
  MaxMemoryCounter counter;

  // no boost
  {
    irs::Filter::ptr q = MakeWildcard(kFieldId, "bar*");

    tests::PreparedFilter prepared{*q, irs::SubReader::empty(), nullptr,
                                   counter};
    ASSERT_EQ(irs::kNoBoost, prepared.Query(0)->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // with boost
  {
    irs::score_t boost = 1.5f;

    irs::Filter::ptr q = irs::CreateByWildcard(
      kFieldId, irs::ViewCast<irs::byte_type>(std::string_view("bar*")), 1024,
      boost);
    irs::Optimize(q);

    tests::PreparedFilter prepared{*q, irs::SubReader::empty(), nullptr,
                                   counter};
    ASSERT_EQ(boost, prepared.Query(0)->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST(by_wildcard_test, test_type_of_prepared_query) {
  MaxMemoryCounter counter;

  // term query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByTerm>(kFooId, "bar"),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "bar"),
                              irs::SubReader::empty(), nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // term query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByTerm>(kFooId, ""),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, ""), irs::SubReader::empty(),
                              nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // term query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByTerm>(kFooId, "foo%"),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "foo\\%"),
                              irs::SubReader::empty(), nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // prefix query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByPrefix>(kFooId, "bar"),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "bar%"),
                              irs::SubReader::empty(), nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // prefix query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByPrefix>(kFooId, "bar"),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "bar%%"),
                              irs::SubReader::empty(), nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // term query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByTerm>(kFooId, "bar%"),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "bar\\%"),
                              irs::SubReader::empty(), nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // all query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByPrefix>(kFooId, ""),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "%"), irs::SubReader::empty(),
                              nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // all query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByPrefix>(kFooId, ""),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "%%"), irs::SubReader::empty(),
                              nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // term query
  {
    tests::PreparedFilter lhs{MakeFilter<irs::ByTerm>(kFooId, "%"),
                              irs::SubReader::empty(), nullptr, counter};
    tests::PreparedFilter rhs{MakeFilter(kFooId, "\\%"),
                              irs::SubReader::empty(), nullptr, counter};
    auto& lhs_ref = *lhs.Query(0);
    auto& rhs_ref = *rhs.Query(0);
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

class WildcardFilterTestCase : public tests::FilterTestCaseBase {};

TEST_P(WildcardFilterTestCase, simple_sequential_order) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  CheckQuery(*MakeWildcard(kEmptyFieldId, ""), Docs{}, Costs{0}, rdr);

  // empty prefix test collector call count for field/term/finish
  {
    Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};
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
    CheckQuery(*MakeWildcard(kPrefixId, "%"), order, docs, rdr);
    ASSERT_EQ(9, finish_count);
    ASSERT_GT(finish_docs_with_field, 0u);  // scorer collected field stats
    ASSERT_GT(finish_docs_with_term, 0u);   // scorer collected term stats
  }

  // match all
  {
    Docs docs{31, 32, 1, 4, 9, 16, 21, 24, 26, 29};
    Costs costs{docs.size()};

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    CheckQuery(*MakeWildcard(kPrefixId, "%"), order, docs, rdr);
  }

  // prefix
  {
    Docs docs{31, 32, 1, 4, 16, 21, 26, 29};
    Costs costs{docs.size()};

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    CheckQuery(*MakeWildcard(kPrefixId, "a%"), order, docs, rdr);
  }
}

TEST_P(WildcardFilterTestCase, simple_sequential) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // empty query
  CheckQuery(*MakeWildcard(kEmptyFieldId, ""), Docs{}, Costs{0}, rdr);

  // empty field
  CheckQuery(*MakeWildcard(kEmptyFieldId, "xyz%"), Docs{}, Costs{0}, rdr);

  // invalid field
  CheckQuery(*MakeWildcard(kInvalidFieldId, "xyz%"), Docs{}, Costs{0}, rdr);

  // invalid prefix
  CheckQuery(*MakeWildcard(kSameId, "xyz_invalid%"), Docs{}, Costs{0}, rdr);

  // empty pattern - no match
  CheckQuery(*MakeWildcard(kDuplicatedId, ""), Docs{}, Costs{0}, rdr);

  // match all
  {
    Docs result;
    for (size_t i = 0; i < 32; ++i) {
      result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }

    Costs costs{result.size()};

    CheckQuery(*MakeWildcard(kSameId, "%"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "___"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "%_"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "_%"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "x_%"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "__z"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "%_z"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "x%_"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "x_%"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "x_z"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "x%z"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "_yz"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "%yz"), result, costs, rdr);
    CheckQuery(*MakeWildcard(kSameId, "xyz"), result, costs, rdr);
  }

  // match nothing
  CheckQuery(*MakeWildcard(kPrefixId, "ab\\%"), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeWildcard(kSameId, "x\\_z"), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeWildcard(kSameId, "x\\%z"), Docs{}, Costs{0}, rdr);
  CheckQuery(*MakeWildcard(kSameId, "_"), Docs{}, Costs{0}, rdr);

  // escaped prefix
  {
    Docs result{10, 11};
    Costs costs{result.size()};

    CheckQuery(*MakeWildcard(kPrefixId, "ab\\\\%"), result, costs, rdr);
  }

  // escaped term
  {
    Docs result{10};
    Costs costs{result.size()};

    CheckQuery(*MakeWildcard(kPrefixId, "ab\\\\\\%"), result, costs, rdr);
  }

  // escaped term
  {
    Docs result{11};
    Costs costs{result.size()};

    CheckQuery(*MakeWildcard(kPrefixId, "ab\\\\\\\\%"), result, costs, rdr);
  }

  // valid prefix
  {
    Docs result;
    for (size_t i = 0; i < 32; ++i) {
      result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }

    Costs costs{result.size()};

    CheckQuery(*MakeWildcard(kSameId, "xyz%"), result, costs, rdr);
  }

  // pattern
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kDuplicatedId, "v_z%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kDuplicatedId, "v%c"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kDuplicatedId, "v%%%%%c"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kDuplicatedId, "%c"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kDuplicatedId, "%_c"), docs, costs, rdr);
  }

  // pattern
  {
    Docs docs{1, 4, 9, 21, 26, 31, 32};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kPrefixId, "%c%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kPrefixId, "%c%%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kPrefixId, "%%%%c%%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kPrefixId, "%%c%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kPrefixId, "%%c%%"), docs, costs, rdr);
  }

  // single digit prefix
  {
    Docs docs{1, 5, 11, 21, 27, 31};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kDuplicatedId, "a%"), docs, costs, rdr);
  }

  CheckQuery(*MakeWildcard(kNameId, "!%"), Docs{28}, Costs{1}, rdr);
  CheckQuery(*MakeWildcard(kPrefixId, "b%"), Docs{9, 24}, Costs{2}, rdr);

  // multiple digit prefix
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kDuplicatedId, "vcz%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kDuplicatedId, "vcz%%%%%"), docs, costs, rdr);
  }

  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};
    CheckQuery(*MakeWildcard(kPrefixId, "abc%"), docs, costs, rdr);
  }

  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kPrefixId, "abc%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kPrefixId, "abc%%"), docs, costs, rdr);
  }

  {
    Docs docs{1, 4, 16, 26};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kPrefixId, "a%d%"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kPrefixId, "a%d%%"), docs, costs, rdr);
  }

  {
    Docs docs{1, 26};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kUtf8Id, "\x25\xD0\xB9"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kUtf8Id, "\x25\x25\xD0\xB9"), docs, costs, rdr);
  }

  {
    Docs docs{26};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kUtf8Id, "\xD0\xB2\x25\xD0\xB9"), docs, costs,
               rdr);
    CheckQuery(*MakeWildcard(kUtf8Id, "\xD0\xB2\x25\x25\xD0\xB9"), docs, costs,
               rdr);
  }

  {
    Docs docs{1, 3};
    Costs costs{docs.size()};

    CheckQuery(*MakeWildcard(kUtf8Id, "\xD0\xBF\x25"), docs, costs, rdr);
    CheckQuery(*MakeWildcard(kUtf8Id, "\xD0\xBF\x25\x25"), docs, costs, rdr);
  }

  // whole word
  CheckQuery(*MakeWildcard(kPrefixId, "bateradsfsfasdf"), Docs{24}, Costs{1},
             rdr);
}

TEST_P(WildcardFilterTestCase, visit) {
  // add segment
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  const irs::field_id field = kPrefixId;

  // read segment
  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];
  // get term dictionary for field
  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);

  {
    auto term = irs::ViewCast<irs::byte_type>(std::string_view("abc"));
    tests::EmptyFilterVisitor visitor;
    auto automaton = irs::FromWildcard(term);
    auto field_visitor = irs::AutomatonFilter::visitor(automaton);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(1, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost},
              }),
              visitor.term_refs<char>());

    visitor.reset();
  }

  {
    auto prefix = irs::ViewCast<irs::byte_type>(std::string_view("ab%"));
    tests::EmptyFilterVisitor visitor;
    auto automaton = irs::FromWildcard(prefix);
    auto field_visitor = irs::AutomatonFilter::visitor(automaton);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(6, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost},
                {"abcd", irs::kNoBoost},
                {"abcde", irs::kNoBoost},
                {"abcdrer", irs::kNoBoost},
                {"abcy", irs::kNoBoost},
                {"abde", irs::kNoBoost}}),
              visitor.term_refs<char>());

    visitor.reset();
  }

  {
    auto wildcard = irs::ViewCast<irs::byte_type>(std::string_view("a_c%"));
    tests::EmptyFilterVisitor visitor;
    auto automaton = irs::FromWildcard(wildcard);
    auto field_visitor = irs::AutomatonFilter::visitor(automaton);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(5, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost},
                {"abcd", irs::kNoBoost},
                {"abcde", irs::kNoBoost},
                {"abcdrer", irs::kNoBoost},
                {"abcy", irs::kNoBoost},
              }),
              visitor.term_refs<char>());

    visitor.reset();
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(wildcard_filter_test, WildcardFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         WildcardFilterTestCase::to_string);
