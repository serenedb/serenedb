////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/multiterm_query.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <type_traits>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

template<typename Filter = irs::ByRegexp>
Filter MakeFilter(std::string_view field, std::string_view value) {
  Filter q;
  *q.mutable_field() = field;
  if constexpr (std::is_same_v<Filter, irs::ByRegexp>) {
    q.mutable_options()->pattern = irs::ViewCast<irs::byte_type>(value);
  } else {
    q.mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  }
  return q;
}

}  // namespace

TEST(by_regexp_test, options) {
  irs::ByRegexpOptions opts;
  ASSERT_TRUE(opts.pattern.empty());
  ASSERT_EQ(1024, opts.scored_terms_limit);
}

TEST(by_regexp_test, ctor) {
  irs::ByRegexp q;
  ASSERT_EQ(irs::Type<irs::ByRegexp>::id(), q.type());
  ASSERT_EQ(irs::ByRegexpOptions{}, q.options());
  ASSERT_TRUE(q.field().empty());
  ASSERT_EQ(irs::kNoBoost, q.Boost());
}

TEST(by_regexp_test, equal) {
  const irs::ByRegexp q = MakeFilter("field", "bar.*");

  ASSERT_EQ(q, MakeFilter("field", "bar.*"));
  ASSERT_NE(q, MakeFilter("field1", "bar.*"));
  ASSERT_NE(q, MakeFilter("field", "bar"));

  irs::ByRegexp q1 = MakeFilter("field", "bar.*");
  q1.mutable_options()->scored_terms_limit = 100;
  ASSERT_NE(q, q1);
}

TEST(by_regexp_test, boost) {
  MaxMemoryCounter counter;

  {
    irs::ByRegexp q = MakeFilter("field", "bar.*");

    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  {
    irs::score_t boost = 1.5f;

    irs::ByRegexp q = MakeFilter("field", "bar.*");
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


TEST(by_regexp_test, test_type_of_prepared_query) {
  MaxMemoryCounter counter;

  {
    auto lhs = MakeFilter<irs::ByTerm>("foo", "bar")
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

  {
    auto lhs = MakeFilter<irs::ByPrefix>("foo", "bar")
                 .prepare({
                   .index = irs::SubReader::empty(),
                   .memory = counter,
                 });
    auto rhs = MakeFilter("foo", "bar.*")
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

  {
    auto lhs = MakeFilter<irs::ByTerm>("foo", "").prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    auto rhs = MakeFilter("foo", "").prepare({
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

//=============================================================================
// Integration/Execution tests with index
// Using regexp_test_data.json
//=============================================================================

class RegexpFilterTestCase : public tests::FilterTestCaseBase {};

TEST_P(RegexpFilterTestCase, by_regexp_foo_dot_star_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // foo.*bar
  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 16, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo.*bar"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_foo_star_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{1, 15, 16};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo*bar"), docs, costs, rdr);
  }
}

// Suffix pattern: .*bar

TEST_P(RegexpFilterTestCase, by_regexp_suffix) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 15, 16, 17, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", ".*bar"), docs, costs, rdr);
  }
}

// Prefix pattern: foo.*

TEST_P(RegexpFilterTestCase, by_regexp_prefix) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo.*"), docs, costs, rdr);
  }
}

// Optional quantifier: ?

TEST_P(RegexpFilterTestCase, by_regexp_optional_colou_r) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{1, 2};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("opt", "colou?r"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_optional_gra_y) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{8, 12};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("opt", "gra?y"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_optional_multiple) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{18};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("opt", "a?bb?"), docs, costs, rdr);
  }
}

// Plus quantifier: +

TEST_P(RegexpFilterTestCase, by_regexp_plus_fo_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{1, 2, 3, 8};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("plus", "fo+bar"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_plus_f_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{4, 11, 15};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("plus", "f+bar"), docs, costs, rdr);
  }
}

// Alternation: |

TEST_P(RegexpFilterTestCase, by_regexp_alternation_two) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // cat|dog
  {
    Docs docs{1, 2, 4, 6, 8, 10, 11, 14, 15, 17, 18, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("alt", "cat|dog"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_alternation_three) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // cat|dog|bird
  {
    Docs docs{1, 2, 3, 4, 6, 7, 8, 10, 11, 12, 14, 15, 16, 17, 18, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("alt", "cat|dog|bird"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_alternation_single) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{9};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("alt", "mouse"), docs, costs, rdr);
  }
}

// Character classes: [...]

TEST_P(RegexpFilterTestCase, by_regexp_char_class_lower_digits) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // [a-z]+[0-9]+ = lowercase letters followed by digits
  // Matches: abc123(1), xyz789(2), def456(4), jkl012(8), pqr678(10),
  //          vwx234(12), bcd890(14), hij222(16), nop444(18), tuv666(20)
  {
    Docs docs{1, 2, 4, 8, 10, 12, 14, 16, 18, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("class", "[a-z]+[0-9]+"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_char_class_upper_digits) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // [A-Z]+[0-9]+ = uppercase letters followed by digits
  // Matches: ABC123(3), GHI789(7), MNO345(9), STU901(11), YZA567(13),
  //          EFG111(15), KLM333(17), QRS555(19)
  {
    Docs docs{3, 7, 9, 11, 13, 15, 17, 19};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("class", "[A-Z]+[0-9]+"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_char_class_digits_letters) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // [0-9]+[a-z]+ = digits followed by lowercase letters
  // Matches: 123abc(5), 456def(6)
  {
    Docs docs{5, 6};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("class", "[0-9]+[a-z]+"), docs, costs, rdr);
  }
}

// Dot

TEST_P(RegexpFilterTestCase, by_regexp_dot_single) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // foo.bar = foo + any single char + bar
  {
    Docs docs{14, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo.bar"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_dot_multiple) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // foo...bar = foo + exactly 3 chars + bar
  {
    Docs docs{2, 3, 4, 6, 7};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo...bar"), docs, costs, rdr);
  }
}

// Combined patterns

TEST_P(RegexpFilterTestCase, by_regexp_combined_group_quantifier) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // (foo|fo).*bar = ("foo" or "fo") + anything + "bar"
  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "(foo|fo).*bar"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_combined_optional_star) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // foo?.*bar = fo + (o?) + (.*) + bar
  // = "fo" or "foo" followed by anything and "bar"
  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo?.*bar"), docs, costs, rdr);
  }
}

// Edge cases

TEST_P(RegexpFilterTestCase, by_regexp_empty_filter) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  CheckQuery(irs::ByRegexp(), Docs{}, Costs{0}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_no_match) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  CheckQuery(MakeFilter("term", "nomatch.*"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("term", "zzz"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("nonexistent", ".*"), Docs{}, Costs{0}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_exact_match) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Exact literal match
  {
    Docs docs{1};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foobar"), docs, costs, rdr);
  }

  {
    Docs docs{9};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", "foo"), docs, costs, rdr);
  }
}

// Visitor tests

TEST_P(RegexpFilterTestCase, visit_literal) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];

  const auto* reader = segment.field("prefix");
  ASSERT_NE(nullptr, reader);

  {
    auto term = irs::ViewCast<irs::byte_type>(std::string_view("abc"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(term);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(1, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost},
              }),
              visitor.term_refs<char>());
  }
}

TEST_P(RegexpFilterTestCase, visit_prefix) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");

  {
    auto prefix = irs::ViewCast<irs::byte_type>(std::string_view("ab.*"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(prefix);
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
  }
}

TEST_P(RegexpFilterTestCase, visit_invalid_pattern) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");

  // Unclosed parenthesis
  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("(abc"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(0, visitor.prepare_calls_counter());
    ASSERT_EQ(0, visitor.visit_calls_counter());
  }

  // Unclosed bracket
  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("[abc"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(0, visitor.prepare_calls_counter());
  }

  // Double quantifier
  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("a**"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(0, visitor.prepare_calls_counter());
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  regexp_filter_test, RegexpFilterTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  RegexpFilterTestCase::to_string);
