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


// Integration/Execution tests with index
// Using regexp_test_data.json


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

  // [A-Z]+[0-9]+
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

  // [0-9]+[a-z]+
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


// Scoring tests (ported from wildcard, using simple_sequential.json)

TEST_P(RegexpFilterTestCase, by_regexp_scoring_custom_sort) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // CustomSort collector counts: .* on "prefix" field
  // 9 unique terms in "prefix" field, 10 docs
  {
    Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};

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
    scorer.prepare_field_collector = [&scorer]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(scorer);
    };
    scorer.prepare_term_collector = [&scorer]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
    };

    CheckQuery(MakeFilter("prefix", ".*"), order, docs, rdr);
    ASSERT_EQ(9, collect_field_count);  // 9 unique terms (1 per term, disjunction)
    ASSERT_EQ(9, collect_term_count);   // 9 different terms
    ASSERT_EQ(9, finish_count);         // 9 unique terms
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_scoring_frequency_sort) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // FrequencySort: .* on "prefix" — ordered by term frequency
  // "abcy" appears in docs 31,32 (freq=2) → first, rest freq=1
  {
    Docs docs{31, 32, 1, 4, 9, 16, 21, 24, 26, 29};

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    CheckQuery(MakeFilter("prefix", ".*"), order, docs, rdr);
  }

  // FrequencySort: a.* on "prefix" — prefix "a"
  {
    Docs docs{31, 32, 1, 4, 16, 21, 26, 29};

    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};

    CheckQuery(MakeFilter("prefix", "a.*"), order, docs, rdr);
  }
}


// Match all / match nothing (using simple_sequential_utf8.json)

TEST_P(RegexpFilterTestCase, by_regexp_match_all) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // All 32 docs have same="xyz"
  Docs all;
  for (size_t i = 0; i < 32; ++i) {
    all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
  }
  Costs all_costs{all.size()};

  // .* matches everything (including "xyz")
  CheckQuery(MakeFilter("same", ".*"), all, all_costs, rdr);

  // ... matches exactly 3-char terms ("xyz" is 3 chars)
  CheckQuery(MakeFilter("same", "..."), all, all_costs, rdr);

  // .+ matches any non-empty string
  CheckQuery(MakeFilter("same", ".+"), all, all_costs, rdr);

  // x.z matches "xyz" (x + any + z)
  CheckQuery(MakeFilter("same", "x.z"), all, all_costs, rdr);

  // x.*z matches "xyz"
  CheckQuery(MakeFilter("same", "x.*z"), all, all_costs, rdr);

  // . matches only single-char terms — "xyz" is 3 chars → no match
  CheckQuery(MakeFilter("same", "."), Docs{}, Costs{0}, rdr);

  // .. matches only 2-char terms → no match
  CheckQuery(MakeFilter("same", ".."), Docs{}, Costs{0}, rdr);

  // empty query
  CheckQuery(irs::ByRegexp(), Docs{}, Costs{0}, rdr);

  // empty field
  CheckQuery(MakeFilter("", "xyz.*"), Docs{}, Costs{0}, rdr);

  // invalid field
  CheckQuery(MakeFilter("same1", "xyz.*"), Docs{}, Costs{0}, rdr);

  // no match prefix
  CheckQuery(MakeFilter("same", "xyz_invalid.*"), Docs{}, Costs{0}, rdr);

  // empty pattern - exact match empty term (no docs have empty "same" value)
  CheckQuery(MakeFilter("duplicated", ""), Docs{}, Costs{0}, rdr);
}


// Wildcard-equivalent patterns (ported from wildcard, using
// simple_sequential_utf8.json)

TEST_P(RegexpFilterTestCase, by_regexp_wildcard_equivalent_patterns) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // --- "duplicated" field patterns ---
  // Wildcard v_z% → regex v.z.*
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("duplicated", "v.z.*"), docs, costs, rdr);
  }

  // Wildcard v%c → regex v.*c
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("duplicated", "v.*c"), docs, costs, rdr);
  }

  // Wildcard %c → regex .*c
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("duplicated", ".*c"), docs, costs, rdr);
  }

  // Wildcard %_c → regex .*.c
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("duplicated", ".*.c"), docs, costs, rdr);
  }

  // Wildcard a% → regex a.*
  {
    Docs docs{1, 5, 11, 21, 27, 31};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("duplicated", "a.*"), docs, costs, rdr);
  }

  // Wildcard vcz% → regex vcz.*
  {
    Docs docs{2, 3, 8, 14, 17, 19, 24};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("duplicated", "vcz.*"), docs, costs, rdr);
  }

  // --- "prefix" field patterns ---
  // Wildcard %c% → regex .*c.*
  {
    Docs docs{1, 4, 9, 21, 26, 31, 32};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("prefix", ".*c.*"), docs, costs, rdr);
  }

  // Wildcard abc% → regex abc.*
  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("prefix", "abc.*"), docs, costs, rdr);
  }

  // Wildcard a%d% → regex a.*d.*
  {
    Docs docs{1, 4, 16, 26};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("prefix", "a.*d.*"), docs, costs, rdr);
  }

  // Wildcard b% → regex b.*
  {
    Docs docs{9, 24};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("prefix", "b.*"), docs, costs, rdr);
  }

  // Exact match
  CheckQuery(MakeFilter("prefix", "bateradsfsfasdf"), Docs{24}, Costs{1}, rdr);

  // Wildcard !% on name → regex !.* (only doc28 has name "!")
  // Actually name="!" is exact, so "!.*" matches "!" (since .* can be empty)
  CheckQuery(MakeFilter("name", "!.*"), Docs{28}, Costs{1}, rdr);
}


// UTF-8 execution tests (using simple_sequential_utf8.json)

TEST_P(RegexpFilterTestCase, by_regexp_utf8_execution) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // utf8 field values:
  // doc1:  "пуй"
  // doc2:  "хублот"
  // doc3:  "проглот"
  // doc14: "обама"
  // doc17: "трамп"
  // doc24: "меркель"
  // doc26: "вий"

  // Prefix п.* → "пуй"(1), "проглот"(3)
  {
    Docs docs{1, 3};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("utf8", "\xD0\xBF.*"), docs, costs, rdr);
  }

  // Suffix .*й → "пуй"(1), "вий"(26)
  {
    Docs docs{1, 26};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("utf8", ".*\xD0\xB9"), docs, costs, rdr);
  }

  // Infix в.*й → "вий"(26)
  {
    Docs docs{26};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("utf8", "\xD0\xB2.*\xD0\xB9"), docs, costs, rdr);
  }

  // Suffix .*лот → "хублот"(2), "проглот"(3)
  {
    Docs docs{2, 3};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("utf8", ".*\xD0\xBB\xD0\xBE\xD1\x82"), docs, costs,
               rdr);
  }

  // Dot with UTF-8: п..  → "пуй"(1)  (п + any + any = 3 codepoints)
  {
    Docs docs{1};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("utf8", "\xD0\xBF.."), docs, costs, rdr);
  }

  // Alternation: пуй|вий → docs 1, 26
  {
    Docs docs{1, 26};
    Costs costs{docs.size()};
    CheckQuery(
      MakeFilter("utf8",
                 "\xD0\xBF\xD1\x83\xD0\xB9|\xD0\xB2\xD0\xB8\xD0\xB9"),
      docs, costs, rdr);
  }

  // .* matches all docs with utf8 field
  {
    Docs docs{1, 2, 3, 14, 17, 24, 26};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("utf8", ".*"), docs, costs, rdr);
  }

  // No match — nonexistent UTF-8 term
  CheckQuery(MakeFilter("utf8", "\xD1\x8F.*"), Docs{}, Costs{0}, rdr);
}


// Cross-validation: ByRegexp vs ByTerm/ByPrefix on real index

TEST_P(RegexpFilterTestCase, by_regexp_cross_validation) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // ByRegexp("term", "foobar") should match same docs as ByTerm("term","foobar")
  {
    Docs regexp_docs, term_docs;
    Costs regexp_costs, term_costs;

    auto regexp_q = MakeFilter("term", "foobar");
    auto term_q = MakeFilter<irs::ByTerm>("term", "foobar");

    // Both should return doc 1
    CheckQuery(regexp_q, Docs{1}, Costs{1}, rdr);
    CheckQuery(term_q, Docs{1}, Costs{1}, rdr);
  }

  // ByRegexp("term", "foo.*") should match same docs as ByPrefix("term","foo")
  {
    Docs expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20};
    Costs costs{expected.size()};

    CheckQuery(MakeFilter("term", "foo.*"), expected, costs, rdr);
    CheckQuery(MakeFilter<irs::ByPrefix>("term", "foo"), expected, costs, rdr);
  }

  // ByRegexp("term", ".*") should match all docs with "term" field
  {
    Docs expected;
    for (irs::doc_id_t i = 1; i <= 20; ++i) {
      expected.push_back(i);
    }
    Costs costs{expected.size()};

    CheckQuery(MakeFilter("term", ".*"), expected, costs, rdr);
    CheckQuery(MakeFilter<irs::ByPrefix>("term", ""), expected, costs, rdr);
  }
}


// Two-segment test


// TODO: Two-segment test



// Negation character class [^...]

TEST_P(RegexpFilterTestCase, by_regexp_negation_char_class) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // [^a-z]+[0-9]+ on "class" field — starts with non-lowercase chars + digits
  // Matches uppercase: ABC123(3), GHI789(7), MNO345(9), STU901(11),
  //   YZA567(13), EFG111(15), KLM333(17), QRS555(19)
  // Also matches digit-first: 123abc(5)→ no, "123" are [^a-z] then "abc" are
  //   not [0-9]. Actually [^a-z]+[0-9]+ means one-or-more non-lowercase then
  //   one-or-more digits.
  //   "123abc": '1','2','3' are [^a-z], then 'a' is not [0-9] → fails as
  //             full match. Actually "123abc" = [^a-z]{3}[a-z]{3} which is
  //             [^a-z]+[0-9]+? No — 'a','b','c' are not [0-9]. So no match.
  //   "456def": same, no match.
  // So only uppercase+digits:
  {
    Docs docs{3, 7, 9, 11, 13, 15, 17, 19};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("class", "[^a-z]+[0-9]+"), docs, costs, rdr);
  }
}


// Invalid patterns — execution smoke test (no crash, 0 results)

TEST_P(RegexpFilterTestCase, by_regexp_invalid_pattern_execution) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Invalid patterns should return 0 documents, not crash
  CheckQuery(MakeFilter("term", "(abc"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("term", "[abc"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("term", "a**"), Docs{}, Costs{0}, rdr);
}


// Filter reuse — prepare() twice on different readers

TEST_P(RegexpFilterTestCase, by_regexp_filter_reuse) {
  MaxMemoryCounter counter;

  auto q = MakeFilter("same", ".*");

  // Prepare on empty reader
  {
    auto prepared = q.prepare({
      .index = irs::SubReader::empty(),
      .memory = counter,
    });
    ASSERT_NE(nullptr, prepared);
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();

  // Now prepare on real reader
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs all;
    for (size_t i = 0; i < 32; ++i) {
      all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }
    Costs costs{all.size()};
    CheckQuery(q, all, costs, rdr);
  }
}


// Visitor tests (including wildcard-like pattern)

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

TEST_P(RegexpFilterTestCase, visit_wildcard_like) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");

  // Regex a.c.* is analogous to wildcard a_c%
  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("a.c.*"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
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
