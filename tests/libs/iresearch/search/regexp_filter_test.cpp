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

#include <chrono>
#include <type_traits>

#include "filter_test_case_base.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/index_utils.hpp"
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

// Scoring (ported from wildcard, using simple_sequential.json)

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
    ASSERT_EQ(9,
              collect_field_count);  // 9 unique terms (1 per term, disjunction)
    ASSERT_EQ(9, collect_term_count);  // 9 different terms
    ASSERT_EQ(9, finish_count);        // 9 unique terms
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

// simple_sequential_utf8.json)

// Wildcard-equivalent patterns (ported from wildcard, using
// simple_sequential_utf8.json)

TEST_P(RegexpFilterTestCase, by_regexp_wildcard_equivalent_patterns) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "duplicated" field patterns
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

  //   "prefix" field patterns
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

// UTF-8 execution (using simple_sequential_utf8.json)

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
      MakeFilter("utf8", "\xD0\xBF\xD1\x83\xD0\xB9|\xD0\xB2\xD0\xB8\xD0\xB9"),
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

  // ByRegexp("term", "foobar") should match same docs as
  // ByTerm("term","foobar")
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

// Visitor API

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

// If unsupported: 0 results, no crash

// Counted quantifiers {n}, {n,}, {n,m} — smoke tests

TEST_P(RegexpFilterTestCase, by_regexp_counted_quantifiers_smoke) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // If {n} is supported, fo{2}bar = foobar → doc1
  // If not supported → 0 results, no crash
  {
    auto q = MakeFilter("term", "fo{2}bar");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
    // Just verify no crash — result depends on support
  }

  // fo{1,3}bar — if supported
  {
    auto q = MakeFilter("term", "fo{1,3}bar");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // fo{2,}bar — if supported
  {
    auto q = MakeFilter("term", "fo{2,}bar");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // .{6} — exactly 6 chars — if supported
  {
    auto q = MakeFilter("term", ".{6}");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }
}

// Anchoring — ^$ behavior

TEST_P(RegexpFilterTestCase, by_regexp_anchoring) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // ^foobar$ should behave like foobar (full match is default)
  CheckQuery(MakeFilter("term", "^foobar$"), Docs{1}, Costs{1}, rdr);

  // ^foo should behave like foo.* or just foo (depends on implementation)
  // Smoke test: no crash
  {
    auto q = MakeFilter("term", "^foo");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // foo$ — smoke test
  {
    auto q = MakeFilter("term", "foo$");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // ^$ — empty string
  {
    auto q = MakeFilter("term", "^$");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }
}

// Greedy quantifiers

TEST_P(RegexpFilterTestCase, by_regexp_greedy_quantifiers) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // a.*a — terms containing 'a' ... 'a' (at least two 'a's)
  // foobar(1):a at pos 4 only → no; foobarbar(4):a at 4,7 → yes;
  // foobarbazbar(5):a at 4,7,10 → yes; foobasbar(6):a at 4,7 → yes;
  // foobaxbar(7):a at 4,7 → yes; foobabar(8):a at 4,6 → yes;
  // fooba(11):a at 4 only → no; foobarbaz(13):a at 4,7 → yes;
  // xfoobar(17):a at 5 only → no; foobarx(18):a at 4 only → no;
  // xfoobarx(19):a at 5 only → no; fooxbar(20):a at 5 only → no;
  // bar(10):a at 1 only → no; fooXbar(14):a at 5 only → no;
  // Actually let me think more carefully about full-match semantics:
  // a.*a means starts with 'a' and ends with 'a'
  // No terms in "term" field start with 'a', so 0 matches.
  // Let's use "alt" field: cat, dog, bird, mouse, fish
  // cat: starts 'c' → no. None start with 'a'.
  // Let's use "opt" field: has "aab"(17), "abb"(18), "aabb"(19), "aaabbb"(20)
  // a.*a: starts with 'a', ends with 'a' → none end with 'a'... "graaaay"(10)?
  // No. Let me try a different pattern.

  // .*bar.*bar.* — terms with "bar" appearing twice
  // foobarbar(4), foobarbazbar(5): yes. Others with single bar: no.
  {
    Docs docs{4, 5};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", ".*bar.*bar.*"), docs, costs, rdr);
  }

  // .*oo.* — terms containing "oo"
  // foobar(1), foo123bar(2), fooXXXbar(3), foobarbar(4), foobarbazbar(5),
  // foobasbar(6), foobaxbar(7), foobabar(8), foo(9), fooba(11), oobar(12),
  // foobarbaz(13), fooXbar(14), foooobar(16), foobarx(18), fooxbar(20)
  // NOT: bar(10), fobar(15), xfoobar(17)→has "oo", xfoobarx(19)→has "oo"
  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19, 20};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("term", ".*oo.*"), docs, costs, rdr);
  }
}

// Case sensitivity

TEST_P(RegexpFilterTestCase, by_regexp_case_sensitivity) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "class" field has mixed case values:
  // lowercase+digits: abc123(1), xyz789(2), def456(4), jkl012(8), pqr678(10),
  //   vwx234(12), bcd890(14), hij222(16), nop444(18), tuv666(20)
  // uppercase+digits: ABC123(3), GHI789(7), MNO345(9), STU901(11),
  //   YZA567(13), EFG111(15), KLM333(17), QRS555(19)
  // digit+lowercase: 123abc(5), 456def(6)

  // [a-z]+ — only lowercase letters, no digits
  // None of the "class" values are pure lowercase (all have digits)
  CheckQuery(MakeFilter("class", "[a-z]+"), Docs{}, Costs{0}, rdr);

  // [A-Z]+ — only uppercase letters
  CheckQuery(MakeFilter("class", "[A-Z]+"), Docs{}, Costs{0}, rdr);

  // Exact case: abc123 matches only doc1
  CheckQuery(MakeFilter("class", "abc123"), Docs{1}, Costs{1}, rdr);

  // ABC123 matches only doc3 (uppercase)
  CheckQuery(MakeFilter("class", "ABC123"), Docs{3}, Costs{1}, rdr);

  // abc123 does NOT match ABC123
  // (verified by checking each returns different doc)
}

// ReDoS / pathological patterns

TEST_P(RegexpFilterTestCase, by_regexp_redos_resistance) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // (a+)+b on "aaaaaaaaaaaaaaac" — should return false quickly
  // "redos" field has: "aaaaaaaaaaaaaac"(s1), "aaaaaaaaaaaaaaac"(s2),
  //   "bbbbbbbbbbbbbbb"(s3), "ab"(s4)
  {
    auto start = std::chrono::steady_clock::now();
    auto q = MakeFilter("redos", "(a+)+b");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
    auto elapsed = std::chrono::steady_clock::now() - start;
    // Must complete in under 5 seconds (generous limit)
    ASSERT_LT(elapsed, std::chrono::seconds(5));
  }

  // (a|a)*b — same pathological pattern
  {
    auto start = std::chrono::steady_clock::now();
    auto q = MakeFilter("redos", "(a|a)*b");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
    auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_LT(elapsed, std::chrono::seconds(5));
  }

  // (.*){10} — exponential blowup
  {
    auto start = std::chrono::steady_clock::now();
    auto q = MakeFilter("redos", "(.*){10}");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
    auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_LT(elapsed, std::chrono::seconds(5));
  }
}

// Metacharacters in data (using regexp_stress_data.json)

TEST_P(RegexpFilterTestCase, by_regexp_metacharacters_in_data) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "meta" field:
  // s1:"a.b", s2:"a*b", s3:"(foo)", s4:"a+b", s5:"a?b",
  // s6:"[x]", s7:"a|b", s8:"a\b", s9:"normal", s10:"axb",
  // s11:"aXb", s12:"a..b"

  // a\.b — literal dot → matches "a.b"(s1)
  CheckQuery(MakeFilter("meta", "a\\.b"), Docs{1}, Costs{1}, rdr);

  // a.b (dot = any) → matches "a.b"(s1), "a*b"(s2), "a+b"(s4),
  //   "a?b"(s5), "a|b"(s7), "a\b"(s8), "axb"(s10), "aXb"(s11)
  {
    Docs docs{1, 2, 4, 5, 7, 8, 10, 11};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("meta", "a.b"), docs, costs, rdr);
  }

  // a\*b — literal star → matches "a*b"(s2)
  CheckQuery(MakeFilter("meta", "a\\*b"), Docs{2}, Costs{1}, rdr);

  // \(foo\) — literal parens → matches "(foo)"(s3)
  CheckQuery(MakeFilter("meta", "\\(foo\\)"), Docs{3}, Costs{1}, rdr);

  // a\+b — literal plus → matches "a+b"(s4)
  CheckQuery(MakeFilter("meta", "a\\+b"), Docs{4}, Costs{1}, rdr);

  // a\?b — literal question → matches "a?b"(s5)
  CheckQuery(MakeFilter("meta", "a\\?b"), Docs{5}, Costs{1}, rdr);

  // \[x\] — literal brackets → matches "[x]"(s6)
  CheckQuery(MakeFilter("meta", "\\[x\\]"), Docs{6}, Costs{1}, rdr);

  // a\|b — literal pipe → matches "a|b"(s7)
  CheckQuery(MakeFilter("meta", "a\\|b"), Docs{7}, Costs{1}, rdr);

  // a\\b — literal backslash → matches "a\b"(s8)
  CheckQuery(MakeFilter("meta", "a\\\\b"), Docs{8}, Costs{1}, rdr);

  // a..b (two dots) → matches "a..b"(s12) and anything else 4-char a__b
  // "a.b"(s1) is 3 chars → no; "a..b"(s12) is 4 chars → yes; "axb"(s10) is
  // 3 chars → no
  CheckQuery(MakeFilter("meta", "a..b"), Docs{12}, Costs{1}, rdr);
}

// Newline and whitespace in terms

TEST_P(RegexpFilterTestCase, by_regexp_whitespace_in_terms) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "ws" field: s1:"a\nb", s2:"a\tb", s3:"a b", s4:"a\r\nb"

  // a.b on "ws" — dot matches \n, \t, space, \r?
  // "a\nb" is 3 chars (a, newline, b) → a.b should match if . matches \n
  // "a\tb" is 3 chars → a.b should match if . matches \t
  // "a b" is 3 chars → a.b matches (space is any char)
  // "a\r\nb" is 4 chars → a.b doesn't match (4 chars)
  // We test what actually happens — at minimum no crash
  {
    auto q = MakeFilter("ws", "a.b");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // Exact match: a literal space
  CheckQuery(MakeFilter("ws", "a b"), Docs{3}, Costs{1}, rdr);
}

// Character class shorthands \d, \w, \s — smoke

TEST_P(RegexpFilterTestCase, by_regexp_shorthand_classes_smoke) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // \d+ — if supported, matches digit-only terms. If not, 0 results no crash.
  {
    auto q = MakeFilter("term", "\\d+");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // \w+ — word characters
  {
    auto q = MakeFilter("term", "\\w+");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // \s — whitespace
  {
    auto q = MakeFilter("term", "\\s");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }
}

// Unsupported constructs — smoke (no crash)

TEST_P(RegexpFilterTestCase, by_regexp_unsupported_constructs_smoke) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // POSIX class
  {
    auto q = MakeFilter("term", "[[:alpha:]]+");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // Non-capturing group
  {
    auto q = MakeFilter("term", "(?:foo)+");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // Backreference
  {
    auto q = MakeFilter("term", "(a)\\1");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // Lookahead
  {
    auto q = MakeFilter("term", "foo(?=bar)");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // Lookbehind
  {
    auto q = MakeFilter("term", "(?<=foo)bar");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }

  // Word boundary
  {
    auto q = MakeFilter("term", "\\bfoo\\b");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    ASSERT_NE(nullptr, prepared);
  }
}

// Very long terms in index

TEST_P(RegexpFilterTestCase, by_regexp_long_terms) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // "long" field has 1000-char terms in s1 (aaa...) and s2 (bbb...)
  // .* should match all docs with "long" field
  {
    Docs docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("long", ".*"), docs, costs, rdr);
  }

  // Exact match of a long literal — aaa...a (1000 a's) → s1
  {
    std::string long_pattern(1000, 'a');
    CheckQuery(MakeFilter("long", long_pattern), Docs{1}, Costs{1}, rdr);
  }

  // Prefix of long term: a.* → matches s1(aaa...) and s7(aaa...z) and s10(abc)
  {
    Docs docs{1, 7, 10};
    Costs costs{docs.size()};
    CheckQuery(MakeFilter("long", "a.*"), docs, costs, rdr);
  }
}

// Boolean queries — And/Or with ByRegexp

TEST_P(RegexpFilterTestCase, by_regexp_boolean_queries) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  // Or(ByRegexp("alt","cat"), ByRegexp("alt","dog"))
  {
    irs::Or disjunction;
    {
      auto& sub = disjunction.add<irs::ByRegexp>();
      *sub.mutable_field() = "alt";
      sub.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("cat"));
    }
    {
      auto& sub = disjunction.add<irs::ByRegexp>();
      *sub.mutable_field() = "alt";
      sub.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    }

    Docs expected{1, 2, 4, 6, 8, 10, 11, 14, 15, 17, 18, 20};
    CheckQuery(disjunction, expected, rdr);
  }

  // And(ByRegexp("term","foo.*"), ByRegexp("alt","cat"))
  {
    irs::And conjunction;
    {
      auto& sub = conjunction.add<irs::ByRegexp>();
      *sub.mutable_field() = "term";
      sub.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("foo.*"));
    }
    {
      auto& sub = conjunction.add<irs::ByRegexp>();
      *sub.mutable_field() = "alt";
      sub.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("cat"));
    }

    Docs expected{1, 4, 8, 14, 20};
    CheckQuery(conjunction, expected, rdr);
  }

  // Or(ByRegexp + ByTerm) — mixed filter types
  {
    irs::Or disjunction;
    {
      auto& sub = disjunction.add<irs::ByRegexp>();
      *sub.mutable_field() = "term";
      sub.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("foobar"));
    }
    {
      auto& sub = disjunction.add<irs::ByTerm>();
      *sub.mutable_field() = "term";
      sub.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("bar"));
    }

    Docs expected{1, 10};
    CheckQuery(disjunction, expected, rdr);
  }
}

// Deleted documents
// TODO: need to fix

TEST_P(RegexpFilterTestCase, by_regexp_deleted_documents) {
  // write data
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  // delete doc with name="doc1" using a separate writer
  {
    auto writer = open_writer(irs::kOmAppend);
    irs::Filter::ptr query_doc1 = std::make_unique<irs::ByTerm>();
    auto& q = static_cast<irs::ByTerm&>(*query_doc1);
    *q.mutable_field() = "name";
    q.mutable_options()->term =
      irs::ViewCast<irs::byte_type>(std::string_view("doc1"));
    writer->GetBatch().Remove(std::move(query_doc1));
    writer->Commit();
  }

  auto rdr = open_reader();

  CheckQuery(MakeFilter("term", "foobar"), Docs{}, rdr);

  {
    Docs docs{2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20};
    CheckQuery(MakeFilter("term", "foo.*"), docs, rdr);
  }
}

// Determinism — same query twice = same results

TEST_P(RegexpFilterTestCase, by_regexp_determinism) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  auto q = MakeFilter("term", "foo.*bar");

  // Run twice, collect results manually
  Docs run1, run2;
  Costs costs1, costs2;

  // First run
  {
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    for (auto& segment : rdr) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->advance() != irs::doc_limits::eof()) {
        run1.push_back(docs->value());
      }
    }
  }

  // Second run
  {
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    for (auto& segment : rdr) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->advance() != irs::doc_limits::eof()) {
        run2.push_back(docs->value());
      }
    }
  }

  ASSERT_EQ(run1, run2);
  ASSERT_FALSE(run1.empty());
}

// Two-segment test

TEST_P(RegexpFilterTestCase, by_regexp_two_segments) {
  auto writer = open_writer();

  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(*writer, gen);
  }
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(*writer, gen);
  }

  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());

  // "same" field only in segment 1 — all 32 docs
  {
    Docs all;
    for (size_t i = 0; i < 32; ++i) {
      all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }
    CheckQuery(MakeFilter("same", ".*"), all, rdr);
  }

  CheckQuery(MakeFilter("nonexistent", ".*"), Docs{}, rdr);
}

// Consolidation / merge

TEST_P(RegexpFilterTestCase, by_regexp_consolidation) {
  auto writer = open_writer();

  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(*writer, gen);
  }
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(*writer, gen);
  }

  // before merge
  {
    auto rdr = open_reader();
    ASSERT_EQ(2, rdr.size());

    Docs all;
    for (size_t i = 0; i < 32; ++i) {
      all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }
    CheckQuery(MakeFilter("same", ".*"), all, rdr);
  }

  // consolidate
  ASSERT_TRUE(writer->Consolidate(
    irs::index_utils::MakePolicy(irs::index_utils::ConsolidateCount())));
  writer->Commit();

  // after merge — 1 segment, same data
  {
    auto rdr = open_reader();
    ASSERT_EQ(1, rdr.size());

    Docs result;
    auto q = MakeFilter("same", ".*");
    auto prepared = q.prepare({
      .index = rdr,
      .memory = irs::IResourceManager::gNoop,
    });
    for (auto& segment : rdr) {
      auto docs = prepared->execute({.segment = segment});
      while (docs->advance() != irs::doc_limits::eof()) {
        result.push_back(docs->value());
      }
    }
    ASSERT_EQ(32, result.size());
  }
}

// Concurrent readers

TEST_P(RegexpFilterTestCase, by_regexp_concurrent_readers) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr1 = open_reader();
  auto rdr2 = open_reader();

  auto q = MakeFilter("term", "foo.*");
  Docs expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20};
  Costs costs{expected.size()};

  // Both readers return same results
  CheckQuery(q, expected, costs, rdr1);
  CheckQuery(q, expected, costs, rdr2);
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(regexp_filter_test, RegexpFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         RegexpFilterTestCase::to_string);
