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

// Unit tests (no index needed)

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
    auto prepared =
      q.prepare({.index = irs::SubReader::empty(), .memory = counter});
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
  {
    irs::score_t boost = 1.5f;
    irs::ByRegexp q = MakeFilter("field", "bar.*");
    q.boost(boost);
    auto prepared =
      q.prepare({.index = irs::SubReader::empty(), .memory = counter});
    ASSERT_EQ(boost, prepared->Boost());
  }
  EXPECT_EQ(counter.current, 0);
  EXPECT_GT(counter.max, 0);
  counter.Reset();
}

TEST(by_regexp_test, type_of_prepared_query) {
  MaxMemoryCounter counter;
  {
    auto lhs =
      MakeFilter<irs::ByTerm>("foo", "bar")
        .prepare({.index = irs::SubReader::empty(), .memory = counter});
    auto rhs =
      MakeFilter("foo", "bar")
        .prepare({.index = irs::SubReader::empty(), .memory = counter});
    auto& lhs_ref = *lhs;
    auto& rhs_ref = *rhs;
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  counter.Reset();
  {
    auto lhs =
      MakeFilter<irs::ByPrefix>("foo", "bar")
        .prepare({.index = irs::SubReader::empty(), .memory = counter});
    auto rhs =
      MakeFilter("foo", "bar.*")
        .prepare({.index = irs::SubReader::empty(), .memory = counter});
    auto& lhs_ref = *lhs;
    auto& rhs_ref = *rhs;
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  counter.Reset();
  {
    auto lhs = MakeFilter<irs::ByTerm>("foo", "").prepare(
      {.index = irs::SubReader::empty(), .memory = counter});
    auto rhs = MakeFilter("foo", "").prepare(
      {.index = irs::SubReader::empty(), .memory = counter});
    auto& lhs_ref = *lhs;
    auto& rhs_ref = *rhs;
    ASSERT_EQ(typeid(lhs_ref), typeid(rhs_ref));
  }
  counter.Reset();
}

// Parametrized tests
class RegexpFilterTestCase : public tests::FilterTestCaseBase {};

// Basic patterns

TEST_P(RegexpFilterTestCase, by_regexp_foo_dot_star_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foo.*bar"),
             Docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 16, 20}, Costs{11}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_foo_star_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foo*bar"), Docs{1, 15, 16}, Costs{3}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_suffix) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", ".*bar"),
             Docs{1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 15, 16, 17, 20},
             Costs{15}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_prefix) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foo.*"),
             Docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20}, Costs{15},
             rdr);
}

// Optional ?

TEST_P(RegexpFilterTestCase, by_regexp_optional_colou_r) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("opt", "colou?r"), Docs{1, 2}, Costs{2}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_optional_gra_y) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("opt", "gra?y"), Docs{8, 12}, Costs{2}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_optional_multiple) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("opt", "a?bb?"), Docs{18}, Costs{1}, rdr);
}

// Plus +

TEST_P(RegexpFilterTestCase, by_regexp_plus_fo_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("plus", "fo+bar"), Docs{1, 2, 3, 8}, Costs{4}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_plus_f_bar) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("plus", "f+bar"), Docs{4, 11, 15}, Costs{3}, rdr);
}

// Alternation |

TEST_P(RegexpFilterTestCase, by_regexp_alternation_two) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("alt", "cat|dog"),
             Docs{1, 2, 4, 6, 8, 10, 11, 14, 15, 17, 18, 20}, Costs{12}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_alternation_three) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("alt", "cat|dog|bird"),
             Docs{1, 2, 3, 4, 6, 7, 8, 10, 11, 12, 14, 15, 16, 17, 18, 20},
             Costs{16}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_alternation_single) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("alt", "mouse"), Docs{9}, Costs{1}, rdr);
}

// Char classes [...]

TEST_P(RegexpFilterTestCase, by_regexp_char_class_lower_digits) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("class", "[a-z]+[0-9]+"),
             Docs{1, 2, 4, 8, 10, 12, 14, 16, 18, 20}, Costs{10}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_char_class_upper_digits) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("class", "[A-Z]+[0-9]+"),
             Docs{3, 7, 9, 11, 13, 15, 17, 19}, Costs{8}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_char_class_digits_letters) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("class", "[0-9]+[a-z]+"), Docs{5, 6}, Costs{2}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_negation_char_class) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("class", "[^a-z]+[0-9]+"),
             Docs{3, 7, 9, 11, 13, 15, 17, 19}, Costs{8}, rdr);
}

// Dot

TEST_P(RegexpFilterTestCase, by_regexp_dot_single) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foo.bar"), Docs{14, 20}, Costs{2}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_dot_multiple) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foo...bar"), Docs{2, 3, 4, 6, 7}, Costs{5},
             rdr);
}

// Combined

TEST_P(RegexpFilterTestCase, by_regexp_combined_group_quantifier) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "(foo|fo).*bar"),
             Docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16, 20}, Costs{12}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_combined_optional_star) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foo?.*bar"),
             Docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16, 20}, Costs{12}, rdr);
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
  CheckQuery(MakeFilter("term", "foobar"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter("term", "foo"), Docs{9}, Costs{1}, rdr);
}

// Scoring

TEST_P(RegexpFilterTestCase, by_regexp_scoring_custom_sort) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    Docs docs{1, 4, 9, 16, 21, 24, 26, 29, 31, 32};
    size_t collect_field_count = 0, collect_term_count = 0, finish_count = 0;
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());
    scorer.collector_collect_field = [&](const irs::SubReader&,
                                         const irs::TermReader&) {
      ++collect_field_count;
    };
    scorer.collector_collect_term =
      [&](const irs::SubReader&, const irs::TermReader&,
          const irs::AttributeProvider&) { ++collect_term_count; };
    scorer.collectors_collect = [&](irs::byte_type*, const irs::FieldCollector*,
                                    const irs::TermCollector*) {
      ++finish_count;
    };
    scorer.prepare_field_collector = [&]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(scorer);
    };
    scorer.prepare_term_collector = [&]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
    };
    CheckQuery(MakeFilter("prefix", ".*"), order, docs, rdr);
    ASSERT_EQ(9, collect_field_count);
    ASSERT_EQ(9, collect_term_count);
    ASSERT_EQ(9, finish_count);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_scoring_frequency_sort) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};
    CheckQuery(MakeFilter("prefix", ".*"), order,
               Docs{31, 32, 1, 4, 9, 16, 21, 24, 26, 29}, rdr);
  }
  {
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};
    CheckQuery(MakeFilter("prefix", "a.*"), order,
               Docs{31, 32, 1, 4, 16, 21, 26, 29}, rdr);
  }
}

// Scoring with Complex patterns (goes through FromRegexp)

TEST_P(RegexpFilterTestCase, by_regexp_scoring_complex_custom_sort) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    // ".*c.*" is Complex (not Prefix/Literal), so it goes through FromRegexp
    Docs docs{1, 4, 9, 21, 26, 31, 32};
    size_t collect_field_count = 0, collect_term_count = 0, finish_count = 0;
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::CustomSort>()};
    auto& scorer = static_cast<tests::sort::CustomSort&>(*order.front());
    scorer.collector_collect_field = [&](const irs::SubReader&,
                                         const irs::TermReader&) {
      ++collect_field_count;
    };
    scorer.collector_collect_term =
      [&](const irs::SubReader&, const irs::TermReader&,
          const irs::AttributeProvider&) { ++collect_term_count; };
    scorer.collectors_collect = [&](irs::byte_type*, const irs::FieldCollector*,
                                    const irs::TermCollector*) {
      ++finish_count;
    };
    scorer.prepare_field_collector = [&]() -> irs::FieldCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::FieldCollector>(scorer);
    };
    scorer.prepare_term_collector = [&]() -> irs::TermCollector::ptr {
      return std::make_unique<tests::sort::CustomSort::TermCollector>(scorer);
    };
    CheckQuery(MakeFilter("prefix", ".*c.*"), order, docs, rdr);
    // Verify collectors were called (terms were scored via automaton path)
    ASSERT_GT(collect_field_count, 0);
    ASSERT_GT(collect_term_count, 0);
    ASSERT_GT(finish_count, 0);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_scoring_complex_frequency_sort) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  // ".*c.*" is Complex -> exercises the full parser + scoring pipeline
  {
    Docs docs{31, 32, 1, 4, 9, 21, 26};
    Costs costs{docs.size()};
    std::array<irs::Scorer::ptr, 1> order{
      std::make_unique<tests::sort::FrequencySort>()};
    CheckQuery(MakeFilter("prefix", ".*c.*"), order, docs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_scoring_complex_with_boost) {
  MaxMemoryCounter counter;
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  // Complex pattern + boost
  {
    irs::score_t boost = 2.5f;
    auto q = MakeFilter("prefix", ".*c.*");
    q.boost(boost);
    auto prepared = q.prepare({.index = rdr, .memory = counter});
    ASSERT_NE(nullptr, prepared);
    ASSERT_EQ(boost, prepared->Boost());
  }
  counter.Reset();
}

TEST_P(RegexpFilterTestCase, by_regexp_scored_terms_limit) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  // scored_terms_limit = 1 -> only 1 term gets scored
  {
    irs::ByRegexp q;
    *q.mutable_field() = "prefix";
    q.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(std::string_view(".*c.*"));
    q.mutable_options()->scored_terms_limit = 1;
    auto prepared =
      q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    ASSERT_NE(nullptr, prepared);
  }
  // scored_terms_limit = 0
  {
    irs::ByRegexp q;
    *q.mutable_field() = "prefix";
    q.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(std::string_view(".*c.*"));
    q.mutable_options()->scored_terms_limit = 0;
    auto prepared =
      q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    ASSERT_NE(nullptr, prepared);
  }
  // scored_terms_limit very large
  {
    irs::ByRegexp q;
    *q.mutable_field() = "prefix";
    q.mutable_options()->pattern =
      irs::ViewCast<irs::byte_type>(std::string_view(".*c.*"));
    q.mutable_options()->scored_terms_limit = 1000000;
    auto prepared =
      q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    ASSERT_NE(nullptr, prepared);
  }
}

// Match all / match nothing

TEST_P(RegexpFilterTestCase, by_regexp_match_all) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  Docs all;
  for (size_t i = 0; i < 32; ++i) {
    all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
  }
  Costs all_costs{all.size()};
  CheckQuery(MakeFilter("same", ".*"), all, all_costs, rdr);
  CheckQuery(MakeFilter("same", "..."), all, all_costs, rdr);
  CheckQuery(MakeFilter("same", ".+"), all, all_costs, rdr);
  CheckQuery(MakeFilter("same", "x.z"), all, all_costs, rdr);
  CheckQuery(MakeFilter("same", "x.*z"), all, all_costs, rdr);
  CheckQuery(MakeFilter("same", "."), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("same", ".."), Docs{}, Costs{0}, rdr);
  CheckQuery(irs::ByRegexp(), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("", "xyz.*"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("same1", "xyz.*"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("same", "xyz_invalid.*"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("duplicated", ""), Docs{}, Costs{0}, rdr);
}

// Wildcard-equivalent

TEST_P(RegexpFilterTestCase, by_regexp_wildcard_equivalent_patterns) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("duplicated", "v.z.*"), Docs{2, 3, 8, 14, 17, 19, 24},
             Costs{7}, rdr);
  CheckQuery(MakeFilter("duplicated", "v.*c"), Docs{2, 3, 8, 14, 17, 19, 24},
             Costs{7}, rdr);
  CheckQuery(MakeFilter("duplicated", ".*c"), Docs{2, 3, 8, 14, 17, 19, 24},
             Costs{7}, rdr);
  CheckQuery(MakeFilter("duplicated", ".*.c"), Docs{2, 3, 8, 14, 17, 19, 24},
             Costs{7}, rdr);
  CheckQuery(MakeFilter("duplicated", "a.*"), Docs{1, 5, 11, 21, 27, 31},
             Costs{6}, rdr);
  CheckQuery(MakeFilter("duplicated", "vcz.*"), Docs{2, 3, 8, 14, 17, 19, 24},
             Costs{7}, rdr);
  CheckQuery(MakeFilter("prefix", ".*c.*"), Docs{1, 4, 9, 21, 26, 31, 32},
             Costs{7}, rdr);
  CheckQuery(MakeFilter("prefix", "abc.*"), Docs{1, 4, 21, 26, 31, 32},
             Costs{6}, rdr);
  CheckQuery(MakeFilter("prefix", "a.*d.*"), Docs{1, 4, 16, 26}, Costs{4}, rdr);
  CheckQuery(MakeFilter("prefix", "b.*"), Docs{9, 24}, Costs{2}, rdr);
  CheckQuery(MakeFilter("prefix", "bateradsfsfasdf"), Docs{24}, Costs{1}, rdr);
  CheckQuery(MakeFilter("name", "!.*"), Docs{28}, Costs{1}, rdr);
}

// UTF-8

TEST_P(RegexpFilterTestCase, by_regexp_utf8_execution) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("utf8", "\xD0\xBF.*"), Docs{1, 3}, Costs{2}, rdr);
  CheckQuery(MakeFilter("utf8", ".*\xD0\xB9"), Docs{1, 26}, Costs{2}, rdr);
  CheckQuery(MakeFilter("utf8", "\xD0\xB2.*\xD0\xB9"), Docs{26}, Costs{1}, rdr);
  CheckQuery(MakeFilter("utf8", ".*\xD0\xBB\xD0\xBE\xD1\x82"), Docs{2, 3},
             Costs{2}, rdr);
  CheckQuery(MakeFilter("utf8", "\xD0\xBF.."), Docs{1}, Costs{1}, rdr);
  CheckQuery(
    MakeFilter("utf8", "\xD0\xBF\xD1\x83\xD0\xB9|\xD0\xB2\xD0\xB8\xD0\xB9"),
    Docs{1, 26}, Costs{2}, rdr);
  CheckQuery(MakeFilter("utf8", ".*"), Docs{1, 2, 3, 14, 17, 24, 26}, Costs{7},
             rdr);
  CheckQuery(MakeFilter("utf8", "\xD1\x8F.*"), Docs{}, Costs{0}, rdr);
}

// Cross-validation vs ByTerm/ByPrefix

TEST_P(RegexpFilterTestCase, by_regexp_cross_validation) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "foobar"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter<irs::ByTerm>("term", "foobar"), Docs{1}, Costs{1}, rdr);
  Docs prefix_expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20};
  CheckQuery(MakeFilter("term", "foo.*"), prefix_expected, Costs{15}, rdr);
  CheckQuery(MakeFilter<irs::ByPrefix>("term", "foo"), prefix_expected,
             Costs{15}, rdr);
  Docs all_expected;
  for (irs::doc_id_t i = 1; i <= 20; ++i) {
    all_expected.push_back(i);
  }
  CheckQuery(MakeFilter("term", ".*"), all_expected, Costs{20}, rdr);
  CheckQuery(MakeFilter<irs::ByPrefix>("term", ""), all_expected, Costs{20},
             rdr);
}

// Invalid patterns

TEST_P(RegexpFilterTestCase, by_regexp_invalid_pattern_execution) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "(abc"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("term", "[abc"), Docs{}, Costs{0}, rdr);
}

// Filter reuse

TEST_P(RegexpFilterTestCase, by_regexp_filter_reuse) {
  MaxMemoryCounter counter;
  auto q = MakeFilter("same", ".*");
  {
    auto prepared =
      q.prepare({.index = irs::SubReader::empty(), .memory = counter});
    ASSERT_NE(nullptr, prepared);
    ASSERT_EQ(irs::kNoBoost, prepared->Boost());
  }
  counter.Reset();
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
    CheckQuery(q, all, Costs{32}, rdr);
  }
}

// Anchoring

TEST_P(RegexpFilterTestCase, by_regexp_anchoring) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "^foobar$"), Docs{1}, Costs{1}, rdr);
  {
    auto q = MakeFilter("term", "^foo");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "foo$");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "^$");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
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
  CheckQuery(MakeFilter("class", "[a-z]+"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("class", "[A-Z]+"), Docs{}, Costs{0}, rdr);
  CheckQuery(MakeFilter("class", "abc123"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter("class", "ABC123"), Docs{3}, Costs{1}, rdr);
}

// Greedy quantifiers

TEST_P(RegexpFilterTestCase, by_regexp_greedy_quantifiers) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", ".*bar.*bar.*"), Docs{4, 5}, Costs{2}, rdr);
  CheckQuery(
    MakeFilter("term", ".*oo.*"),
    Docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 16, 17, 18, 19, 20},
    Costs{18}, rdr);
}

// ReDoS resistance

TEST_P(RegexpFilterTestCase, by_regexp_redos_resistance) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto start = std::chrono::steady_clock::now();
    ASSERT_NE(nullptr, MakeFilter("redos", "(a+)+b")
                         .prepare({.index = rdr,
                                   .memory = irs::IResourceManager::gNoop}));
    ASSERT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(5));
  }
  {
    auto start = std::chrono::steady_clock::now();
    ASSERT_NE(nullptr, MakeFilter("redos", "(a|a)*b")
                         .prepare({.index = rdr,
                                   .memory = irs::IResourceManager::gNoop}));
    ASSERT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(5));
  }
  {
    auto start = std::chrono::steady_clock::now();
    ASSERT_NE(nullptr, MakeFilter("redos", "(.*){10}")
                         .prepare({.index = rdr,
                                   .memory = irs::IResourceManager::gNoop}));
    ASSERT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(5));
  }
}

// Metacharacters in data

TEST_P(RegexpFilterTestCase, by_regexp_metacharacters_in_data) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("meta", "a\\.b"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "a.b"), Docs{1, 2, 4, 5, 7, 8, 10, 11},
             Costs{8}, rdr);
  CheckQuery(MakeFilter("meta", "a\\*b"), Docs{2}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "\\(foo\\)"), Docs{3}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "a\\+b"), Docs{4}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "a\\?b"), Docs{5}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "\\[x\\]"), Docs{6}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "a\\|b"), Docs{7}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "a\\\\b"), Docs{8}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "a..b"), Docs{12}, Costs{1}, rdr);
}

// Whitespace

TEST_P(RegexpFilterTestCase, by_regexp_whitespace_in_terms) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("ws", "a.b");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  CheckQuery(MakeFilter("ws", "a b"), Docs{3}, Costs{1}, rdr);
}

// Long terms

TEST_P(RegexpFilterTestCase, by_regexp_long_terms) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("long", ".*"), Docs{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
             Costs{10}, rdr);
  {
    std::string p(1000, 'a');
    CheckQuery(MakeFilter("long", p), Docs{1}, Costs{1}, rdr);
  }
  CheckQuery(MakeFilter("long", "a.*"), Docs{1, 7, 10}, Costs{3}, rdr);
}

// Boolean queries

TEST_P(RegexpFilterTestCase, by_regexp_boolean_queries) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    irs::Or d;
    {
      auto& s = d.add<irs::ByRegexp>();
      *s.mutable_field() = "alt";
      s.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("cat"));
    }
    {
      auto& s = d.add<irs::ByRegexp>();
      *s.mutable_field() = "alt";
      s.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("dog"));
    }
    CheckQuery(d, Docs{1, 2, 4, 6, 8, 10, 11, 14, 15, 17, 18, 20}, rdr);
  }
  {
    irs::And c;
    {
      auto& s = c.add<irs::ByRegexp>();
      *s.mutable_field() = "term";
      s.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("foo.*"));
    }
    {
      auto& s = c.add<irs::ByRegexp>();
      *s.mutable_field() = "alt";
      s.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("cat"));
    }
    CheckQuery(c, Docs{1, 4, 8, 14, 20}, rdr);
  }
  {
    irs::Or d;
    {
      auto& s = d.add<irs::ByRegexp>();
      *s.mutable_field() = "term";
      s.mutable_options()->pattern =
        irs::ViewCast<irs::byte_type>(std::string_view("foobar"));
    }
    {
      auto& s = d.add<irs::ByTerm>();
      *s.mutable_field() = "term";
      s.mutable_options()->term =
        irs::ViewCast<irs::byte_type>(std::string_view("bar"));
    }
    CheckQuery(d, Docs{1, 10}, rdr);
  }
}

// Determinism

TEST_P(RegexpFilterTestCase, by_regexp_determinism) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  auto q = MakeFilter("term", "foo.*bar");
  Docs run1, run2;
  {
    auto p = q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    for (auto& s : rdr) {
      auto d = p->execute({.segment = s});
      while (d->advance() != irs::doc_limits::eof()) {
        run1.push_back(d->value());
      }
    }
  }
  {
    auto p = q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    for (auto& s : rdr) {
      auto d = p->execute({.segment = s});
      while (d->advance() != irs::doc_limits::eof()) {
        run2.push_back(d->value());
      }
    }
  }
  ASSERT_EQ(run1, run2);
  ASSERT_FALSE(run1.empty());
}

// Two segments

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
  {
    Docs all;
    for (size_t i = 0; i < 32; ++i) {
      all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }
    CheckQuery(MakeFilter("same", ".*"), all, rdr);
  }
  CheckQuery(MakeFilter("nonexistent", ".*"), Docs{}, rdr);
}

// Compaction

TEST_P(RegexpFilterTestCase, by_regexp_compaction) {
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
  {
    auto rdr = open_reader();
    ASSERT_EQ(2, rdr.size());
    Docs all;
    for (size_t i = 0; i < 32; ++i) {
      all.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }
    CheckQuery(MakeFilter("same", ".*"), all, rdr);
  }
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
  {
    auto rdr = open_reader();
    ASSERT_EQ(1, rdr.size());
    Docs result;
    auto q = MakeFilter("same", ".*");
    auto p = q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    for (auto& s : rdr) {
      auto d = p->execute({.segment = s});
      while (d->advance() != irs::doc_limits::eof()) {
        result.push_back(d->value());
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
  Docs expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 13, 14, 16, 18, 20};
  CheckQuery(MakeFilter("term", "foo.*"), expected, Costs{15}, rdr1);
  CheckQuery(MakeFilter("term", "foo.*"), expected, Costs{15}, rdr2);
}

// Visitor API

TEST_P(RegexpFilterTestCase, by_regexp_visit_literal) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");
  ASSERT_NE(nullptr, reader);
  {
    auto term = irs::ViewCast<irs::byte_type>(std::string_view("abc"));
    tests::EmptyFilterVisitor v;
    auto fv = irs::ByRegexp::visitor(term);
    ASSERT_TRUE(fv);
    fv(segment, *reader, v);
    ASSERT_EQ(1, v.prepare_calls_counter());
    ASSERT_EQ(1, v.visit_calls_counter());
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_visit_prefix) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");
  {
    auto p = irs::ViewCast<irs::byte_type>(std::string_view("ab.*"));
    tests::EmptyFilterVisitor v;
    auto fv = irs::ByRegexp::visitor(p);
    fv(segment, *reader, v);
    ASSERT_EQ(1, v.prepare_calls_counter());
    ASSERT_EQ(6, v.visit_calls_counter());
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_visit_wildcard_like) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");
  {
    auto p = irs::ViewCast<irs::byte_type>(std::string_view("a.c.*"));
    tests::EmptyFilterVisitor v;
    auto fv = irs::ByRegexp::visitor(p);
    ASSERT_TRUE(fv);
    fv(segment, *reader, v);
    ASSERT_EQ(1, v.prepare_calls_counter());
    ASSERT_EQ(5, v.visit_calls_counter());
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_visit_invalid_pattern) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");
  {
    auto p = irs::ViewCast<irs::byte_type>(std::string_view("(abc"));
    tests::EmptyFilterVisitor v;
    auto fv = irs::ByRegexp::visitor(p);
    ASSERT_TRUE(fv);
    fv(segment, *reader, v);
    ASSERT_EQ(0, v.prepare_calls_counter());
  }
  {
    auto p = irs::ViewCast<irs::byte_type>(std::string_view("[abc"));
    tests::EmptyFilterVisitor v;
    auto fv = irs::ByRegexp::visitor(p);
    fv(segment, *reader, v);
    ASSERT_EQ(0, v.prepare_calls_counter());
  }
}

// Perl-extension: counted quantifiers {n}, {n,}, {n,m}

TEST_P(RegexpFilterTestCase, by_regexp_counted_quantifiers) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "fo{2}bar"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter("term", "fo{1,3}bar"), Docs{1, 15}, Costs{2}, rdr);
  CheckQuery(MakeFilter("term", "fo{2,}bar"), Docs{1, 16}, Costs{2}, rdr);
  {
    auto q = MakeFilter("term", ".{6}");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Perl-extension: non-capturing groups (?:...)

TEST_P(RegexpFilterTestCase, by_regexp_non_capturing_group) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("term", "(?:foo)+bar");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "(?:fo|ba)+r");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Perl-extension: Perl classes \d \w \s \D \W \S

TEST_P(RegexpFilterTestCase, by_regexp_perl_classes) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("class", "\\d+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    Docs docs;
    for (irs::doc_id_t i = 1; i <= 20; ++i) {
      docs.push_back(i);
    }
    CheckQuery(MakeFilter("class", "\\w+"), docs, Costs{20}, rdr);
  }
  {
    auto q = MakeFilter("class", "\\D+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("class", "\\W+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("class", "\\s");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    Docs docs;
    for (irs::doc_id_t i = 1; i <= 20; ++i) {
      docs.push_back(i);
    }
    CheckQuery(MakeFilter("class", "\\S+"), docs, Costs{20}, rdr);
  }
}

// Perl-extension: word boundary \b \B

TEST_P(RegexpFilterTestCase, by_regexp_word_boundary) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "\\bfoo\\b"), Docs{9}, Costs{1}, rdr);
  {
    auto q = MakeFilter("term", "\\Bfoo");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Perl-extension: case-insensitive (?i:...)

TEST_P(RegexpFilterTestCase, by_regexp_case_insensitive_flag) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("class", "(?i:abc)123");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("class", "(?i:abc).*");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Perl-extension: Unicode property \p{...} \P{...}

TEST_P(RegexpFilterTestCase, by_regexp_unicode_property_classes) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("utf8", "\\p{Cyrillic}+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("utf8", "\\P{Cyrillic}+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Perl-extension: literal quoting \Q...\E

TEST_P(RegexpFilterTestCase, by_regexp_literal_quoting) {
  {
    tests::JsonDocGenerator gen(resource("regexp_stress_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("meta", "\\Q(foo)\\E"), Docs{3}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "\\Qa.b\\E"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter("meta", "\\Qa*b\\E"), Docs{2}, Costs{1}, rdr);
}

// Perl-extension: named captures (?P<n>...)

TEST_P(RegexpFilterTestCase, by_regexp_named_captures) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "(?P<prefix>foo)bar"), Docs{1}, Costs{1}, rdr);
  CheckQuery(MakeFilter("term", "(?P<a>foo|fo).*bar"),
             Docs{1, 2, 3, 4, 5, 6, 7, 8, 14, 15, 16, 20}, Costs{12}, rdr);
}

// Perl-extension: empty alternation branch

TEST_P(RegexpFilterTestCase, by_regexp_empty_alternation_branch) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  CheckQuery(MakeFilter("term", "(|foo)bar"), Docs{1, 10}, Costs{2}, rdr);
  {
    auto q = MakeFilter("term", "a|");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Walker Copy() - shared subtrees (DAG after Simplify)

TEST_P(RegexpFilterTestCase, by_regexp_shared_subtrees) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("term", "(o{2,5}){1,2}");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "(.{1,3}){1,3}");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Large NFA stress

TEST_P(RegexpFilterTestCase, by_regexp_large_nfa) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("term", ".{20}");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "[a-z]{5,10}");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "foo|bar|baz|qux|quux|corge|grault|garply");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// UTF-8 char classes (multi-byte ranges in BuildCharClass)

TEST_P(RegexpFilterTestCase, by_regexp_utf8_char_class) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("utf8", "[\xD0\xB0-\xD1\x8F]+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("utf8",
                        "[\xD0\xB0-\xD1\x8F"
                        "a-z]+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("utf8", "[^\xD0\xB0-\xD1\x8F]+");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// kRegexpAnyByte - \C

TEST_P(RegexpFilterTestCase, by_regexp_any_byte) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    auto q = MakeFilter("term", "\\C{6}");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
  {
    auto q = MakeFilter("term", "foo\\Cbar");
    ASSERT_NE(nullptr, q.prepare({.index = rdr,
                                  .memory = irs::IResourceManager::gNoop}));
  }
}

// Very long pattern

TEST_P(RegexpFilterTestCase, by_regexp_very_long_pattern) {
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  {
    std::string p(2000, 'x');
    ASSERT_NE(nullptr,
              MakeFilter("term", p).prepare(
                {.index = rdr, .memory = irs::IResourceManager::gNoop}));
  }
  {
    std::string p;
    for (int i = 1; i <= 50; ++i) {
      if (i > 1) {
        p += '|';
      }
      p += std::string(i, 'x');
    }
    ASSERT_NE(nullptr,
              MakeFilter("term", p).prepare(
                {.index = rdr, .memory = irs::IResourceManager::gNoop}));
  }
}

// Syntax dialect - POSIX ERE vs Perl

TEST(by_regexp_test, options_syntax_default) {
  irs::ByRegexpOptions opts;
  ASSERT_EQ(irs::RegexpSyntax::Perl, opts.syntax);
}

TEST(by_regexp_test, equal_syntax_differs) {
  irs::ByRegexp q = MakeFilter("field", "bar.*");
  irs::ByRegexp q1 = MakeFilter("field", "bar.*");
  // Identical in every field except syntax -> must compare unequal.
  q1.mutable_options()->syntax = irs::RegexpSyntax::PosixEre;
  ASSERT_NE(q, q1);
  // Changing back restores equality.
  q1.mutable_options()->syntax = irs::RegexpSyntax::Perl;
  ASSERT_EQ(q, q1);
}

TEST_P(RegexpFilterTestCase,
       by_regexp_syntax_perl_accepts_perl_class_baseline) {
  // Baseline counterpart to by_regexp_syntax_posix_rejects_perl_class:
  // with the default Perl syntax \w+ matches every indexed term.  The
  // existing by_regexp_perl_classes already covers this, but keeping a
  // paired test next to the POSIX reject case makes the contrast
  // explicit for reviewers.
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  Docs expected;
  for (irs::doc_id_t i = 1; i <= 20; ++i) {
    expected.push_back(i);
  }
  CheckQuery(MakeFilter("class", "\\w+"), expected, Costs{20}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_syntax_posix_rejects_perl_class) {
  // Same pattern, POSIX mode: \w is not recognized by the POSIX parser,
  // so the resulting query matches nothing.
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  auto q = MakeFilter("class", "\\w+");
  q.mutable_options()->syntax = irs::RegexpSyntax::PosixEre;
  CheckQuery(q, Docs{}, Costs{0}, rdr);
}

TEST_P(RegexpFilterTestCase, by_regexp_syntax_posix_accepts_posix_class) {
  // POSIX mode must handle the POSIX bracket class [[:alpha:]].  Two
  // cross-checks:
  //   (a) [[:alpha:]]+[[:digit:]]+ in POSIX matches the same set as its
  //       Perl-syntax equivalent [a-zA-Z]+[0-9]+ - proves POSIX accepts
  //       the POSIX class at all.
  //   (b) The same [[:alpha:]]+[[:digit:]]+ pattern in POSIX and in
  //       Perl matches the same set - guards against a future
  //       regression where POSIX parses but builds a different
  //       automaton than Perl for a pattern both accept.
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  auto collect = [&](const irs::ByRegexp& q) {
    Docs out;
    auto prepared =
      q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    for (auto& s : rdr) {
      auto d = prepared->execute({.segment = s});
      while (d->advance() != irs::doc_limits::eof()) {
        out.push_back(d->value());
      }
    }
    return out;
  };

  auto q_posix = MakeFilter("class", "[[:alpha:]]+[[:digit:]]+");
  q_posix.mutable_options()->syntax = irs::RegexpSyntax::PosixEre;
  auto q_perl_equiv = MakeFilter("class", "[a-zA-Z]+[0-9]+");
  auto q_perl_same = MakeFilter("class", "[[:alpha:]]+[[:digit:]]+");

  auto posix_docs = collect(q_posix);
  auto perl_equiv_docs = collect(q_perl_equiv);
  auto perl_same_docs = collect(q_perl_same);
  ASSERT_EQ(perl_equiv_docs, posix_docs);  // (a)
  ASSERT_EQ(perl_same_docs, posix_docs);   // (b)
  ASSERT_FALSE(posix_docs.empty());
}

TEST_P(RegexpFilterTestCase, by_regexp_syntax_fast_paths_are_agnostic) {
  // Literal and Prefix patterns take the ByTerm / ByPrefix fast paths
  // in ComputeRegexpType, which operate on raw bytes and do not consult
  // the RegexpSyntax at all.  The two modes must therefore produce
  // identical results for such patterns.  This test guards against a
  // future refactor that accidentally threads syntax into the fast
  // path and breaks the invariant.
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();

  auto run = [&](std::string_view field, std::string_view pattern,
                 irs::RegexpSyntax syntax) {
    auto q = MakeFilter(field, pattern);
    q.mutable_options()->syntax = syntax;
    Docs out;
    auto prepared =
      q.prepare({.index = rdr, .memory = irs::IResourceManager::gNoop});
    for (auto& s : rdr) {
      auto d = prepared->execute({.segment = s});
      while (d->advance() != irs::doc_limits::eof()) {
        out.push_back(d->value());
      }
    }
    return out;
  };

  // Literal pattern -> ByTerm fast path
  {
    auto perl = run("term", "foobar", irs::RegexpSyntax::Perl);
    auto posix = run("term", "foobar", irs::RegexpSyntax::PosixEre);
    ASSERT_EQ(perl, posix);
    ASSERT_FALSE(perl.empty());
  }
  // Prefix pattern -> ByPrefix fast path
  {
    auto perl = run("term", "foo.*", irs::RegexpSyntax::Perl);
    auto posix = run("term", "foo.*", irs::RegexpSyntax::PosixEre);
    ASSERT_EQ(perl, posix);
    ASSERT_FALSE(perl.empty());
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_prepare_with_syntax) {
  // Exercise the static ByRegexp::prepare overload with an explicit
  // syntax argument, covering the path used by phrase_filter and any
  // other direct callers.
  {
    tests::JsonDocGenerator gen(resource("regexp_test_data.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto rdr = open_reader();
  auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("(?:foo)bar"));
  // Perl: non-capturing group parses, prepared query is non-null.
  {
    auto prepared = irs::ByRegexp::Prepare(
      {.index = rdr, .memory = irs::IResourceManager::gNoop}, "term", pattern,
      /*scored_terms_limit=*/1024, irs::RegexpSyntax::Perl);
    ASSERT_NE(nullptr, prepared);
  }
  // POSIX: (?:...) is a Perl extension - prepare still returns a valid
  // non-null pointer (empty query), but matches nothing.  The empty-
  // match case is anchored end-to-end by
  // by_regexp_syntax_posix_rejects_perl_class above.
  {
    auto prepared = irs::ByRegexp::Prepare(
      {.index = rdr, .memory = irs::IResourceManager::gNoop}, "term", pattern,
      /*scored_terms_limit=*/1024, irs::RegexpSyntax::PosixEre);
    ASSERT_NE(nullptr, prepared);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_visit_with_syntax) {
  // Exercise the static ByRegexp::visitor overload with an explicit
  // syntax argument.  A Perl-only pattern in POSIX mode must produce
  // a visitor that never calls Prepare on the underlying
  // FilterVisitor (the parse failed).
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }
  auto index = open_reader();
  auto& segment = index[0];
  const auto* reader = segment.field("prefix");
  ASSERT_NE(nullptr, reader);

  auto p = irs::ViewCast<irs::byte_type>(std::string_view("\\d+"));
  // Perl: \d parses; the visitor is non-null and callable.  We do not
  // assert a specific visit count because the "prefix" field may or
  // may not contain digit-only terms in the fixture.
  {
    auto fv = irs::ByRegexp::visitor(p, irs::RegexpSyntax::Perl);
    ASSERT_TRUE(fv);
    tests::EmptyFilterVisitor v;
    fv(segment, *reader, v);
  }
  // POSIX: \d is a parse error -> visitor is a no-op.
  {
    auto fv = irs::ByRegexp::visitor(p, irs::RegexpSyntax::PosixEre);
    ASSERT_TRUE(fv);
    tests::EmptyFilterVisitor v;
    fv(segment, *reader, v);
    ASSERT_EQ(0, v.prepare_calls_counter());
  }
}

TEST_P(RegexpFilterTestCase, parallel_prepare_parity) {
  {
    auto writer = open_writer(irs::kOmCreate);
    tests::JsonDocGenerator gen_a(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
    tests::JsonDocGenerator gen_b(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
    add_segment(*writer, gen_a);
    add_segment(*writer, gen_b);
  }

  auto rdr = open_reader();
  ASSERT_GE(rdr.size(), 2u);

  auto with_boost = [](irs::ByRegexp q, irs::score_t b) {
    q.boost(b);
    return q;
  };

  auto run = [&](const irs::ByRegexp& f) {
    tests::RunParallelPrepareParity(f, rdr);
  };

  run(MakeFilter("prefix", "abc"));
  run(with_boost(MakeFilter("prefix", "abc"), 2.5f));
  run(MakeFilter("prefix", "ab.*"));
  run(with_boost(MakeFilter("prefix", "ab.*"), 0.5f));
  run(MakeFilter("prefix", "a.c"));
  run(with_boost(MakeFilter("prefix", "a.c"), 1.75f));
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(regexp_filter_test, RegexpFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values(tests::FormatInfo{
                                              "1_5simd"})),
                         RegexpFilterTestCase::to_string);
