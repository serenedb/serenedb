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

class RegexpFilterTestCase : public tests::FilterTestCaseBase {};

TEST_P(RegexpFilterTestCase, by_regexp) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  CheckQuery(irs::ByRegexp(), Docs{}, Costs{0}, rdr);

  CheckQuery(MakeFilter("", "xyz.*"), Docs{}, Costs{0}, rdr);

  CheckQuery(MakeFilter("nonexistent_field", "xyz.*"), Docs{}, Costs{0}, rdr);

  CheckQuery(MakeFilter("same", "xyz_invalid_pattern.*"), Docs{}, Costs{0},
             rdr);

  CheckQuery(MakeFilter("duplicated", ""), Docs{}, Costs{0}, rdr);

  {
    Docs result;
    for (size_t i = 0; i < 32; ++i) {
      result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }

    Costs costs{result.size()};

    CheckQuery(MakeFilter("same", ".*"), result, costs, rdr);
    CheckQuery(MakeFilter("same", "x.*"), result, costs, rdr);
    CheckQuery(MakeFilter("same", "xy.*"), result, costs, rdr);
    CheckQuery(MakeFilter("same", "xyz.*"), result, costs, rdr);
    CheckQuery(MakeFilter("same", "xyz"), result, costs, rdr);
  }

  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};

    CheckQuery(MakeFilter("prefix", "abc.*"), docs, costs, rdr);
  }

  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};

    CheckQuery(MakeFilter("prefix", "a.c.*"), docs, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_complex_patterns) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};

    CheckQuery(MakeFilter("prefix", "(abc|abd).*"), docs, costs, rdr);
  }

  {
    Docs docs{1, 4, 21, 26, 31, 32};
    Costs costs{docs.size()};

    CheckQuery(MakeFilter("prefix", "ab[cd].*"), docs, costs, rdr);
  }

  {
    Docs result;
    for (size_t i = 0; i < 32; ++i) {
      result.push_back(irs::doc_id_t((irs::doc_limits::min)() + i));
    }
    Costs costs{result.size()};

    CheckQuery(MakeFilter("same", "xy+z"), result, costs, rdr);
  }
}

TEST_P(RegexpFilterTestCase, by_regexp_utf8) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential_utf8.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();
}

TEST_P(RegexpFilterTestCase, visit) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  std::string fld = "prefix";
  std::string_view field = std::string_view(fld);

  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];

  const auto* reader = segment.field(field);
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

    visitor.reset();
  }

  {
    auto prefix = irs::ViewCast<irs::byte_type>(std::string_view("ab.*"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(prefix);
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
    auto wildcard = irs::ViewCast<irs::byte_type>(std::string_view("a.c.*"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(wildcard);
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

  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("abc|abde"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(1, visitor.prepare_calls_counter());
    ASSERT_EQ(2, visitor.visit_calls_counter());
    ASSERT_EQ((std::vector<std::pair<std::string_view, irs::score_t>>{
                {"abc", irs::kNoBoost},
                {"abde", irs::kNoBoost},
              }),
              visitor.term_refs<char>());

    visitor.reset();
  }
}

TEST_P(RegexpFilterTestCase, visit_invalid_pattern) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  std::string fld = "prefix";
  std::string_view field = std::string_view(fld);

  auto index = open_reader();
  ASSERT_EQ(1, index.size());
  auto& segment = index[0];

  const auto* reader = segment.field(field);
  ASSERT_NE(nullptr, reader);

  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("(abc"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(0, visitor.prepare_calls_counter());
    ASSERT_EQ(0, visitor.visit_calls_counter());
  }

  {
    auto pattern = irs::ViewCast<irs::byte_type>(std::string_view("[abc"));
    tests::EmptyFilterVisitor visitor;
    auto field_visitor = irs::ByRegexp::visitor(pattern);
    ASSERT_TRUE(field_visitor);
    field_visitor(segment, *reader, visitor);
    ASSERT_EQ(0, visitor.prepare_calls_counter());
    ASSERT_EQ(0, visitor.visit_calls_counter());
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  regexp_filter_test, RegexpFilterTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5avx"},
                                       tests::FormatInfo{"1_5simd"})),
  RegexpFilterTestCase::to_string);
