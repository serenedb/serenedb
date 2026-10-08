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

#include <gtest/gtest.h>

#include <iresearch/search/detail/term_acceptor.hpp>
#include <iresearch/search/detail/term_iterator.hpp>
#include <iresearch/search/detail/term_predicate.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/automaton_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/levenshtein_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/range_filter.hpp>
#include <iresearch/search/filters/regexp_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/filters/wildcard_filter.hpp>
#include <iresearch/utils/regexp_utils.hpp>
#include <iresearch/utils/wildcard_utils.hpp>
#include <string>
#include <string_view>

namespace {

irs::bytes_view B(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

bool Accepts(const irs::TermPredicate& pred, std::string_view term) {
  return pred.Accepts(B(term));
}

irs::TermClause Term(std::string_view term) {
  return irs::TermClause{.term = irs::bstring{B(term)}};
}

irs::Filter::ptr Prefix(std::string_view term) {
  auto f = std::make_unique<irs::ByPrefix>();
  f->mutable_options()->term = irs::bstring{B(term)};
  return f;
}

irs::Filter::ptr NotCompilable() {
  return std::make_unique<irs::AutomatonFilter>();
}

TEST(term_predicate_test, by_term) {
  irs::ByTerm f;
  f.mutable_options()->term = irs::bstring{B("abc")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "ab"));
  EXPECT_FALSE(Accepts(*pred, "abcd"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

TEST(term_predicate_test, should_terms) {
  irs::BooleanFilter f;
  f.Add(Term("abc"), irs::Occur::Should);
  f.Add(Term("xyz"), irs::Occur::Should);
  f.SetMinShouldMatch(1);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "xyz"));
  EXPECT_FALSE(Accepts(*pred, "abd"));
  EXPECT_FALSE(Accepts(*pred, "ab"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

TEST(term_predicate_test, should_terms_min_match_accepts_nothing) {
  irs::BooleanFilter f;
  f.Add(Term("abc"), irs::Occur::Should);
  f.Add(Term("xyz"), irs::Occur::Should);
  f.SetMinShouldMatch(2);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));
  EXPECT_FALSE(Accepts(*pred, "abd"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

TEST(term_predicate_test, by_prefix) {
  irs::ByPrefix f;
  f.mutable_options()->term = irs::bstring{B("ab")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "a"));
  EXPECT_FALSE(Accepts(*pred, "ba"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

TEST(term_predicate_test, by_range) {
  {
    irs::ByRange f;
    auto& rng = f.mutable_options()->range;
    rng.min = irs::bstring{B("b")};
    rng.min_type = irs::BoundType::Inclusive;
    rng.max = irs::bstring{B("d")};
    rng.max_type = irs::BoundType::Exclusive;

    const auto pred = f.CompileTermPredicate();
    ASSERT_NE(nullptr, pred);
    EXPECT_FALSE(Accepts(*pred, "a"));
    EXPECT_TRUE(Accepts(*pred, "b"));
    EXPECT_TRUE(Accepts(*pred, "c"));
    EXPECT_TRUE(Accepts(*pred, "cz"));
    EXPECT_FALSE(Accepts(*pred, "d"));
    EXPECT_FALSE(Accepts(*pred, "e"));
  }
  {
    irs::ByRange f;
    auto& rng = f.mutable_options()->range;
    rng.min = irs::bstring{B("b")};
    rng.min_type = irs::BoundType::Exclusive;

    const auto pred = f.CompileTermPredicate();
    ASSERT_NE(nullptr, pred);
    EXPECT_FALSE(Accepts(*pred, "b"));
    EXPECT_TRUE(Accepts(*pred, "ba"));
    EXPECT_TRUE(Accepts(*pred, "zzz"));
  }
  {
    irs::ByRange f;
    auto& rng = f.mutable_options()->range;
    rng.max = irs::bstring{B("b")};
    rng.max_type = irs::BoundType::Inclusive;

    const auto pred = f.CompileTermPredicate();
    ASSERT_NE(nullptr, pred);
    EXPECT_TRUE(Accepts(*pred, ""));
    EXPECT_TRUE(Accepts(*pred, "b"));
    EXPECT_FALSE(Accepts(*pred, "ba"));
  }
}

TEST(term_predicate_test, automaton) {
  irs::AutomatonFilter f;
  *f.mutable_options() =
    irs::AutomatonOptions{B("a%b"), irs::PatternKind::Wildcard};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "axxb"));
  EXPECT_FALSE(Accepts(*pred, "ba"));
  EXPECT_FALSE(Accepts(*pred, "a"));
}

TEST(term_predicate_test, automaton_without_compiled_not_compilable) {
  irs::AutomatonFilter f;
  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, automaton_fused_kind) {
  auto source = irs::MakePatternSource(B("a%b"), irs::PatternKind::Wildcard);
  ASSERT_NE(nullptr, source);

  const irs::AutomatonOptions fused{B("a%b AND %b"), source};
  EXPECT_EQ(irs::PatternKind::Fused, fused.kind);
  EXPECT_EQ(source, fused.source);

  const irs::AutomatonOptions wildcard{B("a%b AND %b"),
                                       irs::PatternKind::Wildcard};
  EXPECT_EQ(irs::PatternKind::Wildcard, wildcard.kind);
  EXPECT_NE(fused, wildcard);
  EXPECT_EQ(fused, (irs::AutomatonOptions{B("a%b AND %b"), source}));

  irs::AutomatonFilter f;
  *f.mutable_options() = fused;
  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "axxb"));
  EXPECT_FALSE(Accepts(*pred, "ba"));
  EXPECT_FALSE(Accepts(*pred, "a"));
}

TEST(term_predicate_test, not_negates) {
  irs::BooleanFilter f;
  f.Add(Term("abc"), irs::Occur::MustNot);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "abd"));
}

TEST(term_predicate_test, empty_boolean_not_compilable) {
  irs::BooleanFilter f;
  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, and_conjunction) {
  irs::BooleanFilter f;
  f.Add(Prefix("ab"), irs::Occur::Must);
  f.Add(Term("abc"), irs::Occur::MustNot);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abd"));
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));
}

TEST(term_predicate_test, or_disjunction) {
  irs::BooleanFilter f;
  f.Add(Term("xyz"), irs::Occur::Should);
  f.Add(Prefix("ab"), irs::Occur::Should);
  f.SetMinShouldMatch(1);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "xyz"));
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xy"));
}

TEST(term_predicate_test, or_min_match_counts) {
  irs::BooleanFilter f;
  f.Add(Term("a"), irs::Occur::Should);
  f.Add(Term("b"), irs::Occur::Should);
  f.Add(Prefix("a"), irs::Occur::Should);
  f.SetMinShouldMatch(2);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "a"));
  EXPECT_FALSE(Accepts(*pred, "b"));
  EXPECT_FALSE(Accepts(*pred, "ab"));
}

TEST(term_predicate_test, should_without_min_match_not_compilable) {
  irs::BooleanFilter f;
  f.Add(Prefix("a"), irs::Occur::Must);
  f.Add(Term("a"), irs::Occur::Should);
  f.Add(Term("b"), irs::Occur::Should);

  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, non_acceptor_leaf_poisons_tree) {
  {
    irs::BooleanFilter f;
    f.Add(Prefix("ab"), irs::Occur::Must);
    f.Add(NotCompilable(), irs::Occur::Must);
    ASSERT_EQ(nullptr, f.CompileTermPredicate());
  }
  {
    irs::BooleanFilter f;
    f.Add(Prefix("ab"), irs::Occur::Should);
    f.Add(NotCompilable(), irs::Occur::Should);
    f.SetMinShouldMatch(1);
    ASSERT_EQ(nullptr, f.CompileTermPredicate());
  }
}

TEST(term_predicate_test, all_is_neutral_in_conjunction) {
  irs::BooleanFilter f;
  f.Add(Prefix("ab"), irs::Occur::Must);
  f.Add(std::make_unique<irs::All>(), irs::Occur::Must);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));

  irs::BooleanFilter n;
  n.Add(std::make_unique<irs::All>(), irs::Occur::MustNot);
  const auto none = n.CompileTermPredicate();
  ASSERT_NE(nullptr, none);
  EXPECT_FALSE(Accepts(*none, "anything"));
}

TEST(term_predicate_test, wildcard) {
  irs::ByWildcard f;
  f.mutable_options()->term = irs::bstring{B("a%b")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "axxb"));
  EXPECT_FALSE(Accepts(*pred, "ba"));
  EXPECT_FALSE(Accepts(*pred, "a"));
}

TEST(term_predicate_test, wildcard_matches_its_lowering) {
  irs::ByWildcard prefix;
  prefix.mutable_options()->term = irs::bstring{B("ab%")};
  const auto starts = prefix.CompileTermPredicate();
  ASSERT_NE(nullptr, starts);
  EXPECT_TRUE(Accepts(*starts, "ab"));
  EXPECT_TRUE(Accepts(*starts, "ab\xFF"));
  EXPECT_TRUE(Accepts(*starts,
                      "ab\xE0\x80\x80"
                      "c"));
  EXPECT_FALSE(Accepts(*starts, "a"));

  irs::ByWildcard term;
  term.mutable_options()->term = irs::bstring{B("a\\%b")};
  const auto exact = term.CompileTermPredicate();
  ASSERT_NE(nullptr, exact);
  EXPECT_TRUE(Accepts(*exact, "a%b"));
  EXPECT_FALSE(Accepts(*exact, "axb"));
}

TEST(term_predicate_test, regexp_matches_its_lowering) {
  irs::ByRegexp prefix;
  prefix.mutable_options()->pattern = irs::bstring{B("ab.*")};
  const auto starts = prefix.CompileTermPredicate();
  ASSERT_NE(nullptr, starts);
  EXPECT_TRUE(Accepts(*starts, "ab"));
  EXPECT_TRUE(Accepts(*starts, "ab\xFF"));
  EXPECT_FALSE(Accepts(*starts, "a"));

  irs::ByRegexp term;
  term.mutable_options()->pattern = irs::bstring{B("abc")};
  const auto exact = term.CompileTermPredicate();
  ASSERT_NE(nullptr, exact);
  EXPECT_TRUE(Accepts(*exact, "abc"));
  EXPECT_FALSE(Accepts(*exact, "abcd"));

  irs::ByRegexp broken;
  broken.mutable_options()->pattern = irs::bstring{B("a(b")};
  EXPECT_EQ(nullptr, broken.CompileTermPredicate());
}

TEST(term_predicate_test, regexp) {
  irs::ByRegexp f;
  f.mutable_options()->pattern = irs::bstring{B("a.*b")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "axxb"));
  EXPECT_FALSE(Accepts(*pred, "ba"));
  EXPECT_FALSE(Accepts(*pred, "abc"));
}

TEST(term_predicate_test, edit_distance) {
  irs::ByEditDistance f;
  f.mutable_options()->term = irs::bstring{B("abc")};
  f.mutable_options()->max_distance = 1;

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "abd"));
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "abcd"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));
  EXPECT_FALSE(Accepts(*pred, "a"));
}

TEST(term_predicate_test, edit_distance_zero_is_term_match) {
  irs::ByEditDistance f;
  f.mutable_options()->term = irs::bstring{B("abc")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "abd"));
}

TEST(term_predicate_test, all_and_empty) {
  irs::All all;
  const auto all_pred = all.CompileTermPredicate();
  ASSERT_NE(nullptr, all_pred);
  EXPECT_TRUE(Accepts(*all_pred, "anything"));
  EXPECT_TRUE(Accepts(*all_pred, ""));

  irs::Empty empty;
  const auto empty_pred = empty.CompileTermPredicate();
  ASSERT_NE(nullptr, empty_pred);
  EXPECT_FALSE(Accepts(*empty_pred, "anything"));
  EXPECT_FALSE(Accepts(*empty_pred, ""));
}

TEST(term_predicate_test, exclusion) {
  irs::BooleanFilter f;
  f.Add(Prefix("ab"), irs::Occur::Must);
  f.Add(Term("abc"), irs::Occur::MustNot);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "abd"));
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));
}

TEST(term_predicate_test, exclusion_without_include_is_negation) {
  irs::BooleanFilter f;
  f.Add(Prefix("ab"), irs::Occur::MustNot);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_FALSE(Accepts(*pred, "ab"));
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "a"));
  EXPECT_TRUE(Accepts(*pred, "xyz"));
}

TEST(term_predicate_test, exclusion_with_non_compilable_exclude) {
  irs::BooleanFilter f;
  f.Add(Prefix("ab"), irs::Occur::Must);
  f.Add(NotCompilable(), irs::Occur::MustNot);

  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, nested_tree) {
  auto inner = std::make_unique<irs::BooleanFilter>();
  inner->Add(Term("ab"), irs::Occur::Should);
  auto range = std::make_unique<irs::ByRange>();
  range->mutable_options()->range.min = irs::bstring{B("ax")};
  range->mutable_options()->range.min_type = irs::BoundType::Inclusive;
  inner->Add(std::move(range), irs::Occur::Should);
  inner->SetMinShouldMatch(1);

  irs::BooleanFilter f;
  f.Add(Prefix("a"), irs::Occur::Must);
  f.Add(std::move(inner), irs::Occur::Must);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "ax"));
  EXPECT_TRUE(Accepts(*pred, "azz"));
  EXPECT_FALSE(Accepts(*pred, "aa"));
  EXPECT_FALSE(Accepts(*pred, "bx"));
}

TEST(term_predicate_test, and_prefix_with_wildcard) {
  irs::BooleanFilter f;
  f.Add(Prefix("a"), irs::Occur::Must);
  {
    auto w = std::make_unique<irs::ByWildcard>();
    w->mutable_options()->term = irs::bstring{B("%e")};
    f.Add(std::move(w), irs::Occur::Must);
  }

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "aple"));
  EXPECT_TRUE(Accepts(*pred, "ae"));
  EXPECT_TRUE(Accepts(*pred, "apple"));
  EXPECT_FALSE(Accepts(*pred, "apex"));
  EXPECT_FALSE(Accepts(*pred, "e"));
  EXPECT_FALSE(Accepts(*pred, "banana"));
}

TEST(term_predicate_test, and_disjoint_accepts_nothing) {
  irs::BooleanFilter f;
  f.Add(Prefix("a"), irs::Occur::Must);
  f.Add(Term("b"), irs::Occur::Must);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_FALSE(Accepts(*pred, "a"));
  EXPECT_FALSE(Accepts(*pred, "b"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

irs::Filter::ptr RangeOf(const char* min, const char* max, bool min_inclusive,
                         bool max_inclusive) {
  auto f = std::make_unique<irs::ByRange>();
  auto& rng = f->mutable_options()->range;
  if (min) {
    rng.min = irs::bstring{B(min)};
    rng.min_type =
      min_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  if (max) {
    rng.max = irs::bstring{B(max)};
    rng.max_type =
      max_inclusive ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  return f;
}

struct RangePredicate {
  irs::Filter::ptr filter;
  irs::TermPredicate::ptr pred;

  bool Accepts(std::string_view term) const { return pred->Accepts(B(term)); }
};

RangePredicate RangePredicateOf(const char* min, const char* max,
                                bool min_inclusive, bool max_inclusive) {
  auto filter = RangeOf(min, max, min_inclusive, max_inclusive);
  auto pred = filter->CompileTermPredicate();
  return {std::move(filter), std::move(pred)};
}

TEST(term_predicate_test, range_bounded) {
  const auto a = RangePredicateOf("b", "d", true, false);
  ASSERT_NE(nullptr, a.pred);
  EXPECT_FALSE(a.Accepts(""));
  EXPECT_FALSE(a.Accepts("a"));
  EXPECT_FALSE(a.Accepts("azzz"));
  EXPECT_TRUE(a.Accepts("b"));
  EXPECT_TRUE(a.Accepts("ba"));
  EXPECT_TRUE(a.Accepts("c"));
  EXPECT_TRUE(a.Accepts("czzz"));
  EXPECT_FALSE(a.Accepts("d"));
  EXPECT_FALSE(a.Accepts("da"));
}

TEST(term_predicate_test, range_exclusive_min) {
  const auto a = RangePredicateOf("b", "d", false, true);
  ASSERT_NE(nullptr, a.pred);
  EXPECT_FALSE(a.Accepts("b"));
  EXPECT_TRUE(a.Accepts("ba"));
  EXPECT_TRUE(a.Accepts("d"));
  EXPECT_FALSE(a.Accepts("da"));
}

TEST(term_predicate_test, range_shared_prefix_bounds) {
  const auto a = RangePredicateOf("ap", "az", true, true);
  ASSERT_NE(nullptr, a.pred);
  EXPECT_FALSE(a.Accepts("a"));
  EXPECT_FALSE(a.Accepts("ao"));
  EXPECT_TRUE(a.Accepts("ap"));
  EXPECT_TRUE(a.Accepts("apple"));
  EXPECT_TRUE(a.Accepts("avocado"));
  EXPECT_TRUE(a.Accepts("az"));
  EXPECT_FALSE(a.Accepts("aza"));
  EXPECT_FALSE(a.Accepts("b"));
}

TEST(term_predicate_test, range_min_is_prefix_of_max) {
  const auto a = RangePredicateOf("ab", "abz", false, false);
  ASSERT_NE(nullptr, a.pred);
  EXPECT_FALSE(a.Accepts("ab"));
  EXPECT_TRUE(a.Accepts("aba"));
  EXPECT_TRUE(a.Accepts("abyzzz"));
  EXPECT_FALSE(a.Accepts("abz"));
  EXPECT_FALSE(a.Accepts("abza"));
}

TEST(term_predicate_test, range_half_open) {
  const auto lower = RangePredicateOf("m", nullptr, true, false);
  ASSERT_NE(nullptr, lower.pred);
  EXPECT_FALSE(lower.Accepts("lzz"));
  EXPECT_TRUE(lower.Accepts("m"));
  EXPECT_TRUE(lower.Accepts("zzz"));

  const auto upper = RangePredicateOf(nullptr, "m", false, false);
  ASSERT_NE(nullptr, upper.pred);
  EXPECT_TRUE(upper.Accepts(""));
  EXPECT_TRUE(upper.Accepts("lzz"));
  EXPECT_FALSE(upper.Accepts("m"));
  EXPECT_FALSE(upper.Accepts("ma"));
}

TEST(term_predicate_test, range_degenerate) {
  const auto point = RangePredicateOf("abc", "abc", true, true);
  ASSERT_NE(nullptr, point.pred);
  EXPECT_TRUE(point.Accepts("abc"));
  EXPECT_FALSE(point.Accepts("ab"));
  EXPECT_FALSE(point.Accepts("abca"));

  const auto none = RangePredicateOf("abc", "abc", true, false);
  ASSERT_NE(nullptr, none.pred);
  EXPECT_FALSE(none.Accepts("abc"));

  const auto inverted = RangePredicateOf("d", "b", true, true);
  ASSERT_NE(nullptr, inverted.pred);
  EXPECT_FALSE(inverted.Accepts("c"));

  const auto everything = RangePredicateOf(nullptr, nullptr, false, false);
  ASSERT_NE(nullptr, everything.pred);
  EXPECT_TRUE(everything.Accepts(""));
  EXPECT_TRUE(everything.Accepts("zzz"));
}

TEST(term_predicate_test, range_utf8_bytewise_order) {
  const auto a = RangePredicateOf("\xce\xb1", "\xcf\x89", true, true);
  ASSERT_NE(nullptr, a.pred);
  EXPECT_FALSE(a.Accepts("z"));
  EXPECT_TRUE(a.Accepts("\xce\xb1"));
  EXPECT_TRUE(a.Accepts("\xce\xbc"));
  EXPECT_TRUE(a.Accepts("\xce\xbc\xce\xb1"));
  EXPECT_TRUE(a.Accepts("\xcf\x89"));
  EXPECT_FALSE(a.Accepts("\xcf\x89\xce\xb1"));
  EXPECT_FALSE(a.Accepts("\xf0\x9f\x98\x80"));
}

TEST(term_predicate_test, and_range_with_prefix) {
  irs::BooleanFilter f;
  f.Add(Prefix("a"), irs::Occur::Must);
  f.Add(RangeOf("ap", "az", true, true), irs::Occur::Must);

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "apple"));
  EXPECT_FALSE(Accepts(*pred, "aa"));
  EXPECT_FALSE(Accepts(*pred, "b"));
}

TEST(acceptor_fusion_test, union_of_prefixes) {
  irs::ByRegexp f;
  f.mutable_options()->pattern = irs::bstring{B("(?:ax.*)|(?:ban.*)")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ax"));
  EXPECT_TRUE(Accepts(*pred, "axle"));
  EXPECT_TRUE(Accepts(*pred, "banana"));
  EXPECT_FALSE(Accepts(*pred, "apple"));
  EXPECT_FALSE(Accepts(*pred, "b"));
  EXPECT_FALSE(Accepts(*pred, "c"));
}

TEST(acceptor_fusion_test, union_regexp_with_regexp) {
  irs::ByRegexp f;
  f.mutable_options()->pattern = irs::bstring{B("(?:.*x.*)|(?:a.*e)")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "axle"));
  EXPECT_TRUE(Accepts(*pred, "apple"));
  EXPECT_FALSE(Accepts(*pred, "banana"));
}

TEST(acceptor_fusion_test, union_regexp_with_prefix) {
  irs::ByRegexp f;
  f.mutable_options()->pattern = irs::bstring{B("(?:.*x.*)|(?:ban.*)")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "axle"));
  EXPECT_TRUE(Accepts(*pred, "banana"));
  EXPECT_FALSE(Accepts(*pred, "apple"));
}

TEST(acceptor_fusion_test, union_large_fan_in) {
  std::string pattern;
  for (char c = 'a'; c <= 'z'; ++c) {
    pattern += pattern.empty() ? "(?:" : "|(?:";
    pattern += c;
    pattern += "[0-9]{3}.*)";
  }

  irs::ByRegexp f;
  f.mutable_options()->pattern = irs::bstring{B(pattern)};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  for (char c = 'a'; c <= 'z'; ++c) {
    const std::string term = std::string(1, c) + "123tail";
    EXPECT_TRUE(Accepts(*pred, term)) << "term: " << term;
  }
  EXPECT_FALSE(Accepts(*pred, "a12tail"));
  EXPECT_FALSE(Accepts(*pred, "0123tail"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

TEST(term_bounds_test, upper_bound_of) {
  const auto upper = [](std::string_view prefix) {
    const auto bound = irs::UpperBoundOf(B(prefix));
    return std::string{irs::ViewCast<char>(irs::bytes_view{bound})};
  };

  EXPECT_EQ("bus", upper("bur"));
  EXPECT_EQ("b", upper("a"));
  EXPECT_EQ("ab", upper("aa"));
  EXPECT_EQ("", upper(""));
  EXPECT_EQ("", upper("\xFF"));
  EXPECT_EQ("", upper("\xFF\xFF"));
  EXPECT_EQ("b", upper("a\xFF"));
  EXPECT_EQ("b", upper("a\xFF\xFF"));

  constexpr std::string_view kPrefixes[]{"bur", "a", "", "\xFF", "a\xFF", "az"};
  constexpr std::string_view kKeys[]{
    "",   "a",   "az",     "az\xFF", "a\xFF", "a\xFF\x01", "b",
    "bu", "bur", "burden", "bus",    "\xFF",  "\xFF\xFF",
  };
  for (const auto prefix : kPrefixes) {
    const auto bound = irs::UpperBoundOf(B(prefix));
    for (const auto key : kKeys) {
      const bool in_range = B(key) >= B(prefix) &&
                            (bound.empty() || B(key) < irs::bytes_view{bound});
      EXPECT_EQ(B(key).starts_with(B(prefix)), in_range)
        << "prefix: '" << prefix << "' key: '" << key << "'";
    }
  }
}

}  // namespace
