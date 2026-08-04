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

#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_predicate.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace {

irs::bytes_view B(std::string_view s) {
  return irs::ViewCast<irs::byte_type>(s);
}

bool Accepts(const irs::TermPredicate& pred, std::string_view term) {
  return pred.Accepts(B(term));
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

TEST(term_predicate_test, by_terms) {
  irs::ByTerms f;
  f.mutable_options()->terms.emplace(B("abc"));
  f.mutable_options()->terms.emplace(B("xyz"));

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "xyz"));
  EXPECT_FALSE(Accepts(*pred, "abd"));
  EXPECT_FALSE(Accepts(*pred, "ab"));
  EXPECT_FALSE(Accepts(*pred, ""));
}

TEST(term_predicate_test, by_terms_min_match_not_compilable) {
  irs::ByTerms f;
  f.mutable_options()->terms.emplace(B("abc"));
  f.mutable_options()->terms.emplace(B("xyz"));
  f.mutable_options()->min_match = 2;

  ASSERT_EQ(nullptr, f.CompileTermPredicate());
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
  *f.mutable_options() = irs::AutomatonOptions{
    B("a%b"), irs::PatternKind::Wildcard, irs::RegexpSyntax::Perl, 1024};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "axxb"));
  EXPECT_FALSE(Accepts(*pred, "ba"));
  EXPECT_FALSE(Accepts(*pred, "a"));
}

TEST(term_predicate_test, automaton_without_source_not_compilable) {
  irs::AutomatonFilter f;
  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, not_negates) {
  irs::Not f;
  f.filter<irs::ByTerm>().mutable_options()->term = irs::bstring{B("abc")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "abd"));
}

TEST(term_predicate_test, empty_not_not_compilable) {
  irs::Not f;
  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, and_conjunction) {
  irs::And f;
  f.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};
  f.add<irs::Not>().filter<irs::ByTerm>().mutable_options()->term =
    irs::bstring{B("abc")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abd"));
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));
}

TEST(term_predicate_test, empty_and_not_compilable) {
  irs::And f;
  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, or_disjunction) {
  irs::Or f;
  f.add<irs::ByTerm>().mutable_options()->term = irs::bstring{B("xyz")};
  f.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "xyz"));
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xy"));
}

TEST(term_predicate_test, or_min_match_counts) {
  irs::Or f;
  f.min_match_count(2);
  f.add<irs::ByTerm>().mutable_options()->term = irs::bstring{B("a")};
  f.add<irs::ByTerm>().mutable_options()->term = irs::bstring{B("b")};
  f.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("a")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "a"));
  EXPECT_FALSE(Accepts(*pred, "b"));
  EXPECT_FALSE(Accepts(*pred, "ab"));
}

TEST(term_predicate_test, or_min_match_exceeds_size_not_compilable) {
  irs::Or f;
  f.min_match_count(3);
  f.add<irs::ByTerm>().mutable_options()->term = irs::bstring{B("a")};
  f.add<irs::ByTerm>().mutable_options()->term = irs::bstring{B("b")};

  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, non_acceptor_leaf_poisons_tree) {
  const auto poison = [](auto& filter) {
    auto& terms = filter.template add<irs::ByTerms>();
    terms.mutable_options()->terms.emplace(B("abc"));
    terms.mutable_options()->min_match = 2;
  };

  irs::And f;
  f.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};
  poison(f);
  ASSERT_EQ(nullptr, f.CompileTermPredicate());

  irs::Or o;
  o.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};
  poison(o);
  ASSERT_EQ(nullptr, o.CompileTermPredicate());
}

TEST(term_predicate_test, all_is_neutral_in_conjunction) {
  irs::And f;
  f.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};
  f.add<irs::All>();

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));

  irs::Not n;
  n.filter<irs::All>();
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
  irs::Exclusion f;
  f.include<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};
  f.exclude<irs::ByTerm>().mutable_options()->term = irs::bstring{B("abc")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "abd"));
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_FALSE(Accepts(*pred, "xyz"));
}

TEST(term_predicate_test, exclusion_without_include_is_negation) {
  irs::Exclusion f;
  f.exclude<irs::ByTerm>().mutable_options()->term = irs::bstring{B("abc")};

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_FALSE(Accepts(*pred, "abc"));
  EXPECT_TRUE(Accepts(*pred, "abd"));
}

TEST(term_predicate_test, exclusion_with_non_compilable_exclude) {
  irs::Exclusion f;
  f.include<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("ab")};
  auto& terms = f.exclude<irs::ByTerms>();
  terms.mutable_options()->terms.emplace(B("abc"));
  terms.mutable_options()->min_match = 2;

  ASSERT_EQ(nullptr, f.CompileTermPredicate());
}

TEST(term_predicate_test, nested_tree) {
  irs::And f;
  f.add<irs::ByPrefix>().mutable_options()->term = irs::bstring{B("a")};
  auto& inner = f.add<irs::Or>();
  inner.add<irs::ByTerm>().mutable_options()->term = irs::bstring{B("ab")};
  auto& range = inner.add<irs::ByRange>();
  range.mutable_options()->range.min = irs::bstring{B("ax")};
  range.mutable_options()->range.min_type = irs::BoundType::Inclusive;

  const auto pred = f.CompileTermPredicate();
  ASSERT_NE(nullptr, pred);
  EXPECT_TRUE(Accepts(*pred, "ab"));
  EXPECT_TRUE(Accepts(*pred, "ax"));
  EXPECT_TRUE(Accepts(*pred, "azz"));
  EXPECT_FALSE(Accepts(*pred, "aa"));
  EXPECT_FALSE(Accepts(*pred, "bx"));
}

}  // namespace
