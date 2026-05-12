#include <gtest/gtest.h>

#include "iresearch/utils/automaton_combinators.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace {

bool Accepts(const irs::automaton& a, std::string_view s) {
  return static_cast<bool>(irs::Accept(a, irs::ViewCast<irs::byte_type>(s)));
}

void AssertLanguage(const irs::automaton& a,
                    std::initializer_list<std::string_view> yes,
                    std::initializer_list<std::string_view> no) {
  ASSERT_NE(a.Start(), fst::kNoStateId) << "Automaton has no start state";
  for (auto s : yes) {
    EXPECT_TRUE(Accepts(a, s)) << "Expected to accept: \"" << s << "\"";
  }
  for (auto s : no) {
    EXPECT_FALSE(Accepts(a, s)) << "Expected to reject: \"" << s << "\"";
  }
}

void AssertSameLanguage(const irs::automaton& a, const irs::automaton& b,
                        std::initializer_list<std::string_view> strings) {
  ASSERT_NE(a.Start(), fst::kNoStateId);
  ASSERT_NE(b.Start(), fst::kNoStateId);
  for (auto s : strings) {
    EXPECT_EQ(Accepts(a, s), Accepts(b, s))
      << "Automata disagree on: \"" << s << "\"";
  }
}

TEST(IntersectAutomatons, WildcardAndWildcard) {
  const auto a =
    irs::IntersectAutomatons(irs::FromWildcard("c%"), irs::FromWildcard("%at"));
  AssertLanguage(a, {"cat", "coat", "chat", "cat"},
                 {"car", "bat", "dog", "at", "c", ""});
}

TEST(IntersectAutomatons, WildcardAndRegexp) {
  const auto a = irs::IntersectAutomatons(irs::FromWildcard("pro%"),
                                          irs::FromRegexp("^[a-z]{5,8}$"));
  AssertLanguage(a, {"proof", "probe", "profit", "profile"},
                 {"pro", "ProMax", "pro1", "cat", "profiler123"});
}

TEST(IntersectAutomatons, RegexpAndRegexp) {
  const auto a = irs::IntersectAutomatons(irs::FromRegexp("^[0-9]+$"),
                                          irs::FromRegexp("^[1-9][0-9]*$"));
  AssertLanguage(a, {"1", "42", "100", "9876"},
                 {"0", "007", "0123", "abc", "1a"});
}

TEST(IntersectAutomatons, EmptyWhenNoCommonStrings) {
  const auto a = irs::IntersectAutomatons(irs::FromRegexp("^[a-z]+$"),
                                          irs::FromRegexp("^[0-9]+$"));
  AssertLanguage(a, {}, {"abc", "123", "a1b2", ""});
}

TEST(IntersectAutomatons, ThreePatternChain) {
  const auto step1 =
    irs::IntersectAutomatons(irs::FromWildcard("c%"), irs::FromWildcard("%t"));
  const auto step2 = irs::IntersectAutomatons(step1, irs::FromWildcard("c_t"));
  AssertLanguage(step2, {"cat", "cot", "cut"}, {"coat", "cart", "bat", "ct"});
}

TEST(IntersectAutomatons, IdentityWithSelf) {
  const auto a = irs::FromWildcard("cat%");
  const auto inter = irs::IntersectAutomatons(a, a);
  AssertSameLanguage(a, inter, {"cat", "cats", "catfish", "dog", "ca", ""});
}

TEST(UnionAutomatons, WildcardOrWildcard) {
  const auto a =
    irs::UnionAutomatons(irs::FromWildcard("cat%"), irs::FromWildcard("dog%"));
  AssertLanguage(a, {"cat", "cats", "catfish", "dog", "dogs"},
                 {"bat", "fish", "ca", "do", ""});
}

TEST(UnionAutomatons, WildcardOrRegexp) {
  const auto a = irs::UnionAutomatons(irs::FromWildcard("cat%"),
                                      irs::FromRegexp("^[0-9]+$"));
  AssertLanguage(a, {"cat", "cats", "123", "0", "42", "cat1x"},
                 {"dog", "abc", ""});
}

TEST(UnionAutomatons, RegexpOrRegexp) {
  const auto a = irs::UnionAutomatons(irs::FromRegexp("^[a-z]+$"),
                                      irs::FromRegexp("^[0-9]+$"));
  AssertLanguage(a, {"abc", "z", "123", "0"}, {"Abc", "1a", "ABC", ""});
}

TEST(UnionAutomatons, OverlappingRanges) {
  const auto a =
    irs::UnionAutomatons(irs::FromWildcard("c_t"), irs::FromWildcard("%at"));
  AssertLanguage(a, {"cat", "cot", "bat", "hat", "rat"},
                 {"car", "dog", "cats", ""});
}

TEST(UnionAutomatons, SupersetRelation) {
  const auto narrow = irs::FromWildcard("cat%");
  const auto all = irs::FromWildcard("%");
  const auto u = irs::UnionAutomatons(narrow, all);
  AssertLanguage(u, {"cat", "dog", "xyz", "123"}, {});
}

TEST(UnionAutomatonsDeMorgan, SameResultAsRefinedDeterminize_Wildcard) {
  const auto a1 = irs::FromWildcard("cat%");
  const auto a2 = irs::FromWildcard("dog%");
  AssertSameLanguage(irs::UnionAutomatons(a1, a2),
                     irs::UnionAutomatonsDeMorgan(a1, a2),
                     {"cat", "cats", "dog", "dogs", "bat", "fish", ""});
}

TEST(UnionAutomatonsDeMorgan, SameResultAsRefinedDeterminize_CrossType) {
  const auto a1 = irs::FromWildcard("cat%");
  const auto a2 = irs::FromRegexp("^[0-9]+$");
  AssertSameLanguage(irs::UnionAutomatons(a1, a2),
                     irs::UnionAutomatonsDeMorgan(a1, a2),
                     {"cat", "cats", "123", "dog", "abc", ""});
}

TEST(UnionAutomatonsDeMorgan, SameResultAsRefinedDeterminize_Regexp) {
  const auto a1 = irs::FromRegexp("^[a-z]+$");
  const auto a2 = irs::FromRegexp("^[0-9]+$");
  AssertSameLanguage(irs::UnionAutomatons(a1, a2),
                     irs::UnionAutomatonsDeMorgan(a1, a2),
                     {"abc", "z", "123", "0", "Abc", "1a", ""});
}

TEST(UnionAutomatonsDeMorgan, SameResultWithOverlappingRanges) {
  const auto a1 = irs::FromWildcard("_a%");
  const auto a2 = irs::FromWildcard("%at");
  AssertSameLanguage(irs::UnionAutomatons(a1, a2),
                     irs::UnionAutomatonsDeMorgan(a1, a2),
                     {"cat", "bat", "hat", "cab", "car", "sat", "at", "a", ""});
}

}  // namespace
