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

#include "filter_test_case_base.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/exclude_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "tests_shared.hpp"

namespace {

template<typename Filter>
Filter MakeFilter(std::string_view field, std::string_view term) {
  Filter q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

template<typename Filter>
Filter& Append(irs::BooleanFilter& root, std::string_view name,
               std::string_view term) {
  auto& sub = root.add<Filter>();
  *sub.mutable_field() = name;
  sub.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return sub;
}

}  // namespace
namespace tests {

TEST(filter_optimizer_test, not_becomes_exclude) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto& node = sdb::basics::downCast<irs::Not>(*root);
  node.boost(2.5F);
  node.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("name", "A");

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclude>::id(), root->type());
  auto& exclude = sdb::basics::downCast<irs::Exclude>(*root);
  ASSERT_EQ(2.5F, exclude.Boost());
  ASSERT_NE(nullptr, exclude.Child());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), exclude.Child()->type());
}

TEST(filter_optimizer_test, empty_not_becomes_empty) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, not_all_becomes_empty) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();
  sdb::basics::downCast<irs::Not>(*root).filter<irs::All>();

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Empty>::id(), root->type());
}

TEST(filter_optimizer_test, double_negation_unwraps) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto& inner_not = sdb::basics::downCast<irs::Not>(*root).filter<irs::Not>();
  auto& term = inner_not.filter<irs::ByTerm>();
  term = MakeFilter<irs::ByTerm>("name", "A");
  const auto* term_ptr = &term;

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
  ASSERT_EQ(term_ptr, root.get());
}

TEST(filter_optimizer_test, triple_negation_keeps_one) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto& mid = sdb::basics::downCast<irs::Not>(*root).filter<irs::Not>();
  auto& inner = mid.filter<irs::Not>();
  inner.filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("name", "A");

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::Exclude>::id(), root->type());
  auto& exclude = sdb::basics::downCast<irs::Exclude>(*root);
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), exclude.Child()->type());
}

TEST(filter_optimizer_test, deep_negation_chain) {
  irs::Filter::ptr root = std::make_unique<irs::Not>();
  auto* current = &sdb::basics::downCast<irs::Not>(*root);
  for (size_t i = 1; i < 50; ++i) {
    current = &current->filter<irs::Not>();
  }
  current->filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("name", "A");

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), root->type());
}

TEST(filter_optimizer_test, not_inside_and) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  Append<irs::ByTerm>(and_root, "name", "A");
  and_root.add<irs::Not>().filter<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>("name", "B");

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::Exclude>::id(), and_root[1].type());
}

TEST(filter_optimizer_test, flatten_and) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  auto& inner = and_root.add<irs::And>();
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  Append<irs::ByTerm>(and_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::And>::id(), root->type());
  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, flatten_and_deep) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  auto& mid = and_root.add<irs::And>();
  auto& inner = mid.add<irs::And>();
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(mid, "name", "B");
  Append<irs::ByTerm>(and_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[2].type());
}

TEST(filter_optimizer_test, flatten_and_boost_gate) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  auto& inner = and_root.add<irs::And>();
  inner.boost(2.F);
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  Append<irs::ByTerm>(and_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
}

TEST(filter_optimizer_test, flatten_and_merge_type_gate) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  auto& inner = and_root.add<irs::And>();
  inner.merge_type(irs::ScoreMergeType::Max);
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  Append<irs::ByTerm>(and_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
}

TEST(filter_optimizer_test, flatten_and_empty_inner_gate) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  and_root.add<irs::And>();
  Append<irs::ByTerm>(and_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(2, and_root.size());
  ASSERT_EQ(irs::Type<irs::And>::id(), and_root[0].type());
  ASSERT_EQ(0, sdb::basics::downCast<irs::And>(and_root[0]).size());
}

TEST(filter_optimizer_test, flatten_or) {
  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  auto& inner = or_root.add<irs::Or>();
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  Append<irs::ByTerm>(or_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(3, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_parent_min_match_gate) {
  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  or_root.min_match_count(2);
  auto& inner = or_root.add<irs::Or>();
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  Append<irs::ByTerm>(or_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_inner_min_match_gate) {
  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  auto& inner = or_root.add<irs::Or>();
  inner.min_match_count(2);
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  Append<irs::ByTerm>(or_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, flatten_or_zero_min_match_gate) {
  irs::Filter::ptr root = std::make_unique<irs::Or>();
  auto& or_root = sdb::basics::downCast<irs::Or>(*root);
  auto& inner = or_root.add<irs::Or>();
  inner.min_match_count(0);
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(or_root, "name", "C");

  irs::Optimize(root);

  ASSERT_EQ(2, or_root.size());
}

TEST(filter_optimizer_test, mixed_boolean_filter_subtrees) {
  irs::Filter::ptr root = std::make_unique<irs::MixedBooleanFilter>();
  auto& mixed = sdb::basics::downCast<irs::MixedBooleanFilter>(*root);
  mixed.GetRequired().add<irs::Not>().filter<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>("name", "A");
  mixed.GetOptional().add<irs::Not>().filter<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>("name", "B");

  irs::Optimize(root);

  ASSERT_EQ(irs::Type<irs::MixedBooleanFilter>::id(), root->type());
  ASSERT_EQ(1, mixed.GetRequired().size());
  ASSERT_EQ(irs::Type<irs::Exclude>::id(), mixed.GetRequired()[0].type());
  ASSERT_EQ(1, mixed.GetOptional().size());
  ASSERT_EQ(irs::Type<irs::Exclude>::id(), mixed.GetOptional()[0].type());
}

TEST(filter_optimizer_test, idempotent) {
  const auto make = [] {
    irs::Filter::ptr root = std::make_unique<irs::And>();
    auto& and_root = sdb::basics::downCast<irs::And>(*root);
    auto& inner = and_root.add<irs::And>();
    Append<irs::ByTerm>(inner, "name", "A");
    inner.add<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("name", "B");
    auto& sub_or = and_root.add<irs::Or>();
    auto& sub_sub_or = sub_or.add<irs::Or>();
    Append<irs::ByTerm>(sub_sub_or, "name", "C");
    sub_or.add<irs::Not>().filter<irs::Not>().filter<irs::ByTerm>() =
      MakeFilter<irs::ByTerm>("name", "D");
    return root;
  };

  auto once = make();
  auto twice = make();
  irs::Optimize(once);
  irs::Optimize(twice);
  irs::Optimize(twice);

  ASSERT_TRUE(*once == *twice);
}

TEST(filter_optimizer_test, leaf_root_pointer_identity) {
  irs::Filter::ptr root =
    std::make_unique<irs::ByTerm>(MakeFilter<irs::ByTerm>("name", "A"));
  const auto* raw = root.get();

  irs::Optimize(root);

  ASSERT_EQ(raw, root.get());
}

TEST(filter_optimizer_test, custom_rule_subset) {
  irs::Filter::ptr root = std::make_unique<irs::And>();
  auto& and_root = sdb::basics::downCast<irs::And>(*root);
  auto& inner = and_root.add<irs::And>();
  Append<irs::ByTerm>(inner, "name", "A");
  Append<irs::ByTerm>(inner, "name", "B");
  and_root.add<irs::Not>().filter<irs::ByTerm>() =
    MakeFilter<irs::ByTerm>("name", "C");

  irs::Optimize(root, {}, irs::kDefaultRules.subspan(1));

  ASSERT_EQ(3, and_root.size());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[0].type());
  ASSERT_EQ(irs::Type<irs::ByTerm>::id(), and_root[1].type());
  ASSERT_EQ(irs::Type<irs::Not>::id(), and_root[2].type());
}

class FilterOptimizerTestCase : public FilterTestCaseBase {};

TEST_P(FilterOptimizerTestCase, optimized_equals_naive) {
  {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen);
  }

  auto rdr = open_reader();

  Docs all_docs(32);
  std::iota(all_docs.begin(), all_docs.end(), 1);

  Docs all_but_first(31);
  std::iota(all_but_first.begin(), all_but_first.end(), 2);

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::Not>();
      sdb::basics::downCast<irs::Not>(*root).filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, all_but_first, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, all_but_first, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::And>();
      auto& and_root = sdb::basics::downCast<irs::And>(*root);
      Append<irs::ByTerm>(and_root, "duplicated", "abcd");
      and_root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{5, 11, 21, 27, 31}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{5, 11, 21, 27, 31}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::Or>();
      auto& or_root = sdb::basics::downCast<irs::Or>(*root);
      Append<irs::ByTerm>(or_root, "duplicated", "abcd");
      or_root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, all_docs, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, all_docs, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::Not>();
      sdb::basics::downCast<irs::Not>(*root)
        .filter<irs::Not>()
        .filter<irs::ByTerm>() = MakeFilter<irs::ByTerm>("name", "A");
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{1}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{1}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::Or>();
      auto& or_root = sdb::basics::downCast<irs::Or>(*root);
      Append<irs::ByTerm>(or_root, "name", "V");
      or_root.add<irs::Not>().filter<irs::All>();
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{22}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{22}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::And>();
      auto& and_root = sdb::basics::downCast<irs::And>(*root);
      Append<irs::ByTerm>(and_root, "name", "V");
      and_root.add<irs::Not>().filter<irs::All>();
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    CheckQuery(*optimized, Docs{}, rdr);
  }

  {
    const auto make = [] {
      irs::Filter::ptr root = std::make_unique<irs::And>();
      auto& and_root = sdb::basics::downCast<irs::And>(*root);
      auto& inner = and_root.add<irs::And>();
      Append<irs::ByTerm>(inner, "duplicated", "abcd");
      Append<irs::ByTerm>(inner, "same", "xyz");
      and_root.add<irs::Not>().filter<irs::ByTerm>() =
        MakeFilter<irs::ByTerm>("name", "A");
      return root;
    };
    auto naive = make();
    CheckQuery(*naive, Docs{5, 11, 21, 27, 31}, rdr);
    auto optimized = make();
    irs::Optimize(optimized);
    ASSERT_EQ(3, sdb::basics::downCast<irs::And>(*optimized).size());
    CheckQuery(*optimized, Docs{5, 11, 21, 27, 31}, rdr);
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(filter_optimizer_test, FilterOptimizerTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         FilterOptimizerTestCase::to_string);

}  // namespace tests
