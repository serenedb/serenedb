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
#include "iresearch/search/filter_rules.hpp"
#include "tests_shared.hpp"

namespace {
template<typename Filter>
Filter MakeFilter(const std::string_view& field, const std::string_view term) {
  Filter q;
  *q.mutable_field() = field;
  q.mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return q;
}

class FilterRuleTestCase : public tests::FilterTestCaseBase {};

TEST_P(FilterRuleTestCase, test) {
  {
    irs::FilterRulesConstructor constructor;

    constructor.Add<irs::NotFilterRule>();

    irs::Filter::ptr root = std::make_unique<irs::Not>();
    auto& not_filter = sdb::basics::downCast<irs::Not>(*root);
    auto& sub_not_filter = not_filter.filter<irs::Not>();
    sub_not_filter.filter<irs::Empty>();
    root = constructor.Apply(std::move(root));

    ASSERT_EQ(root->type(), irs::Type<irs::Empty>::id());
  }

  {
    irs::FilterRulesConstructor constructor;

    constructor.Add<irs::AndFlatteningFilterRule>();

    irs::Filter::ptr root = std::make_unique<irs::And>();
    auto& and_filter = sdb::basics::downCast<irs::And>(*root);
    auto& sub_and_filter = and_filter.add<irs::And>();
    sub_and_filter.add<irs::Empty>();
    sub_and_filter.add<irs::Empty>();
    and_filter.add<irs::Empty>();
    root = constructor.Apply(std::move(root));

    auto& result = sdb::basics::downCast<irs::And>(*root);
    ASSERT_EQ(result.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(result[i].type(), irs::Type<irs::Empty>::id());
    }
  }

  {
    irs::FilterRulesConstructor constructor;

    constructor.Add<irs::OrFlatteningFilterRule>();

    irs::Filter::ptr root = std::make_unique<irs::Or>();
    auto& or_filter = sdb::basics::downCast<irs::Or>(*root);
    auto& sub_or_filter = or_filter.add<irs::Or>();
    sub_or_filter.add<irs::Empty>();
    sub_or_filter.add<irs::Empty>();
    or_filter.add<irs::Empty>();
    root = constructor.Apply(std::move(root));

    auto& result = sdb::basics::downCast<irs::Or>(*root);
    ASSERT_EQ(result.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(or_filter[i].type(), irs::Type<irs::Empty>::id());
    }
  }

  {
    irs::FilterRulesConstructor constructor;

    constructor.Add<irs::ByTermsFilterRule>();

    irs::Filter::ptr root = std::make_unique<irs::And>();
    auto& and_filter = sdb::basics::downCast<irs::And>(*root);
    and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "cat");
    and_filter.add<irs::ByTerm>() = MakeFilter<irs::ByTerm>("animal", "dog");
    root = constructor.Apply(std::move(root));

    ASSERT_EQ(root->type(), irs::Type<irs::ByTerms>::id());
    auto& by_terms = sdb::basics::downCast<irs::ByTerms>(*root);

    const auto& field = by_terms.field();
    ASSERT_EQ(field, "animal");

    const auto& options = by_terms.options();
    ASSERT_EQ(options.min_match, 2);
    // ASSERT_EQ(options.terms.contains(""))
  }
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(rule_filter_test, FilterRuleTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            ::testing::Values("1_5simd")),
                         FilterRuleTestCase::to_string);
}  // namespace

