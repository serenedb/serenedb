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

#include <iresearch/search/filters/term_ranges_filter.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kNameFieldId = tests::FieldIdFor("name");
inline constexpr irs::field_id kInvalidNameFieldId =
  tests::FieldIdFor("invalid_name");

struct Range {
  std::string_view min;
  std::string_view max;
};

irs::ByTermRanges MakeFilter(irs::field_id field,
                             const std::vector<Range>& ranges) {
  irs::ByTermRanges filter;
  *filter.mutable_field_id() = field;
  for (const auto& [min, max] : ranges) {
    filter.mutable_options()->ranges.emplace_back(
      irs::bstring{irs::ViewCast<irs::byte_type>(min)},
      irs::bstring{irs::ViewCast<irs::byte_type>(max)});
  }
  return filter;
}

class TermRangesFilterTestCase : public tests::FilterTestCaseBase {
 protected:
  void ByTermRangesSequential() {
    {
      tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                  &tests::GenericJsonFieldFactory);
      add_segment(gen);
    }
    auto rdr = open_reader();

    CheckQuery(irs::ByTermRanges(), Docs{}, rdr);
    CheckQuery(MakeFilter(kNameFieldId, {}), Docs{}, rdr);
    CheckQuery(MakeFilter(kInvalidNameFieldId, {{"A", "Z"}}), Docs{}, rdr);

    CheckQuery(MakeFilter(kNameFieldId, {{"A", "C"}, {"F", "F"}, {"X", "Z"}}),
               Docs{1, 2, 3, 6, 24, 25, 26}, Costs{7}, rdr);

    CheckQuery(
      MakeFilter(
        kNameFieldId,
        {{"AA", "AZ"}, {"BA", "BZ"}, {"D", "D"}, {"Q", "R"}, {"ZZ", "ZZZ"}}),
      Docs{4, 17, 18}, Costs{3}, rdr);

    CheckQuery(MakeFilter(kNameFieldId, {{"!", "%"}, {"~", "~"}}),
               Docs{27, 28, 30, 31, 32}, Costs{5}, rdr);

    CheckQuery(MakeFilter(kNameFieldId, {{"0", "9"}, {"~~", "~~~"}}), Docs{},
               rdr);
  }
};

TEST_P(TermRangesFilterTestCase, by_term_ranges) { ByTermRangesSequential(); }

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(term_ranges_filter_test, TermRangesFilterTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         TermRangesFilterTestCase::to_string);

}  // namespace
