////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/norm.hpp>

#include "tests_shared.hpp"

TEST(token_attributes_test, offset) {
  static_assert("offset" == irs::Type<irs::OffsAttr>::name());

  irs::OffsAttr offs;
  ASSERT_EQ(0, offs.start);
  ASSERT_EQ(0, offs.end);
}

TEST(token_attributes_test, increment) {
  static_assert("increment" == irs::Type<irs::IncAttr>::name());

  irs::IncAttr inc;
  ASSERT_EQ(1, inc.value);
}

TEST(token_attributes_test, TermAttr) {
  static_assert("term" == irs::Type<irs::TermAttr>::name());

  irs::TermAttr term;
  ASSERT_TRUE(irs::IsNull(term.value));
}

TEST(token_attributes_test, payload) {
  static_assert("payload" == irs::Type<irs::PayAttr>::name());

  irs::PayAttr pay;
  ASSERT_TRUE(irs::IsNull(pay.value));
}

TEST(token_attributes_test, document) {
  static_assert("document" == irs::Type<irs::DocAttr>::name());

  irs::DocAttr doc;
  ASSERT_TRUE(!irs::doc_limits::valid(doc.value));
}

TEST(token_attributes_test, frequency) {
  static_assert("frequency" == irs::Type<irs::FreqAttr>::name());

  irs::FreqAttr freq;
  ASSERT_EQ(0, freq.value);
}

TEST(token_attributes_test, Norm) {
  static_assert("norm" == irs::Type<irs::Norm>::name());
}

TEST(token_attributes_test, position) {
  static_assert("position" == irs::Type<irs::PosAttr>::name());
}

TEST(token_attributes_test, AttrProviderChangeAttr) {
  static_assert("attribute_provider_change" ==
                irs::Type<irs::AttrProviderChangeAttr>::name());
}
