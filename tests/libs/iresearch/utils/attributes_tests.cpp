////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/attributes.hpp"
#include "tests_shared.hpp"

using namespace irs;

TEST(attributes_tests, duplicate_register) {
  struct DummyAttribute : public irs::Attribute {};

  static bool gInitialExpected = true;

  // check required for tests with repeat (static maps are not cleared between
  // runs)
  if (gInitialExpected) {
    ASSERT_FALSE(irs::Attributes::exists(irs::Type<DummyAttribute>::name()));
    ASSERT_FALSE(irs::Attributes::get(irs::Type<DummyAttribute>::get().name()));

    irs::AttributeRegistrar initial(irs::Type<DummyAttribute>::get());
    ASSERT_EQ(!gInitialExpected, !initial);
  }

  // next test iteration will not be able to register the same attribute
  gInitialExpected = false;
  irs::AttributeRegistrar duplicate(irs::Type<DummyAttribute>::get());
  ASSERT_TRUE(!duplicate);

  ASSERT_TRUE(irs::Attributes::exists(irs::Type<DummyAttribute>::get().name()));
  ASSERT_TRUE(irs::Attributes::get(irs::Type<DummyAttribute>::name()));
}
