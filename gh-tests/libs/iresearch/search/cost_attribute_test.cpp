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

#include <iresearch/search/filter.hpp>
#include <limits>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

static_assert("cost" == irs::Type<irs::CostAttr>::name());
static_assert((std::numeric_limits<irs::CostAttr::Type>::max)() ==
              irs::CostAttr::kMax);

TEST(cost_attribute_test, ctor) {
  irs::CostAttr cost;
  ASSERT_EQ(0, cost.estimate());
}

TEST(cost_attribute_test, estimation) {
  irs::CostAttr cost;
  ASSERT_EQ(0, cost.estimate());

  // explicit estimation
  {
    auto est = 7;

    // set estimation value and check
    {
      cost.reset(est);
      ASSERT_EQ(est, cost.estimate());
    }
  }

  // implicit estimation
  {
    auto evaluated = false;
    auto est = 7;

    cost.reset([&evaluated, est]() noexcept {
      evaluated = true;
      return est;
    });
    ASSERT_FALSE(evaluated);
    ASSERT_EQ(est, cost.estimate());
    ASSERT_TRUE(evaluated);
  }
}

TEST(cost_attribute_test, lazy_estimation) {
  irs::CostAttr cost;
  ASSERT_EQ(0, cost.estimate());

  auto evaluated = false;
  auto est = 7;

  // set estimation function and evaluate
  {
    evaluated = false;
    cost.reset([&evaluated, est]() noexcept {
      evaluated = true;
      return est;
    });
    ASSERT_FALSE(evaluated);
    ASSERT_EQ(est, cost.estimate());
    ASSERT_TRUE(evaluated);
  }

  // ensure value is cached
  {
    evaluated = false;
    ASSERT_EQ(est, cost.estimate());
    ASSERT_FALSE(evaluated);
  }

  // change estimation func
  {
    evaluated = false;
    cost.reset([&evaluated, est]() noexcept {
      evaluated = true;
      return est + 1;
    });
    ASSERT_FALSE(evaluated);
    ASSERT_EQ(est + 1, cost.estimate());
    ASSERT_TRUE(evaluated);
  }

  // set value directly
  {
    evaluated = false;
    cost.reset(est + 2);
    ASSERT_FALSE(evaluated);
    ASSERT_EQ(est + 2, cost.estimate());
    ASSERT_FALSE(evaluated);
  }
}

TEST(cost_attribute_test, extract) {
  struct BasicAttributeProvider : irs::AttributeProvider {
    irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
      return type == irs::Type<irs::CostAttr>::id() ? cost : nullptr;
    }

    irs::CostAttr* cost{};
  } attrs;

  ASSERT_EQ(irs::CostAttr::Type(irs::CostAttr::kMax),
            irs::CostAttr::extract(attrs));

  ASSERT_EQ(5, irs::CostAttr::extract(attrs, 5));

  irs::CostAttr cost;
  attrs.cost = &cost;

  auto est = 7;
  auto evaluated = false;

  // set estimation function and evaluate
  {
    cost.reset([&evaluated, est]() noexcept {
      evaluated = true;
      return est;
    });
    ASSERT_FALSE(evaluated);
    ASSERT_EQ(est, irs::CostAttr::extract(attrs));
    ASSERT_TRUE(evaluated);
  }

  // change estimation func
  {
    evaluated = false;
    cost.reset([&evaluated, est]() noexcept {
      evaluated = true;
      return est + 1;
    });
    ASSERT_FALSE(evaluated);
    ASSERT_EQ(est + 1, irs::CostAttr::extract(attrs, 3));
    ASSERT_TRUE(evaluated);
  }

  // clear
  {
    evaluated = false;
    ASSERT_EQ(est + 1, irs::CostAttr::extract(attrs, 3));
    ASSERT_FALSE(evaluated);
  }
}
