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

#include <iresearch/utils/ref_counter.hpp>
#include <unordered_map>

#include "gtest/gtest.h"

namespace tests {
class RefCounterTests : public ::testing::Test {
  void SetUp() final {
    // Code here will be called immediately after the constructor (right before
    // each test).
  }

  void TearDown() final {
    // Code here will be called immediately after each test (right before the
    // destructor).
  }
};
}  // namespace tests

using namespace tests;

TEST_F(RefCounterTests, test_ref_counter_add) {
  {
    irs::RefCounter<int> refs;

    ASSERT_EQ(0, refs.find(1));
    ASSERT_EQ(0, refs.find(2));

    auto ref1a = refs.add(1);
    auto* ref1a_val = ref1a.get();

    ASSERT_EQ(2, ref1a.use_count());
    ASSERT_EQ(1, *(ref1a.get()));
    ASSERT_EQ(1, refs.find(1));

    auto ref2 = refs.add(2);

    ASSERT_EQ(2, ref2.use_count());
    ASSERT_EQ(2, *(ref2.get()));
    ASSERT_EQ(1, refs.find(2));

    auto ref1b = refs.add(1);

    ASSERT_EQ(3, ref1b.use_count());
    ASSERT_EQ(1, *(ref1b.get()));
    ASSERT_EQ(ref1a_val, ref1b.get());
    ASSERT_EQ(2, refs.find(1));

    ref1a.reset();
    ASSERT_EQ(2, ref1b.use_count());
    ASSERT_EQ(1, *(ref1b.get()));
    ASSERT_EQ(1, refs.find(1));

    ref1b.reset();
    ASSERT_EQ(0, refs.find(1));

    auto ref1c = refs.add(1);

    ASSERT_EQ(2, ref1c.use_count());
    ASSERT_EQ(1, *(ref1c.get()));
    ASSERT_EQ(ref1a_val, ref1c.get());
    ASSERT_EQ(1, refs.find(1));
  }

  {
    irs::RefCounter<std::string> refs;

    ASSERT_EQ(0, refs.find("abc"));
    ASSERT_EQ(0, refs.find("def"));

    auto ref1a = refs.add("abc");
    auto* ref1a_val = ref1a.get();

    ASSERT_EQ(2, ref1a.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1a.get()));
    ASSERT_EQ(1, refs.find("abc"));

    auto ref2 = refs.add("def");

    ASSERT_EQ(2, ref2.use_count());
    ASSERT_EQ(std::string("def"), *(ref2.get()));
    ASSERT_EQ(1, refs.find("def"));

    auto ref1b = refs.add("abc");

    ASSERT_EQ(3, ref1b.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1b.get()));
    ASSERT_EQ(ref1a_val, ref1b.get());
    ASSERT_EQ(2, refs.find("abc"));

    ref1a.reset();
    ASSERT_EQ(2, ref1b.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1b.get()));
    ASSERT_EQ(1, refs.find("abc"));

    ref1b.reset();
    ASSERT_EQ(0, refs.find("abc"));

    auto ref1c = refs.add("abc");

    ASSERT_EQ(2, ref1c.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1c.get()));
    ASSERT_EQ(ref1a_val, ref1c.get());
    ASSERT_EQ(1, refs.find("abc"));
  }
}

TEST_F(RefCounterTests, test_ref_counter_contains) {
  irs::RefCounter<int> refs;

  ASSERT_FALSE(refs.contains(1));
  ASSERT_FALSE(refs.contains(2));

  auto ref = refs.add(1);

  ASSERT_TRUE(refs.contains(1));
  ref.reset();
  ASSERT_TRUE(refs.contains(1));
}

TEST_F(RefCounterTests, test_ref_counter_visit) {
  irs::RefCounter<int> refs;

  auto ref1a = refs.add(1);
  auto ref1b = refs.add(1);
  auto ref2a = refs.add(2);
  auto ref2b = refs.add(2);
  auto ref2c = refs.add(2);

  // test full visitation
  {
    std::unordered_map<int, size_t> expected = {{1, 2}, {2, 3}};
    auto visitor = [&expected](const int& key, size_t count) -> bool {
      auto itr = expected.find(key);
      EXPECT_EQ(itr->second, count);
      expected.erase(itr);
      return true;
    };

    ASSERT_TRUE(refs.visit(visitor));
    ASSERT_TRUE(expected.empty());
  }

  // test early terminate
  {
    std::unordered_map<int, size_t> expected = {{1, 2}, {2, 3}};
    auto visitor = [&expected](const int& key, size_t count) -> bool {
      auto itr = expected.find(key);
      EXPECT_EQ(itr->second, count);
      expected.erase(itr);
      return false;
    };

    ASSERT_FALSE(refs.visit(visitor));
    ASSERT_EQ(1, expected.size());  // only 1 element removed out of 2
  }

  // test remove unused
  {
    std::unordered_map<int, size_t> expected = {{1, 0}, {2, 2}};
    auto visitor = [&expected](const int& key, size_t count) -> bool {
      auto itr = expected.find(key);
      EXPECT_EQ(itr->second, count);
      expected.erase(itr);
      return true;
    };

    ref1a.reset();
    ref1b.reset();
    ref2a.reset();
    ASSERT_FALSE(expected.empty());
    ASSERT_TRUE(refs.visit(visitor, true));
    ASSERT_TRUE(expected.empty());
  }

  // test empty
  {
    auto visitor = [](const int& /*key*/, size_t /*count*/) -> bool {
      return true;
    };

    ASSERT_FALSE(refs.empty());
    ASSERT_TRUE(refs.visit(visitor, true));
    ASSERT_FALSE(refs.empty());
    ref2b.reset();
    ref2c.reset();
    ASSERT_FALSE(refs.empty());
    ASSERT_TRUE(refs.visit(visitor, true));
    ASSERT_TRUE(refs.empty());
  }
}
