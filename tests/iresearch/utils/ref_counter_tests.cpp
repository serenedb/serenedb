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

#include <absl/algorithm/container.h>

#include <iresearch/utils/ref_counter.hpp>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

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

    ASSERT_EQ(1, ref1a.use_count());
    ASSERT_EQ(1, *(ref1a.get()));
    ASSERT_EQ(1, refs.find(1));

    auto ref2 = refs.add(2);

    ASSERT_EQ(1, ref2.use_count());
    ASSERT_EQ(2, *(ref2.get()));
    ASSERT_EQ(1, refs.find(2));

    auto ref1b = refs.add(1);

    ASSERT_EQ(2, ref1b.use_count());
    ASSERT_EQ(1, *(ref1b.get()));
    ASSERT_EQ(ref1a_val, ref1b.get());
    ASSERT_EQ(2, refs.find(1));

    ref1a.reset();
    ASSERT_EQ(1, ref1b.use_count());
    ASSERT_EQ(1, *(ref1b.get()));
    ASSERT_EQ(1, refs.find(1));

    ref1b.reset();
    ASSERT_EQ(0, refs.find(1));

    auto ref1c = refs.add(1);

    ASSERT_EQ(1, ref1c.use_count());
    ASSERT_EQ(1, *(ref1c.get()));
    ASSERT_EQ(1, refs.find(1));
  }

  {
    irs::RefCounter<std::string> refs;

    ASSERT_EQ(0, refs.find("abc"));
    ASSERT_EQ(0, refs.find("def"));

    auto ref1a = refs.add("abc");
    auto* ref1a_val = ref1a.get();

    ASSERT_EQ(1, ref1a.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1a.get()));
    ASSERT_EQ(1, refs.find("abc"));

    auto ref2 = refs.add("def");

    ASSERT_EQ(1, ref2.use_count());
    ASSERT_EQ(std::string("def"), *(ref2.get()));
    ASSERT_EQ(1, refs.find("def"));

    auto ref1b = refs.add("abc");

    ASSERT_EQ(2, ref1b.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1b.get()));
    ASSERT_EQ(ref1a_val, ref1b.get());
    ASSERT_EQ(2, refs.find("abc"));

    ref1a.reset();
    ASSERT_EQ(1, ref1b.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1b.get()));
    ASSERT_EQ(1, refs.find("abc"));

    ref1b.reset();
    ASSERT_EQ(0, refs.find("abc"));

    auto ref1c = refs.add("abc");

    ASSERT_EQ(1, ref1c.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1c.get()));
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
    ASSERT_TRUE(refs.visit(visitor));
    ASSERT_TRUE(expected.empty());
    refs.ForgetUnused([](int) { return true; });
    ASSERT_FALSE(refs.contains(1));
    ASSERT_TRUE(refs.contains(2));
  }

  // test empty
  {
    const auto forget = [](int) { return true; };

    ASSERT_FALSE(refs.empty());
    refs.ForgetUnused(forget);
    ASSERT_FALSE(refs.empty());
    ref2b.reset();
    ref2c.reset();
    ASSERT_FALSE(refs.empty());
    refs.ForgetUnused(forget);
    ASSERT_TRUE(refs.empty());
  }
}

TEST_F(RefCounterTests, test_ref_counter_forget_unused_keeps_what_it_declines) {
  irs::RefCounter<int> refs;
  refs.Track(1);
  refs.Track(2);
  auto ref3 = refs.add(3);
  ASSERT_EQ(0, refs.find(1));
  ASSERT_EQ(1, refs.find(3));

  std::vector<int> asked;
  refs.ForgetUnused([&](int key) {
    asked.push_back(key);
    return key != 2;
  });
  absl::c_sort(asked);
  ASSERT_EQ((std::vector<int>{1, 2}), asked);
  ASSERT_FALSE(refs.contains(1));
  ASSERT_TRUE(refs.contains(2));
  ASSERT_TRUE(refs.contains(3));

  refs.Track(3);
  ASSERT_EQ(1, refs.find(3));
}

TEST_F(RefCounterTests, test_ref_counter_notifies_the_last_release) {
  irs::RefCounter<std::string> refs;
  std::vector<std::string> released;
  refs.OnRelease(
    [&](const std::string& key) noexcept { released.push_back(key); });

  auto first = refs.add("abc");
  auto second = refs.add(std::string_view{"abc"});
  auto other = refs.add("def");
  first.reset();
  ASSERT_TRUE(released.empty());
  second.reset();
  ASSERT_EQ((std::vector<std::string>{"abc"}), released);

  auto again = refs.add("abc");
  ASSERT_EQ(1, refs.find("abc"));
  again.reset();
  ASSERT_EQ((std::vector<std::string>{"abc", "abc"}), released);

  refs.Track("ghi");
  ASSERT_EQ(2, released.size());

  refs.remove("def");
  ASSERT_EQ(2, released.size());
  other.reset();
  ASSERT_EQ((std::vector<std::string>{"abc", "abc", "def"}), released);
}

TEST_F(RefCounterTests, test_ref_counter_reference_outlives_the_counter) {
  std::vector<std::string> released;
  irs::RefCounter<std::string>::ref_t ref;
  {
    irs::RefCounter<std::string> refs;
    refs.OnRelease(
      [&](const std::string& key) noexcept { released.push_back(key); });
    ref = refs.add("abc");
  }
  ASSERT_EQ("abc", *ref);
  ref.reset();
  ASSERT_EQ((std::vector<std::string>{"abc"}), released);
}
