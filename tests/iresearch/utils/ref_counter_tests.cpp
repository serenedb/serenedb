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
#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/notification.h>
#include <absl/time/time.h>

#include <iresearch/utils/ref_counter.hpp>
#include <thread>
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

template<typename Key>
std::vector<Key> RemoveUnused(irs::RefCounter<Key>& refs) {
  return refs.RemoveUnused([](std::vector<Key>& unused) {
    absl::c_sort(unused);
    return unused;
  });
}

}  // namespace tests

using namespace tests;

TEST_F(RefCounterTests, test_ref_counter_add) {
  {
    irs::RefCounter<int> refs;

    auto ref1a = refs.add(1);
    auto* ref1a_val = ref1a.get();

    ASSERT_EQ(2, ref1a.use_count());
    ASSERT_EQ(1, *(ref1a.get()));

    auto ref2 = refs.add(2);

    ASSERT_EQ(2, ref2.use_count());
    ASSERT_EQ(2, *(ref2.get()));

    auto ref1b = refs.add(1);

    ASSERT_EQ(3, ref1b.use_count());
    ASSERT_EQ(1, *(ref1b.get()));
    ASSERT_EQ(ref1a_val, ref1b.get());

    ref1a.reset();
    ASSERT_EQ(2, ref1b.use_count());
    ASSERT_EQ(1, *(ref1b.get()));

    ref1b.reset();

    auto ref1c = refs.add(1);

    ASSERT_EQ(2, ref1c.use_count());
    ASSERT_EQ(1, *(ref1c.get()));
    ASSERT_EQ(ref1a_val, ref1c.get());
  }

  {
    irs::RefCounter<std::string> refs;

    auto ref1a = refs.add("abc");
    auto* ref1a_val = ref1a.get();

    ASSERT_EQ(2, ref1a.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1a.get()));

    auto ref2 = refs.add("def");

    ASSERT_EQ(2, ref2.use_count());
    ASSERT_EQ(std::string("def"), *(ref2.get()));

    auto ref1b = refs.add("abc");

    ASSERT_EQ(3, ref1b.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1b.get()));
    ASSERT_EQ(ref1a_val, ref1b.get());

    ref1a.reset();
    ASSERT_EQ(2, ref1b.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1b.get()));

    ref1b.reset();

    auto ref1c = refs.add("abc");

    ASSERT_EQ(2, ref1c.use_count());
    ASSERT_EQ(std::string("abc"), *(ref1c.get()));
    ASSERT_EQ(ref1a_val, ref1c.get());
  }
}

TEST_F(RefCounterTests, test_ref_counter_remove_unused) {
  irs::RefCounter<int> refs;

  auto ref1a = refs.add(1);
  auto ref1b = refs.add(1);
  auto ref2a = refs.add(2);
  auto ref2b = refs.add(2);
  auto ref2c = refs.add(2);

  ASSERT_TRUE(RemoveUnused(refs).empty());

  ref1a.reset();
  ref1b.reset();
  ref2a.reset();
  ASSERT_EQ(std::vector<int>{1}, RemoveUnused(refs));
  ASSERT_TRUE(RemoveUnused(refs).empty());

  ref2b.reset();
  ref2c.reset();
  ASSERT_EQ(std::vector<int>{2}, RemoveUnused(refs));
  ASSERT_TRUE(RemoveUnused(refs).empty());

  auto ref1c = refs.add(1);
  ASSERT_EQ(2, ref1c.use_count());
  ASSERT_TRUE(RemoveUnused(refs).empty());
}

TEST_F(RefCounterTests, test_ref_counter_waits_only_for_keys_being_removed) {
  irs::RefCounter<int> refs;
  refs.add(1);
  absl::Notification removing;
  absl::Notification proceed;
  absl::Notification other_added;
  absl::Notification removed_added;
  std::vector<std::jthread> threads;
  absl::Cleanup release = [&] { proceed.Notify(); };
  threads.emplace_back([&] {
    refs.RemoveUnused([&](std::vector<int>& unused) {
      removing.Notify();
      proceed.WaitForNotification();
      return unused.size();
    });
  });
  removing.WaitForNotification();

  threads.emplace_back([&] {
    refs.add(2);
    other_added.Notify();
  });
  EXPECT_TRUE(other_added.WaitForNotificationWithTimeout(absl::Seconds(30)));

  threads.emplace_back([&] {
    refs.add(1);
    removed_added.Notify();
  });
  EXPECT_FALSE(
    removed_added.WaitForNotificationWithTimeout(absl::Milliseconds(50)));
  std::move(release).Invoke();
  EXPECT_TRUE(removed_added.WaitForNotificationWithTimeout(absl::Seconds(30)));
}

TEST_F(RefCounterTests, test_ref_counter_keeps_what_was_not_removed) {
  irs::RefCounter<int> refs;
  refs.add(1);
  refs.add(2);

  refs.RemoveUnused([](std::vector<int>& unused) {
    std::erase(unused, 2);
    return 0;
  });
  ASSERT_EQ(std::vector<int>{2}, RemoveUnused(refs));
  ASSERT_TRUE(RemoveUnused(refs).empty());
}

TEST_F(RefCounterTests, test_ref_counter_remove) {
  irs::RefCounter<int> refs;

  auto ref = refs.add(1);
  auto* value = ref.get();
  ASSERT_TRUE(refs.remove(1));
  ASSERT_FALSE(refs.remove(1));
  ASSERT_EQ(1, ref.use_count());

  auto readded = refs.add(1);
  ASSERT_NE(value, readded.get());
  ASSERT_EQ(1, *readded);
}
