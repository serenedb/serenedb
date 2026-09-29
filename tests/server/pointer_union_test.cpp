////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include "server/utils/pointer_union.h"

using sdb::PointerUnion;

namespace {

struct A {
  int value = 1;
};

struct B {
  char value = 'b';
};

struct Unrelated;

using Union = PointerUnion<A, B>;

static_assert(Union::kHolds<A>);
static_assert(Union::kHolds<const B>);
static_assert(!Union::kHolds<Unrelated>);

TEST(PointerUnion, EmptyByDefault) {
  const Union u;
  EXPECT_TRUE(u.IsNull());
  EXPECT_FALSE(u.Is<A>());
  EXPECT_FALSE(u.Is<B>());
  EXPECT_EQ(u.Get<A>(), nullptr);
  EXPECT_EQ(u.Get<B>(), nullptr);
}

TEST(PointerUnion, GetsOnlyTheStoredType) {
  A a;
  B b;
  Union u;
  u.Set(&a);
  EXPECT_TRUE(u.Is<A>());
  EXPECT_EQ(u.Get<A>(), &a);
  EXPECT_EQ(u.Get<B>(), nullptr);

  u.Set(&b);
  EXPECT_TRUE(u.Is<B>());
  EXPECT_EQ(u.Get<B>(), &b);
  EXPECT_EQ(u.Get<A>(), nullptr);
  EXPECT_EQ(u.Get<B>()->value, 'b');
}

TEST(PointerUnion, ConstIsTheSameType) {
  const A a;
  Union u;
  u.Set(&a);
  EXPECT_EQ(u.Get<const A>(), &a);
  EXPECT_EQ(u.Get<const A>()->value, 1);
}

TEST(PointerUnion, NullAndResetClear) {
  A a;
  Union u;
  u.Set(&a);
  u.Set<A>(nullptr);
  EXPECT_TRUE(u.IsNull());
  EXPECT_EQ(u.Get<A>(), nullptr);

  u.Set(&a);
  u.Reset();
  EXPECT_TRUE(u.IsNull());
  EXPECT_FALSE(u.Is<A>());
}

TEST(PointerUnion, KeepsHeapAddressesIntact) {
  for (int i = 0; i < 1000; ++i) {
    auto a = std::make_unique<A>();
    auto b = std::make_unique<B>();
    Union u;
    u.Set(a.get());
    ASSERT_EQ(u.Get<A>(), a.get());
    u.Set(b.get());
    ASSERT_EQ(u.Get<B>(), b.get());
  }
}

}  // namespace
