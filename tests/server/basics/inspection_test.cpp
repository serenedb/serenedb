////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2022 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <vpack/builder.h>
#include <vpack/literal.h>
#include <vpack/value.h>
#include <vpack/value_type.h>

#include <iostream>
#include <list>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <variant>
#include <vector>

#include "basics/logger/logger.h"
#include "inspection_test_helper.h"
#include "vpack/serializer.h"

namespace {
using namespace sdb;

struct VPackInspectionTest : public ::testing::Test {};

TEST_F(VPackInspectionTest, serialize) {
  vpack::Builder builder;
  const Dummy d{.i = 42, .d = 123.456, .b = true, .s = "foobar"};
  vpack::WriteObject(builder, d);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(d.i, slice.get("i").getInt());
  EXPECT_EQ(d.d, slice.get("d").getDouble());
  EXPECT_EQ(d.b, slice.get("b").getBool());
  EXPECT_EQ(d.s, slice.get("s").copyString());
}

TEST_F(VPackInspectionTest, deserialize) {
  vpack::Builder builder;
  builder.openObject();
  builder.add("i", 42);
  builder.add("d", 123.456);
  builder.add("b", true);
  builder.add("s", "foobar");
  builder.close();

  Dummy d;
  vpack::ReadObject(builder.slice(), d);
  EXPECT_EQ(42, d.i);
  EXPECT_EQ(123.456, d.d);
  EXPECT_EQ(true, d.b);
  EXPECT_EQ("foobar", d.s);
}

TEST_F(VPackInspectionTest, deserialize_throws) {
  vpack::Builder builder;
  builder.openObject();
  builder.close();

  EXPECT_THROW(
    {
      try {
        Dummy d;
        vpack::ReadObject(builder.slice(), d);
      } catch (sdb::basics::Exception& e) {
        EXPECT_TRUE(std::string(e.what()).starts_with(
          "Incompatible format, object should have at least"))
          << "Actual error message: " << e.what();
        throw;
      }
    },
    sdb::basics::Exception);
}

struct IncludesVPackBuilder {
  vpack::Builder builder{};
};

TEST_F(VPackInspectionTest, StructIncludingVPackBuilder) {
  vpack::Builder builder;
  builder.openObject();
  builder.add("key", "value");
  builder.close();
  const auto my_struct = IncludesVPackBuilder{.builder = builder};

  {
    vpack::Builder serialized_my_struct;
    vpack::WriteObject(serialized_my_struct, my_struct);

    auto slice = serialized_my_struct.slice();
    ASSERT_TRUE(slice.isObject());
    EXPECT_EQ("value", slice.get("builder").get("key").copyString());
  }

  {
    vpack::Builder serialized_my_struct;
    serialized_my_struct.openObject();
    serialized_my_struct.add("builder");
    serialized_my_struct.openObject();
    serialized_my_struct.add("key", "value");
    serialized_my_struct.close();
    serialized_my_struct.close();

    IncludesVPackBuilder deserialized_my_struct;
    vpack::ReadObject(serialized_my_struct.slice(), deserialized_my_struct);

    ASSERT_TRUE(deserialized_my_struct.builder.slice().binaryEquals(
      my_struct.builder.slice()));
  }
}

}  // namespace

using namespace vpack;

namespace {
struct ErrorTTest {
  std::string s;
  size_t id;
  bool operator==(const ErrorTTest&) const = default;
};

}  // namespace

TEST(VPackWithStatus, statust_test_deserialize) {
  auto test_slice = R"({
    "s": "ReturnNode",
    "id": 3
  })"_vpack;

  ErrorTTest res;
  auto r = vpack::ReadObjectNothrow(test_slice, res);

  ASSERT_TRUE(r.ok());

  EXPECT_EQ(res.s, "ReturnNode");
  EXPECT_EQ(res.id, 3u);
}

TEST(VPackWithStatus, statust_test_deserialize_fail) {
  auto test_slice = R"({
    "s": "ReturnNode",
    "id": 3,
    "fehler": 2
  })"_vpack;

  ErrorTTest res;
  auto r = vpack::ReadObjectNothrow(test_slice, res, {.skip_unknown = false});

  ASSERT_FALSE(r.ok());
  EXPECT_EQ(r.errorMessage(), "Found unexpected attribute 'fehler'");
}
