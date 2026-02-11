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

#include <gtest/gtest.h>
#include <vpack/builder.h>
#include <vpack/string.h>

#include "inspection_test_helper.h"
#include "vpack/serializer.h"

namespace {
using namespace sdb;

struct VPackLoadInspectorTest : public ::testing::Test {
  vpack::Builder builder;
};

TEST_F(VPackLoadInspectorTest, load_empty_object) {
  builder.openObject();
  builder.close();

  auto d = AnEmptyObject{};
  auto result = vpack::ReadObjectNothrow(builder.slice(), d);
  ASSERT_TRUE(result.ok());
}

TEST_F(VPackLoadInspectorTest, load_int) {
  builder.add(42);

  int x = 0;
  auto result = vpack::ReadObjectNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(42, x);
}

TEST_F(VPackLoadInspectorTest, load_double) {
  builder.add(123.456);

  double x = 0;
  auto result = vpack::ReadObjectNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(123.456, x);
}

TEST_F(VPackLoadInspectorTest, load_bool) {
  builder.add(true);

  bool x = false;
  auto result = vpack::ReadObjectNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(true, x);
}

TEST_F(VPackLoadInspectorTest, load_string) {
  builder.add("foobar");

  std::string x;
  auto result = vpack::ReadObjectNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ("foobar", x);
}

TEST_F(VPackLoadInspectorTest, load_object) {
  builder.openObject();
  builder.add("i", 42);
  builder.add("d", 123.456);
  builder.add("b", true);
  builder.add("s", "foobar");
  builder.close();

  Dummy d;
  auto result = vpack::ReadObjectNothrow(builder.slice(), d);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(42, d.i);
  EXPECT_EQ(123.456, d.d);
  EXPECT_EQ(true, d.b);
  EXPECT_EQ("foobar", d.s);
}

TEST_F(VPackLoadInspectorTest, load_nested_object) {
  builder.openObject();
  builder.add("dummy");
  builder.openObject();
  builder.add("i", 42);
  builder.add("d", 123);
  builder.add("b", true);
  builder.add("s", "foobar");
  builder.close();
  builder.close();

  Nested n;
  auto result = vpack::ReadObjectNothrow(builder.slice(), n);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(42, n.dummy.i);
  EXPECT_EQ(123, n.dummy.d);
  EXPECT_EQ(true, n.dummy.b);
  EXPECT_EQ("foobar", n.dummy.s);
}

TEST_F(VPackLoadInspectorTest, load_nested_object_without_nesting) {
  builder.openObject();
  builder.add("value", 42);
  builder.close();

  TypedInt c;
  auto result = vpack::ReadObjectNothrow(builder.slice(), c);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(42, c.value);
}

TEST_F(VPackLoadInspectorTest, load_list) {
  builder.openObject();
  builder.add("vec");
  builder.openArray();
  builder.openObject();
  builder.add("value", 1);
  builder.close();
  builder.openObject();
  builder.add("value", 2);
  builder.close();
  builder.openObject();
  builder.add("value", 3);
  builder.close();
  builder.close();
  builder.add("list");
  builder.openArray();
  builder.add(4);
  builder.add(5);
  builder.close();
  builder.close();

  List l;
  auto result = vpack::ReadObjectNothrow(builder.slice(), l);
  ASSERT_TRUE(result.ok());

  EXPECT_EQ(3u, l.vec.size());
  EXPECT_EQ(1, l.vec[0].value);
  EXPECT_EQ(2, l.vec[1].value);
  EXPECT_EQ(3, l.vec[2].value);
  EXPECT_EQ((std::list<int>{4, 5}), l.list);
}

TEST_F(VPackLoadInspectorTest, load_map) {
  builder.openObject();
  builder.add("map");
  builder.openObject();
  builder.add("1");
  builder.openObject();
  builder.add("value", 1);
  builder.close();
  builder.add("2");
  builder.openObject();
  builder.add("value", 2);
  builder.close();
  builder.add("3");
  builder.openObject();
  builder.add("value", 3);
  builder.close();
  builder.close();
  builder.add("unordered");
  builder.openObject();
  builder.add("4", 4);
  builder.add("5", 5);
  builder.close();
  builder.close();

  Map m;
  auto result = vpack::ReadObjectNothrow(builder.slice(), m);
  ASSERT_TRUE(result.ok());

  EXPECT_EQ(
    (std::map<std::string, TypedInt>{{"1", {1}}, {"2", {2}}, {"3", {3}}}),
    m.map);
  EXPECT_EQ((std::unordered_map<std::string, int>{{"4", 4}, {"5", 5}}),
            m.unordered);
}

TEST_F(VPackLoadInspectorTest, load_transformed_map) {
  auto add = [&](auto k, auto v) {
    builder.openArray();
    builder.add(k);
    builder.openArray();
    builder.add(v);
    builder.close();
    builder.close();
  };

  builder.openArray();
  builder.openArray();
  add(1, 1);
  add(2, 2);
  add(3, 3);
  builder.close();
  builder.close();

  TransformedMap m;
  auto result = vpack::ReadTupleNothrow(builder.slice(), m);
  ASSERT_TRUE(result.ok()) << result.errorMessage();

  EXPECT_EQ((std::map<int, TypedInt>{{1, {1}}, {2, {2}}, {3, {3}}}), m.map);
}

TEST_F(VPackLoadInspectorTest, load_set) {
  builder.openObject();
  builder.add("set");
  builder.openArray();
  builder.openObject();
  builder.add("value", 1);
  builder.close();
  builder.openObject();
  builder.add("value", 2);
  builder.close();
  builder.openObject();
  builder.add("value", 3);
  builder.close();
  builder.close();
  builder.add("unordered");
  builder.openArray();
  builder.add(4);
  builder.add(5);
  builder.close();
  builder.close();

  Set s;
  auto result = vpack::ReadObjectNothrow(builder.slice(), s);
  ASSERT_TRUE(result.ok());

  EXPECT_EQ((std::set<TypedInt>{{1}, {2}, {3}}), s.set);
  EXPECT_EQ((std::unordered_set<int>{4, 5}), s.unordered);
}

TEST_F(VPackLoadInspectorTest, load_tuples) {
  builder.openObject();

  builder.add("tuple");
  builder.openArray();
  builder.add("foo");
  builder.add(42);
  builder.add(12.34);
  builder.close();

  builder.add("pair");
  builder.openArray();
  builder.add(987);
  builder.add("bar");
  builder.close();

  builder.add("array1");
  builder.openArray();
  builder.add("a");
  builder.add("b");
  builder.close();

  builder.add("array2");
  builder.openArray();
  builder.add(1);
  builder.add(2);
  builder.add(3);
  builder.close();

  builder.close();

  Tuple t;
  auto result = vpack::ReadObjectNothrow(builder.slice(), t);
  ASSERT_TRUE(result.ok()) << result.errorMessage();

  Tuple expected{.tuple = {"foo", 42, 12.34},
                 .pair = {987, "bar"},
                 .array1 = {"a", "b"},
                 .array2 = {1, 2, 3}};
  EXPECT_EQ(expected.tuple, t.tuple);
  EXPECT_EQ(expected.pair, t.pair);
  EXPECT_EQ(expected.array1[0], t.array1[0]);
  EXPECT_EQ(expected.array1[1], t.array1[1]);
  EXPECT_EQ(expected.array2, t.array2);
}

TEST_F(VPackLoadInspectorTest, load_slice) {
  {
    builder.openObject();
    builder.add("dummy");
    builder.openObject();
    builder.add("i", 42);
    builder.add("b", true);
    builder.add("s", "foobar");
    builder.close();
    builder.close();

    vpack::String slice;
    auto result = vpack::ReadObjectNothrow(builder.slice(), slice);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(slice.isObject());
    slice = slice.get("dummy");
    ASSERT_TRUE(slice.isObject());
    EXPECT_EQ(42, slice.get("i").getInt());
    EXPECT_EQ(true, slice.get("b").getBool());
    EXPECT_EQ("foobar", slice.get("s").stringView());
  }

  {
    builder.clear();
    builder.add("foobar");

    vpack::String slice;
    auto result = vpack::ReadTupleNothrow(builder.slice(), slice);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("foobar", slice.stringView());
  }

  {
    builder.clear();
    builder.add("foobar");

    vpack::Slice slice;
    auto result = vpack::ReadTupleNothrow(builder.slice(), slice);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("foobar", slice.stringView());
  }
}

TEST_F(VPackLoadInspectorTest, load_builder) {
  {
    builder.openObject();
    builder.add("dummy");
    builder.openObject();
    builder.add("i", 42);
    builder.add("b", true);
    builder.add("s", "foobar");
    builder.close();
    builder.close();

    vpack::Builder new_builder;
    auto result = vpack::ReadTupleNothrow(builder.slice(), new_builder);
    ASSERT_TRUE(result.ok());
    auto slice = new_builder.slice();
    ASSERT_TRUE(slice.isObject());
    slice = slice.get("dummy");
    ASSERT_TRUE(slice.isObject());
    EXPECT_EQ(42, slice.get("i").getInt());
    EXPECT_EQ(true, slice.get("b").getBool());
    EXPECT_EQ("foobar", slice.get("s").stringView());
  }

  {
    builder.clear();
    builder.add("foobar");

    vpack::Builder new_builder;
    auto result = vpack::ReadObjectNothrow(builder.slice(), new_builder);
    auto slice = builder.slice();
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("foobar", slice.stringView());
  }
}

TEST_F(VPackLoadInspectorTest, load_optional) {
  builder.openObject();
  builder.add("y", "blubb");

  builder.add("vec");
  builder.openArray();
  builder.add(1);
  builder.add(vpack::Value{vpack::ValueType::Null});
  builder.add(3);
  builder.close();

  builder.add("map");
  builder.openObject();
  builder.add("1", 1);
  builder.add("2", vpack::Value{vpack::ValueType::Null});
  builder.add("3", 3);
  builder.close();

  builder.add("a", vpack::Value{vpack::ValueType::Null});
  builder.close();

  Optional o{.a = 1, .b = 2, .x = 42, .y = {}, .vec = {}, .map = {}};
  auto result = vpack::ReadObjectNothrow(builder.slice(), o);
  ASSERT_TRUE(result.ok());

  Optional expected{.a = std::nullopt,
                    .b = 2,
                    .x = 42,
                    .y = "blubb",
                    .vec = {1, std::nullopt, 3},
                    .map = {{"1", 1}, {"2", std::nullopt}, {"3", 3}}};
  EXPECT_EQ(expected.a, o.a);
  EXPECT_EQ(expected.b, o.b);
  EXPECT_EQ(expected.x, o.x);
  EXPECT_EQ(expected.y, o.y);
  ASSERT_EQ(expected.vec, o.vec);
  EXPECT_EQ(expected.map, o.map);
}

TEST_F(VPackLoadInspectorTest, load_non_default_constructible_type_vec) {
  builder.openArray();
  builder.add(42);
  builder.close();

  auto vec = std::vector<NonDefaultConstructibleIntLike>();
  auto result = vpack::ReadTupleNothrow(builder.slice(), vec);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(vec, decltype(vec){NonDefaultConstructibleIntLike{42}});
}

TEST_F(VPackLoadInspectorTest, load_non_default_constructible_type_map) {
  builder.openObject();
  builder.add("foo", 42);
  builder.close();

  auto map = std::map<std::string, NonDefaultConstructibleIntLike>();
  auto result = vpack::ReadObjectNothrow(builder.slice(), map);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(map, (decltype(map){{"foo", NonDefaultConstructibleIntLike{42}}}));
}

TEST_F(VPackLoadInspectorTest, load_non_default_constructible_type_optional) {
  builder.add(42);

  auto x = std::optional<NonDefaultConstructibleIntLike>();
  auto result = vpack::ReadTupleNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(x, (decltype(x){NonDefaultConstructibleIntLike{42}}));
}

TEST_F(VPackLoadInspectorTest, load_non_default_constructible_type_unique_ptr) {
  builder.add(42);

  auto x = std::unique_ptr<NonDefaultConstructibleIntLike>();
  auto result = vpack::ReadTupleNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(*x, NonDefaultConstructibleIntLike{42});
}

TEST_F(VPackLoadInspectorTest, load_non_default_constructible_type_shared_ptr) {
  builder.add(42);

  auto x = std::shared_ptr<NonDefaultConstructibleIntLike>();
  auto result = vpack::ReadTupleNothrow(builder.slice(), x);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(*x, NonDefaultConstructibleIntLike{42});
}

TEST_F(VPackLoadInspectorTest, load_optional_pointer) {
  builder.openObject();
  builder.add("vec");
  builder.openArray();
  builder.add(1);
  builder.add(vpack::Value(vpack::ValueType::Null));
  builder.add(2);
  builder.close();

  builder.add("a", vpack::Value(vpack::ValueType::Null));

  builder.add("b", 42);

  builder.add("d");
  builder.openObject();
  builder.add("value", 43);
  builder.close();

  builder.add("x", vpack::Value(vpack::ValueType::Null));

  builder.close();

  Pointer p{.a = std::make_shared<int>(0),
            .b = std::make_shared<int>(0),
            .c = std::make_unique<int>(0),
            .d = std::make_unique<TypedInt>(0),
            .vec = {},
            .x = std::make_shared<int>(0),
            .y = std::make_shared<int>(0)};
  auto result = vpack::ReadObjectNothrow(builder.slice(), p);
  ASSERT_TRUE(result.ok()) << result.errorMessage();

  EXPECT_EQ(nullptr, p.a);
  ASSERT_NE(nullptr, p.b);
  EXPECT_EQ(42, *p.b);
  EXPECT_EQ(0, *p.c);
  ASSERT_NE(nullptr, p.d);
  EXPECT_EQ(43, p.d->value);

  ASSERT_EQ(3u, p.vec.size());
  ASSERT_NE(nullptr, p.vec[0]);
  EXPECT_EQ(1, *p.vec[0]);
  EXPECT_EQ(nullptr, p.vec[1]);
  ASSERT_NE(nullptr, p.vec[2]);
  EXPECT_EQ(2, *p.vec[2]);

  EXPECT_EQ(nullptr, p.x);
  ASSERT_NE(nullptr, p.y);
  EXPECT_EQ(0, *p.y);
}

TEST_F(VPackLoadInspectorTest, error_expecting_int) {
  builder.add("foo");

  int i{};
  auto result = vpack::ReadTupleNothrow(builder.slice(), i);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), i);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_int16) {
  builder.add(123456789);

  int16_t i{};
  auto result = vpack::ReadTupleNothrow(builder.slice(), i);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), i);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_double) {
  builder.add("foo");

  double d{};
  auto result = vpack::ReadTupleNothrow(builder.slice(), d);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), d);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_bool) {
  builder.add(42);

  bool b{};
  auto result = vpack::ReadTupleNothrow(builder.slice(), b);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), b);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_string) {
  builder.add(42);

  std::string s;
  auto result = vpack::ReadTupleNothrow(builder.slice(), s);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), s);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_array) {
  builder.add(42);

  std::vector<int> v;
  auto result = vpack::ReadTupleNothrow(builder.slice(), v);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), v);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_object) {
  builder.add(42);

  Dummy d;
  auto result = vpack::ReadTupleNothrow(builder.slice(), d);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), d);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_tuple_array_too_short) {
  builder.openArray();
  builder.add("foo");
  builder.add(42);
  builder.close();

  std::tuple<std::string, int, double> t;
  auto result = vpack::ReadTupleNothrow(builder.slice(), t);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), t);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_tuple_array_too_large) {
  builder.openArray();
  builder.add("foo");
  builder.add(42);
  builder.add(123.456);
  builder.close();

  std::tuple<std::string, int> t;
  auto result = vpack::ReadTupleNothrow(builder.slice(), t);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), t);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_c_style_array_too_short) {
  builder.openArray();
  builder.add(1);
  builder.add(2);
  builder.close();

  std::array<int, 4> a;
  auto result = vpack::ReadTupleNothrow(builder.slice(), a);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), a);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_c_style_array_too_long) {
  builder.openArray();
  builder.add(1);
  builder.add(2);
  builder.add(3);
  builder.add(4);
  builder.close();

  std::array<int, 3> a;
  auto result = vpack::ReadTupleNothrow(builder.slice(), a);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), a);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_type_on_path) {
  builder.openObject();
  builder.add("dummy");
  builder.openObject();
  builder.add("i", "foo");
  builder.close();
  builder.close();

  Nested n;
  auto result = vpack::ReadTupleNothrow(builder.slice(), n);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), n);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_type_on_path_with_array) {
  builder.openObject();
  builder.add("vec");
  builder.openArray();
  builder.openObject();
  builder.add("i", 1);
  builder.close();
  builder.openObject();
  builder.add("i", 2);
  builder.close();
  builder.openObject();
  builder.add("i", "foobar");
  builder.close();
  builder.close();
  builder.close();

  List l;

  auto result = vpack::ReadTupleNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_type_on_path_with_map) {
  builder.openObject();
  builder.add("map");
  builder.openObject();
  builder.add("1");
  builder.openObject();
  builder.add("i", 1);
  builder.close();
  builder.add("2");
  builder.openObject();
  builder.add("i", 2);
  builder.close();
  builder.add("3");
  builder.openObject();
  builder.add("i", "foobar");
  builder.close();
  builder.close();
  builder.close();

  Map m;

  auto result = vpack::ReadTupleNothrow(builder.slice(), m);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), m);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_type_on_path_with_tuple) {
  builder.openObject();

  builder.add("tuple");
  builder.openArray();
  builder.add("foo");
  builder.add(42);
  builder.add("bar");
  builder.close();

  builder.close();

  Tuple l;
  auto result = vpack::ReadTupleNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest,
       error_expecting_type_on_path_with_c_style_array) {
  builder.openObject();

  builder.add("tuple");
  builder.openArray();
  builder.add("foo");
  builder.add(42);
  builder.add(0);
  builder.close();

  builder.add("pair");
  builder.openArray();
  builder.add(987);
  builder.add("bar");
  builder.close();

  builder.add("array1");
  builder.openArray();
  builder.add("a");
  builder.add(42);
  builder.close();

  builder.close();

  Tuple l;
  auto result = vpack::ReadTupleNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_expecting_type_on_path_with_std_array) {
  builder.openObject();

  builder.add("tuple");
  builder.openArray();
  builder.add("foo");
  builder.add(42);
  builder.add(0);
  builder.close();

  builder.add("pair");
  builder.openArray();
  builder.add(987);
  builder.add("bar");
  builder.close();

  builder.add("array1");
  builder.openArray();
  builder.add("a");
  builder.add("b");
  builder.close();

  builder.add("array2");
  builder.openArray();
  builder.add(1);
  builder.add(2);
  builder.add("foo");
  builder.close();

  builder.close();

  Tuple l;
  auto result = vpack::ReadTupleNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());

  result = vpack::ReadObjectNothrow(builder.slice(), l);
  ASSERT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, error_missing_field) {
  builder.openObject();
  builder.add("dummy");
  builder.openObject();
  builder.add("s", "foo");
  builder.close();
  builder.close();

  Nested n;
  auto result = vpack::ReadObjectNothrow(builder.slice(), n);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(
    "Failed to parse attribute 'dummy': Incompatible format, object should "
    "have at least 4 mandatory attributes",
    result.errorMessage());
}

TEST_F(VPackLoadInspectorTest, error_found_unexpected_attribute) {
  builder.openObject();
  builder.add("i");
  builder.openObject();
  builder.add("value", 42);
  builder.close();
  builder.add("should_not_be_here", 123);
  builder.close();

  Container c;
  auto result =
    vpack::ReadObjectNothrow(builder.slice(), c, {.skip_unknown = false});
  ASSERT_FALSE(result.ok());
  EXPECT_EQ("Found unexpected attribute 'should_not_be_here'",
            result.errorMessage());
}

TEST_F(VPackLoadInspectorTest, load_object_ignoring_unknown_attributes) {
  builder.openObject();
  builder.add("i");
  builder.openObject();
  builder.add("value", 42);
  builder.close();
  builder.add("ignore_me", 123);
  builder.close();

  Container c;
  auto result =
    vpack::ReadObjectNothrow(builder.slice(), c, {.skip_unknown = true});
  ASSERT_TRUE(result.ok());
}

TEST_F(VPackLoadInspectorTest, load_object_with_fallbacks) {
  builder.openObject();
  builder.close();

  Fallback f;
  Dummy expected = f.d;
  auto result = vpack::ReadObjectNothrow(builder.slice(), f, {.strict = false});
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(0, f.i);
  EXPECT_EQ("", f.s);
  EXPECT_EQ(expected, f.d);
  EXPECT_EQ(0, f.dynamic);
}

TEST_F(VPackLoadInspectorTest, load_object_with_fallback_reference) {
  builder.openObject();
  builder.add("x", 42);
  builder.close();

  FallbackReference f;
  auto result = vpack::ReadObjectNothrow(builder.slice(), f, {.strict = false});
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(42, f.x);
  EXPECT_EQ(0, f.y);
}

TEST_F(VPackLoadInspectorTest, load_object_ignoring_missing_fields) {
  builder.openObject();
  builder.close();

  FallbackReference f{.x = 1, .y = 2};
  auto result = vpack::ReadObjectNothrow(builder.slice(), f, {.strict = false});
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(1, f.x);
  EXPECT_EQ(2, f.y);
}

TEST_F(VPackLoadInspectorTest, load_object_with_invariant_fulfilled) {
  builder.openObject();
  builder.add("i", 42);
  builder.add("s", "foobar");
  builder.close();

  Invariant i;
  auto result = vpack::ReadObjectNothrow(builder.slice(), i);
  ASSERT_TRUE(result.ok()) << result.errorMessage();
  EXPECT_EQ(42, i.i);
  EXPECT_EQ("foobar", i.s);
}

TEST_F(VPackLoadInspectorTest, load_object_with_invariant_and_fallback) {
  builder.openObject();
  builder.close();

  InvariantAndFallback i;
  auto result = vpack::ReadObjectNothrow(builder.slice(), i, {.strict = false});
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(0, i.i);
  EXPECT_EQ("", i.s);
}

TEST_F(VPackLoadInspectorTest, load_type_with_custom_specialization) {
  builder.openObject();
  builder.add("i", 42);
  builder.add("s", "foobar");
  builder.close();

  Specialization s;
  auto result = vpack::ReadObjectNothrow(builder.slice(), s);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(42, s.i);
  EXPECT_EQ("foobar", s.s);
}

TEST_F(VPackLoadInspectorTest, load_type_with_explicitly_ignored_fields) {
  builder.openObject();
  builder.add("s", "foobar");
  builder.add("ignore", "something");
  builder.close();

  ExplicitIgnore e;
  auto result = vpack::ReadObjectNothrow(builder.slice(), e);
  ASSERT_TRUE(result.ok());
}

TEST_F(VPackLoadInspectorTest, load_type_with_unsafe_fields) {
  builder.openObject();
  builder.add("view", "foobar");
  builder.add("slice", "blubb");
  builder.close();

  Unsafe u;
  auto result = vpack::ReadObjectNothrow(builder.slice(), u);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(builder.slice().get("view").stringView(), u.view);
  EXPECT_EQ(builder.slice().get("view").stringView().data(), u.view.data());
  EXPECT_EQ(builder.slice().get("slice").start(), u.slice.start());
}

TEST_F(VPackLoadInspectorTest, load_string_enum) {
  builder.openArray();
  builder.add("value1");
  builder.add("value2");
  builder.close();

  std::vector<MyStringEnum> enums;
  auto result = vpack::ReadObjectNothrow(builder.slice(), enums);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(2u, enums.size());
  EXPECT_EQ(MyStringEnum::Value1, enums[0]);
  EXPECT_EQ(MyStringEnum::Value2, enums[1]);
}

TEST_F(VPackLoadInspectorTest, load_int_enum) {
  builder.openArray();
  builder.add(1);
  builder.add(2);
  builder.close();

  std::vector<MyIntEnum> enums;
  auto result = vpack::ReadTupleNothrow(builder.slice(), enums);
  ASSERT_TRUE(result.ok()) << result.errorMessage();
  ASSERT_EQ(2u, enums.size());
  EXPECT_EQ(MyIntEnum::Value1, enums[0]);
  EXPECT_EQ(MyIntEnum::Value2, enums[1]);
}

TEST_F(VPackLoadInspectorTest, load_mixed_enum) {
  builder.openArray();
  builder.add("value1");
  builder.add("value1");
  builder.add("value2");
  builder.add("value2");
  builder.close();

  std::vector<MyMixedEnum> enums;
  auto result = vpack::ReadObjectNothrow(builder.slice(), enums);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(4u, enums.size());
  EXPECT_EQ(MyMixedEnum::Value1, enums[0]);
  EXPECT_EQ(MyMixedEnum::Value1, enums[1]);
  EXPECT_EQ(MyMixedEnum::Value2, enums[2]);
  EXPECT_EQ(MyMixedEnum::Value2, enums[3]);
}

TEST_F(VPackLoadInspectorTest, load_string_enum_returns_error_when_not_string) {
  builder.add(42);

  MyStringEnum my_enum;
  auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
  EXPECT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest, load_int_enum_returns_error_when_not_int) {
  builder.add("foobar");

  MyIntEnum my_enum;
  auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
  EXPECT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest,
       load_mixed_enum_returns_error_when_not_string_or_int) {
  builder.add(false);

  MyMixedEnum my_enum;
  auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
  EXPECT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest,
       load_string_enum_returns_error_when_value_is_unknown) {
  builder.add("unknownValue");

  MyStringEnum my_enum;
  auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
  EXPECT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest,
       load_int_enum_returns_error_when_value_is_unknown) {
  builder.add(42);

  MyIntEnum my_enum;
  auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
  EXPECT_FALSE(result.ok());
}

TEST_F(VPackLoadInspectorTest,
       load_mixed_enum_returns_error_when_value_is_unknown) {
  {
    builder.add("unknownValue");

    MyMixedEnum my_enum;
    auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
    EXPECT_FALSE(result.ok());
  }
  {
    builder.clear();
    builder.add(42);

    MyMixedEnum my_enum;
    auto result = vpack::ReadObjectNothrow(builder.slice(), my_enum);
    EXPECT_FALSE(result.ok());
  }
}

TEST(VPackLoadInspectorContext, deserialize_with_context) {
  vpack::Builder builder;
  builder.openObject();
  builder.close();

  {
    Context ctxt{.default_int = 42, .min_int = 0, .default_string = "foobar"};
    WithContext data;
    auto result = vpack::ReadObjectNothrow(builder.slice(), data, {}, ctxt);
    EXPECT_EQ(42, data.i);
    EXPECT_EQ("foobar", data.s);
  }

  {
    Context ctxt{.default_int = -1, .min_int = -2, .default_string = "blubb"};
    WithContext data;
    auto result = vpack::ReadObjectNothrow(builder.slice(), data, {}, ctxt);
    EXPECT_EQ(-1, data.i);
    EXPECT_EQ("blubb", data.s);
  }
}

}  // namespace
