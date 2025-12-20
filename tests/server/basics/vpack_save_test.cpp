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
#include <vpack/parser.h>

#include "basics/errors.h"
#include "inspection_test_helper.h"
#include "vpack/serializer.h"
#include "vpack/vpack_helper.h"

namespace {
using namespace sdb;

struct VPackSaveInspectorTest : public ::testing::Test {
  vpack::Builder builder;
};

TEST_F(VPackSaveInspectorTest, store_empty_object) {
  auto empty = AnEmptyObject{};
  vpack::WriteObject(builder, empty);
  EXPECT_TRUE(builder.slice().isNull());
}

TEST_F(VPackSaveInspectorTest, store_int) {
  int x = 42;
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x, builder.slice().getInt());
}

TEST_F(VPackSaveInspectorTest, store_double) {
  double x = 123.456;
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x, builder.slice().getDouble());
}

TEST_F(VPackSaveInspectorTest, store_bool) {
  bool x = true;
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x, builder.slice().getBool());
}

TEST_F(VPackSaveInspectorTest, store_string) {
  std::string x = "foobar";
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x, builder.slice().copyString());
}

TEST_F(VPackSaveInspectorTest, store_object) {
  static_assert(vpack::kIsWritable<Dummy, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Dummy, vpack::ObjectFormat>);

  const Dummy f{.i = 42, .d = 123.456, .b = true, .s = "foobar"};
  vpack::WriteObject(builder, f);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(f.i, slice.get("i").getInt());
  EXPECT_EQ(f.d, slice.get("d").getDouble());
  EXPECT_EQ(f.b, slice.get("b").getBool());
  EXPECT_EQ(f.s, slice.get("s").copyString());
}

TEST_F(VPackSaveInspectorTest, store_nested_object) {
  static_assert(vpack::kIsWritable<Nested, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Nested, vpack::ObjectFormat>);

  Nested b{.dummy = {.i = 42, .d = 123.456, .b = true, .s = "foobar"}};
  vpack::Builder builder;
  vpack::WriteObject(builder, b);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  auto d = slice.get("dummy");
  ASSERT_TRUE(d.isObject());
  EXPECT_EQ(b.dummy.i, d.get("i").getInt());
  EXPECT_EQ(b.dummy.d, d.get("d").getDouble());
  EXPECT_EQ(b.dummy.b, d.get("b").getBool());
  EXPECT_EQ(b.dummy.s, d.get("s").copyString());
}

TEST_F(VPackSaveInspectorTest, store_nested_object_without_nesting) {
  static_assert(vpack::kIsWritable<Container, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Container, vpack::ObjectFormat>);

  TypedInt c{.value = 4};
  vpack::Builder builder;
  vpack::WriteObject(builder, c);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(c.value, slice.get("value").getInt());
}

TEST_F(VPackSaveInspectorTest, store_list) {
  static_assert(vpack::kIsWritable<List, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<List, vpack::ObjectFormat>);

  List l{.vec = {{1}, {2}, {3}}, .list = {4, 5}};
  vpack::WriteObject(builder, l);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  auto list = slice.get("vec");
  ASSERT_TRUE(list.isArray());
  ASSERT_EQ(3u, list.length());
  EXPECT_EQ(l.vec.at(0).value, list.at(0).get("value").getInt());
  EXPECT_EQ(l.vec.at(1).value, list.at(1).get("value").getInt());
  EXPECT_EQ(l.vec.at(2).value, list.at(2).get("value").getInt());

  list = slice.get("list");
  ASSERT_TRUE(list.isArray());
  ASSERT_EQ(2u, list.length());
  auto it = l.list.begin();
  EXPECT_EQ(*it++, list.at(0).getInt());
  EXPECT_EQ(*it++, list.at(1).getInt());
}

TEST_F(VPackSaveInspectorTest, store_map) {
  static_assert(vpack::kIsWritable<Map, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Map, vpack::ObjectFormat>);

  Map m{.map = {{"1", {1}}, {"2", {2}}, {"3", {3}}},
        .unordered = {{"4", 4}, {"5", 5}}};
  vpack::WriteObject(builder, m);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  auto obj = slice.get("map");
  ASSERT_TRUE(obj.isObject());
  ASSERT_EQ(3u, obj.length());
  EXPECT_EQ(m.map["1"].value, obj.get("1").get("value").getInt());
  EXPECT_EQ(m.map["2"].value, obj.get("2").get("value").getInt());
  EXPECT_EQ(m.map["3"].value, obj.get("3").get("value").getInt());

  obj = slice.get("unordered");
  ASSERT_TRUE(obj.isObject());
  ASSERT_EQ(2u, obj.length());
  EXPECT_EQ(m.unordered["4"], obj.get("4").getInt());
  EXPECT_EQ(m.unordered["5"], obj.get("5").getInt());
}

TEST_F(VPackSaveInspectorTest, store_transformed_map) {
  TransformedMap m{.map = {{1, {1}}, {2, {2}}, {3, {3}}}};
  vpack::WriteObject(builder, m);

  auto expected = vpack::Parser::fromJson(R"({
    "map" : {
      "1" : { "value" : 1 },
      "2" : { "value" : 2 },
      "3" : { "value" : 3 }
    }
  })");

  vpack::Slice slice = builder.slice();
  EXPECT_TRUE(sdb::basics::VPackHelper::equals(expected->slice(), slice));
}

TEST_F(VPackSaveInspectorTest, store_set) {
  static_assert(vpack::kIsWritable<Set, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Set, vpack::ObjectFormat>);

  Set s{.set = {{1}, {2}, {3}}, .unordered = {4, 5}};
  vpack::WriteObject(builder, s);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  auto list = slice.get("set");
  ASSERT_TRUE(list.isArray());
  ASSERT_EQ(3u, list.length());

  auto s2 = std::set<int>();
  s2.insert(static_cast<int>(list.at(0).get("value").getInt()));
  s2.insert(static_cast<int>(list.at(1).get("value").getInt()));
  s2.insert(static_cast<int>(list.at(2).get("value").getInt()));

  ASSERT_EQ(s2, std::set<int>({1, 2, 3}));

  list = slice.get("unordered");
  ASSERT_TRUE(list.isArray());
  ASSERT_EQ(2u, list.length());

  auto us = std::unordered_set<int>();
  us.insert(static_cast<int>(list.at(0).getInt()));
  us.insert(static_cast<int>(list.at(1).getInt()));
  ASSERT_EQ(us, s.unordered);
}

TEST_F(VPackSaveInspectorTest, store_tuples) {
  static_assert(vpack::kIsWritable<Tuple, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Tuple, vpack::ObjectFormat>);

  Tuple t{.tuple = {"foo", 42, 12.34},
          .pair = {987, "bar"},
          .array1 = {"a", "b"},
          .array2 = {1, 2, 3}};
  vpack::WriteObject(builder, t);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  auto list = slice.get("tuple");
  ASSERT_EQ(3u, list.length());
  EXPECT_EQ(std::get<0>(t.tuple), list.at(0).copyString());
  EXPECT_EQ(std::get<1>(t.tuple), list.at(1).getInt());
  EXPECT_EQ(std::get<2>(t.tuple), list.at(2).getDouble());

  list = slice.get("pair");
  ASSERT_EQ(2u, list.length());
  EXPECT_EQ(std::get<0>(t.pair), list.at(0).getInt());
  EXPECT_EQ(std::get<1>(t.pair), list.at(1).copyString());

  list = slice.get("array1");
  ASSERT_EQ(2u, list.length());
  EXPECT_EQ(t.array1[0], list.at(0).copyString());
  EXPECT_EQ(t.array1[1], list.at(1).copyString());

  list = slice.get("array2");
  ASSERT_EQ(3u, list.length());
  EXPECT_EQ(t.array2[0], list.at(0).getInt());
  EXPECT_EQ(t.array2[1], list.at(1).getInt());
  EXPECT_EQ(t.array2[2], list.at(2).getInt());
}

TEST_F(VPackSaveInspectorTest, store_optional) {
  static_assert(vpack::kIsWritable<Optional, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Optional, vpack::ObjectFormat>);

  Optional o{.a = std::nullopt,
             .b = std::nullopt,
             .x = std::nullopt,
             .y = "blubb",
             .vec = {1, std::nullopt, 3},
             .map = {{"1", 1}, {"2", std::nullopt}, {"3", 3}}};
  vpack::WriteObject(builder, o);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(3u, slice.length());
  EXPECT_EQ("blubb", slice.get("y").copyString());

  auto vec = slice.get("vec");
  ASSERT_TRUE(vec.isArray());
  ASSERT_EQ(3u, vec.length());
  EXPECT_EQ(1, vec.at(0).getInt());
  EXPECT_TRUE(vec.at(1).isNull());
  EXPECT_EQ(3, vec.at(2).getInt());

  auto map = slice.get("map");
  ASSERT_TRUE(map.isObject());
  ASSERT_EQ(3u, map.length());
  EXPECT_EQ(1, map.get("1").getInt());
  EXPECT_TRUE(map.get("2").isNull());
  EXPECT_EQ(3, map.get("3").getInt());
}

TEST_F(VPackSaveInspectorTest, store_optional_pointer) {
  static_assert(vpack::kIsWritable<Pointer, vpack::ObjectFormat>);
  static_assert(vpack::kIsReadable<Pointer, vpack::ObjectFormat>);

  Pointer p{.a = nullptr,
            .b = std::make_shared<int>(42),
            .c = nullptr,
            .d = std::make_unique<TypedInt>(43),
            .vec = {},
            .x = {},
            .y = {}};
  p.vec.push_back(std::make_unique<int>(1));
  p.vec.push_back(nullptr);
  p.vec.push_back(std::make_unique<int>(2));
  vpack::WriteObject(builder, p);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(3u, slice.length()) << slice.toString();
  EXPECT_EQ(42, slice.get("b").getInt());
  EXPECT_EQ(43, slice.get("d").get("value").getInt());
  auto vec = slice.get("vec");
  EXPECT_TRUE(vec.isArray());
  EXPECT_EQ(3u, vec.length());
  EXPECT_EQ(1, vec.at(0).getInt());
  EXPECT_TRUE(vec.at(1).isNull());
  EXPECT_EQ(2, vec.at(2).getInt());
}

TEST_F(VPackSaveInspectorTest, store_non_default_constructible_type_vec) {
  auto vec = std::vector<NonDefaultConstructibleIntLike>{
    NonDefaultConstructibleIntLike{42}};
  vpack::WriteObject(builder, vec);
  ASSERT_TRUE(builder.slice().isArray());
  EXPECT_EQ(vec.at(0).value, builder.slice().at(0).getInt());
}

TEST_F(VPackSaveInspectorTest, store_non_default_constructible_type_map) {
  auto vec = std::map<std::string, NonDefaultConstructibleIntLike>{
    {"foo", NonDefaultConstructibleIntLike{42}}};
  vpack::WriteObject(builder, vec);
  ASSERT_TRUE(builder.slice().isObject());
  EXPECT_EQ(vec.at("foo").value, builder.slice().get("foo").getInt());
}

TEST_F(VPackSaveInspectorTest, store_non_default_constructible_type_optional) {
  auto x = std::optional<NonDefaultConstructibleIntLike>{
    NonDefaultConstructibleIntLike{42}};
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x.value().value, builder.slice().getInt());
}

TEST_F(VPackSaveInspectorTest,
       store_non_default_constructible_type_unique_ptr) {
  auto x = std::make_unique<NonDefaultConstructibleIntLike>(
    NonDefaultConstructibleIntLike{42});
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x->value, builder.slice().getInt());
}

TEST_F(VPackSaveInspectorTest,
       store_non_default_constructible_type_shared_ptr) {
  auto x = std::make_shared<NonDefaultConstructibleIntLike>(
    NonDefaultConstructibleIntLike{42});
  vpack::WriteObject(builder, x);
  EXPECT_EQ(x->value, builder.slice().getInt());
}

TEST_F(VPackSaveInspectorTest, store_object_with_optional_field_transform) {
  OptionalFieldTransform f{.x = 1, .y = std::nullopt, .z = 3};
  vpack::WriteObject(builder, f);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(2u, slice.length());
  EXPECT_EQ(1, slice.get("x").getInt());
  EXPECT_EQ(3, slice.get("z").getInt());
}

TEST_F(VPackSaveInspectorTest, store_type_with_custom_specialization) {
  Specialization s{.i = 42, .s = "foobar"};
  vpack::WriteObject(builder, s);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(s.i, slice.get("i").getInt());
  EXPECT_EQ(s.s, slice.get("s").copyString());
}

TEST_F(VPackSaveInspectorTest, store_type_with_explicitly_ignored_fields) {
  ExplicitIgnore e{.s = "foobar"};
  vpack::WriteObject(builder, e);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ(1u, slice.length());
}

TEST_F(VPackSaveInspectorTest, store_type_with_unsafe_fields) {
  vpack::Builder local_builder;
  local_builder.add("blubb");
  Unsafe u{
    .view = "foobar",
    .slice = local_builder.slice(),
  };
  vpack::WriteObject(builder, u);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isObject());
  EXPECT_EQ("foobar", slice.get("view").copyString());
  EXPECT_EQ("blubb", slice.get("slice").copyString());
}

TEST_F(VPackSaveInspectorTest, store_string_enum) {
  std::vector<MyStringEnum> enums{MyStringEnum::Value1, MyStringEnum::Value2,
                                  MyStringEnum::Value3};
  vpack::WriteObject(builder, enums);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isArray());
  ASSERT_EQ(3u, slice.length());
  EXPECT_EQ("value1", slice.at(0).copyString());
  EXPECT_EQ("value2", slice.at(1).copyString());
  EXPECT_EQ("value2", slice.at(2).copyString());
}

TEST_F(VPackSaveInspectorTest, store_int_enum) {
  std::vector<MyIntEnum> enums{MyIntEnum::Value1, MyIntEnum::Value2,
                               MyIntEnum::Value3};
  vpack::WriteObject(builder, enums);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isArray());
  ASSERT_EQ(3u, slice.length());
  EXPECT_EQ("value1", slice.at(0).stringView());
  EXPECT_EQ("value2", slice.at(1).stringView());
  EXPECT_EQ("value2", slice.at(2).stringView());
}

TEST_F(VPackSaveInspectorTest, store_mixed_enum) {
  std::vector<MyMixedEnum> enums{MyMixedEnum::Value1, MyMixedEnum::Value2};
  vpack::WriteObject(builder, enums);

  vpack::Slice slice = builder.slice();
  ASSERT_TRUE(slice.isArray());
  ASSERT_EQ(2u, slice.length());
  EXPECT_EQ("value1", slice.at(0).copyString());
  EXPECT_EQ("value2", slice.at(1).copyString());
}

TEST_F(VPackSaveInspectorTest,
       store_string_enum_returns_error_for_unknown_value) {
  vpack::WriteObject(builder, 42);

  MyStringEnum val;
  auto r = vpack::ReadObjectNothrow(builder.slice(), val);
  EXPECT_FALSE(r.ok());
  EXPECT_EQ(sdb::ERROR_BAD_PARAMETER, r.errorNumber());
}

TEST(VPackSaveInspectorContext, serialize_with_context) {
  struct Context {
    int default_int;
    int min_int;
    std::string default_string;
  };

  Context ctxt{};
  vpack::Builder builder;

  WithContext data{.i = 42, .s = "foobar"};
  vpack::WriteObject(builder, data, ctxt);
  EXPECT_EQ(42, builder.slice().get("i").getInt());
  EXPECT_EQ("foobar", builder.slice().get("s").copyString());
}

}  // namespace
