////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <absl/container/flat_hash_set.h>

#include <initializer_list>
#include <iostream>
#include <ostream>
#include <string>

#include "tests-common.h"
#include "vpack/string.h"

using SliceTypes = testing::Types<vpack::Slice, vpack::String>;
template<class>
struct SliceTest : testing::Test {};
TYPED_TEST_SUITE(SliceTest, SliceTypes);

static unsigned char gLocalBuffer[4096];

TYPED_TEST(SliceTest, NoneFactory) {
  TypeParam s = Slice::noneSlice();
  ASSERT_TRUE(s.isNone());
}

TYPED_TEST(SliceTest, NullFactory) {
  TypeParam s = Slice::nullSlice();
  ASSERT_TRUE(s.isNull());
}

TYPED_TEST(SliceTest, ZeroFactory) {
  TypeParam s = Slice::zeroSlice();
  ASSERT_TRUE(s.isSmallInt());
  ASSERT_EQ(0UL, s.getUInt());
}

TYPED_TEST(SliceTest, FalseFactory) {
  TypeParam s = Slice::falseSlice();
  ASSERT_TRUE(s.isBool());
  ASSERT_FALSE(s.getBool());
}

TYPED_TEST(SliceTest, TrueFactory) {
  TypeParam s = Slice::trueSlice();
  ASSERT_TRUE(s.isBool());
  ASSERT_TRUE(s.getBool());
}

TYPED_TEST(SliceTest, EmptyArrayFactory) {
  TypeParam s = Slice::emptyArraySlice();
  ASSERT_TRUE(s.isArray() && s.length() == 0);
}

TYPED_TEST(SliceTest, EmptyObjectFactory) {
  TypeParam s = Slice::emptyObjectSlice();
  ASSERT_TRUE(s.isObject() && s.length() == 0);
}

TYPED_TEST(SliceTest, GetEmptyPath) {
  const std::string value(
    "{\"foo\":{\"bar\":{\"baz\":3,\"bark\":4},\"qux\":5},\"poo\":6}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  {
    std::vector<std::string> lookup;
    ASSERT_VPACK_EXCEPTION(s.get(lookup), Exception::kInvalidAttributePath);
    ASSERT_VPACK_EXCEPTION(s.get(lookup.begin(), lookup.end()),
                           Exception::kInvalidAttributePath);
  }

  {
    std::vector<std::string_view> lookup;
    ASSERT_VPACK_EXCEPTION(s.get(lookup), Exception::kInvalidAttributePath);
    ASSERT_VPACK_EXCEPTION(s.get(lookup.begin(), lookup.end()),
                           Exception::kInvalidAttributePath);
  }
}

TYPED_TEST(SliceTest, GetOnNonObject) {
  TypeParam s = Slice::nullSlice();

  {
    std::vector<std::string> lookup{"foo"};
    ASSERT_VPACK_EXCEPTION(s.get(lookup), Exception::kInvalidValueType);
    ASSERT_VPACK_EXCEPTION(s.get(lookup.begin(), lookup.end()),
                           Exception::kInvalidValueType);
  }

  {
    std::vector<std::string_view> lookup{std::string_view{"foo"}};
    ASSERT_VPACK_EXCEPTION(s.get(lookup), Exception::kInvalidValueType);
    ASSERT_VPACK_EXCEPTION(s.get(lookup.begin(), lookup.end()),
                           Exception::kInvalidValueType);
  }
}

TYPED_TEST(SliceTest, GetOnNestedObject) {
  const std::string value(
    "{\"foo\":{\"bar\":{\"baz\":3,\"bark\":4},\"qux\":5},\"poo\":6}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  auto run_test = [s](auto lookup) {
    lookup.emplace_back("foo");
    ASSERT_TRUE(s.get(lookup).isObject());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isObject());

    lookup.emplace_back("boo");  // foo.boo does not exist
    ASSERT_TRUE(s.get(lookup).isNone());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNone());
    lookup.pop_back();

    lookup.emplace_back("bar");  // foo.bar
    ASSERT_TRUE(s.get(lookup).isObject());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isObject());

    lookup.emplace_back("baz");  // foo.bar.baz
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());

    lookup.emplace_back("bat");  // foo.bar.baz.bat
    ASSERT_TRUE(s.get(lookup).isNone());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNone());
    lookup.pop_back();  // foo.bar.baz
    lookup.pop_back();  // foo.bar

    lookup.emplace_back("bark");  // foo.bar.bark
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(4, s.get(lookup).getInt());
    ASSERT_EQ(4, s.get(lookup.begin(), lookup.end()).getInt());

    lookup.emplace_back("bat");  // foo.bar.bark.bat
    ASSERT_TRUE(s.get(lookup).isNone());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNone());
    lookup.pop_back();  // foo.bar.bark
    lookup.pop_back();  // foo.bar

    lookup.emplace_back("borg");  // foo.bar.borg
    ASSERT_TRUE(s.get(lookup).isNone());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNone());
    lookup.pop_back();  // foo.bar
    lookup.pop_back();  // foo

    lookup.emplace_back("qux");  // foo.qux
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(5, s.get(lookup).getInt());
    ASSERT_EQ(5, s.get(lookup.begin(), lookup.end()).getInt());
    lookup.pop_back();  // foo

    lookup.emplace_back("poo");  // foo.poo
    ASSERT_TRUE(s.get(lookup).isNone());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNone());
    lookup.pop_back();  // foo
    lookup.pop_back();  // {}

    lookup.emplace_back("poo");  // poo
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(6, s.get(lookup).getInt());
    ASSERT_EQ(6, s.get(lookup.begin(), lookup.end()).getInt());
  };

  run_test(std::vector<std::string>());
  run_test(std::vector<std::string_view>());

  {
    auto lookup = {"foo", "bar", "baz"};
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());
  }

  {
    std::initializer_list<const char*> lookup = {"foo", "bar", "baz"};
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());
  }
}

TYPED_TEST(SliceTest, GetWithIterator) {
  const std::string value(
    "{\"foo\":{\"bar\":{\"baz\":3,\"bark\":4},\"qux\":5},\"poo\":6}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  {
    auto lookup = {"foo", "bar", "baz"};
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());
  }

  {
    std::initializer_list<const char*> lookup = {"foo", "bar", "baz"};
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());
  }

  {
    std::initializer_list<std::string> lookup = {"foo", "bar", "baz"};
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());
  }

  {
    std::initializer_list<std::string_view> lookup = {std::string_view("foo"),
                                                      std::string_view("bar"),
                                                      std::string_view("baz")};
    ASSERT_TRUE(s.get(lookup).isNumber());
    ASSERT_TRUE(s.get(lookup.begin(), lookup.end()).isNumber());
    ASSERT_EQ(3, s.get(lookup).getInt());
    ASSERT_EQ(3, s.get(lookup.begin(), lookup.end()).getInt());
  }
}

TYPED_TEST(SliceTest, SliceStart) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(0x18UL, s.head());
  ASSERT_EQ(0x18UL, *s.start());
  ASSERT_EQ('\x18', *s.template startAs<char>());
  ASSERT_EQ('\x18', *s.template startAs<unsigned char>());
  ASSERT_EQ(0x18UL, *s.template startAs<uint8_t>());

  ASSERT_EQ(s.start(), s.begin());
  ASSERT_EQ(s.start() + 1, s.end());
}

TYPED_TEST(SliceTest, ToJsonNull) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("null", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonFalse) {
  const std::string value("false");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("false", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonTrue) {
  const std::string value("true");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("true", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonNumber) {
  const std::string value("-12345");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("-12345", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonString) {
  const std::string value("\"foobarbaz\"");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("\"foobarbaz\"", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonArrayEmpty) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ(0x01, s.head());
  ASSERT_EQ("[]", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonArray) {
  const std::string value("[1,2,3,4,5]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("[1,2,3,4,5]", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonArrayCompact) {
  const std::string value("[1,2,3,4,5]");

  Options options;
  options.build_unindexed_arrays = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ(0x13, s.head());

  ASSERT_EQ("[1,2,3,4,5]", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonObjectEmpty) {
  const std::string value("{}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ(0x0a, s.head());

  ASSERT_EQ("{}", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonObject) {
  const std::string value("{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ(0x14, s.head());

  ASSERT_EQ("{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5}", s.toJson());
}

TYPED_TEST(SliceTest, ToJsonObjectCompact) {
  const std::string value("{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());
  ASSERT_EQ("{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5}", s.toJson());
}

TYPED_TEST(SliceTest, InvalidGetters) {
  const std::string value("[null,true,1,\"foo\",[],{}]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.at(0).getBool(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).getInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).getUInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).getDouble(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).copyString(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).stringView(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).length(), Exception::kInvalidValueType);

  ASSERT_VPACK_EXCEPTION(s.at(1).getInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).getUInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).getDouble(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).copyString(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).stringView(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).length(), Exception::kInvalidValueType);

  ASSERT_VPACK_EXCEPTION(s.at(2).getBool(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).copyString(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).stringView(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).length(), Exception::kInvalidValueType);

  ASSERT_VPACK_EXCEPTION(s.at(3).getBool(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).getInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).getUInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).getDouble(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).length(), Exception::kInvalidValueType);

  ASSERT_VPACK_EXCEPTION(s.at(4).getBool(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).getInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).getUInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).getDouble(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).copyString(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).stringView(), Exception::kInvalidValueType);

  ASSERT_VPACK_EXCEPTION(s.at(5).getBool(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(5).getInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(5).getUInt(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(5).getDouble(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(5).stringView(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(5).copyString(), Exception::kInvalidValueType);
}

TYPED_TEST(SliceTest, LengthNull) {
  const std::string value("null");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.length(), Exception::kInvalidValueType);
}

TYPED_TEST(SliceTest, LengthTrue) {
  const std::string value("true");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_VPACK_EXCEPTION(s.length(), Exception::kInvalidValueType);
}

TYPED_TEST(SliceTest, LengthArrayEmpty) {
  const std::string value("[]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(0UL, s.length());
}

TYPED_TEST(SliceTest, LengthArray) {
  const std::string value("[1,2,3,4,5,6,7,8,\"foo\",\"bar\"]");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(10UL, s.length());
}

TYPED_TEST(SliceTest, LengthArrayCompact) {
  const std::string value("[1,2,3,4,5,6,7,8,\"foo\",\"bar\"]");

  Options options;
  options.build_unindexed_arrays = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(0x13, s.head());
  ASSERT_EQ(10UL, s.length());
}

TYPED_TEST(SliceTest, LengthObjectEmpty) {
  const std::string value("{}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(0UL, s.length());
}

TYPED_TEST(SliceTest, LengthObject) {
  const std::string value(
    "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5,\"f\":6,\"g\":7,\"h\":8,\"i\":"
    "\"foo\",\"j\":\"bar\"}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(10UL, s.length());
}

TYPED_TEST(SliceTest, LengthObjectCompact) {
  const std::string value(
    "{\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"e\":5,\"f\":6,\"g\":7,\"h\":8,\"i\":"
    "\"foo\",\"j\":\"bar\"}");

  Options options;
  options.build_unindexed_objects = true;

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  TypeParam s(builder->start());

  ASSERT_EQ(0x14, s.head());
  ASSERT_EQ(10UL, s.length());
}

TYPED_TEST(SliceTest, Null) {
  gLocalBuffer[0] = 0x18;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Null, slice.type());
  ASSERT_TRUE(slice.isNull());
  ASSERT_EQ(1ULL, slice.byteSize());
}

TYPED_TEST(SliceTest, False) {
  gLocalBuffer[0] = 0x19;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Bool, slice.type());
  ASSERT_TRUE(slice.isBool());
  ASSERT_TRUE(slice.isFalse());
  ASSERT_FALSE(slice.isTrue());
  ASSERT_EQ(1ULL, slice.byteSize());
  ASSERT_FALSE(slice.getBool());
}

TYPED_TEST(SliceTest, True) {
  gLocalBuffer[0] = 0x1a;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Bool, slice.type());
  ASSERT_TRUE(slice.isBool());
  ASSERT_FALSE(slice.isFalse());
  ASSERT_TRUE(slice.isTrue());
  ASSERT_EQ(1ULL, slice.byteSize());
  ASSERT_TRUE(slice.getBool());
}

TYPED_TEST(SliceTest, Double) {
  gLocalBuffer[0] = 0x1f;

  double value = 23.5;
  DumpDouble(value, reinterpret_cast<uint8_t*>(gLocalBuffer) + 1);

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Double, slice.type());
  ASSERT_TRUE(slice.isDouble());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_DOUBLE_EQ(value, slice.getDouble());
}

TYPED_TEST(SliceTest, DoubleNegative) {
  gLocalBuffer[0] = 0x1f;

  double value = -999.91355;
  DumpDouble(value, reinterpret_cast<uint8_t*>(gLocalBuffer) + 1);

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Double, slice.type());
  ASSERT_TRUE(slice.isDouble());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_DOUBLE_EQ(value, slice.getDouble());
}

TYPED_TEST(SliceTest, SmallInt) {
  int64_t expected[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -6, -5, -4, -3, -2, -1};

  for (int i = 0; i < 16; ++i) {
    gLocalBuffer[0] = 0x36 + expected[i];

    Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));
    ASSERT_EQ(ValueType::SmallInt, slice.type());
    ASSERT_TRUE(slice.isSmallInt());
    ASSERT_EQ(1ULL, slice.byteSize());

    ASSERT_EQ(expected[i], slice.getSmallIntUnchecked());
    ASSERT_EQ(expected[i], slice.getInt());
  }
}

TYPED_TEST(SliceTest, Int1) {
  gLocalBuffer[0] = 0x20;
  uint8_t value = 0x3;
  memcpy(&gLocalBuffer[1], (void*)&value, sizeof(value));

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(2ULL, slice.byteSize());

  ASSERT_EQ(3LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int2) {
  gLocalBuffer[0] = 0x21;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(3ULL, slice.byteSize());
  ASSERT_EQ(16931LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int3) {
  gLocalBuffer[0] = 0x22;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(4ULL, slice.byteSize());
  ASSERT_EQ(-3350802LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int4) {
  gLocalBuffer[0] = 0x23;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0x7c;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(5ULL, slice.byteSize());
  ASSERT_EQ(2087076387LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int5) {
  gLocalBuffer[0] = 0x24;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0x6f;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(6ULL, slice.byteSize());
  ASSERT_EQ(-239816876306LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int6) {
  gLocalBuffer[0] = 0x25;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0x3f;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(7ULL, slice.byteSize());
  ASSERT_EQ(-35183670796562LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int7) {
  gLocalBuffer[0] = 0x26;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0x3f;
  *p++ = 0x5a;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(8ULL, slice.byteSize());
  ASSERT_EQ(-12701557622776082LL, slice.getInt());
}

TYPED_TEST(SliceTest, Int8) {
  gLocalBuffer[0] = 0x27;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0x3f;
  *p++ = 0xfa;
  *p++ = 0x6f;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(8068832049729258019LL, slice.getInt());
}

TYPED_TEST(SliceTest, IntMax) {
  Builder b;
  b.add(INT64_MAX);

  Slice slice(b.slice());

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(INT64_MAX, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt1) {
  gLocalBuffer[0] = 0x20;
  uint8_t value = 0xa3;
  memcpy(&gLocalBuffer[1], (void*)&value, sizeof(value));

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(2ULL, slice.byteSize());

  ASSERT_EQ(-93LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt2) {
  gLocalBuffer[0] = 0x21;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0xe2;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(3ULL, slice.byteSize());
  ASSERT_EQ(-7645LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt3) {
  gLocalBuffer[0] = 0x22;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0xd6;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(4ULL, slice.byteSize());
  ASSERT_EQ(-7020818LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt4) {
  gLocalBuffer[0] = 0x23;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(5ULL, slice.byteSize());
  ASSERT_EQ(-1402584541LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt5) {
  gLocalBuffer[0] = 0x24;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(6ULL, slice.byteSize());
  ASSERT_EQ(-549054521618LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt6) {
  gLocalBuffer[0] = 0x25;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0xef;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(7ULL, slice.byteSize());
  ASSERT_EQ(-131940694040850LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt7) {
  gLocalBuffer[0] = 0x26;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0xef;
  *p++ = 0xfa;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(8ULL, slice.byteSize());
  ASSERT_EQ(-35316312782872850LL, slice.getInt());
}

TYPED_TEST(SliceTest, NegInt8) {
  gLocalBuffer[0] = 0x27;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0xef;
  *p++ = 0xfa;
  *p++ = 0x8e;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(-8143933094758039005LL, slice.getInt());
}

TYPED_TEST(SliceTest, IntMin) {
  Builder b;
  b.add(INT64_MIN);

  Slice slice(b.slice());

  ASSERT_EQ(ValueType::Int, slice.type());
  ASSERT_TRUE(slice.isInt());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(INT64_MIN, slice.getInt());
  ASSERT_VPACK_EXCEPTION(slice.getUInt(), Exception::kNumberOutOfRange);
}

TYPED_TEST(SliceTest, UInt1) {
  gLocalBuffer[0] = 0x28;
  uint8_t value = 0x36 + 3;
  memcpy(&gLocalBuffer[1], (void*)&value, sizeof(value));

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(2ULL, slice.byteSize());
  ASSERT_EQ(value, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt2) {
  gLocalBuffer[0] = 0x29;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(3ULL, slice.byteSize());
  ASSERT_EQ(0x4223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt3) {
  gLocalBuffer[0] = 0x2a;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(4ULL, slice.byteSize());
  ASSERT_EQ(0x664223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt4) {
  gLocalBuffer[0] = 0x2b;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(5ULL, slice.byteSize());
  ASSERT_EQ(0xac664223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt5) {
  gLocalBuffer[0] = 0x2c;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(6ULL, slice.byteSize());
  ASSERT_EQ(0xffac664223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt6) {
  gLocalBuffer[0] = 0x2d;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0xee;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(7ULL, slice.byteSize());
  ASSERT_EQ(0xeeffac664223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt7) {
  gLocalBuffer[0] = 0x2e;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0xee;
  *p++ = 0x59;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(8ULL, slice.byteSize());
  ASSERT_EQ(0x59eeffac664223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UInt8) {
  gLocalBuffer[0] = 0x2f;
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = 0x23;
  *p++ = 0x42;
  *p++ = 0x66;
  *p++ = 0xac;
  *p++ = 0xff;
  *p++ = 0xee;
  *p++ = 0x59;
  *p++ = 0xab;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(0xab59eeffac664223ULL, slice.getUInt());
}

TYPED_TEST(SliceTest, UIntMax) {
  Builder b;
  b.add(UINT64_MAX);

  Slice slice(b.slice());

  ASSERT_EQ(ValueType::UInt, slice.type());
  ASSERT_TRUE(slice.isUInt());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(UINT64_MAX, slice.getUInt());
  ASSERT_VPACK_EXCEPTION(slice.getInt(), Exception::kNumberOutOfRange);
}

TYPED_TEST(SliceTest, ArrayEmpty) {
  gLocalBuffer[0] = 0x01;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Array, slice.type());
  ASSERT_TRUE(slice.isArray());
  ASSERT_TRUE(slice.isEmptyArray());
  ASSERT_EQ(1ULL, slice.byteSize());
  ASSERT_EQ(0ULL, slice.length());
}

TYPED_TEST(SliceTest, ObjectEmpty) {
  gLocalBuffer[0] = 0x0a;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::Object, slice.type());
  ASSERT_TRUE(slice.isObject());
  ASSERT_TRUE(slice.isEmptyObject());
  ASSERT_EQ(1ULL, slice.byteSize());
  ASSERT_EQ(0ULL, slice.length());
}

TYPED_TEST(SliceTest, StringNoString) {
  Slice slice;

  ASSERT_FALSE(slice.isString());
  ASSERT_VPACK_EXCEPTION(slice.copyString(), Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(slice.stringView(), Exception::kInvalidValueType);
}

TYPED_TEST(SliceTest, StringEmpty) {
  gLocalBuffer[0] = 0x80;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));

  ASSERT_EQ(ValueType::String, slice.type());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(1ULL, slice.byteSize());
  ASSERT_EQ("", slice.stringView());
  ASSERT_EQ("", slice.copyString());
}

TYPED_TEST(SliceTest, StringLengths) {
  Builder builder;

  for (size_t i = 0; i < 255; ++i) {
    builder.clear();

    std::string temp;
    for (size_t j = 0; j < i; ++j) {
      temp.push_back('x');
    }

    builder.add(temp);

    Slice slice = builder.slice();

    ASSERT_TRUE(slice.isString());
    ASSERT_EQ(ValueType::String, slice.type());

    ASSERT_EQ(i, slice.stringView().size());

    if (i <= 126) {
      ASSERT_EQ(i + 1, slice.byteSize());
    } else {
      ASSERT_EQ(i + 1 + 4, slice.byteSize());
    }
  }
}

TYPED_TEST(SliceTest, String1) {
  gLocalBuffer[0] = 0x80 + static_cast<char>(strlen("foobar"));

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = (uint8_t)'f';
  *p++ = (uint8_t)'o';
  *p++ = (uint8_t)'o';
  *p++ = (uint8_t)'b';
  *p++ = (uint8_t)'a';
  *p++ = (uint8_t)'r';

  ASSERT_EQ(ValueType::String, slice.type());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(7ULL, slice.byteSize());
  ASSERT_EQ("foobar", slice.copyString());
  ASSERT_EQ("foobar", slice.stringView());
}

TYPED_TEST(SliceTest, String2) {
  gLocalBuffer[0] = 0x88;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = (uint8_t)'1';
  *p++ = (uint8_t)'2';
  *p++ = (uint8_t)'3';
  *p++ = (uint8_t)'f';
  *p++ = (uint8_t)'\r';
  *p++ = (uint8_t)'\t';
  *p++ = (uint8_t)'\n';
  *p++ = (uint8_t)'x';

  ASSERT_EQ(ValueType::String, slice.type());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ("123f\r\t\nx", slice.copyString());
  ASSERT_EQ("123f\r\t\nx", slice.stringView());
}

TYPED_TEST(SliceTest, StringNullBytes) {
  gLocalBuffer[0] = 0x88;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  *p++ = (uint8_t)'\0';
  *p++ = (uint8_t)'1';
  *p++ = (uint8_t)'2';
  *p++ = (uint8_t)'\0';
  *p++ = (uint8_t)'3';
  *p++ = (uint8_t)'4';
  *p++ = (uint8_t)'\0';
  *p++ = (uint8_t)'x';

  ASSERT_EQ(ValueType::String, slice.type());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(9ULL, slice.byteSize());
  ASSERT_EQ(8U, slice.stringView().size());

  {
    std::string s(slice.copyString());
    ASSERT_EQ(8ULL, s.size());
    ASSERT_EQ('\0', s[0]);
    ASSERT_EQ('1', s[1]);
    ASSERT_EQ('2', s[2]);
    ASSERT_EQ('\0', s[3]);
    ASSERT_EQ('3', s[4]);
    ASSERT_EQ('4', s[5]);
    ASSERT_EQ('\0', s[6]);
    ASSERT_EQ('x', s[7]);
  }

  {
    std::string_view s(slice.stringView());
    ASSERT_EQ(8ULL, s.size());
    ASSERT_EQ('\0', s[0]);
    ASSERT_EQ('1', s[1]);
    ASSERT_EQ('2', s[2]);
    ASSERT_EQ('\0', s[3]);
    ASSERT_EQ('3', s[4]);
    ASSERT_EQ('4', s[5]);
    ASSERT_EQ('\0', s[6]);
    ASSERT_EQ('x', s[7]);
  }
}

TYPED_TEST(SliceTest, StringLong) {
  gLocalBuffer[0] = 0xff;

  Slice slice(reinterpret_cast<const uint8_t*>(&gLocalBuffer[0]));
  uint8_t* p = (uint8_t*)&gLocalBuffer[1];
  // length
  *p++ = (uint8_t)6;
  *p++ = (uint8_t)0;
  *p++ = (uint8_t)0;
  *p++ = (uint8_t)0;

  *p++ = (uint8_t)'f';
  *p++ = (uint8_t)'o';
  *p++ = (uint8_t)'o';
  *p++ = (uint8_t)'b';
  *p++ = (uint8_t)'a';
  *p++ = (uint8_t)'r';

  ASSERT_EQ(ValueType::String, slice.type());
  ASSERT_TRUE(slice.isString());
  ASSERT_EQ(11ULL, slice.byteSize());
  ASSERT_EQ("foobar", slice.copyString());
  ASSERT_EQ("foobar", slice.stringView());
}

TYPED_TEST(SliceTest, ArrayCases1) {
  uint8_t buf[] = {0x02, 0x05, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases2) {
  uint8_t buf[] = {0x02, 0x06, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases3) {
  uint8_t buf[] = {0x02, 0x08, 0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases4) {
  uint8_t buf[] = {0x02, 0x0c, 0x00, 0x00,     0x00,     0x00,
                   0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases5) {
  uint8_t buf[] = {0x03, 0x06, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases6) {
  uint8_t buf[] = {0x03, 0x08, 0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases7) {
  uint8_t buf[] = {0x03, 0x0c, 0x00, 0x00,     0x00,     0x00,
                   0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases8) {
  uint8_t buf[] = {0x04, 0x08, 0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases9) {
  uint8_t buf[] = {0x04, 0x0c, 0x00, 0x00,     0x00,     0x00,
                   0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases10) {
  uint8_t buf[] = {0x05, 0x0c, 0x00, 0x00,     0x00,     0x00,
                   0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases11) {
  uint8_t buf[] = {0x06,     0x09, 0x03, 0x36 + 1, 0x36 + 2,
                   0x36 + 3, 0x03, 0x04, 0x05};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases12) {
  uint8_t buf[] = {0x06,     0x0b,     0x03, 0x00, 0x00, 0x36 + 1,
                   0x36 + 2, 0x36 + 3, 0x05, 0x06, 0x07};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases13) {
  uint8_t buf[] = {0x06, 0x0f,     0x03,     0x00,     0x00, 0x00, 0x00, 0x00,
                   0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3, 0x09, 0x0a, 0x0b};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases14) {
  uint8_t buf[] = {0x07,     0x0e, 0x00, 0x03, 0x00, 0x36 + 1, 0x36 + 2,
                   0x36 + 3, 0x05, 0x00, 0x06, 0x00, 0x07,     0x00};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases15) {
  uint8_t buf[] = {0x07, 0x12, 0x00, 0x03,     0x00,     0x00,
                   0x00, 0x00, 0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3,
                   0x09, 0x00, 0x0a, 0x00,     0x0b,     0x00};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases16) {
  uint8_t buf[] = {0x08, 0x18,     0x00,     0x00,     0x00, 0x03, 0x00, 0x00,
                   0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3, 0x09, 0x00, 0x00, 0x00,
                   0x0a, 0x00,     0x00,     0x00,     0x0b, 0x00, 0x00, 0x00};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCases17) {
  uint8_t buf[] = {0x09, 0x2c,     0x00,     0x00,     0x00, 0x00, 0x00, 0x00,
                   0x00, 0x36 + 1, 0x36 + 2, 0x36 + 3, 0x09, 0x00, 0x00, 0x00,
                   0x00, 0x00,     0x00,     0x00,     0x0a, 0x00, 0x00, 0x00,
                   0x00, 0x00,     0x00,     0x00,     0x0b, 0x00, 0x00, 0x00,
                   0x00, 0x00,     0x00,     0x00,     0x03, 0x00, 0x00, 0x00,
                   0x00, 0x00,     0x00,     0x00};
  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
}

TYPED_TEST(SliceTest, ArrayCasesCompact) {
  uint8_t buf[] = {0x13,     0x08,     0x36 + 0, 0x36 + 1,
                   0x36 + 2, 0x36 + 3, 0x36 + 4, 0x05};

  TypeParam s(buf);
  ASSERT_TRUE(s.isArray());
  ASSERT_FALSE(s.isEmptyArray());
  ASSERT_EQ(5ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.at(0);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(0LL, ss.getInt());
  ss = s.at(1);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(1LL, ss.getInt());
  ss = s.at(4);
  ASSERT_TRUE(ss.isSmallInt());
  ASSERT_EQ(4LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases1) {
  uint8_t buf[] = {
    0x0b,     0x00,     0x03, 0x80 + 1, 0x61, 0x36 + 1, 0x80 + 1, 0x62,
    0x36 + 2, 0x80 + 1, 0x63, 0x36 + 3, 0x03, 0x06,     0x09,
  };
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases2) {
  uint8_t buf[] = {
    0x0b, 0x00,     0x03,     0x00, 0x00,     0x80 + 1,
    0x61, 0x36 + 1, 0x80 + 1, 0x62, 0x36 + 2, 0x80 + 1,
    0x63, 0x36 + 3, 0x05,     0x08, 0x0b,
  };
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases3) {
  uint8_t buf[] = {
    0x0b,     0x00,     0x03,     0x00,     0x00,     0x00,     0x00,
    0x00,     0x00,     0x80 + 1, 0x61,     0x36 + 1, 0x80 + 1, 0x62,
    0x36 + 2, 0x80 + 1, 0x63,     0x36 + 3, 0x09,     0x0c,     0x0f,
  };
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases7) {
  uint8_t buf[] = {
    0x0c,     0x00,     0x00, 0x03,     0x00,     0x80 + 1, 0x61,
    0x36 + 1, 0x80 + 1, 0x62, 0x36 + 2, 0x80 + 1, 0x63,     0x36 + 3,
    0x05,     0x00,     0x08, 0x00,     0x0b,     0x00,
  };
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases8) {
  uint8_t buf[] = {0x0c,     0x00, 0x00,     0x03,     0x00, 0x00,
                   0x00,     0x00, 0x00,     0x80 + 1, 0x61, 0x36 + 1,
                   0x80 + 1, 0x62, 0x36 + 2, 0x80 + 1, 0x63, 0x36 + 3,
                   0x09,     0x00, 0x0c,     0x00,     0x0f, 0x00};
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases11) {
  uint8_t buf[] = {0x0d,     0x00, 0x00,     0x00,     0x00, 0x03,
                   0x00,     0x00, 0x00,     0x80 + 1, 0x61, 0x36 + 1,
                   0x80 + 1, 0x62, 0x36 + 2, 0x80 + 1, 0x63, 0x36 + 3,
                   0x09,     0x00, 0x00,     0x00,     0x0c, 0x00,
                   0x00,     0x00, 0x0f,     0x00,     0x00, 0x00};
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCases13) {
  uint8_t buf[] = {
    0x0e, 0x00,     0x00, 0x00,     0x00,     0x00, 0x00,     0x00,
    0x00, 0x80 + 1, 0x61, 0x36 + 1, 0x80 + 1, 0x62, 0x36 + 2, 0x80 + 1,
    0x63, 0x36 + 3, 0x09, 0x00,     0x00,     0x00, 0x00,     0x00,
    0x00, 0x00,     0x0c, 0x00,     0x00,     0x00, 0x00,     0x00,
    0x00, 0x00,     0x0f, 0x00,     0x00,     0x00, 0x00,     0x00,
    0x00, 0x00,     0x03, 0x00,     0x00,     0x00, 0x00,     0x00,
    0x00, 0x00,
  };
  buf[1] = sizeof(buf);  // set the bytelength
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(3ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ObjectCompact) {
  const uint8_t buf[] = {
    0x14,     0x0f, 0x80 + 1, 0x61,     0x36 + 0, 0x80 + 1, 0x62, 0x36 + 1,
    0x80 + 1, 0x63, 0x36 + 2, 0x80 + 1, 0x64,     0x36 + 3, 0x04,
  };
  TypeParam s(buf);
  ASSERT_TRUE(s.isObject());
  ASSERT_FALSE(s.isEmptyObject());
  ASSERT_EQ(4ULL, s.length());
  ASSERT_EQ(sizeof(buf), s.byteSize());
  Slice ss = s.get("a");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(0LL, ss.getInt());
  ss = s.get("b");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(1LL, ss.getInt());
  ss = s.get("c");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(2LL, ss.getInt());
  ss = s.get("d");
  EXPECT_TRUE(ss.isSmallInt());
  EXPECT_EQ(3LL, ss.getInt());
}

TYPED_TEST(SliceTest, ToStringNull) {
  const std::string value("null");

  std::shared_ptr<Builder> b = Parser::fromJson(value);
  TypeParam s(b->start());

  ASSERT_EQ("null", s.toString());
}

TYPED_TEST(SliceTest, ToStringArray) {
  const std::string value("[1,2,3,4,5]");

  std::shared_ptr<Builder> b = Parser::fromJson(value);
  TypeParam s(b->start());

  ASSERT_EQ("[\n  1,\n  2,\n  3,\n  4,\n  5\n]", s.toString());
}

TYPED_TEST(SliceTest, ToStringArrayCompact) {
  Options options;
  options.build_unindexed_arrays = true;

  const std::string value("[1,2,3,4,5]");

  std::shared_ptr<Builder> b = Parser::fromJson(value, &options);
  TypeParam s(b->start());

  ASSERT_EQ(0x13, s.head());
  ASSERT_EQ("[\n  1,\n  2,\n  3,\n  4,\n  5\n]", s.toString());
}

TYPED_TEST(SliceTest, ToStringArrayEmpty) {
  const std::string value("[]");

  std::shared_ptr<Builder> b = Parser::fromJson(value);
  TypeParam s(b->start());

  ASSERT_EQ("[\n]", s.toString());
}

TYPED_TEST(SliceTest, ToStringObjectEmpty) {
  const std::string value("{ }");

  std::shared_ptr<Builder> b = Parser::fromJson(value);
  TypeParam s(b->start());

  ASSERT_EQ("{\n}", s.toString());
}
/* TODO(mbkkt) move to serenedb
TYPED_TEST(SliceTest, EqualToUniqueValues) {
  std::string const value("[1,2,3,4,null,true,\"foo\",\"bar\"]");

  Parser parser;
  parser.parse(value);

  absl::flat_hash_set<Slice, NormalizedCompare::Hash, NormalizedCompare::Equal>
      values(11, NormalizedCompare::Hash(), NormalizedCompare::Equal());
  for (auto it : ArrayIterator(Slice(parser.start()))) {
    values.emplace(it);
  }

  ASSERT_EQ(8UL, values.size());
}

TYPED_TEST(SliceTest, EqualToDuplicateValuesNumbers) {
  std::string const value("[1,2,3,4,1,2,3,4,5,9,1]");

  Parser parser;
  parser.parse(value);

  absl::flat_hash_set<Slice, NormalizedCompare::Hash, NormalizedCompare::Equal>
      values(11, NormalizedCompare::Hash(), NormalizedCompare::Equal());
  for (auto it : ArrayIterator(Slice(parser.start()))) {
    values.emplace(it);
  }

  ASSERT_EQ(6UL, values.size());  // 1,2,3,4,5,9
}

TYPED_TEST(SliceTest, EqualToBiggerNumbers) {
  std::string const value("[1024,1025,1031,1024,1029,1025]");

  Parser parser;
  parser.parse(value);

  absl::flat_hash_set<Slice, NormalizedCompare::Hash, NormalizedCompare::Equal>
      values(11, NormalizedCompare::Hash(), NormalizedCompare::Equal());
  for (auto it : ArrayIterator(Slice(parser.start()))) {
    values.emplace(it);
  }

  ASSERT_EQ(4UL, values.size());  // 1024, 1025, 1029, 1031
}

TYPED_TEST(SliceTest, EqualToDuplicateValuesStrings) {
  std::string const value(
      "[\"foo\",\"bar\",\"baz\",\"bart\",\"foo\",\"bark\",\"qux\",\"foo\"]");

  Parser parser;
  parser.parse(value);

  absl::flat_hash_set<Slice, NormalizedCompare::Hash, NormalizedCompare::Equal>
      values(11, NormalizedCompare::Hash(), NormalizedCompare::Equal());
  for (auto it : ArrayIterator(Slice(parser.start()))) {
    values.emplace(it);
  }

  ASSERT_EQ(6UL, values.size());  // "foo", "bar", "baz", "bart", "bark", "qux"
}
*/

TYPED_TEST(SliceTest, EqualToNull) {
  std::shared_ptr<Builder> b1 = Parser::fromJson("null");
  Slice s1 = b1->slice();
  std::shared_ptr<Builder> b2 = Parser::fromJson("null");
  Slice s2 = b2->slice();

  ASSERT_TRUE(s1.binaryEquals(s2));
  ASSERT_TRUE(s2.binaryEquals(s1));
}

TYPED_TEST(SliceTest, EqualToInt) {
  std::shared_ptr<Builder> b1 = Parser::fromJson("-128885355");
  Slice s1 = b1->slice();
  std::shared_ptr<Builder> b2 = Parser::fromJson("-128885355");
  Slice s2 = b2->slice();

  ASSERT_TRUE(s1.binaryEquals(s2));
  ASSERT_TRUE(s2.binaryEquals(s1));
}

TYPED_TEST(SliceTest, EqualToUInt) {
  std::shared_ptr<Builder> b1 = Parser::fromJson("128885355");
  Slice s1 = b1->slice();
  std::shared_ptr<Builder> b2 = Parser::fromJson("128885355");
  Slice s2 = b2->slice();

  ASSERT_TRUE(s1.binaryEquals(s2));
  ASSERT_TRUE(s2.binaryEquals(s1));
}

TYPED_TEST(SliceTest, EqualToDouble) {
  std::shared_ptr<Builder> b1 = Parser::fromJson("-128885355.353");
  Slice s1 = b1->slice();
  std::shared_ptr<Builder> b2 = Parser::fromJson("-128885355.353");
  Slice s2 = b2->slice();

  ASSERT_TRUE(s1.binaryEquals(s2));
  ASSERT_TRUE(s2.binaryEquals(s1));
}

TYPED_TEST(SliceTest, EqualToString) {
  std::shared_ptr<Builder> b1 = Parser::fromJson("\"this is a test string\"");
  Slice s1 = b1->slice();
  std::shared_ptr<Builder> b2 = Parser::fromJson("\"this is a test string\"");
  Slice s2 = b2->slice();

  ASSERT_TRUE(s1.binaryEquals(s2));
  ASSERT_TRUE(s2.binaryEquals(s1));
}

TYPED_TEST(SliceTest, EqualToDirectInvocation) {
  const std::string value("[1024,1025,1026,1027,1028]");

  Parser parser;
  parser.parse(value);

  int comparisons = 0;
  ArrayIterator it(Slice(parser.start()));
  while (it.valid()) {
    ArrayIterator it2(Slice(parser.start()));
    while (it2.valid()) {
      if (it.index() != it2.index()) {
        ASSERT_FALSE(it.value().binaryEquals(it2.value()));
        ASSERT_FALSE(it2.value().binaryEquals(it.value()));
        ++comparisons;
      }
      it2.next();
    }
    it.next();
  }
  ASSERT_EQ(20, comparisons);
}

TYPED_TEST(SliceTest, EqualToDirectInvocationSmallInts) {
  const std::string value("[1,2,3,4,5]");

  Parser parser;
  parser.parse(value);

  int comparisons = 0;
  ArrayIterator it(Slice(parser.start()));
  while (it.valid()) {
    ArrayIterator it2(Slice(parser.start()));
    while (it2.valid()) {
      if (it.index() != it2.index()) {
        ASSERT_FALSE(it.value().binaryEquals(it2.value()));
        ASSERT_FALSE(it2.value().binaryEquals(it.value()));
        ++comparisons;
      }
      it2.next();
    }
    it.next();
  }
  ASSERT_EQ(20, comparisons);
}

TYPED_TEST(SliceTest, EqualToDirectInvocationLongStrings) {
  std::shared_ptr<Builder> b1 = Parser::fromJson(
    "\"thisisalongstring.dddddddddddddddddddddddddddds......................."
    "....................................................."
    "longerthan127chars\"");
  std::shared_ptr<Builder> b2 = Parser::fromJson(
    "\"thisisalongstring.dddddddddddddddddddddddddddds.................eek!.."
    "........................................................."
    "longerthan127chars\"");

  ASSERT_TRUE(b1->slice().binaryEquals(b1->slice()));
  ASSERT_TRUE(b2->slice().binaryEquals(b2->slice()));
  ASSERT_FALSE(b1->slice().binaryEquals(b2->slice()));
  ASSERT_FALSE(b2->slice().binaryEquals(b1->slice()));
}

TYPED_TEST(SliceTest, Hashing) {
  // for (size_t i = 0; i < 256; ++i) {
  //   auto& out = std::cerr << "/* 0x" << std::hex << std::setw(2)
  //                         << std::setfill('0') << i << " */ 0x"
  //                         << std::setw(16);
  //   if (SliceStaticData::FixedTypeLengths[i] != 1) {
  //     out << 0 << ",\n";
  //   } else {
  //     Builder b;
  //     uint8_t val = static_cast<uint8_t>(i);
  //     b.add(Slice(&val));
  //     out << b.slice().hashSlow() << ",\n";
  //   }
  // }
  for (size_t i = 0; i < 256; ++i) {
    if (slice_static_data::kFixedTypeLengths[i] != 1) {
      // not a one-byte type
      continue;
    }
    Builder b;
    uint8_t val = static_cast<uint8_t>(i);
    b.add(Slice(&val));

    EXPECT_EQ(slice_static_data::kPrecalculatedHashesForDefaultSeed[i],
              b.slice().hash())
      << i;
    EXPECT_EQ(slice_static_data::kPrecalculatedHashesForDefaultSeed[i],
              b.slice().hashSlow())
      << i;
    EXPECT_EQ(slice_static_data::kPrecalculatedHashesForDefaultSeed[i],
              b.slice().hash(Slice::kDefaultSeed64))
      << i;
    EXPECT_EQ(slice_static_data::kPrecalculatedHashesForDefaultSeed[i],
              b.slice().hashSlow(Slice::kDefaultSeed64))
      << i;
  }
}

TYPED_TEST(SliceTest, VolatileHashNull) {
  std::shared_ptr<Builder> b = Parser::fromJson("null");
  TypeParam s = b->slice();

  EXPECT_EQ(3119587240069238513ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashDouble) {
  std::shared_ptr<Builder> b = Parser::fromJson("-345354.35532352");
  TypeParam s = b->slice();

  EXPECT_EQ(5629496456750199849ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashString) {
  std::shared_ptr<Builder> b = Parser::fromJson("\"this is a test string\"");
  TypeParam s = b->slice();

  EXPECT_EQ(15384472586036096374ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashStringEmpty) {
  std::shared_ptr<Builder> b = Parser::fromJson("\"\"");
  TypeParam s = b->slice();

  EXPECT_EQ(2753460619086381837ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashStringShort) {
  std::shared_ptr<Builder> b = Parser::fromJson("\"123456\"");
  TypeParam s = b->slice();

  EXPECT_EQ(4997166149821630380ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashArray) {
  std::shared_ptr<Builder> b = Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]");
  TypeParam s = b->slice();

  EXPECT_EQ(11314214455195692033ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashObject) {
  std::shared_ptr<Builder> b = Parser::fromJson(
    "{\"one\":1,\"two\":2,\"three\":3,\"four\":4,\"five\":5,\"six\":6,"
    "\"seven\":7}");
  TypeParam s = b->slice();

  EXPECT_EQ(5454299594508083170ULL, s.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashArrayDouble) {
  Builder b1;
  b1.openArray();
  b1.add(-1.0);
  b1.add(0);
  b1.add(1.0);
  b1.add(2.0);
  b1.add(3.0);
  b1.add(42.0);
  b1.add(-42.0);
  b1.add(123456.0);
  b1.add(-123456.0);
  b1.close();

  Builder b2;
  b2.openArray();
  b2.add(-1);
  b2.add(0);
  b2.add(1);
  b2.add(2);
  b2.add(3);
  b2.add(42);
  b2.add(-42);
  b2.add(123456);
  b2.add(-123456);
  b2.close();

  EXPECT_EQ(3237818552344116048ULL, b1.slice().volatileHash());
  EXPECT_EQ(15516159787402432789ULL, b2.slice().volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashArrayIndexed) {
  Options options;

  options.build_unindexed_arrays = false;
  std::shared_ptr<Builder> b1 =
    Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]", &options);
  Slice s1 = b1->slice();

  options.build_unindexed_arrays = true;
  std::shared_ptr<Builder> b2 =
    Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]", &options);
  Slice s2 = b2->slice();

  EXPECT_EQ(11314214455195692033ULL, s1.volatileHash());
  EXPECT_EQ(3379643125582344523ULL, s2.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashArrayNested) {
  Options options;

  options.build_unindexed_arrays = false;
  std::shared_ptr<Builder> b1 = Parser::fromJson(
    "[-4.0,1,2.0,-4345.0,4,5,6,7,8,9,10,[1,9,-42,45.0]]", &options);
  Slice s1 = b1->slice();

  options.build_unindexed_arrays = true;
  std::shared_ptr<Builder> b2 = Parser::fromJson(
    "[-4.0,1,2.0,-4345.0,4,5,6,7,8,9,10,[1,9,-42,45.0]]", &options);
  Slice s2 = b2->slice();

  EXPECT_EQ(12805592972703493073ULL, s1.volatileHash());
  EXPECT_EQ(10151171761178017614ULL, s2.volatileHash());
}

TYPED_TEST(SliceTest, VolatileHashObjectOrder) {
  Options options;

  options.build_unindexed_objects = false;
  std::shared_ptr<Builder> b1 = Parser::fromJson(
    "{\"one\":1,\"two\":2,\"three\":3,\"four\":4,\"five\":5,\"six\":6,"
    "\"seven\":7}",
    &options);
  Slice s1 = b1->slice();

  options.build_unindexed_objects = false;
  std::shared_ptr<Builder> b2 = Parser::fromJson(
    "{\"seven\":7,\"six\":6,\"five\":5,\"four\":4,\"three\":3,\"two\":2,"
    "\"one\":1}",
    &options);
  Slice s2 = b2->slice();

  EXPECT_EQ(5454299594508083170ULL, s1.volatileHash());
  EXPECT_EQ(9789731722494820545ULL, s2.volatileHash());
}

TYPED_TEST(SliceTest, GetNumericValueIntNoLoss) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(1);
  b.add(-1);
  b.add(10);
  b.add(-10);
  b.add(INT64_MAX);
  b.add(-3453.32);
  b.add(2343323453.3232235);
  b.close();

  TypeParam s = Slice(b.start());

  ASSERT_EQ(1, s.at(0).template getNumber<int64_t>());
  ASSERT_EQ(-1, s.at(1).template getNumber<int64_t>());
  ASSERT_EQ(10, s.at(2).template getNumber<int64_t>());
  ASSERT_EQ(-10, s.at(3).template getNumber<int64_t>());
  ASSERT_EQ(INT64_MAX, s.at(4).template getNumber<int64_t>());
  ASSERT_EQ(-3453, s.at(5).template getNumber<int64_t>());
  ASSERT_EQ(2343323453, s.at(6).template getNumber<int64_t>());

  ASSERT_EQ(1, s.at(0).template getNumber<int16_t>());
  ASSERT_EQ(-1, s.at(1).template getNumber<int16_t>());
  ASSERT_EQ(10, s.at(2).template getNumber<int16_t>());
  ASSERT_EQ(-10, s.at(3).template getNumber<int16_t>());
  ASSERT_VPACK_EXCEPTION(s.at(4).template getNumber<int16_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(-3453, s.at(5).template getNumber<int16_t>());
  ASSERT_VPACK_EXCEPTION(s.at(6).template getNumber<int16_t>(),
                         Exception::kNumberOutOfRange);
}

TYPED_TEST(SliceTest, GetNumericValueUIntNoLoss) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(1);
  b.add(-1);
  b.add(10);
  b.add(-10);
  b.add(INT64_MAX);
  b.add(-3453.32);
  b.add(2343323453.3232235);
  b.close();

  TypeParam s = Slice(b.start());

  ASSERT_EQ(1ULL, s.at(0).template getNumber<uint64_t>());
  ASSERT_VPACK_EXCEPTION(s.at(1).template getNumber<uint64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(10ULL, s.at(2).template getNumber<uint64_t>());
  ASSERT_VPACK_EXCEPTION(s.at(3).template getNumber<uint64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(static_cast<uint64_t>(INT64_MAX),
            s.at(4).template getNumber<uint64_t>());
  ASSERT_VPACK_EXCEPTION(s.at(5).template getNumber<uint64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(2343323453ULL, s.at(6).template getNumber<uint64_t>());

  ASSERT_EQ(1ULL, s.at(0).template getNumber<uint16_t>());
  ASSERT_VPACK_EXCEPTION(s.at(1).template getNumber<uint16_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(10ULL, s.at(2).template getNumber<uint16_t>());
  ASSERT_VPACK_EXCEPTION(s.at(3).template getNumber<uint16_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_VPACK_EXCEPTION(s.at(4).template getNumber<uint16_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_VPACK_EXCEPTION(s.at(5).template getNumber<uint16_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_VPACK_EXCEPTION(s.at(6).template getNumber<uint16_t>(),
                         Exception::kNumberOutOfRange);
}

TYPED_TEST(SliceTest, GetNumericValueDoubleNoLoss) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(1);
  b.add(-1);
  b.add(10);
  b.add(-10);
  b.add(INT64_MAX);
  b.add(-3453.32);
  b.add(2343323453.3232235);
  b.add(static_cast<uint64_t>(10000));
  b.close();

  TypeParam s = Slice(b.start());

  ASSERT_DOUBLE_EQ(1., s.at(0).template getNumber<double>());
  ASSERT_DOUBLE_EQ(-1., s.at(1).template getNumber<double>());
  ASSERT_DOUBLE_EQ(10., s.at(2).template getNumber<double>());
  ASSERT_DOUBLE_EQ(-10., s.at(3).template getNumber<double>());
  ASSERT_DOUBLE_EQ(static_cast<double>(INT64_MAX),
                   s.at(4).template getNumber<double>());
  ASSERT_DOUBLE_EQ(-3453.32, s.at(5).template getNumber<double>());
  ASSERT_DOUBLE_EQ(2343323453.3232235, s.at(6).template getNumber<double>());
  ASSERT_DOUBLE_EQ(10000., s.at(7).template getNumber<double>());
}

TYPED_TEST(SliceTest, GetNumericValueWrongSource) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.add(Value(ValueType::Null));
  b.add(true);
  b.add("foo");
  b.add("bar");
  b.add(Value(ValueType::Array));
  b.close();
  b.add(Value(ValueType::Object));
  b.close();
  b.close();

  TypeParam s = Slice(b.start());

  ASSERT_VPACK_EXCEPTION(s.at(0).template getNumber<int64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).template getNumber<uint64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(0).template getNumber<double>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).template getNumber<int64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).template getNumber<uint64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(1).template getNumber<double>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).template getNumber<int64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).template getNumber<uint64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(2).template getNumber<double>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).template getNumber<int64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).template getNumber<uint64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(3).template getNumber<double>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).template getNumber<int64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).template getNumber<uint64_t>(),
                         Exception::kInvalidValueType);
  ASSERT_VPACK_EXCEPTION(s.at(4).template getNumber<double>(),
                         Exception::kInvalidValueType);
}

TYPED_TEST(SliceTest, Reassign) {
  uint8_t buf1[] = {0x19};
  uint8_t buf2[] = {0x1a};
  TypeParam s(buf1);
  ASSERT_TRUE(s.isBool());
  ASSERT_FALSE(s.getBool());
  s.set(buf2);
  ASSERT_TRUE(s.isBool());
  ASSERT_TRUE(s.getBool());
}

TYPED_TEST(SliceTest, IsNumber) {
  TypeParam s;
  Builder b;

  // number 0
  b.clear();
  b.add(int(0));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int>());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_TRUE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());

  ASSERT_EQ(int(0), s.template getNumber<int>());
  ASSERT_EQ(int64_t(0), s.template getNumber<int64_t>());
  ASSERT_EQ(uint64_t(0), s.template getNumber<uint64_t>());
  ASSERT_EQ(double(0.0), s.template getNumber<double>());

  // positive int
  b.clear();
  b.add(int(42));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int>());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_TRUE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());

  ASSERT_EQ(int(42), s.template getNumber<int>());
  ASSERT_EQ(int64_t(42), s.template getNumber<int64_t>());
  ASSERT_EQ(uint64_t(42), s.template getNumber<uint64_t>());
  ASSERT_EQ(double(42.0), s.template getNumber<double>());

  // negative int
  b.clear();
  b.add(int(-2));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int>());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_FALSE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());

  ASSERT_EQ(int(-2), s.template getNumber<int>());
  ASSERT_EQ(int64_t(-2), s.template getNumber<int64_t>());
  ASSERT_VPACK_EXCEPTION(s.template getNumber<uint64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(double(-2.0), s.template getNumber<double>());

  // positive big int
  b.clear();
  b.add(int64_t(INT64_MAX));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_TRUE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());

  ASSERT_EQ(int64_t(INT64_MAX), s.template getNumber<int64_t>());
  ASSERT_EQ(uint64_t(INT64_MAX), s.template getNumber<uint64_t>());
  ASSERT_EQ(double(INT64_MAX), s.template getNumber<double>());

  // negative big int
  b.clear();
  b.add(int64_t(INT64_MIN));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_FALSE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());

  ASSERT_EQ(int64_t(INT64_MIN), s.template getNumber<int64_t>());
  ASSERT_VPACK_EXCEPTION(s.template getNumber<uint64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(double(INT64_MIN), s.template getNumber<double>());

  // positive big uint
  b.clear();
  b.add(uint64_t(UINT64_MAX));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_FALSE(s.template isNumber<int64_t>());
  ASSERT_TRUE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());

  ASSERT_VPACK_EXCEPTION(s.template getNumber<int64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_EQ(uint64_t(UINT64_MAX), s.template getNumber<uint64_t>());
  ASSERT_EQ(double(UINT64_MAX), s.template getNumber<double>());

  // negative double
  b.clear();
  b.add(double(-1.25));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_VPACK_EXCEPTION(s.template getNumber<uint64_t>(),
                         Exception::kNumberOutOfRange);
  ASSERT_TRUE(s.template isNumber<double>());

  // positive double
  b.clear();
  b.add(double(1.25));
  s = b.slice();

  ASSERT_TRUE(s.isNumber());
  ASSERT_TRUE(s.template isNumber<int64_t>());
  ASSERT_TRUE(s.template isNumber<uint64_t>());
  ASSERT_TRUE(s.template isNumber<double>());
}
