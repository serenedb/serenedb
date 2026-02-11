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

#include <ostream>
#include <string>

#include "tests-common.h"

TEST(TypesTest, TestGroups) {
  ASSERT_EQ(ValueType::None, ValueTypeGroup(ValueType::None));
  ASSERT_EQ(ValueType::Null, ValueTypeGroup(ValueType::Null));
  ASSERT_EQ(ValueType::Bool, ValueTypeGroup(ValueType::Bool));
  ASSERT_EQ(ValueType::Double, ValueTypeGroup(ValueType::Double));
  ASSERT_EQ(ValueType::String, ValueTypeGroup(ValueType::String));
  ASSERT_EQ(ValueType::Array, ValueTypeGroup(ValueType::Array));
  ASSERT_EQ(ValueType::Object, ValueTypeGroup(ValueType::Object));
  ASSERT_EQ(ValueType::Double, ValueTypeGroup(ValueType::Int));
  ASSERT_EQ(ValueType::Double, ValueTypeGroup(ValueType::UInt));
  ASSERT_EQ(ValueType::Double, ValueTypeGroup(ValueType::SmallInt));
}

TEST(TypesTest, TestNames) {
  ASSERT_STREQ("none", ValueTypeName(ValueType::None).data());
  ASSERT_STREQ("null", ValueTypeName(ValueType::Null).data());
  ASSERT_STREQ("bool", ValueTypeName(ValueType::Bool).data());
  ASSERT_STREQ("double", ValueTypeName(ValueType::Double).data());
  ASSERT_STREQ("string", ValueTypeName(ValueType::String).data());
  ASSERT_STREQ("array", ValueTypeName(ValueType::Array).data());
  ASSERT_STREQ("object", ValueTypeName(ValueType::Object).data());
  ASSERT_STREQ("int", ValueTypeName(ValueType::Int).data());
  ASSERT_STREQ("uint", ValueTypeName(ValueType::UInt).data());
  ASSERT_STREQ("smallint", ValueTypeName(ValueType::SmallInt).data());
}

TEST(TypesTest, TestNamesArrays) {
  const uint8_t arrays[] = {0x01, 0x02, 0x03, 0x04, 0x05,
                            0x06, 0x07, 0x08, 0x9};
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[0]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[1]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[2]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[3]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[4]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[5]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[6]).type()).data());
  ASSERT_STREQ("array", ValueTypeName(Slice(&arrays[7]).type()).data());
}

TEST(TypesTest, TestNamesObjects) {
  const uint8_t objects[] = {0x0a, 0x0b, 0x0c, 0x0d, 0x0e};
  ASSERT_STREQ("object", ValueTypeName(Slice(&objects[0]).type()).data());
  ASSERT_STREQ("object", ValueTypeName(Slice(&objects[1]).type()).data());
  ASSERT_STREQ("object", ValueTypeName(Slice(&objects[2]).type()).data());
  ASSERT_STREQ("object", ValueTypeName(Slice(&objects[3]).type()).data());
  ASSERT_STREQ("object", ValueTypeName(Slice(&objects[4]).type()).data());
}

TEST(TypesTest, TestInvalidType) {
  ASSERT_STREQ("unknown", ValueTypeName(static_cast<ValueType>(0xff)).data());
}

TEST(TypesTest, TestStringifyObject) {
  Builder b;
  b.add(Value(ValueType::Object));
  b.close();

  Slice s(b.start());
  std::ostringstream out;
  out << s.type();

  ASSERT_EQ("object", out.str());
}

TEST(TypesTest, TestStringifyArray) {
  Builder b;
  b.add(Value(ValueType::Array));
  b.close();

  Slice s(b.start());
  std::ostringstream out;
  out << s.type();

  ASSERT_EQ("array", out.str());
}
