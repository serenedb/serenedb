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

#include "tests-common.h"

TEST(ExceptionTest, TestMessages) {
  ASSERT_STREQ("Internal error", Exception::message(Exception::kInternalError));
  ASSERT_STREQ("Type has no equivalent in JSON",
               Exception::message(Exception::kNoJsonEquivalent));
  ASSERT_STREQ("Parse error", Exception::message(Exception::kParseError));
  ASSERT_STREQ("Unexpected control character",
               Exception::message(Exception::kUnexpectedControlCharacter));
  ASSERT_STREQ("Duplicate attribute name",
               Exception::message(Exception::kDuplicateAttributeName));
  ASSERT_STREQ("Index out of bounds",
               Exception::message(Exception::kIndexOutOfBounds));
  ASSERT_STREQ("Number out of range",
               Exception::message(Exception::kNumberOutOfRange));
  ASSERT_STREQ("Invalid UTF-8 sequence",
               Exception::message(Exception::kInvalidUtf8Sequence));
  ASSERT_STREQ("Invalid attribute path",
               Exception::message(Exception::kInvalidAttributePath));
  ASSERT_STREQ("Invalid value type for operation",
               Exception::message(Exception::kInvalidValueType));
  ASSERT_STREQ("Array size does not match tuple size",
               Exception::message(Exception::kBadTupleSize));
  ASSERT_STREQ("Too deep nesting in Array/Object",
               Exception::message(Exception::kTooDeepNesting));
  ASSERT_STREQ("Builder value not yet sealed",
               Exception::message(Exception::kBuilderNotSealed));
  ASSERT_STREQ("Need open Object",
               Exception::message(Exception::kBuilderNeedOpenObject));
  ASSERT_STREQ("Need open Array",
               Exception::message(Exception::kBuilderNeedOpenArray));
  ASSERT_STREQ("Need subvalue in current Object or Array",
               Exception::message(Exception::kBuilderNeedSubvalue));
  ASSERT_STREQ("Need open compound value (Array or Object)",
               Exception::message(Exception::kBuilderNeedOpenCompound));
  ASSERT_STREQ("Unexpected type",
               Exception::message(Exception::kBuilderUnexpectedType));
  ASSERT_STREQ("Unexpected value",
               Exception::message(Exception::kBuilderUnexpectedValue));
  ASSERT_STREQ("Invalid type found in binary data",
               Exception::message(Exception::kValidatorInvalidType));
  ASSERT_STREQ("Invalid length found in binary data",
               Exception::message(Exception::kValidatorInvalidLength));

  ASSERT_STREQ("Unknown error", Exception::message(Exception::kUnknownError));
  ASSERT_STREQ("Unknown error",
               Exception::message(static_cast<Exception::ExceptionType>(0)));
  ASSERT_STREQ(
    "Unknown error",
    Exception::message(static_cast<Exception::ExceptionType>(99999)));
}

TEST(ExceptionTest, TestStringification) {
  std::string message;
  try {
    Builder b;
    b.close();
  } catch (const Exception& ex) {
    std::ostringstream out;
    out << ex;
    message = out.str();
  }
  ASSERT_EQ("[Exception Need open compound value (Array or Object)]", message);
}
