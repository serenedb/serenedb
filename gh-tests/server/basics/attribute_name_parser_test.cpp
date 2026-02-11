////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
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

#include "basics/attribute_name_parser.h"
#include "basics/common.h"
#include "basics/exceptions.h"
#include "gtest/gtest.h"

using namespace sdb;
using namespace sdb::basics;

////////////////////////////////////////////////////////////////////////////////
/// test_simpleString
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_simpleString) {
  std::string input = "test";
  std::vector<AttributeName> result;

  ParseAttributeString(input, result, false);

  EXPECT_EQ(result.size(), static_cast<size_t>(1));
  EXPECT_EQ(result[0].name, input);
  EXPECT_FALSE(result[0].should_expand);
}

////////////////////////////////////////////////////////////////////////////////
/// test_subAttribute
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_subAttribute) {
  std::string input = "foo.bar";
  std::vector<AttributeName> result;

  ParseAttributeString(input, result, false);

  EXPECT_EQ(result.size(), static_cast<size_t>(2));
  EXPECT_EQ(result[0].name, "foo");
  EXPECT_FALSE(result[0].should_expand);
  EXPECT_EQ(result[1].name, "bar");
  EXPECT_FALSE(result[1].should_expand);
}

////////////////////////////////////////////////////////////////////////////////
/// test_subsubAttribute
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_subsubAttribute) {
  std::string input = "foo.bar.baz";
  std::vector<AttributeName> result;

  ParseAttributeString(input, result, false);

  EXPECT_EQ(result.size(), static_cast<size_t>(3));
  EXPECT_EQ(result[0].name, "foo");
  EXPECT_FALSE(result[0].should_expand);
  EXPECT_EQ(result[1].name, "bar");
  EXPECT_FALSE(result[1].should_expand);
  EXPECT_EQ(result[2].name, "baz");
  EXPECT_FALSE(result[2].should_expand);
}

////////////////////////////////////////////////////////////////////////////////
/// test_expandAttribute
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_expandAttribute) {
  std::string input = "foo[*]";
  std::vector<AttributeName> result;

  ParseAttributeString(input, result, true);

  EXPECT_EQ(result.size(), static_cast<size_t>(1));
  EXPECT_EQ(result[0].name, "foo");
  EXPECT_TRUE(result[0].should_expand);
}

////////////////////////////////////////////////////////////////////////////////
/// test_expandSubAttribute
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_expandSubAttribute) {
  std::string input = "foo.bar[*]";
  std::vector<AttributeName> result;

  ParseAttributeString(input, result, true);

  EXPECT_EQ(result.size(), static_cast<size_t>(2));
  EXPECT_EQ(result[0].name, "foo");
  EXPECT_FALSE(result[0].should_expand);
  EXPECT_EQ(result[1].name, "bar");
  EXPECT_TRUE(result[1].should_expand);
}

////////////////////////////////////////////////////////////////////////////////
/// test_expandedSubAttribute
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_expandedSubAttribute) {
  std::string input = "foo[*].bar";
  std::vector<AttributeName> result;

  ParseAttributeString(input, result, true);

  EXPECT_EQ(result.size(), static_cast<size_t>(2));
  EXPECT_EQ(result[0].name, "foo");
  EXPECT_TRUE(result[0].should_expand);
  EXPECT_EQ(result[1].name, "bar");
  EXPECT_FALSE(result[1].should_expand);
}

////////////////////////////////////////////////////////////////////////////////
/// test_invalidAttributeAfterExpand
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_invalidAttributeAfterExpand) {
  std::string input = "foo[*]bar";
  std::vector<AttributeName> result;

  try {
    ParseAttributeString(input, result, false);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
  }

  try {
    ParseAttributeString(input, result, true);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_SERVER_ATTRIBUTE_PARSER_FAILED);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test_nonClosing[
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_nonClosingBracket) {
  std::string input = "foo[*bar";
  std::vector<AttributeName> result;

  try {
    ParseAttributeString(input, result, false);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
  }

  try {
    ParseAttributeString(input, result, true);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_SERVER_ATTRIBUTE_PARSER_FAILED);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test_nonClosing[2
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_nonClosingBracket2) {
  std::string input = "foo[ * ].baz";
  std::vector<AttributeName> result;

  try {
    ParseAttributeString(input, result, false);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
  }

  try {
    ParseAttributeString(input, result, true);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_SERVER_ATTRIBUTE_PARSER_FAILED);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test_nonAsterisk
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_nonAsterisk) {
  std::string input = "foo[0]";
  std::vector<AttributeName> result;

  try {
    ParseAttributeString(input, result, false);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
  }

  try {
    ParseAttributeString(input, result, true);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_SERVER_ATTRIBUTE_PARSER_FAILED);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test_nonAsterisk
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_nonAsterisk2) {
  std::string input = "foo[0].value";
  std::vector<AttributeName> result;

  try {
    ParseAttributeString(input, result, false);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
  }

  try {
    ParseAttributeString(input, result, true);
    EXPECT_TRUE(false);
  } catch (Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_SERVER_ATTRIBUTE_PARSER_FAILED);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test_reverseTransform
////////////////////////////////////////////////////////////////////////////////
/*
TEST(AttributeNameParserTest, test_reverseTransform) {
  std::string input = "foo[*].bar.baz[*]";
  std::vector<AttributeName> result;
  SdbParseAttributeString(input, result, true);

  std::string output = "";
  SdbAttributeNamesToString(result, output);
  EXPECT_EQ(output, input);
}
*/

////////////////////////////////////////////////////////////////////////////////
/// test_reverseTransformSimple
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_reverseTransformSimple) {
  std::string input = "i";
  std::vector<AttributeName> result;
  ParseAttributeString(input, result, false);

  std::string output = "";
  AttributeNamesToString(result, output);
  EXPECT_EQ(output, input);
}

////////////////////////////////////////////////////////////////////////////////
/// test_reverseTransformSimpleMultiAttributes
////////////////////////////////////////////////////////////////////////////////

TEST(AttributeNameParserTest, test_reverseTransformSimpleMultiAttributes) {
  std::string input = "a.j";
  std::vector<AttributeName> result;
  ParseAttributeString(input, result, false);

  std::string output = "";
  AttributeNamesToString(result, output);
  EXPECT_EQ(output, input);
}
