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

#include <functional>
#include <memory>
#include <string>

#include "basics/common.h"
#include "basics/logger/escaper.h"
#include "gtest/gtest.h"

using namespace sdb;

class EscaperTest : public ::testing::Test {
 protected:
  std::string _ascii_visible_chars;
  std::string _big_string;
  std::string _control_chars;

  EscaperTest() {
    for (int i = 33; i <= 126; ++i) {
      _ascii_visible_chars += i;
    }
    for (int i = 0; i <= 31; ++i) {
      _control_chars += i;
    }
    while (_big_string.size() < 1000) {
      _big_string += _ascii_visible_chars;
    }
  }
};

void VerifyExpectedValues(
  const std::string& input_string, const std::string& expected_output,
  const std::function<void(const std::string&, std::string&)>& writer_fn) {
  std::string output;
  writer_fn(input_string, output);
  EXPECT_EQ(output, expected_output);
}

TEST_F(EscaperTest, test_suppress_control_retain_unicode) {
  std::function<void(const std::string&, std::string&)> writer_fn =
    &Escaper<ControlCharsSuppressor,
             UnicodeCharsRetainer>::writeIntoOutputBuffer;
  VerifyExpectedValues(_ascii_visible_chars, _ascii_visible_chars, writer_fn);
  VerifyExpectedValues(_big_string, _big_string, writer_fn);
  VerifyExpectedValues(_control_chars, "                                ",
                       writer_fn);
  VerifyExpectedValues("€", "€", writer_fn);
  VerifyExpectedValues(" €  ", " €  ", writer_fn);
  VerifyExpectedValues("mötör", "mötör", writer_fn);
  VerifyExpectedValues("\t mötör", "  mötör", writer_fn);
  VerifyExpectedValues("maçã", "maçã", writer_fn);
  VerifyExpectedValues("\nmaçã", " maçã", writer_fn);
  VerifyExpectedValues("犬", "犬", writer_fn);
  VerifyExpectedValues("犬\r", "犬 ", writer_fn);
  VerifyExpectedValues("", "", writer_fn);
  VerifyExpectedValues("a", "a", writer_fn);
  VerifyExpectedValues("𐍈", "𐍈", writer_fn);    //\uD800\uDF48
  VerifyExpectedValues("𐍈 ", "𐍈 ", writer_fn);  //\uD800\uDF48
  std::string valid_unicode = "€";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "? ", writer_fn);
  VerifyExpectedValues("\x07", " ", writer_fn);
  VerifyExpectedValues(std::string("\0", 1), " ", writer_fn);
  valid_unicode = "𐍈";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "? ", writer_fn);
}

TEST_F(EscaperTest, test_suppress_control_escape_unicode) {
  std::function<void(const std::string&, std::string&)> writer_fn =
    &Escaper<ControlCharsSuppressor,
             UnicodeCharsEscaper>::writeIntoOutputBuffer;
  VerifyExpectedValues(_ascii_visible_chars, _ascii_visible_chars, writer_fn);
  VerifyExpectedValues(_big_string, _big_string, writer_fn);
  VerifyExpectedValues(_control_chars, "                                ",
                       writer_fn);
  VerifyExpectedValues("€", "\\u20AC", writer_fn);
  VerifyExpectedValues(" €  ", " \\u20AC  ", writer_fn);
  VerifyExpectedValues("mötör", "m\\u00F6t\\u00F6r", writer_fn);
  VerifyExpectedValues("\tmötör", " m\\u00F6t\\u00F6r", writer_fn);
  VerifyExpectedValues("maçã", "ma\\u00E7\\u00E3", writer_fn);
  VerifyExpectedValues("\nmaçã", " ma\\u00E7\\u00E3", writer_fn);
  VerifyExpectedValues("犬", "\\u72AC", writer_fn);
  VerifyExpectedValues("犬\r", "\\u72AC ", writer_fn);
  VerifyExpectedValues("", "", writer_fn);
  VerifyExpectedValues("a", "a", writer_fn);
  VerifyExpectedValues("𐍈", "\\uD800\\uDF48", writer_fn);    //\uD800\uDF48
  VerifyExpectedValues("𐍈 ", "\\uD800\\uDF48 ", writer_fn);  //\uD800\uDF48
  std::string valid_unicode = "€";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "? ", writer_fn);
  valid_unicode = "𐍈";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "? ", writer_fn);
  VerifyExpectedValues("\x07", " ", writer_fn);
  VerifyExpectedValues(std::string("\0", 1), " ", writer_fn);
}

TEST_F(EscaperTest, test_escape_control_retain_unicode) {
  std::function<void(const std::string&, std::string&)> writer_fn =
    &Escaper<ControlCharsEscaper, UnicodeCharsRetainer>::writeIntoOutputBuffer;
  VerifyExpectedValues(_ascii_visible_chars, _ascii_visible_chars, writer_fn);
  VerifyExpectedValues(_big_string, _big_string, writer_fn);
  VerifyExpectedValues(
    _control_chars,
    "\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\b\\t\\n\\x0B\\f\\r"
    "\\x0E\\x0F\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1A\\x1B\\"
    "x1C\\x1D\\x1E\\x1F",
    writer_fn);
  VerifyExpectedValues("€", "€", writer_fn);
  VerifyExpectedValues(" €  ", " €  ", writer_fn);
  VerifyExpectedValues("mötör", "mötör", writer_fn);
  VerifyExpectedValues("\tmötör", "\\tmötör", writer_fn);
  VerifyExpectedValues("maçã", "maçã", writer_fn);
  VerifyExpectedValues("\nmaçã", "\\nmaçã", writer_fn);
  VerifyExpectedValues("犬", "犬", writer_fn);
  VerifyExpectedValues("犬\r", "犬\\r", writer_fn);
  VerifyExpectedValues("", "", writer_fn);
  VerifyExpectedValues("a", "a", writer_fn);
  VerifyExpectedValues("𐍈", "𐍈", writer_fn);    //\uD800\uDF48
  VerifyExpectedValues("𐍈 ", "𐍈 ", writer_fn);  //\uD800\uDF48
  std::string valid_unicode = "€";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "?\\n", writer_fn);
  valid_unicode = "𐍈";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "?\\n", writer_fn);
  VerifyExpectedValues("\x07", "\\x07", writer_fn);
  VerifyExpectedValues(std::string("\0", 1), "\\x00", writer_fn);
}

TEST_F(EscaperTest, test_escape_control_escape_unicode) {
  std::function<void(const std::string&, std::string&)> writer_fn =
    &Escaper<ControlCharsEscaper, UnicodeCharsEscaper>::writeIntoOutputBuffer;
  VerifyExpectedValues(_ascii_visible_chars, _ascii_visible_chars, writer_fn);
  VerifyExpectedValues(_big_string, _big_string, writer_fn);
  VerifyExpectedValues(
    _control_chars,
    "\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\b\\t\\n\\x0B\\f\\r"
    "\\x0E\\x0F\\x10\\x11\\x12\\x13\\x14\\x15\\x16\\x17\\x18\\x19\\x1A\\x1B\\"
    "x1C\\x1D\\x1E\\x1F",
    writer_fn);
  VerifyExpectedValues("€", "\\u20AC", writer_fn);
  VerifyExpectedValues(" €  ", " \\u20AC  ", writer_fn);
  VerifyExpectedValues("mötör", "m\\u00F6t\\u00F6r", writer_fn);
  VerifyExpectedValues("\tmötör", "\\tm\\u00F6t\\u00F6r", writer_fn);
  VerifyExpectedValues("maçã", "ma\\u00E7\\u00E3", writer_fn);
  VerifyExpectedValues("\nmaçã", "\\nma\\u00E7\\u00E3", writer_fn);
  VerifyExpectedValues("犬", "\\u72AC", writer_fn);
  VerifyExpectedValues("犬\r", "\\u72AC\\r", writer_fn);
  VerifyExpectedValues("", "", writer_fn);
  VerifyExpectedValues("a", "a", writer_fn);
  VerifyExpectedValues("𐍈", "\\uD800\\uDF48", writer_fn);    //\uD800\uDF48
  VerifyExpectedValues("𐍈 ", "\\uD800\\uDF48 ", writer_fn);  //\uD800\uDF48
  std::string valid_unicode = "€";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "?\\n", writer_fn);
  valid_unicode = "𐍈";
  VerifyExpectedValues(valid_unicode.substr(0, 1), "?", writer_fn);
  VerifyExpectedValues(valid_unicode.substr(0, 1) + "\n", "?\\n", writer_fn);
  VerifyExpectedValues("\x07", "\\x07", writer_fn);
  VerifyExpectedValues(std::string("\0", 1), "\\x00", writer_fn);
}
