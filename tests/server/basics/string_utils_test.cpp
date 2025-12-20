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

#include <iomanip>
#include <sstream>

#include "basics/common.h"
#include "basics/directories.h"
#include "basics/files.h"
#include "basics/string_utils.h"
#include "basics/utf8_helper.h"
#include "gtest/gtest.h"
#include "icu-helper.h"

using namespace sdb;
using namespace sdb::basics;
using namespace std::string_literals;

class StringUtilsTest : public ::testing::Test {
 protected:
  StringUtilsTest() {}
};

////////////////////////////////////////////////////////////////////////////////
/// test_tolower
////////////////////////////////////////////////////////////////////////////////

TEST_F(StringUtilsTest, test_tolower) {
  EXPECT_EQ(absl::AsciiStrToLower(""), "");
  EXPECT_EQ(absl::AsciiStrToLower(" "), " ");
  EXPECT_EQ(absl::AsciiStrToLower("12345"), "12345");
  EXPECT_EQ(absl::AsciiStrToLower("a"), "a");
  EXPECT_EQ(absl::AsciiStrToLower("A"), "a");
  EXPECT_EQ(absl::AsciiStrToLower("ä"), "ä");
  EXPECT_EQ(absl::AsciiStrToLower("Ä"), "Ä");
  EXPECT_EQ(absl::AsciiStrToLower("HeLlO WoRlD!"), "hello world!");
  EXPECT_EQ(absl::AsciiStrToLower("hello-world-nono "), "hello-world-nono ");
  EXPECT_EQ(absl::AsciiStrToLower("HELLo-world-NONO "), "hello-world-nono ");
  EXPECT_EQ(absl::AsciiStrToLower(" The quick \r\nbrown Fox"),
            " the quick \r\nbrown fox");
}

////////////////////////////////////////////////////////////////////////////////
/// test_toupper
////////////////////////////////////////////////////////////////////////////////

TEST_F(StringUtilsTest, test_toupper) {
  EXPECT_EQ(absl::AsciiStrToUpper(""), "");
  EXPECT_EQ(absl::AsciiStrToUpper(" "), " ");
  EXPECT_EQ(absl::AsciiStrToUpper("12345"), "12345");
  EXPECT_EQ(absl::AsciiStrToUpper("a"), "A");
  EXPECT_EQ(absl::AsciiStrToUpper("A"), "A");
  EXPECT_EQ(absl::AsciiStrToUpper("ä"), "ä");
  EXPECT_EQ(absl::AsciiStrToUpper("Ä"), "Ä");
  EXPECT_EQ(absl::AsciiStrToUpper("HeLlO WoRlD!"), "HELLO WORLD!");
  EXPECT_EQ(absl::AsciiStrToUpper("hello-world-nono "), "HELLO-WORLD-NONO ");
  EXPECT_EQ(absl::AsciiStrToUpper("HELLo-world-NONO "), "HELLO-WORLD-NONO ");
}

////////////////////////////////////////////////////////////////////////////////
/// test_uint64
////////////////////////////////////////////////////////////////////////////////

TEST_F(StringUtilsTest, test_uint64) {
  EXPECT_EQ(0ULL, string_utils::Uint64("abc"s));
  EXPECT_EQ(0ULL, string_utils::Uint64("ABC"s));
  EXPECT_EQ(0ULL, string_utils::Uint64(" foo"s));
  EXPECT_EQ(0ULL, string_utils::Uint64(""s));
  EXPECT_EQ(0ULL, string_utils::Uint64(" "s));
  EXPECT_EQ(12ULL, string_utils::Uint64("012"s));
  EXPECT_EQ(12ULL, string_utils::Uint64("00012"s));
  EXPECT_EQ(1234ULL, string_utils::Uint64("1234"s));
  EXPECT_EQ(1234ULL, string_utils::Uint64("1234a"s));
  EXPECT_EQ(0ULL, string_utils::Uint64("-1"s));
  EXPECT_EQ(0ULL, string_utils::Uint64("-12345"s));
  EXPECT_EQ(1234ULL, string_utils::Uint64("1234.56"s));
  EXPECT_EQ(0ULL,
            string_utils::Uint64("1234567890123456789012345678901234567890"s));
  EXPECT_EQ(0ULL, string_utils::Uint64("@"s));

  EXPECT_EQ(0ULL, string_utils::Uint64("0"s));
  EXPECT_EQ(1ULL, string_utils::Uint64("1"s));
  EXPECT_EQ(12ULL, string_utils::Uint64("12"s));
  EXPECT_EQ(123ULL, string_utils::Uint64("123"s));
  EXPECT_EQ(1234ULL, string_utils::Uint64("1234"s));
  EXPECT_EQ(1234ULL, string_utils::Uint64("01234"s));
  EXPECT_EQ(9ULL, string_utils::Uint64("9"s));
  EXPECT_EQ(9ULL, string_utils::Uint64("09"s));
  EXPECT_EQ(9ULL, string_utils::Uint64("0009"s));
  EXPECT_EQ(12345678ULL, string_utils::Uint64("12345678"s));
  EXPECT_EQ(1234567800ULL, string_utils::Uint64("1234567800"s));
  EXPECT_EQ(1234567890123456ULL, string_utils::Uint64("1234567890123456"s));
  EXPECT_EQ(UINT64_MAX, string_utils::Uint64(std::to_string(UINT64_MAX)));
}

////////////////////////////////////////////////////////////////////////////////
/// test_uint64_trusted
////////////////////////////////////////////////////////////////////////////////

TEST_F(StringUtilsTest, test_uint64_trusted) {
  EXPECT_EQ(0ULL, string_utils::Uint64Trusted("0"));
  EXPECT_EQ(1ULL, string_utils::Uint64Trusted("1"));
  EXPECT_EQ(12ULL, string_utils::Uint64Trusted("12"));
  EXPECT_EQ(123ULL, string_utils::Uint64Trusted("123"));
  EXPECT_EQ(1234ULL, string_utils::Uint64Trusted("1234"));
  EXPECT_EQ(1234ULL, string_utils::Uint64Trusted("01234"));
  EXPECT_EQ(9ULL, string_utils::Uint64Trusted("9"));
  EXPECT_EQ(9ULL, string_utils::Uint64Trusted("0009"));
  EXPECT_EQ(12345678ULL, string_utils::Uint64Trusted("12345678"));
  EXPECT_EQ(1234567800ULL, string_utils::Uint64Trusted("1234567800"));
  EXPECT_EQ(1234567890123456ULL,
            string_utils::Uint64Trusted("1234567890123456"));
  EXPECT_EQ(UINT64_MAX,
            string_utils::Uint64Trusted(std::to_string(UINT64_MAX)));
}

TEST_F(StringUtilsTest, test_encodeHex) {
  EXPECT_EQ("", string_utils::EncodeHex(""));

  EXPECT_EQ("00", string_utils::EncodeHex(std::string("\x00", 1)));
  EXPECT_EQ("01", string_utils::EncodeHex("\x01"));
  EXPECT_EQ("02", string_utils::EncodeHex("\x02"));
  EXPECT_EQ("03", string_utils::EncodeHex("\x03"));
  EXPECT_EQ("04", string_utils::EncodeHex("\x04"));
  EXPECT_EQ("05", string_utils::EncodeHex("\x05"));
  EXPECT_EQ("06", string_utils::EncodeHex("\x06"));
  EXPECT_EQ("07", string_utils::EncodeHex("\x07"));
  EXPECT_EQ("08", string_utils::EncodeHex("\x08"));
  EXPECT_EQ("09", string_utils::EncodeHex("\x09"));
  EXPECT_EQ("0a", string_utils::EncodeHex("\x0a"));
  EXPECT_EQ("0b", string_utils::EncodeHex("\x0b"));
  EXPECT_EQ("0c", string_utils::EncodeHex("\x0c"));
  EXPECT_EQ("0d", string_utils::EncodeHex("\x0d"));
  EXPECT_EQ("0e", string_utils::EncodeHex("\x0e"));
  EXPECT_EQ("0f", string_utils::EncodeHex("\x0f"));

  EXPECT_EQ("10", string_utils::EncodeHex("\x10"));
  EXPECT_EQ("42", string_utils::EncodeHex("\x42"));
  EXPECT_EQ("ff", string_utils::EncodeHex("\xff"));
  EXPECT_EQ("aa0009", string_utils::EncodeHex(std::string("\xaa\x00\x09", 3)));
  EXPECT_EQ("000102", string_utils::EncodeHex(std::string("\x00\x01\x02", 3)));
  EXPECT_EQ("00010203",
            string_utils::EncodeHex(std::string("\x00\x01\x02\03", 4)));
  EXPECT_EQ("20", string_utils::EncodeHex(" "));
  EXPECT_EQ("2a2a", string_utils::EncodeHex("**"));
  EXPECT_EQ("616263646566", string_utils::EncodeHex("abcdef"));
  EXPECT_EQ("4142434445462047", string_utils::EncodeHex("ABCDEF G"));
  EXPECT_EQ(
    "54686520517569636b2062726f776e20466f78206a756d706564206f7665722074686520"
    "6c617a7920646f6721",
    string_utils::EncodeHex("The Quick brown Fox jumped over the lazy dog!"));
  EXPECT_EQ(
    "446572204bc3b674c3b67220737072c3bc6e6720c3bc62657220646965204272c3bc636b"
    "65",
    string_utils::EncodeHex("Der Kötör sprüng über die Brücke"));
  EXPECT_EQ("c3a4c3b6c3bcc39fc384c396c39ce282acc2b5",
            string_utils::EncodeHex("äöüßÄÖÜ€µ"));
}

TEST_F(StringUtilsTest, test_decodeHex) {
  EXPECT_EQ("", string_utils::DecodeHex(""));

  EXPECT_EQ(std::string("\x00", 1), string_utils::DecodeHex("00"));
  EXPECT_EQ("\x01", string_utils::DecodeHex("01"));
  EXPECT_EQ("\x02", string_utils::DecodeHex("02"));
  EXPECT_EQ("\x03", string_utils::DecodeHex("03"));
  EXPECT_EQ("\x04", string_utils::DecodeHex("04"));
  EXPECT_EQ("\x05", string_utils::DecodeHex("05"));
  EXPECT_EQ("\x06", string_utils::DecodeHex("06"));
  EXPECT_EQ("\x07", string_utils::DecodeHex("07"));
  EXPECT_EQ("\x08", string_utils::DecodeHex("08"));
  EXPECT_EQ("\x09", string_utils::DecodeHex("09"));
  EXPECT_EQ("\x0a", string_utils::DecodeHex("0a"));
  EXPECT_EQ("\x0b", string_utils::DecodeHex("0b"));
  EXPECT_EQ("\x0c", string_utils::DecodeHex("0c"));
  EXPECT_EQ("\x0d", string_utils::DecodeHex("0d"));
  EXPECT_EQ("\x0e", string_utils::DecodeHex("0e"));
  EXPECT_EQ("\x0f", string_utils::DecodeHex("0f"));
  EXPECT_EQ("\x0a", string_utils::DecodeHex("0A"));
  EXPECT_EQ("\x0b", string_utils::DecodeHex("0B"));
  EXPECT_EQ("\x0c", string_utils::DecodeHex("0C"));
  EXPECT_EQ("\x0d", string_utils::DecodeHex("0D"));
  EXPECT_EQ("\x0e", string_utils::DecodeHex("0E"));
  EXPECT_EQ("\x0f", string_utils::DecodeHex("0F"));

  EXPECT_EQ("\x1a", string_utils::DecodeHex("1a"));
  EXPECT_EQ("\x2b", string_utils::DecodeHex("2b"));
  EXPECT_EQ("\x3c", string_utils::DecodeHex("3c"));
  EXPECT_EQ("\x4d", string_utils::DecodeHex("4d"));
  EXPECT_EQ("\x5e", string_utils::DecodeHex("5e"));
  EXPECT_EQ("\x6f", string_utils::DecodeHex("6f"));
  EXPECT_EQ("\x7a", string_utils::DecodeHex("7A"));
  EXPECT_EQ("\x8b", string_utils::DecodeHex("8B"));
  EXPECT_EQ("\x9c", string_utils::DecodeHex("9C"));
  EXPECT_EQ("\xad", string_utils::DecodeHex("AD"));
  EXPECT_EQ("\xbe", string_utils::DecodeHex("BE"));
  EXPECT_EQ("\xcf", string_utils::DecodeHex("CF"));
  EXPECT_EQ("\xdf", string_utils::DecodeHex("df"));
  EXPECT_EQ("\xef", string_utils::DecodeHex("eF"));
  EXPECT_EQ("\xff", string_utils::DecodeHex("ff"));

  EXPECT_EQ(" ", string_utils::DecodeHex("20"));
  EXPECT_EQ("**", string_utils::DecodeHex("2a2a"));
  EXPECT_EQ("abcdef", string_utils::DecodeHex("616263646566"));
  EXPECT_EQ("ABCDEF G", string_utils::DecodeHex("4142434445462047"));

  EXPECT_EQ(
    "The Quick brown Fox jumped over the lazy dog!",
    string_utils::DecodeHex("54686520517569636b2062726f776e20466f78206a756d706"
                            "564206f76657220746865206c617a7920646f6721"));
  EXPECT_EQ(
    "Der Kötör sprüng über die Brücke",
    string_utils::DecodeHex("446572204bc3b674c3b67220737072c3bc6e6720c3b"
                            "c62657220646965204272c3bc636b65"));
  EXPECT_EQ("äöüßÄÖÜ€µ",
            string_utils::DecodeHex("c3a4c3b6c3bcc39fc384c396c39ce282acc2b5"));

  EXPECT_EQ("", string_utils::DecodeHex("1"));
  EXPECT_EQ("", string_utils::DecodeHex(" "));
  EXPECT_EQ("", string_utils::DecodeHex(" 2"));
  EXPECT_EQ("", string_utils::DecodeHex("1 "));
  EXPECT_EQ("", string_utils::DecodeHex("12 "));
  EXPECT_EQ("", string_utils::DecodeHex("x"));
  EXPECT_EQ("", string_utils::DecodeHex("X"));
  EXPECT_EQ("", string_utils::DecodeHex("@@@"));
  EXPECT_EQ("", string_utils::DecodeHex("111"));
  EXPECT_EQ("", string_utils::DecodeHex("1 2 3"));
  EXPECT_EQ("", string_utils::DecodeHex("1122334"));
  EXPECT_EQ("", string_utils::DecodeHex("112233 "));
  EXPECT_EQ("", string_utils::DecodeHex(" 112233"));
  EXPECT_EQ("", string_utils::DecodeHex("abcdefgh"));
}

TEST_F(StringUtilsTest, test_encodeURLComponent) {
  EXPECT_EQ("", string_utils::EncodeUriComponent(""));
  EXPECT_EQ("%20", string_utils::EncodeUriComponent(" "));
  EXPECT_EQ("%25", string_utils::EncodeUriComponent("%"));
  EXPECT_EQ("abc", string_utils::EncodeUriComponent("abc"));
  EXPECT_EQ("abc%20abc", string_utils::EncodeUriComponent("abc abc"));
  EXPECT_EQ(".!%3A%24%25%40b013%2F-",
            string_utils::EncodeUriComponent(".!:$%@b013/-"));

  std::string result;

  string_utils::EncodeUriComponent(result, "", 0);
  EXPECT_EQ("", result);

  string_utils::EncodeUriComponent(result, " ", 1);
  EXPECT_EQ("%20", result);

  string_utils::EncodeUriComponent(result, "%", 1);
  EXPECT_EQ("%20%25", result);

  string_utils::EncodeUriComponent(result, "abc", 3);
  EXPECT_EQ("%20%25abc", result);

  string_utils::EncodeUriComponent(result, "abc abc", 7);
  EXPECT_EQ("%20%25abcabc%20abc", result);

  string_utils::EncodeUriComponent(result, ".!:$%@b013/-", 12);
  EXPECT_EQ("%20%25abcabc%20abc.!%3A%24%25%40b013%2F-", result);
}

TEST_F(StringUtilsTest, formatSize) {
  EXPECT_EQ("0 bytes", string_utils::FormatSize(0ULL));
  EXPECT_EQ("1 byte", string_utils::FormatSize(1ULL));
  EXPECT_EQ("2 bytes", string_utils::FormatSize(2ULL));
  EXPECT_EQ("3 bytes", string_utils::FormatSize(3ULL));
  EXPECT_EQ("16 bytes", string_utils::FormatSize(16ULL));
  EXPECT_EQ("255 bytes", string_utils::FormatSize(255ULL));
  EXPECT_EQ("256 bytes", string_utils::FormatSize(256ULL));
  EXPECT_EQ("999 bytes", string_utils::FormatSize(999ULL));
  EXPECT_EQ("1.0 KB", string_utils::FormatSize(1000ULL));
  EXPECT_EQ("1.0 KB", string_utils::FormatSize(1001ULL));
  EXPECT_EQ("1.9 KB", string_utils::FormatSize(1999ULL));
  EXPECT_EQ("2.0 KB", string_utils::FormatSize(2000ULL));
  EXPECT_EQ("9.9 KB", string_utils::FormatSize(9999ULL));
  EXPECT_EQ("10.0 KB", string_utils::FormatSize(10000ULL));
  EXPECT_EQ("999.9 KB", string_utils::FormatSize(999999ULL));
  EXPECT_EQ("1.0 MB", string_utils::FormatSize(1000000ULL));
  EXPECT_EQ("16.0 MB", string_utils::FormatSize(16000000ULL));
  EXPECT_EQ("240.0 MB", string_utils::FormatSize(240000000ULL));
  EXPECT_EQ("1.0 GB", string_utils::FormatSize(1000000000ULL));
  EXPECT_EQ("17.8 GB", string_utils::FormatSize(17800000000ULL));
  EXPECT_EQ("246.4 GB", string_utils::FormatSize(246463000000ULL));
  EXPECT_EQ("246.4 GB", string_utils::FormatSize(246463000000ULL));
  EXPECT_EQ("999.9 GB", string_utils::FormatSize(999900000000ULL));
  EXPECT_EQ("1.0 TB", string_utils::FormatSize(1000000000000ULL));
  EXPECT_EQ("1.9 TB", string_utils::FormatSize(1900000000000ULL));
}
