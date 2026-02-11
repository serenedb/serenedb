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

#include <array>
#include <ostream>
#include <string>
#include <utility>

#include "tests-common.h"

namespace vpack {

extern void EnableNativeStringFunctions();
extern void EnableBuiltinStringFunctions();

}  // namespace vpack

TEST(ParserTest, CreateWithoutOptions) {
  ASSERT_VPACK_EXCEPTION(new Parser(nullptr), Exception::kInternalError);
}

TEST(ParserTest, GarbageCatchVPackException) {
  const std::string value("z");

  Parser parser;
  try {
    parser.parse(value);
    ASSERT_TRUE(false);
  } catch (const Exception& ex) {
    ASSERT_STREQ("Expecting digit", ex.what());
  }
}

TEST(ParserTest, GarbageCatchStdException) {
  const std::string value("z");

  Parser parser;
  try {
    parser.parse(value);
    ASSERT_TRUE(false);
  } catch (const std::exception& ex) {
    ASSERT_STREQ("Expecting digit", ex.what());
  }
}

TEST(ParserTest, Garbage1) {
  const std::string value("z");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, Garbage2) {
  const std::string value("foo");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1U, parser.errorPos());
}

TEST(ParserTest, Garbage3) {
  const std::string value("truth");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, Garbage4) {
  const std::string value("tru");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(2U, parser.errorPos());
}

TEST(ParserTest, Garbage5) {
  const std::string value("truebar");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, Garbage6) {
  const std::string value("fals");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, Garbage7) {
  const std::string value("falselaber");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(5U, parser.errorPos());
}

TEST(ParserTest, Garbage8) {
  const std::string value("zauberzauber");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, Garbage9) {
  const std::string value("true,");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, Punctuation1) {
  const std::string value(",");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, Punctuation2) {
  const std::string value("/");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, Punctuation3) {
  const std::string value("@");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, Punctuation4) {
  const std::string value(":");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, Punctuation5) {
  const std::string value("!");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, NullInvalid) {
  const std::string value("nork");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1U, parser.errorPos());
}

TEST(ParserTest, Null) {
  const std::string value("null");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Null, 1ULL);

  CheckDump(s, value);
}

TEST(ParserTest, FalseInvalid) {
  const std::string value("fork");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1U, parser.errorPos());
}

TEST(ParserTest, False) {
  const std::string value("false");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Bool, 1ULL);
  ASSERT_FALSE(s.getBool());

  CheckDump(s, value);
}

TEST(ParserTest, TrueInvalid) {
  const std::string value("tork");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1U, parser.errorPos());
}

TEST(ParserTest, True) {
  const std::string value("true");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Bool, 1ULL);
  ASSERT_TRUE(s.getBool());

  CheckDump(s, value);
}

TEST(ParserTest, Zero) {
  const std::string value("0");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::SmallInt, 1ULL);
  ASSERT_EQ(0, s.getSmallIntUnchecked());

  CheckDump(s, value);
}

TEST(ParserTest, ZeroInvalid) {
  const std::string value("00");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1u, parser.errorPos());
}

TEST(ParserTest, NumberIncomplete) {
  const std::string value("-");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0u, parser.errorPos());
}

TEST(ParserTest, Int1) {
  const std::string value("1");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::SmallInt, 1ULL);
  ASSERT_EQ(1, s.getSmallIntUnchecked());

  CheckDump(s, value);
}

TEST(ParserTest, IntM1) {
  const std::string value("-1");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::SmallInt, 1ULL);
  ASSERT_EQ(-1, s.getSmallIntUnchecked());

  CheckDump(s, value);
}

TEST(ParserTest, Int2) {
  const std::string value("100000");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::UInt, 4ULL);
  ASSERT_EQ(100000ULL, s.getUInt());

  CheckDump(s, value);
}

TEST(ParserTest, Int3) {
  const std::string value("-100000");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Int, 4ULL);
  ASSERT_EQ(-100000LL, s.getInt());

  CheckDump(s, value);
}

TEST(ParserTest, UIntMaxNeg) {
  std::string value("-");
  value.append(std::to_string(UINT64_MAX));

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  // handle rounding errors
  ASSERT_DOUBLE_EQ(-18446744073709551615., s.getDouble());
}

TEST(ParserTest, IntMin) {
  const std::string value(std::to_string(INT64_MIN));

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Int, 9ULL);
  ASSERT_EQ(INT64_MIN, s.getInt());

  CheckDump(s, value);
}

TEST(ParserTest, IntMinMinusOne) {
  const std::string value("-9223372036854775809");  // INT64_MIN - 1

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_DOUBLE_EQ(-9223372036854775809., s.getDouble());
}

TEST(ParserTest, IntMax) {
  const std::string value(std::to_string(INT64_MAX));

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::UInt, 9ULL);
  ASSERT_EQ(static_cast<uint64_t>(INT64_MAX), s.getUInt());

  CheckDump(s, value);
}

TEST(ParserTest, IntMaxPlusOne) {
  const std::string value("9223372036854775808");  // INT64_MAX + 1

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::UInt, 9ULL);
  ASSERT_EQ(static_cast<uint64_t>(INT64_MAX) + 1, s.getUInt());

  CheckDump(s, value);
}

TEST(ParserTest, UIntMax) {
  const std::string value(std::to_string(UINT64_MAX));

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::UInt, 9ULL);
  ASSERT_EQ(UINT64_MAX, s.getUInt());

  CheckDump(s, value);
}

TEST(ParserTest, UIntMaxPlusOne) {
  const std::string value("18446744073709551616");  // UINT64_MAX + 1

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_DOUBLE_EQ(18446744073709551616., s.getDouble());
}

TEST(ParserTest, Double1) {
  const std::string value("1.0124");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(1.0124, s.getDouble());

  CheckDump(s, value);
}

TEST(ParserTest, Double2) {
  const std::string value("-1.0124");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(-1.0124, s.getDouble());

  CheckDump(s, value);
}

TEST(ParserTest, DoubleScientificWithoutDot1) {
  const std::string value("-3e12");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(-3.e12, s.getDouble());

  const std::string value_out("-3000000000000");
  CheckDump(s, value_out);
}

TEST(ParserTest, DoubleScientificWithoutDot2) {
  const std::string value("3e12");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(3e12, s.getDouble());

  const std::string value_out("3000000000000");
  CheckDump(s, value_out);
}

TEST(ParserTest, DoubleScientific1) {
  const std::string value("-1.0124e42");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(-1.0124e42, s.getDouble());

  const std::string value_out("-1.0124e+42");
  CheckDump(s, value_out);
}

TEST(ParserTest, DoubleScientific2) {
  const std::string value("-1.0124e+42");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(-1.0124e42, s.getDouble());

  CheckDump(s, value);
}

TEST(ParserTest, DoubleScientific3) {
  const std::string value("3122243.0124e-42");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(3122243.0124e-42, s.getDouble());

  const std::string value_out("3.1222430124e-36");
  CheckDump(s, value_out);
}

TEST(ParserTest, DoubleScientific4) {
  const std::string value("2335431.0124E-42");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(2335431.0124E-42, s.getDouble());

  const std::string value_out("2.3354310124e-36");
  CheckDump(s, value_out);
}

TEST(ParserTest, DoubleScientific5) {
  const std::string value("3122243.0124e+42");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_EQ(3122243.0124e+42, s.getDouble());

  const std::string value_out("3.1222430124e+48");
  CheckDump(s, value_out);
}

TEST(ParserTest, DoubleNeg) {
  const std::string value("-184467440737095516161");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_DOUBLE_EQ(-184467440737095516161., s.getDouble());
}

TEST(ParserTest, DoublePrecision1) {
  const std::string value("0.3");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_DOUBLE_EQ(0.3, s.getDouble());
}

TEST(ParserTest, DoublePrecision2) {
  const std::string value("0.33");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_DOUBLE_EQ(0.33, s.getDouble());
}

TEST(ParserTest, DoublePrecision3) {
  const std::string value("0.67");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Double, 9ULL);
  ASSERT_DOUBLE_EQ(0.67, s.getDouble());
}

TEST(ParserTest, DoubleBroken1) {
  const std::string value("1234.");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kParseError);
}

TEST(ParserTest, DoubleBroken2) {
  const std::string value("1234.a");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kParseError);
}

TEST(ParserTest, DoubleBrokenExponent) {
  const std::string value("1234.33e");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kParseError);
}

TEST(ParserTest, DoubleBrokenExponent2) {
  const std::string value("1234.33e-");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kParseError);
}

TEST(ParserTest, DoubleBrokenExponent3) {
  const std::string value("1234.33e+");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kParseError);
}

TEST(ParserTest, DoubleBrokenExponent4) {
  const std::string value("1234.33ea");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kParseError);
}

TEST(ParserTest, DoubleBrokenExponent5) {
  const std::string value(
    "1e22222222222222222222222222222222222222222222222222222222222222");

  ASSERT_VPACK_EXCEPTION(Parser::fromJson(value), Exception::kNumberOutOfRange);
}

TEST(ParserTest, IntMinusInf) {
  const std::string value(
    "-99999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "9999999999999999999999999999999999999999999999999999999999999999");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kNumberOutOfRange);
}

TEST(ParserTest, IntPlusInf) {
  const std::string value(
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999999999999"
    "999999999999999999999999999999999999999999999999999999999999999");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kNumberOutOfRange);
}

TEST(ParserTest, DoubleMinusInf) {
  const std::string value("-1.2345e999");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kNumberOutOfRange);
}

TEST(ParserTest, DoublePlusInf) {
  const std::string value("1.2345e999");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kNumberOutOfRange);
}

TEST(ParserTest, Empty) {
  const std::string value("");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, WhitespaceOnly) {
  const std::string value("  ");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1U, parser.errorPos());
}

TEST(ParserTest, LongerString1) {
  const std::string value("\"01234567890123456789012345678901\"");
  ASSERT_EQ(
    0U, (value.size() - 2) % 16);  // string payload should be a multiple of 16

  Options options;
  options.validate_utf8_strings = true;

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.builder().slice());

  std::string parsed = s.copyString();
  ASSERT_EQ(value.substr(1, value.size() - 2), parsed);
}

TEST(ParserTest, LongerString2) {
  const std::string value("\"this is a long string (longer than 16 bytes)\"");

  Parser parser;
  parser.parse(value);
  Slice s(parser.builder().slice());

  std::string parsed = s.copyString();
  ASSERT_EQ(value.substr(1, value.size() - 2), parsed);
}

TEST(ParserTest, UnterminatedStringLiteral) {
  const std::string value("\"der hund");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(8U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeSequence) {
  const std::string value("\"der hund\\");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(9U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence1) {
  const std::string value("\"\\u\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence2) {
  const std::string value("\"\\u0\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence3) {
  const std::string value("\"\\u01\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(5U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence4) {
  const std::string value("\"\\u012\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(6U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence5) {
  const std::string value("\"\\u");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(2U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence6) {
  const std::string value("\"\\u1");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence7) {
  const std::string value("\"\\u12");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence8) {
  const std::string value("\"\\u123");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(5U, parser.errorPos());
}

TEST(ParserTest, UnterminatedEscapeUnicodeSequence9) {
  const std::string value("\"\\u1234");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(6U, parser.errorPos());
}

TEST(ParserTest, InvalidEscapeUnicodeSequence1) {
  const std::string value("\"\\uz\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, InvalidEscapeUnicodeSequence2) {
  const std::string value("\"\\uzzzz\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, InvalidEscapeUnicodeSequence3) {
  const std::string value("\"\\U----\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(2U, parser.errorPos());
}

TEST(ParserTest, InvalidEscapeSequence1) {
  const std::string value("\"\\x\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(2U, parser.errorPos());
}

TEST(ParserTest, InvalidEscapeSequence2) {
  const std::string value("\"\\z\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(2U, parser.errorPos());
}

TEST(ParserTest, StringLiteral) {
  const std::string value("\"der hund ging in den wald und aß den fuxx\"");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  const std::string correct = "der hund ging in den wald und aß den fuxx";
  CheckBuild(s, ValueType::String, 1 + correct.size());
  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  std::string value_out = "\"der hund ging in den wald und aß den fuxx\"";
  CheckDump(s, value_out);
}

TEST(ParserTest, StringLiteralEmpty) {
  const std::string value("\"\"");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::String, 1ULL);
  std::string correct;
  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  CheckDump(s, value);
}

TEST(ParserTest, StringTwoByteUTF8) {
  Options options;
  options.validate_utf8_strings = true;

  const std::string value("\"\xc2\xa2\"");

  Parser parser(&options);
  parser.parse(value);

  std::shared_ptr<Builder> b = parser.steal();
  Slice s(b->slice());
  ASSERT_EQ(value, s.toJson());
}

TEST(ParserTest, StringThreeByteUTF8) {
  Options options;
  options.validate_utf8_strings = true;

  const std::string value("\"\xe2\x82\xac\"");

  Parser parser(&options);
  parser.parse(value);

  std::shared_ptr<Builder> b = parser.steal();
  Slice s(b->slice());
  ASSERT_EQ(value, s.toJson());
}

TEST(ParserTest, StringFourByteUTF8) {
  Options options;
  options.validate_utf8_strings = true;

  const std::string value("\"\xf0\xa4\xad\xa2\"");

  Parser parser(&options);
  parser.parse(value);

  std::shared_ptr<Builder> b = parser.steal();
  Slice s(b->slice());
  ASSERT_EQ(value, s.toJson());
}

TEST(ParserTest, StringLiteralInvalidUtfValue1) {
  Options options;
  options.validate_utf8_strings = true;

  std::string value;
  value.push_back('"');
  value.push_back(static_cast<unsigned char>(0x80));
  value.push_back('"');

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kInvalidUtf8Sequence);
  ASSERT_EQ(1U, parser.errorPos());

  options.validate_utf8_strings = false;
  ASSERT_EQ(1ULL, parser.parse(value));
}

TEST(ParserTest, StringLiteralInvalidUtfValue2) {
  Options options;
  options.validate_utf8_strings = true;

  std::string value;
  value.push_back('"');
  value.push_back(static_cast<unsigned char>(0xff));
  value.push_back(static_cast<unsigned char>(0xff));
  value.push_back('"');

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kInvalidUtf8Sequence);
  ASSERT_EQ(1U, parser.errorPos());
  options.validate_utf8_strings = false;
  ASSERT_EQ(1ULL, parser.parse(value));
}

TEST(ParserTest, StringLiteralInvalidUtfValueLongString) {
  Options options;
  options.validate_utf8_strings = true;

  std::string value;
  value.push_back('"');
  for (size_t i = 0; i < 100; ++i) {
    value.push_back(static_cast<unsigned char>(0x80));
  }
  value.push_back('"');

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kInvalidUtf8Sequence);
  ASSERT_EQ(1U, parser.errorPos());
  options.validate_utf8_strings = false;
  ASSERT_EQ(1ULL, parser.parse(value));
}

TEST(ParserTest, StringLiteralControlCharacter) {
  for (char c = 0; c < 0x20; c++) {
    std::string value;
    value.push_back('"');
    value.push_back(c);
    value.push_back('"');

    Parser parser;
    ASSERT_VPACK_EXCEPTION(parser.parse(value),
                           Exception::kUnexpectedControlCharacter);
    ASSERT_EQ(1U, parser.errorPos());
  }
}

TEST(ParserTest, StringLiteralUnfinishedUtfSequence1) {
  const std::string value("\"\\u\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(3U, parser.errorPos());
}

TEST(ParserTest, StringLiteralUnfinishedUtfSequence2) {
  const std::string value("\"\\u0\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, StringLiteralUnfinishedUtfSequence3) {
  const std::string value("\"\\u01\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(5U, parser.errorPos());
}

TEST(ParserTest, StringLiteralUnfinishedUtfSequence4) {
  const std::string value("\"\\u012\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(6U, parser.errorPos());
}

TEST(ParserTest, StringLiteralUtf8SequenceLowerCase) {
  const std::string value("\"der m\\u00d6ter\"");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::String, 11ULL);
  std::string correct = "der m\xc3\x96ter";
  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  const std::string value_out("\"der mÖter\"");
  CheckDump(s, value_out);
}

TEST(ParserTest, StringLiteralUtf8SequenceUpperCase) {
  const std::string value("\"der m\\u00D6ter\"");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  std::string correct = "der mÖter";
  CheckBuild(s, ValueType::String, 1 + correct.size());
  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  CheckDump(s, std::string("\"der mÖter\""));
}

TEST(ParserTest, StringLiteralUtf8Chars) {
  const std::string value("\"der mötör klötörte mät dän fößen\"");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  std::string correct = "der mötör klötörte mät dän fößen";
  CheckBuild(s, ValueType::String, 1 + correct.size());
  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  CheckDump(s, value);
}

TEST(ParserTest, StringLiteralWithSpecials) {
  // std::string const
  // value("\"der\\thund\\nging\\rin\\v\\fden\\\\wald\\\"und\\b\\nden'fux\"");
  const std::string value(
    "\"\\n\\t\\f\\b\\u000Bder\\thund\\nging\\rin\\fden\\\\wald\\\"und\\b\\nde"
    "n'fux\"");

  Parser parser;
  ValueLength len = parser.parse(value);

  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  std::string correct =
    "\n\t\f\b\u000Bder\thund\nging\rin\fden\\wald\"und\b\nden'fux";
  CheckBuild(s, ValueType::String, 1 + correct.size());

  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  const std::string value_out(
    "\"\\n\\t\\f\\b\\u000Bder\\thund\\nging\\rin\\fden\\\\wald\\\"und\\b\\nde"
    "n'fux\"");
  CheckDump(s, value_out);
}

TEST(ParserTest, StringLiteralWithSurrogatePairs) {
  const std::string value("\"\\ud800\\udc00\\udbff\\udfff\\udbc8\\udf45\"");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  std::string correct = "\xf0\x90\x80\x80\xf4\x8f\xbf\xbf\xf4\x82\x8d\x85";
  CheckBuild(s, ValueType::String, 1 + correct.size());
  ASSERT_EQ(correct, s.stringView());
  ASSERT_EQ(correct, s.copyString());

  const std::string value_out(
    "\"\xf0\x90\x80\x80\xf4\x8f\xbf\xbf\xf4\x82\x8d\x85\"");
  CheckDump(s, value_out);
}

TEST(ParserTest, StringLiteralWithInvalidSurrogatePairs) {
  constexpr std::array<std::string_view, 6> kValues = {
    "\"\\udc89\"",         // low surrogate, not preceeded by high surrogate
    "\"\\udc89\\udc89\"",  // 2 low surrogates
    "\"\\ud801\"",         // high surrogate, not followed by low surrogate
    "\"\\ud801a\"",        // high surrogate, not followed by low surrogate
    "\"\\ud801ab\"",       // high surrogate, not followed by low surrogate
    "\"\\ud801\\ud801\"",  // 2 high surrogates
  };

  Options options;
  options.validate_utf8_strings = true;

  Parser parser(&options);
  for (const auto& value : kValues) {
    ASSERT_VPACK_EXCEPTION(parser.parse(value),
                           Exception::kInvalidUtf8Sequence);
  }
}

TEST(ParserTest, StringLiteralWithInvalidSurrogatePairsDisabled) {
  std::array<std::pair<std::string_view, size_t>, 6> values = {
    std::make_pair(
      "\"\\udc89\"",
      size_t(0)),  // low surrogate, not preceeded by high surrogate
    std::make_pair("\"\\udc89\\udc89\"", size_t(0)),  // 2 low surrogates
    std::make_pair("\"\\ud801\"",
                   size_t(3)),  // high surrogate, not followed by low surrogate
    std::make_pair("\"\\ud801a\"",
                   size_t(4)),  // high surrogate, not followed by low surrogate
    std::make_pair("\"\\ud801ab\"",
                   size_t(5)),  // high surrogate, not followed by low surrogate
    std::make_pair("\"\\ud801\\ud801\"",
                   size_t(3)),  // 2 high surrogates
  };

  Options options;
  options.validate_utf8_strings = false;

  for (const auto& [value, length] : values) {
    Parser parser(&options);
    parser.parse(value);
    auto builder = parser.steal();
    ASSERT_TRUE(builder->slice().isString());
    ASSERT_EQ(length, builder->slice().stringView().size());
  }
}

TEST(ParserTest, EmptyArray) {
  const std::string value("[]");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 1);
  ASSERT_EQ(0ULL, s.length());

  CheckDump(s, value);
}

TEST(ParserTest, WhitespacedArray) {
  const std::string value("  [    ]   ");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 1);
  ASSERT_EQ(0ULL, s.length());

  const std::string value_out = "[]";
  CheckDump(s, value_out);
}

TEST(ParserTest, Array1) {
  const std::string value("[1]");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 3);
  ASSERT_EQ(1ULL, s.length());
  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(1ULL, ss.getUInt());

  CheckDump(s, value);
}

TEST(ParserTest, Array2) {
  const std::string value("[1,2]");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 4);
  ASSERT_EQ(2ULL, s.length());
  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(1ULL, ss.getUInt());
  ss = s.at(1);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(2ULL, ss.getUInt());

  CheckDump(s, value);
}

TEST(ParserTest, Array3) {
  const std::string value("[-1,2, 4.5, 3, -99.99]");
  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 29);
  ASSERT_EQ(5ULL, s.length());

  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(-1LL, ss.getInt());

  ss = s.at(1);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(2ULL, ss.getUInt());

  ss = s.at(2);
  CheckBuild(ss, ValueType::Double, 9);
  ASSERT_EQ(4.5, ss.getDouble());

  ss = s.at(3);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(3ULL, ss.getUInt());

  ss = s.at(4);
  CheckBuild(ss, ValueType::Double, 9);
  ASSERT_EQ(-99.99, ss.getDouble());

  const std::string value_out = "[-1,2,4.5,3,-99.99]";
  CheckDump(s, value_out);
}

TEST(ParserTest, Array4) {
  const std::string value(
    "[\"foo\", \"bar\", \"baz\", null, true, false, -42.23 ]");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 34);
  ASSERT_EQ(7ULL, s.length());

  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::String, 4);
  std::string correct = "foo";
  ASSERT_EQ(correct, ss.copyString());

  ss = s.at(1);
  CheckBuild(ss, ValueType::String, 4);
  correct = "bar";
  ASSERT_EQ(correct, ss.copyString());

  ss = s.at(2);
  CheckBuild(ss, ValueType::String, 4);
  correct = "baz";
  ASSERT_EQ(correct, ss.copyString());

  ss = s.at(3);
  CheckBuild(ss, ValueType::Null, 1);

  ss = s.at(4);
  CheckBuild(ss, ValueType::Bool, 1);
  ASSERT_TRUE(ss.getBool());

  ss = s.at(5);
  CheckBuild(ss, ValueType::Bool, 1);
  ASSERT_FALSE(ss.getBool());

  ss = s.at(6);
  CheckBuild(ss, ValueType::Double, 9);
  ASSERT_EQ(-42.23, ss.getDouble());

  const std::string value_out =
    "[\"foo\",\"bar\",\"baz\",null,true,false,-42.23]";
  CheckDump(s, value_out);
}

TEST(ParserTest, NestedArray1) {
  const std::string value("[ [ ] ]");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 3);
  ASSERT_EQ(1ULL, s.length());

  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, ss.length());

  const std::string value_out = "[[]]";
  CheckDump(s, value_out);
}

TEST(ParserTest, NestedArray2) {
  const std::string value("[ [ ],[[]],[],[ [[ [], [ ], [ ] ], [ ] ] ], [] ]");
  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 27);
  ASSERT_EQ(5ULL, s.length());

  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, ss.length());

  ss = s.at(1);
  CheckBuild(ss, ValueType::Array, 3);
  ASSERT_EQ(1ULL, ss.length());

  Slice sss = ss.at(0);
  CheckBuild(sss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, sss.length());

  ss = s.at(2);
  CheckBuild(ss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, ss.length());

  ss = s.at(3);
  CheckBuild(ss, ValueType::Array, 13);
  ASSERT_EQ(1ULL, ss.length());

  sss = ss.at(0);
  CheckBuild(sss, ValueType::Array, 11);
  ASSERT_EQ(2ULL, sss.length());

  Slice ssss = sss.at(0);
  CheckBuild(ssss, ValueType::Array, 5);
  ASSERT_EQ(3ULL, ssss.length());

  Slice sssss = ssss.at(0);
  CheckBuild(sssss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, sssss.length());

  sssss = ssss.at(1);
  CheckBuild(sssss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, sssss.length());

  sssss = ssss.at(2);
  CheckBuild(sssss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, sssss.length());

  ssss = sss.at(1);
  CheckBuild(ssss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, ssss.length());

  ss = s.at(4);
  CheckBuild(ss, ValueType::Array, 1);
  ASSERT_EQ(0ULL, ss.length());

  const std::string value_out = "[[],[[]],[],[[[[],[],[]],[]]],[]]";
  CheckDump(s, value_out);
}

TEST(ParserTest, NestedArray3) {
  const std::string value(
    "[ [ \"foo\", [ \"bar\", \"baz\", null ], true, false ], -42.23 ]");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Array, 42);
  ASSERT_EQ(2ULL, s.length());

  Slice ss = s.at(0);
  CheckBuild(ss, ValueType::Array, 28);
  ASSERT_EQ(4ULL, ss.length());

  Slice sss = ss.at(0);
  CheckBuild(sss, ValueType::String, 4);
  std::string correct = "foo";
  ASSERT_EQ(correct, sss.copyString());

  sss = ss.at(1);
  CheckBuild(sss, ValueType::Array, 15);
  ASSERT_EQ(3ULL, sss.length());

  Slice ssss = sss.at(0);
  CheckBuild(ssss, ValueType::String, 4);
  correct = "bar";
  ASSERT_EQ(correct, ssss.copyString());

  ssss = sss.at(1);
  CheckBuild(ssss, ValueType::String, 4);
  correct = "baz";
  ASSERT_EQ(correct, ssss.copyString());

  ssss = sss.at(2);
  CheckBuild(ssss, ValueType::Null, 1);

  sss = ss.at(2);
  CheckBuild(sss, ValueType::Bool, 1);
  ASSERT_TRUE(sss.getBool());

  sss = ss.at(3);
  CheckBuild(sss, ValueType::Bool, 1);
  ASSERT_FALSE(sss.getBool());

  ss = s.at(1);
  CheckBuild(ss, ValueType::Double, 9);
  ASSERT_EQ(-42.23, ss.getDouble());

  const std::string value_out =
    "[[\"foo\",[\"bar\",\"baz\",null],true,false],-42.23]";
  CheckDump(s, value_out);
}

TEST(ParserTest, NestedArrayInvalid1) {
  const std::string value("[ [ ]");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, NestedArrayInvalid2) {
  const std::string value("[ ] ]");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, NestedArrayInvalid3) {
  const std::string value("[ [ \"foo\", [ \"bar\", \"baz\", null ] ]");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(34U, parser.errorPos());
}

TEST(ParserTest, ArrayNestingCloseToLimit1) {
  Options options;
  options.nesting_limit = 6;

  const std::string value("[[[[[]]]]]");

  Parser parser(&options);
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);
}

TEST(ParserTest, ArrayNestingCloseToLimit2) {
  Options options;
  options.nesting_limit = 6;

  const std::string value("[1, [2, [3, [4, [5] ] ] ] ]");

  Parser parser(&options);
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);
}

TEST(ParserTest, ArrayNestingBeyondLimit1) {
  Options options;
  options.nesting_limit = 5;

  const std::string value("[[[[[[]]]]]]");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kTooDeepNesting);
}

TEST(ParserTest, ArrayNestingBeyondLimit2) {
  Options options;
  options.nesting_limit = 5;

  const std::string value("[1, [2, [3, [4, [5, [6] ] ] ] ] ]");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kTooDeepNesting);
}

TEST(ParserTest, ArrayNestingCloseToLimitReusingParser) {
  Options options;
  options.nesting_limit = 6;

  const std::string value("[1, [2, [3, [4, [5] ] ] ] ]");

  Parser parser(&options);
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  // reuse parser object
  len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  // intentionally broken array
  const std::string value_broken("[1, [2, [3, [4, [5");
  ASSERT_VPACK_EXCEPTION(parser.parse(value_broken), Exception::kParseError);

  // parse again with same parser object
  ASSERT_VPACK_EXCEPTION(parser.parse(value_broken), Exception::kParseError);

  // parse a valid value with same parser object
  len = parser.parse(value);
  ASSERT_EQ(1ULL, len);
}

TEST(ParserTest, BrokenArray1) {
  const std::string value("[");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, BrokenArray2) {
  const std::string value("[,");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(1U, parser.errorPos());
}

TEST(ParserTest, BrokenArray3) {
  const std::string value("[1,");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(2U, parser.errorPos());
}

TEST(ParserTest, ShortArrayMembers) {
  std::string value("[");
  for (size_t i = 0; i < 255; ++i) {
    if (i > 0) {
      value.push_back(',');
    }
    value.append(std::to_string(i));
  }
  value.push_back(']');

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  ASSERT_EQ(7ULL, s.head());
  CheckBuild(s, ValueType::Array, 1019);
  ASSERT_EQ(255ULL, s.length());

  for (size_t i = 0; i < 255; ++i) {
    Slice ss = s.at(i);
    if (i <= 9) {
      CheckBuild(ss, ValueType::SmallInt, 1);
    } else {
      CheckBuild(ss, ValueType::UInt, 2);
    }
    ASSERT_EQ(i, ss.getUInt());
  }
}

TEST(ParserTest, LongArrayFewMembers) {
  std::string single("0123456789abcdef");
  single.append(single);
  single.append(single);
  single.append(single);
  single.append(single);
  single.append(single);
  single.append(single);  // 1024 bytes

  std::string value("[");
  for (size_t i = 0; i < 65; ++i) {
    if (i > 0) {
      value.push_back(',');
    }
    value.push_back('"');
    value.append(single);
    value.push_back('"');
  }
  value.push_back(']');

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  ASSERT_EQ(4ULL, s.head());
  CheckBuild(s, ValueType::Array, 66894);
  ASSERT_EQ(65ULL, s.length());

  for (size_t i = 0; i < 65; ++i) {
    Slice ss = s.at(i);
    CheckBuild(ss, ValueType::String, 1029);
    ASSERT_EQ(single, ss.stringView());
  }
}

TEST(ParserTest, LongArrayManyMembers) {
  std::string value("[");
  for (size_t i = 0; i < 256; ++i) {
    if (i > 0) {
      value.push_back(',');
    }
    value.append(std::to_string(i));
  }
  value.push_back(']');

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  ASSERT_EQ(7ULL, s.head());
  CheckBuild(s, ValueType::Array, 1023);
  ASSERT_EQ(256ULL, s.length());

  for (size_t i = 0; i < 256; ++i) {
    Slice ss = s.at(i);
    if (i <= 9) {
      CheckBuild(ss, ValueType::SmallInt, 1);
    } else {
      CheckBuild(ss, ValueType::UInt, 2);
    }
    ASSERT_EQ(i, ss.getUInt());
  }
}

TEST(ParserTest, EmptyObject) {
  const std::string value("{}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 1);
  ASSERT_EQ(0ULL, s.length());

  CheckDump(s, value);
}

TEST(ParserTest, BrokenObject1) {
  const std::string value("{");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, BrokenObject2) {
  const std::string value("{,");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, BrokenObject3) {
  const std::string value("{1,");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(0U, parser.errorPos());
}

TEST(ParserTest, BrokenObject4) {
  const std::string value("{\"foo");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(4U, parser.errorPos());
}

TEST(ParserTest, BrokenObject5) {
  const std::string value("{\"foo\"");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(5U, parser.errorPos());
}

TEST(ParserTest, BrokenObject6) {
  const std::string value("{\"foo\":");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(6U, parser.errorPos());
}

TEST(ParserTest, BrokenObject7) {
  const std::string value("{\"foo\":\"foo");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(10U, parser.errorPos());
}

TEST(ParserTest, BrokenObject8) {
  const std::string value("{\"foo\":\"foo\", ");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(13U, parser.errorPos());
}

TEST(ParserTest, BrokenObject9) {
  const std::string value("{\"foo\":\"foo\", }");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(13U, parser.errorPos());
}

TEST(ParserTest, BrokenObject10) {
  const std::string value("{\"foo\" }");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(6U, parser.errorPos());
}

TEST(ParserTest, BrokenObject11) {
  const std::string value("{\"foo\" : [");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
  ASSERT_EQ(9U, parser.errorPos());
}

TEST(ParserTest, ObjectSimple1) {
  const std::string value("{ \"foo\" : 1}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 8);
  ASSERT_EQ(1ULL, s.length());

  Slice ss = s.keyAt(0);
  CheckBuild(ss, ValueType::String, 4);

  std::string correct = "foo";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(0);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(1, ss.getSmallIntUnchecked());

  std::string value_out = "{\"foo\":1}";
  CheckDump(s, value_out);
}

TEST(ParserTest, ObjectSimple2) {
  const std::string value("{ \"foo\" : \"bar\", \"baz\":true}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 18);
  ASSERT_EQ(2ULL, s.length());

  Slice ss = s.keyAt(0);
  CheckBuild(ss, ValueType::String, 4);
  std::string correct = "baz";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(0);
  CheckBuild(ss, ValueType::Bool, 1);
  ASSERT_TRUE(ss.getBool());

  ss = s.keyAt(1);
  CheckBuild(ss, ValueType::String, 4);
  correct = "foo";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(1);
  CheckBuild(ss, ValueType::String, 4);
  correct = "bar";
  ASSERT_EQ(correct, ss.copyString());

  std::string value_out = "{\"baz\":true,\"foo\":\"bar\"}";
  CheckDump(s, value_out);
}

TEST(ParserTest, ObjectDenseNotation) {
  const std::string value("{\"a\":\"b\",\"c\":\"d\"}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 13);
  ASSERT_EQ(2ULL, s.length());

  Slice ss = s.keyAt(0);
  CheckBuild(ss, ValueType::String, 2);
  std::string correct = "a";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(0);
  CheckBuild(ss, ValueType::String, 2);
  correct = "b";
  ASSERT_EQ(correct, ss.copyString());

  ss = s.keyAt(1);
  CheckBuild(ss, ValueType::String, 2);
  correct = "c";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(1);
  CheckBuild(ss, ValueType::String, 2);
  correct = "d";
  ASSERT_EQ(correct, ss.copyString());

  CheckDump(s, value);
}

TEST(ParserTest, ObjectReservedKeys) {
  const std::string value(
    "{ \"null\" : \"true\", \"false\":\"bar\", \"true\":\"foo\"}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 35);
  ASSERT_EQ(3ULL, s.length());

  Slice ss = s.keyAt(0);
  CheckBuild(ss, ValueType::String, 6);
  std::string correct = "false";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(0);
  CheckBuild(ss, ValueType::String, 4);
  correct = "bar";
  ASSERT_EQ(correct, ss.copyString());

  ss = s.keyAt(1);
  CheckBuild(ss, ValueType::String, 5);
  correct = "null";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(1);
  CheckBuild(ss, ValueType::String, 5);
  correct = "true";
  ASSERT_EQ(correct, ss.copyString());

  ss = s.keyAt(2);
  CheckBuild(ss, ValueType::String, 5);
  correct = "true";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(2);
  CheckBuild(ss, ValueType::String, 4);
  correct = "foo";
  ASSERT_EQ(correct, ss.copyString());

  const std::string value_out =
    "{\"false\":\"bar\",\"null\":\"true\",\"true\":\"foo\"}";
  CheckDump(s, value_out);
}

TEST(ParserTest, ObjectMixed) {
  const std::string value(
    "{\"foo\":null,\"bar\":true,\"baz\":13.53,\"qux\":[1],\"quz\":{}}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 43);
  ASSERT_EQ(5ULL, s.length());

  Slice ss = s.keyAt(0);
  CheckBuild(ss, ValueType::String, 4);
  std::string correct = "bar";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(0);
  CheckBuild(ss, ValueType::Bool, 1);
  ASSERT_TRUE(ss.getBool());

  ss = s.keyAt(1);
  CheckBuild(ss, ValueType::String, 4);
  correct = "baz";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(1);
  CheckBuild(ss, ValueType::Double, 9);
  ASSERT_EQ(13.53, ss.getDouble());

  ss = s.keyAt(2);
  CheckBuild(ss, ValueType::String, 4);
  correct = "foo";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(2);
  CheckBuild(ss, ValueType::Null, 1);

  ss = s.keyAt(3);
  CheckBuild(ss, ValueType::String, 4);
  correct = "qux";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(3);
  CheckBuild(ss, ValueType::Array, 3);

  Slice sss = ss.at(0);
  CheckBuild(sss, ValueType::SmallInt, 1);
  ASSERT_EQ(1ULL, sss.getUInt());

  ss = s.keyAt(4);
  CheckBuild(ss, ValueType::String, 4);
  correct = "quz";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(4);
  CheckBuild(ss, ValueType::Object, 1);
  ASSERT_EQ(0ULL, ss.length());

  const std::string value_out(
    "{\"bar\":true,\"baz\":13.53,\"foo\":null,\"qux\":[1],\"quz\":{}}");
  CheckDump(s, value_out);
}

TEST(ParserTest, ObjectInvalidQuotes) {
  const std::string value("{'foo':'bar' }");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
}

TEST(ParserTest, ObjectMissingQuotes) {
  const std::string value("{foo:\"bar\" }");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
}

TEST(ParserTest, ShortObjectMembers) {
  std::string value("{");
  for (size_t i = 0; i < 255; ++i) {
    if (i > 0) {
      value.push_back(',');
    }
    value.append("\"test");
    if (i < 100) {
      value.push_back('0');
      if (i < 10) {
        value.push_back('0');
      }
    }
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.push_back('}');

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  ASSERT_EQ(0xcULL, s.head());
  CheckBuild(s, ValueType::Object, 3059);
  ASSERT_EQ(255ULL, s.length());

  for (size_t i = 0; i < 255; ++i) {
    Slice sk = s.keyAt(i);
    auto str = sk.stringView();
    std::string key("test");
    if (i < 100) {
      key.push_back('0');
      if (i < 10) {
        key.push_back('0');
      }
    }
    key.append(std::to_string(i));

    ASSERT_EQ(key, str);
    Slice sv = s.valueAt(i);
    if (i <= 9) {
      CheckBuild(sv, ValueType::SmallInt, 1);
    } else {
      CheckBuild(sv, ValueType::UInt, 2);
    }
    ASSERT_EQ(i, sv.getUInt());
  }
}

TEST(ParserTest, LongObjectFewMembers) {
  std::string single("0123456789abcdef");
  single.append(single);
  single.append(single);
  single.append(single);
  single.append(single);
  single.append(single);
  single.append(single);  // 1024 bytes

  std::string value("{");
  for (size_t i = 0; i < 64; ++i) {
    if (i > 0) {
      value.push_back(',');
    }
    value.append("\"test");
    if (i < 100) {
      value.push_back('0');
      if (i < 10) {
        value.push_back('0');
      }
    }
    value.append(std::to_string(i));
    value.append("\":\"");
    value.append(single);
    value.push_back('\"');
  }
  value.push_back('}');

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  ASSERT_EQ(0x0dULL, s.head());  // object with offset size 4
  CheckBuild(s, ValueType::Object, 66633);
  ASSERT_EQ(64ULL, s.length());

  for (size_t i = 0; i < 64; ++i) {
    Slice sk = s.keyAt(i);
    auto str = sk.stringView();
    std::string key("test");
    if (i < 100) {
      key.push_back('0');
      if (i < 10) {
        key.push_back('0');
      }
    }
    key.append(std::to_string(i));

    ASSERT_EQ(key, str);
    Slice sv = s.valueAt(i);
    str = sv.stringView();
    ASSERT_EQ(single, str);
  }
}

TEST(ParserTest, LongObjectManyMembers) {
  std::string value("{");
  for (size_t i = 0; i < 256; ++i) {
    if (i > 0) {
      value.push_back(',');
    }
    value.append("\"test");
    if (i < 100) {
      value.push_back('0');
      if (i < 10) {
        value.push_back('0');
      }
    }
    value.append(std::to_string(i));
    value.append("\":");
    value.append(std::to_string(i));
  }
  value.push_back('}');

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  ASSERT_EQ(0x0cULL, s.head());  // long object
  CheckBuild(s, ValueType::Object, 3071);
  ASSERT_EQ(256ULL, s.length());

  for (size_t i = 0; i < 256; ++i) {
    Slice sk = s.keyAt(i);
    auto str = sk.stringView();
    std::string key("test");
    if (i < 100) {
      key.push_back('0');
      if (i < 10) {
        key.push_back('0');
      }
    }
    key.append(std::to_string(i));

    ASSERT_EQ(str, key);
    Slice sv = s.valueAt(i);
    if (i <= 9) {
      CheckBuild(sv, ValueType::SmallInt, 1);
    } else {
      CheckBuild(sv, ValueType::UInt, 2);
    }
    ASSERT_EQ(i, sv.getUInt());
  }
}

TEST(ParserTest, Utf8Bom) {
  const std::string value("\xef\xbb\xbf{\"foo\":1}");

  Parser parser;
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  CheckBuild(s, ValueType::Object, 8);
  ASSERT_EQ(1ULL, s.length());

  Slice ss = s.keyAt(0);
  CheckBuild(ss, ValueType::String, 4);
  std::string correct = "foo";
  ASSERT_EQ(correct, ss.copyString());
  ss = s.valueAt(0);
  CheckBuild(ss, ValueType::SmallInt, 1);
  ASSERT_EQ(1ULL, ss.getUInt());

  std::string value_out = "{\"foo\":1}";
  CheckDump(s, value_out);
}

TEST(ParserTest, Utf8BomBroken) {
  const std::string value("\xef\xbb");

  Parser parser;
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kParseError);
}

TEST(ParserTest, DuplicateAttributesAllowed) {
  const std::string value("{\"foo\":1,\"foo\":2}");

  Parser parser;
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());

  Slice v = s.get("foo");
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1ULL, v.getUInt());
}

TEST(ParserTest, DuplicateAttributesDisallowed) {
  Options options;
  options.check_attribute_uniqueness = true;

  const std::string value("{\"foo\":1,\"foo\":2}");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value),
                         Exception::kDuplicateAttributeName);
}

TEST(ParserTest, DuplicateAttributesDisallowedUnsortedInput) {
  Options options;
  options.check_attribute_uniqueness = true;

  const std::string value("{\"foo\":1,\"bar\":3,\"foo\":2}");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value),
                         Exception::kDuplicateAttributeName);
}

TEST(ParserTest, DuplicateSubAttributesAllowed) {
  Options options;
  options.check_attribute_uniqueness = true;

  const std::string value(
    "{\"foo\":{\"bar\":1},\"baz\":{\"bar\":2},\"bar\":{\"foo\":23,\"baz\":9}"
    "}");

  Parser parser(&options);
  parser.parse(value);
  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->start());
  Slice v = s.get(std::vector<std::string>({"foo", "bar"}));
  ASSERT_TRUE(v.isNumber());
  ASSERT_EQ(1ULL, v.getUInt());
}

TEST(ParserTest, DuplicateSubAttributesDisallowed) {
  Options options;
  options.check_attribute_uniqueness = true;

  const std::string value(
    "{\"roo\":{\"bar\":1,\"abc\":true,\"def\":7,\"abc\":2}}");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value),
                         Exception::kDuplicateAttributeName);
}

TEST(ParserTest, DuplicateAttributesSortedObjects) {
  Options options;
  options.build_unindexed_objects = false;
  options.check_attribute_uniqueness = true;

  for (size_t i = 1; i < 20; ++i) {
    std::string value;
    value.push_back('{');
    for (size_t j = 0; j < i; ++j) {
      if (j != 0) {
        value.push_back(',');
      }
      value.push_back('"');
      value.append("test");
      value.append(std::to_string(j));
      value.append("\":true");
    }
    // now push a duplicate
    value.append(",\"test0\":false");
    value.push_back('}');

    Parser parser(&options);
    ASSERT_VPACK_EXCEPTION(parser.parse(value),
                           Exception::kDuplicateAttributeName);
  }
}

TEST(ParserTest, NoDuplicateAttributesSortedObjects) {
  Options options;
  options.build_unindexed_objects = false;
  options.check_attribute_uniqueness = true;

  for (size_t i = 1; i < 20; ++i) {
    std::string value;
    value.push_back('{');
    for (size_t j = 0; j < i; ++j) {
      if (j != 0) {
        value.push_back(',');
      }
      value.push_back('"');
      value.append("test");
      value.append(std::to_string(j));
      value.append("\":true");
    }
    value.push_back('}');

    Parser parser(&options);
    ASSERT_TRUE(parser.parse(value) > 0);
  }
}

TEST(ParserTest, DuplicateAttributesUnsortedObjects) {
  Options options;
  options.build_unindexed_objects = true;
  options.check_attribute_uniqueness = true;

  for (size_t i = 1; i < 20; ++i) {
    std::string value;
    value.push_back('{');
    for (size_t j = 0; j < i; ++j) {
      if (j != 0) {
        value.push_back(',');
      }
      value.push_back('"');
      value.append("test");
      value.append(std::to_string(j));
      value.append("\":true");
    }
    // now push a duplicate
    value.append(",\"test0\":false");
    value.push_back('}');

    Parser parser(&options);
    ASSERT_VPACK_EXCEPTION(parser.parse(value),
                           Exception::kDuplicateAttributeName);
  }
}

TEST(ParserTest, NoDuplicateAttributesUnsortedObjects) {
  Options options;
  options.build_unindexed_objects = true;
  options.check_attribute_uniqueness = true;

  for (size_t i = 1; i < 20; ++i) {
    std::string value;
    value.push_back('{');
    for (size_t j = 0; j < i; ++j) {
      if (j != 0) {
        value.push_back(',');
      }
      value.push_back('"');
      value.append("test");
      value.append(std::to_string(j));
      value.append("\":true");
    }
    value.push_back('}');

    Parser parser(&options);
    ASSERT_TRUE(parser.parse(value) > 0);
  }
}

TEST(ParserTest, ObjectNestingCloseToLimit1) {
  Options options;
  options.nesting_limit = 6;

  const std::string value("{\"a\":{\"b\":{\"c\":{\"d\":{}}}}}");

  Parser parser(&options);
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);
}

TEST(ParserTest, ObjectNestingCloseToLimit2) {
  Options options;
  options.nesting_limit = 6;

  const std::string value("{ \"a\": { \"b\": { \"c\": { \"d\": { } } } } }");

  Parser parser(&options);
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);
}

TEST(ParserTest, ObjectNestingBeyondLimit1) {
  Options options;
  options.nesting_limit = 5;

  const std::string value("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{}}}}}}");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kTooDeepNesting);
}

TEST(ParserTest, ObjectNestingCloseToLimitReusingParser) {
  Options options;
  options.nesting_limit = 6;

  const std::string value("{ \"a\": { \"b\": { \"c\": { \"d\": { } } } } }");

  Parser parser(&options);
  ValueLength len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  // reuse parser object
  len = parser.parse(value);
  ASSERT_EQ(1ULL, len);

  // intentionally broken object
  const std::string value_broken("{ \"a\": { \"b\": { \"c\": { \"d\": {");
  ASSERT_VPACK_EXCEPTION(parser.parse(value_broken), Exception::kParseError);

  // parse again with same parser object
  ASSERT_VPACK_EXCEPTION(parser.parse(value_broken), Exception::kParseError);

  // parse a valid value with same parser object
  len = parser.parse(value);
  ASSERT_EQ(1ULL, len);
}

TEST(ParserTest, ObjectNestingBeyondLimit2) {
  Options options;
  options.nesting_limit = 5;

  const std::string value(
    "{ \"a\": { \"b\": { \"c\": { \"d\": { \"e\": { } } } } } }");

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kTooDeepNesting);
}

TEST(ParserTest, FromJsonString) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3}");

  Options options;
  std::shared_ptr<Builder> b = Parser::fromJson(value, &options);

  Slice s(b->start());
  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_FALSE(s.hasKey("qux"));
}

TEST(ParserTest, FromJsonChar) {
  const char* value = "{\"foo\":1,\"bar\":2,\"baz\":3}";

  Options options;
  std::shared_ptr<Builder> b = Parser::fromJson(value, strlen(value), &options);

  Slice s(b->start());
  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_FALSE(s.hasKey("qux"));
}

TEST(ParserTest, FromJsonUInt8) {
  const char* value = "{\"foo\":1,\"bar\":2,\"baz\":3}";

  Options options;
  std::shared_ptr<Builder> b = Parser::fromJson(
    reinterpret_cast<const uint8_t*>(value), strlen(value), &options);

  Slice s(b->start());
  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_FALSE(s.hasKey("qux"));
}

TEST(ParserTest, KeepTopLevelOpenFalse) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3}");

  Options options;
  options.keep_top_level_open = false;
  std::shared_ptr<Builder> b = Parser::fromJson(value, &options);
  ASSERT_TRUE(b->isClosed());

  Slice s(b->start());
  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_FALSE(s.hasKey("qux"));
}

TEST(ParserTest, KeepTopLevelOpenTrue) {
  const std::string value("{\"foo\":1,\"bar\":2,\"baz\":3}");

  Options options;
  options.keep_top_level_open = true;
  std::shared_ptr<Builder> b = Parser::fromJson(value, &options);
  ASSERT_FALSE(b->isClosed());

  ASSERT_VPACK_EXCEPTION(b->start(), Exception::kBuilderNotSealed);
  b->close();
  ASSERT_TRUE(b->isClosed());

  Slice s(b->start());
  ASSERT_TRUE(s.hasKey("foo"));
  ASSERT_TRUE(s.hasKey("bar"));
  ASSERT_TRUE(s.hasKey("baz"));
  ASSERT_FALSE(s.hasKey("qux"));
}

TEST(ParserTest, UseNonSSEStringCopy) {
  // modify global function pointer!
  EnableBuiltinStringFunctions();

  const std::string value(
    "\"der\\thund\\nging\\rin\\fden\\\\wald\\\"und\\b\\nden'fux\"");

  Parser parser;
  parser.parse(value);

  std::shared_ptr<Builder> builder = parser.steal();
  Slice s(builder->slice());

  ASSERT_EQ("der\thund\nging\rin\fden\\wald\"und\b\nden'fux", s.copyString());
}

TEST(ParserTest, UseNonSSEUtf8CheckValidString) {
  Options options;
  options.validate_utf8_strings = true;

  // modify global function pointer!
  EnableBuiltinStringFunctions();

  const std::string value("\"the quick brown fox jumped over the lazy dog\"");

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.builder().slice());

  std::string parsed = s.copyString();
  ASSERT_EQ(value.substr(1, value.size() - 2), parsed);  // strip quotes
}

TEST(ParserTest, UseNonSSEUtf8CheckValidStringEscaped) {
  Options options;
  options.validate_utf8_strings = true;

  // modify global function pointer!
  EnableBuiltinStringFunctions();

  const std::string value(
    "\"the quick brown\\tfox\\r\\njumped \\\"over\\\" the lazy dog\"");

  Parser parser(&options);
  parser.parse(value);
  Slice s(parser.builder().slice());

  std::string parsed = s.copyString();
  ASSERT_EQ("the quick brown\tfox\r\njumped \"over\" the lazy dog", parsed);
}

TEST(ParserTest, UseNonSSEUtf8CheckInvalidUtf8) {
  Options options;
  options.validate_utf8_strings = true;

  // modify global function pointer!
  EnableBuiltinStringFunctions();

  std::string value;
  value.push_back('"');
  for (size_t i = 0; i < 100; ++i) {
    value.push_back(static_cast<unsigned char>(0x80));
  }
  value.push_back('"');

  Parser parser(&options);
  ASSERT_VPACK_EXCEPTION(parser.parse(value), Exception::kInvalidUtf8Sequence);
  ASSERT_EQ(1U, parser.errorPos());
  options.validate_utf8_strings = false;
  ASSERT_EQ(1ULL, parser.parse(value));
}

TEST(ParserTest, UseNonSSEWhitespaceCheck) {
  EnableBuiltinStringFunctions();

  // modify global function pointer!
  const std::string value("\"foo                 bar\"");
  const std::string all(
    "                                                                        "
    "          " +
    value + "                            ");

  Parser parser;
  parser.parse(all);
  std::shared_ptr<Builder> builder = parser.steal();

  Slice s(builder->slice());

  ASSERT_EQ(value.substr(1, value.size() - 2), s.copyString());
}

TEST(ParserTest, ClearBuilderOption) {
  Options options;
  options.clear_builder_before_parse = false;
  Parser parser(&options);
  parser.parse(std::string("0"));
  parser.parse(std::string("1"));
  std::shared_ptr<Builder> builder = parser.steal();

  ASSERT_EQ(builder->buffer()->size(), 2UL);
  uint8_t* p = builder->start();
  ASSERT_EQ(p[0], 0x36 + 0);
  ASSERT_EQ(p[1], 0x36 + 1);
}

TEST(ParserTest, UseBuilderOnStack) {
  Builder builder;
  {
    Parser parser(builder);
    const Builder& inner_builder(parser.builder());

    ASSERT_EQ(&builder, &inner_builder);
    parser.parse("17");
    ASSERT_EQ(inner_builder.size(), 2UL);
    ASSERT_EQ(builder.size(), 2UL);
  }
  ASSERT_EQ(builder.size(), 2UL);
}

TEST(ParserTest, UseBuilderOnStackForArrayValue) {
  Builder builder;
  builder.openArray();
  {
    Options opt;
    opt.clear_builder_before_parse = false;
    Parser parser(builder, &opt);
    const Builder& inner_builder(parser.builder());

    ASSERT_EQ(&builder, &inner_builder);
    parser.parse("17");
  }
  builder.close();
  ASSERT_EQ(4UL, builder.size());
}

TEST(ParserTest, UseBuilderOnStackOptionsNullPtr) {
  Builder builder;
  Parser* parser = nullptr;
  ASSERT_VPACK_EXCEPTION(parser = new Parser(builder, nullptr),
                         Exception::kInternalError);
  if (parser == nullptr) {
    parser = new Parser(builder);
  }
  parser->parse("[17]");
  ASSERT_EQ(4UL, builder.size());
  delete parser;
}

TEST(ParserTest, EmptyAttributeName) {
  Builder builder;
  Parser parser(builder);
  parser.parse(R"({"":123,"a":"abc"})");
  Slice s = builder.slice();

  ASSERT_EQ(2UL, s.length());

  Slice ss = s.get("");
  ASSERT_FALSE(ss.isNone());
  ASSERT_TRUE(ss.isInteger());
  ASSERT_EQ(123L, ss.getInt());

  ss = s.get("a");
  ASSERT_FALSE(ss.isNone());
  ASSERT_TRUE(ss.isString());
  ASSERT_EQ(std::string("abc"), ss.copyString());

  ss = s.get("b");
  ASSERT_TRUE(ss.isNone());
}
