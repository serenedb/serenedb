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

TEST(ValidatorTest, NoOptions) {
  ASSERT_VPACK_EXCEPTION(Validator(nullptr), Exception::kInternalError);
}

TEST(ValidatorTest, ReservedValue1) {
  constexpr std::string_view kValue("\x15", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidType);
}

TEST(ValidatorTest, ReservedValue2) {
  constexpr std::string_view kValue("\x16", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidType);
}

TEST(ValidatorTest, ReservedValue3) {
  constexpr std::string_view kValue("\x67", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidType);
}

TEST(ValidatorTest, NoneValue) {
  constexpr std::string_view kValue("\x00", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, NullValue) {
  constexpr std::string_view kValue("\x18", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, NullValueWithExtra) {
  constexpr std::string_view kValue("\x18\x41", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, FalseValue) {
  constexpr std::string_view kValue("\x19", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, FalseValueWithExtra) {
  constexpr std::string_view kValue("\x19\x41", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, TrueValue) {
  constexpr std::string_view kValue("\x1a", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, TrueValueWithExtra) {
  constexpr std::string_view kValue("\x1a\x41", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, DoubleValue) {
  constexpr std::string_view kValue("\x1f\x00\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, DoubleValueTruncated) {
  constexpr std::string_view kValue("\x1f", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, DoubleValueTooShort) {
  constexpr std::string_view kValue("\x1f\x00\x00\x00\x00\x00\x00\x00", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, DoubleValueTooLong) {
  constexpr std::string_view kValue("\x1f\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                                    10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, SmallInt) {
  for (uint8_t i = 0; i <= 9; ++i) {
    std::string value;
    value.push_back(0x30 + i);

    Validator validator;
    ASSERT_TRUE(validator.validate(value.data(), value.size()));
  }
}

TEST(ValidatorTest, SmallIntWithExtra) {
  constexpr std::string_view kValue("\x30\x41", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, SmallIntNegative) {
  for (uint8_t i = 0; i <= 5; ++i) {
    std::string value;
    value.push_back(0x3a + i);

    Validator validator;
    ASSERT_TRUE(validator.validate(value.data(), value.size()));
  }
}

TEST(ValidatorTest, SmallIntNegativeWithExtra) {
  constexpr std::string_view kValue("\x3a\x41", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, IntPositiveOneByte) {
  constexpr std::string_view kValue("\x20\x00", 2);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, IntPositiveOneByteTooShort) {
  constexpr std::string_view kValue("\x20", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, IntPositiveOneByteWithExtra) {
  constexpr std::string_view kValue("\x20\x00\x41", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, IntPositiveTwoBytes) {
  constexpr std::string_view kValue("\x21\x00\x00", 3);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, IntPositiveTwoBytesTooShort) {
  constexpr std::string_view kValue("\x21", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, IntPositiveTwoBytesWithExtra) {
  constexpr std::string_view kValue("\x21\x00\x00\x41", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, IntPositiveEightBytes) {
  constexpr std::string_view kValue("\x27\x00\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, IntPositiveEightBytesTooShort) {
  constexpr std::string_view kValue("\x27", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, IntPositiveEightBytesWithExtra) {
  constexpr std::string_view kValue("\x27\x00\x00\x00\x00\x00\x00\x00\x00\x41",
                                    10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, UIntPositiveOneByte) {
  constexpr std::string_view kValue("\x28\x00", 2);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, UIntPositiveOneByteTooShort) {
  constexpr std::string_view kValue("\x28", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, UIntPositiveOneByteWithExtra) {
  constexpr std::string_view kValue("\x28\x00\x41", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, UIntPositiveTwoBytes) {
  constexpr std::string_view kValue("\x29\x00\x00", 3);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, UIntPositiveTwoBytesTooShort) {
  constexpr std::string_view kValue("\x29", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, UIntPositiveTwoBytesWithExtra) {
  constexpr std::string_view kValue("\x29\x00\x00\x41", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, UIntPositiveEightBytes) {
  constexpr std::string_view kValue("\x2f\x00\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, UIntPositiveEightBytesTooShort) {
  constexpr std::string_view kValue("\x2f", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, UIntPositiveEightBytesWithExtra) {
  constexpr std::string_view kValue("\x2f\x00\x00\x00\x00\x00\x00\x00\x00\x41",
                                    10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, StringEmpty) {
  constexpr std::string_view kValue("\x80", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringEmptyWithExtra) {
  constexpr std::string_view kValue("\x80\x41", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, StringValidLength) {
  constexpr std::string_view kValue("\x83\x41\x42\x43", 4);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringLongerThanSpecified) {
  constexpr std::string_view kValue("\x82\x41\x42\x43", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, StringShorterThanSpecified) {
  constexpr std::string_view kValue("\x83\x41\x42", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, StringValidUtf8Empty) {
  constexpr std::string_view kValue("\x80", 1);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringValidUtf8OneByte) {
  constexpr std::string_view kValue("\x81\x0a", 2);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringValidUtf8TwoBytes) {
  constexpr std::string_view kValue("\x82\xc2\xa2", 3);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringValidUtf8ThreeBytes) {
  constexpr std::string_view kValue("\x83\xe2\x82\xac", 4);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringValidUtf8FourBytes) {
  constexpr std::string_view kValue("\x84\xf0\xa4\xad\xa2", 5);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringValidUtf8Long) {
  constexpr std::string_view kValue("\xff\x04\x00\x00\x00\x40\x41\x42\x43", 9);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringInvalidUtf8NoValidation) {
  constexpr std::string_view kValue("\x81\xff", 2);

  Options options;
  options.validate_utf8_strings = false;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringInvalidUtf8WithValidation1) {
  constexpr std::string_view kValue("\x81\x80", 2);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8WithValidation2) {
  constexpr std::string_view kValue("\x81\xff", 2);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8WithValidation3) {
  constexpr std::string_view kValue("\x82\xff\x70", 3);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8WithValidation4) {
  constexpr std::string_view kValue("\x83\xff\xff\x07", 4);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8WithValidation5) {
  constexpr std::string_view kValue("\x84\xff\xff\xff\x07", 5);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8Long) {
  constexpr std::string_view kValue("\xff\x04\x00\x00\x00\xff\xff\xff\x07", 9);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringValidUtf8ObjectWithValidation) {
  constexpr std::string_view kValue("\x0b\x08\x01\x81\x41\x81\x41\x03", 8);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringInvalidUtf8ObjectKeyWithValidation) {
  constexpr std::string_view kValue("\x0b\x08\x01\x81\x80\x81\x41\x03", 8);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8ObjectValueWithValidation) {
  constexpr std::string_view kValue("\x0b\x08\x01\x81\x41\x81\x80\x03", 8);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8ObjectLongKeyWithValidation) {
  constexpr std::string_view kValue(
    "\x0b\x0c\x01\xff\x01\x00\x00\x00\x80\x81\x41\x03", 12);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8ObjectLongValueWithValidation) {
  constexpr std::string_view kValue(
    "\x0b\x0c\x01\x81\x41\xff\x01\x00\x00\x00\x80\x03", 12);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringValidUtf8CompactObjectWithValidation) {
  constexpr std::string_view kValue("\x14\x07\x81\x41\x81\x41\x01", 7);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, StringInvalidUtf8CompactObjectKeyWithValidation) {
  constexpr std::string_view kValue("\x14\x07\x81\x80\x81\x41\x01", 7);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8CompactObjectLongKeyWithValidation) {
  constexpr std::string_view kValue(
    "\x14\x0b\xff\x01\x00\x00\x00\x80\x81\x41\x01", 11);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8CompactObjectValueWithValidation) {
  constexpr std::string_view kValue("\x14\x07\x81\x41\x81\x80\x01", 7);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, StringInvalidUtf8CompactLongObjectValueWithValidation) {
  constexpr std::string_view kValue(
    "\x14\x0b\x81\x41\xff\x01\x00\x00\x00\x80\x01", 11);

  Options options;
  options.validate_utf8_strings = true;
  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kInvalidUtf8Sequence);
}

TEST(ValidatorTest, LongStringEmpty) {
  constexpr std::string_view kValue("\xff\x00\x00\x00\x00", 5);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, LongStringNonEmpty) {
  constexpr std::string_view kValue("\xff\x01\x00\x00\x00\x41", 6);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, LongStringTooShort) {
  constexpr std::string_view kValue("\xff", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, LongStringShorterThanSpecified1) {
  constexpr std::string_view kValue("\xff\x01\x00\x00\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, LongStringShorterThanSpecified2) {
  constexpr std::string_view kValue("\xff\x03\x00\x00\x00\x41\x42", 11);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, LongStringLongerThanSpecified1) {
  constexpr std::string_view kValue("\xff\x00\x00\x00\x00\x41", 10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, LongStringLongerThanSpecified2) {
  constexpr std::string_view kValue("\xff\x01\x00\x00\x00\x41\x42", 11);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, EmptyArray) {
  constexpr std::string_view kValue("\x01", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, EmptyArrayWithExtra) {
  constexpr std::string_view kValue("\x01\x02", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByte) {
  constexpr std::string_view kValue("\x02\x03\x18", 3);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayOneBytePadded) {
  constexpr std::string_view kValue("\x02\x0a\x00\x00\x00\x00\x00\x00\x00\x18",
                                    10);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayOneByteInvalidPaddings) {
  constexpr std::string_view kValue1("\x02\x09\x00\x00\x00\x00\x00\x00\x18", 9);
  constexpr std::string_view kValue2("\x02\x08\x00\x00\x00\x00\x00\x18", 8);
  constexpr std::string_view kValue3("\x02\x07\x00\x00\x00\x00\x18", 7);
  constexpr std::string_view kValue4("\x02\x06\x00\x00\x00\x18", 6);
  constexpr std::string_view kValue5("\x02\x05\x00\x00\x18", 5);
  constexpr std::string_view kValue6("\x02\x04\x00\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue4.data(), kValue4.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue5.data(), kValue5.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue6.data(), kValue6.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteTooShort) {
  constexpr std::string_view kValue("\x02\x04\x18", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteTooShortBytesize0) {
  constexpr std::string_view kValue("\x02", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteTooShortBytesize1) {
  constexpr std::string_view kValue("\x02\x05", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteMultipleMembers) {
  constexpr std::string_view kValue("\x02\x05\x18\x18\x18", 5);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayOneByteTooFewMembers1) {
  constexpr std::string_view kValue("\x02\x05\x18", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteTooFewMembers2) {
  constexpr std::string_view kValue("\x02\x05\x18\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteMultipleMembersDifferentSizes) {
  constexpr std::string_view kValue("\x02\x05\x18\x28\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytes) {
  constexpr std::string_view kValue("\x03\x04\x00\x18", 4);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayTwoBytesPadded) {
  constexpr std::string_view kValue("\x03\x0a\x00\x00\x00\x00\x00\x00\x00\x18",
                                    10);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayTwoBytesInvalidPaddings) {
  constexpr std::string_view kValue1("\x03\x09\x00\x00\x00\x00\x00\x00\x18", 9);
  constexpr std::string_view kValue2("\x03\x08\x00\x00\x00\x00\x00\x18", 8);
  constexpr std::string_view kValue3("\x03\x07\x00\x00\x00\x00\x18", 7);
  constexpr std::string_view kValue4("\x03\x06\x00\x00\x00\x18", 6);
  constexpr std::string_view kValue5("\x03\x05\x00\x00\x18", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue4.data(), kValue4.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue5.data(), kValue5.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesTooShort) {
  constexpr std::string_view kValue("\x03\x05\x00\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesTooShortBytesize0) {
  constexpr std::string_view kValue("\x03", 1);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesTooShortBytesize1) {
  constexpr std::string_view kValue("\x03\x05", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesTooShortBytesize2) {
  constexpr std::string_view kValue("\x03\x05\x00", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesMultipleMembers) {
  constexpr std::string_view kValue("\x03\x06\x00\x18\x18\x18", 6);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayTwoBytesTooFewMembers1) {
  constexpr std::string_view kValue("\x03\x05\x00\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesTooFewMembers2) {
  constexpr std::string_view kValue("\x03\x06\x00\x18\x18", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesMultipleMembersDifferentSizes) {
  constexpr std::string_view kValue("\x03\x06\x00\x18\x28\x00", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourBytes) {
  constexpr std::string_view kValue("\x04\x06\x00\x00\x00\x18", 6);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayFourBytesPadded) {
  constexpr std::string_view kValue("\x04\x0a\x00\x00\x00\x00\x00\x00\x00\x18",
                                    10);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayFourBytesInvalidPaddings) {
  constexpr std::string_view kValue1("\x04\x09\x00\x00\x00\x00\x00\x00\x18", 9);
  constexpr std::string_view kValue2("\x04\x08\x00\x00\x00\x00\x00\x18", 8);
  constexpr std::string_view kValue3("\x04\x07\x00\x00\x00\x00\x18", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourBytesTooShort) {
  constexpr std::string_view kValue("\x04\x05\x00\x00\x00\x18", 6);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayFourBytesMultipleMembers) {
  constexpr std::string_view kValue("\x04\x08\x00\x00\x00\x18\x18\x18", 8);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayFourBytesTooFewMembers1) {
  constexpr std::string_view kValue("\x04\x07\x00\x00\x00\x18", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourBytesTooFewMembers2) {
  constexpr std::string_view kValue("\x04\x08\x00\x00\x00\x18\x18", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourBytesMultipleMembersDifferentSizes) {
  constexpr std::string_view kValue("\x04\x08\x00\x00\x00\x18\x28\x00", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightBytes) {
  constexpr std::string_view kValue("\x05\x0a\x00\x00\x00\x00\x00\x00\x00\x18",
                                    10);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayEightBytesTooShort) {
  constexpr std::string_view kValue("\x05\x09\x00\x00\x00\x00\x00\x00\x00\x18",
                                    10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightBytesTooShortBytesize) {
  constexpr std::string_view kValue("\x05\x0a\x00\x00\x00\x00\x00\x00\x00\x00",
                                    10);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayEightBytesMultipleMembers) {
  constexpr std::string_view kValue(
    "\x05\x0c\x00\x00\x00\x00\x00\x00\x00\x18\x18\x18", 12);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayEightBytesTooFewMembers1) {
  constexpr std::string_view kValue("\x05\x0b\x00\x00\x00\x00\x00\x00\x00\x18",
                                    10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightBytesTooFewMembers2) {
  constexpr std::string_view kValue(
    "\x05\x0c\x00\x00\x00\x00\x00\x00\x00\x18\x18", 11);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightBytesMultipleMembersDifferentSizes) {
  constexpr std::string_view kValue(
    "\x05\x0c\x00\x00\x00\x00\x00\x00\x00\x18\x28\x00", 12);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexed) {
  constexpr std::string_view kValue("\x06\x05\x01\x18\x03", 5);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayOneByteIndexedEmpty) {
  constexpr std::string_view kValue("\x06\x03\x00", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexedPadding) {
  constexpr std::string_view kValue(
    "\x06\x0b\x01\x00\x00\x00\x00\x00\x00\x18\x09", 11);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayOneByteIndexedInvalidPaddings) {
  constexpr std::string_view kValue1("\x06\x0a\x01\x00\x00\x00\x00\x00\x18\x08",
                                     10);
  constexpr std::string_view kValue2("\x06\x09\x01\x00\x00\x00\x00\x18\x07", 9);
  constexpr std::string_view kValue3("\x06\x08\x01\x00\x00\x00\x18\x06", 8);
  constexpr std::string_view kValue4("\x06\x07\x01\x00\x00\x18\x05", 7);
  constexpr std::string_view kValue5("\x06\x06\x01\x00\x18\x04", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue4.data(), kValue4.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue5.data(), kValue5.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexedTooShort) {
  constexpr std::string_view kValue("\x06\x05\x01\x18\x03", 5);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayOneByteIndexedIndexOutOfBounds) {
  std::string value("\x06\x05\x01\x18\x00", 5);

  for (size_t i = 0; i < value.size() + 10; ++i) {
    if (i == 3) {
      continue;
    }
    value.at(value.size() - 1) = (char)i;

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(value.data(), value.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayOneByteIndexedTooManyMembers1) {
  constexpr std::string_view kValue("\x06\x09\x02\x18\x18\x18\x03\x04\x05", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexedTooManyMembers2) {
  constexpr std::string_view kValue("\x06\x08\x02\x18\x18\x03\x04\x05", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexedTooManyMembers3) {
  constexpr std::string_view kValue("\x06\x08\x03\x18\x18\x18\x03\x04", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexedTooManyMembers4) {
  constexpr std::string_view kValue("\x06\x08\x03\x18\x18\x03\x04\x05", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteIndexedRepeatedValues) {
  constexpr std::string_view kValue("\x06\x07\x02\x18\x18\x03\x03", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexed) {
  constexpr std::string_view kValue("\x07\x08\x00\x01\x00\x18\x05\x00", 8);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayTwoByteIndexedEmpty) {
  constexpr std::string_view kValue("\x07\x05\x00\x00\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoBytesIndexedPadded) {
  constexpr std::string_view kValue(
    "\x07\x0c\x00\x01\x00\x00\x00\x00\x00\x18\x09\x00", 12);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayTwoBytesIndexedInvalidPaddings) {
  constexpr std::string_view kValue1(
    "\x07\x0b\x00\x01\x00\x00\x00\x00\x18\x08\x00", 11);
  constexpr std::string_view kValue2("\x07\x0a\x00\x01\x00\x00\x00\x18\x07\x00",
                                     10);
  constexpr std::string_view kValue3("\x07\x09\x00\x01\x00\x00\x18\x06\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedTooShort) {
  constexpr std::string_view kValue("\x07\x08\x00\x01\x00\x18\x05\x00", 8);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayTwoByteIndexedInvalidLength1) {
  constexpr std::string_view kValue("\x07\x00", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedInvalidLength2) {
  constexpr std::string_view kValue("\x07\x00\x00", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedInvalidLength3) {
  constexpr std::string_view kValue("\x07\x00\x00\x00", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedInvalidLength5) {
  constexpr std::string_view kValue("\x07\x00\x00\x00\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedInvalidLength6) {
  constexpr std::string_view kValue("\x07\x05\x00\x00\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedInvalidLength7) {
  constexpr std::string_view kValue("\x07\x05\x00\x01\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedIndexOutOfBounds) {
  std::string value("\x07\x08\x00\x01\x00\x18\x00\x00", 8);

  for (size_t i = 0; i < value.size() + 10; ++i) {
    if (i == 5) {
      continue;
    }
    value.at(value.size() - 2) = (char)i;

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(value.data(), value.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayTwoByteIndexedTooManyMembers1) {
  constexpr std::string_view kValue(
    "\x07\x0e\x00\x02\x00\x18\x18\x18\x05\x00\x06\x00\x07\x00", 14);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedTooManyMembers2) {
  constexpr std::string_view kValue(
    "\x07\x0d\x00\x02\x00\x18\x18\x05\x00\x06\x00\x07\x00", 13);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedTooManyMembers3) {
  constexpr std::string_view kValue(
    "\x07\x0c\x00\x03\x00\x18\x18\x18\x05\x00\x06\x00", 12);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedTooManyMembers4) {
  constexpr std::string_view kValue(
    "\x07\x0b\x00\x03\x00\x18\x18\x05\x00\x06\x00", 11);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayTwoByteIndexedRepeatedValues) {
  constexpr std::string_view kValue("\x07\x09\x00\x02\x00\x18\x18\x05\x05", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexed) {
  constexpr std::string_view kValue(
    "\x08\x0e\x00\x00\x00\x01\x00\x00\x00\x18\x09\x00\x00\x00", 14);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayFourByteIndexedEmpty) {
  constexpr std::string_view kValue("\x08\x09\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexedTooShort) {
  constexpr std::string_view kValue(
    "\x08\x0e\x00\x00\x00\x01\x00\x00\x00\x18\x09\x00\x00\x00", 14);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayFourByteIndexedInvalidLength) {
  constexpr std::string_view kValue("\x08\x00\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexedIndexOutOfBounds) {
  std::string value("\x08\x0e\x00\x00\x00\x01\x00\x00\x00\x18\x00\x00\x00\x00",
                    14);

  for (size_t i = 0; i < value.size() + 10; ++i) {
    if (i == 9) {
      continue;
    }
    value.at(value.size() - 4) = (char)i;

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(value.data(), value.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayFourByteIndexedTooManyMembers1) {
  constexpr std::string_view kValue(
    "\x08\x18\x00\x00\x00\x02\x00\x00\x00\x18\x18\x18\x09\x00\x00\x00\x0a\x00"
    "\x00\x00\x0b\x00\x00\x00",
    24);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexedTooManyMembers2) {
  constexpr std::string_view kValue(
    "\x08\x17\x00\x00\x00\x02\x00\x00\x00\x18\x18\x09\x00\x00\x00\x0a\x00\x00"
    "\x00\x0b\x00\x00\x00",
    23);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexedTooManyMembers3) {
  constexpr std::string_view kValue(
    "\x08\x14\x00\x00\x00\x03\x00\x00\x00\x18\x18\x18\x09\x00\x00\x00\x0a\x00"
    "\x00\x00",
    20);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexedTooManyMembers4) {
  constexpr std::string_view kValue(
    "\x08\x14\x00\x00\x00\x03\x00\x00\x00\x18\x18\x09\x00\x00\x00\x0a\x00\x00"
    "\x00",
    20);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayFourByteIndexedRepeatedValues) {
  constexpr std::string_view kValue(
    "\x08\x13\x00\x00\x00\x02\x00\x00\x00\x18\x18\x09\x00\x00\x00\x09\x00\x00"
    "\x00",
    19);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexed) {
  constexpr std::string_view kValue(
    "\x09\x1a\x00\x00\x00\x00\x00\x00\x00\x18\x09\x00\x00\x00\x00\x00\x00\x00"
    "\x01\x00\x00\x00\x00\x00\x00\x00",
    26);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayEightByteIndexedEmpty) {
  constexpr std::string_view kValue(
    "\x09\x11\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 17);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexedTooShort) {
  constexpr std::string_view kValue(
    "\x09\x1a\x00\x00\x00\x00\x00\x00\x00\x18\x09\x00\x00\x00\x00\x00\x00\x00"
    "\x01\x00\x00\x00\x00\x00\x00\x00",
    26);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayEightByteIndexedInvalidLength) {
  constexpr std::string_view kValue("\x09\x00\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexedIndexOutOfBounds) {
  std::string value(
    "\x09\x1a\x00\x00\x00\x00\x00\x00\x00\x18\x09\x00\x00\x00\x00\x00\x00\x00"
    "\x01\x00\x00\x00\x00\x00\x00\x00",
    26);

  for (size_t i = 0; i < value.size() + 10; ++i) {
    if (i == 9) {
      continue;
    }
    value.at(value.size() - 16) = (char)i;

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(value.data(), value.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ArrayEightByteIndexedTooManyMembers1) {
  constexpr std::string_view kValue(
    "\x09\x2c\x00\x00\x00\x00\x00\x00\x00\x18\x18\x18\x09\x00\x00\x00\x00\x00"
    "\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00"
    "\x02\x00\x00\x00\x00\x00\x00\x00",
    44);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexedTooManyMembers2) {
  constexpr std::string_view kValue(
    "\x09\x2b\x00\x00\x00\x00\x00\x00\x00\x18\x18\x09\x00\x00\x00\x00\x00\x00"
    "\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x02"
    "\x00\x00\x00\x00\x00\x00\x00",
    43);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexedTooManyMembers3) {
  constexpr std::string_view kValue(
    "\x09\x24\x00\x00\x00\x00\x00\x00\x00\x18\x18\x18\x09\x00\x00\x00\x00\x00"
    "\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00"
    "\x00",
    36);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexedTooManyMembers4) {
  constexpr std::string_view kValue(
    "\x09\x23\x00\x00\x00\x00\x00\x00\x00\x18\x18\x09\x00\x00\x00\x00\x00\x00"
    "\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00",
    35);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEightByteIndexedRepeatedValues) {
  constexpr std::string_view kValue(
    "\x09\x23\x00\x00\x00\x00\x00\x00\x00\x18\x18\x09\x00\x00\x00\x00\x00\x00"
    "\x00\x09\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00",
    35);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayOneByteWithExtraPadding) {
  Options options;
  options.padding_behavior = Options::PaddingBehavior::kUsePadding;
  Builder b(&options);
  b.openArray(false);
  b.add(1);
  b.add(2);
  b.add(3);
  b.close();

  Slice s = b.slice();
  const uint8_t* data = s.start();

  ASSERT_EQ(0x05, data[0]);
  ASSERT_EQ(0x0c, data[1]);
  ASSERT_EQ(0x00, data[2]);
  ASSERT_EQ(0x00, data[3]);
  ASSERT_EQ(0x00, data[4]);
  ASSERT_EQ(0x00, data[5]);
  ASSERT_EQ(0x00, data[6]);
  ASSERT_EQ(0x00, data[7]);
  ASSERT_EQ(0x00, data[8]);
  ASSERT_EQ(0x36 + 1, data[9]);
  ASSERT_EQ(0x36 + 2, data[10]);
  ASSERT_EQ(0x36 + 3, data[11]);

  Validator validator;
  ASSERT_TRUE(validator.validate(data, s.byteSize()));
}

TEST(ValidatorTest, ArrayCompact) {
  constexpr std::string_view kValue("\x13\x04\x18\x01", 4);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayCompactWithExtra) {
  constexpr std::string_view kValue("\x13\x04\x18\x01\x41", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort1) {
  constexpr std::string_view kValue("\x13\x04", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort2) {
  constexpr std::string_view kValue("\x13\x04\x18", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort3) {
  constexpr std::string_view kValue("\x13\x80", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort4) {
  constexpr std::string_view kValue("\x13\x80\x80", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort5) {
  constexpr std::string_view kValue("\x13\x80\x05\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort6) {
  constexpr std::string_view kValue("\x13\x04\x18\x02", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort7) {
  constexpr std::string_view kValue("\x13\x04\x18\xff", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort8) {
  constexpr std::string_view kValue("\x13\x04\x06\x01", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactTooShort9) {
  constexpr std::string_view kValue("\x13\x81", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactEmpty) {
  constexpr std::string_view kValue("\x13\x04\x18\x00", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactNrItemsWrong1) {
  constexpr std::string_view kValue("\x13\x04\x18\x81", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactNrItemsWrong2) {
  constexpr std::string_view kValue("\x13\x05\x18\x81\x81", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactNrItemsWrong3) {
  constexpr std::string_view kValue("\x13\x05\x18\x01\x80", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactNrItemsExceedsBytesize) {
  constexpr std::string_view kValue("\x13\x06\x82\x40\x40\x02", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactMemberExceedsBytesize1) {
  constexpr std::string_view kValue("\x13\x06\x83\x40\x40\x02", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactMemberExceedsBytesize2) {
  constexpr std::string_view kValue("\x13\x06\x84\x40\x40\x02", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactMemberExceedsBytesize3) {
  constexpr std::string_view kValue("\x13\x06\x85\x40\x40\x02", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayCompactManyEntries) {
  Builder b;
  b.openArray(true);
  for (size_t i = 0; i < 2048; ++i) {
    b.add(i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x13');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ArrayEqualSize) {
  constexpr std::string_view kValue("\x02\x04\x01\x18", 4);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayEqualSizeMultiple) {
  constexpr std::string_view kValue("\x02\x04\x18\x18", 4);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ArrayEqualSizeMultipleWithExtra) {
  constexpr std::string_view kValue("\x02\x04\x18\x18\x41", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEqualSizeTooShort) {
  constexpr std::string_view kValue("\x02\x05\x18\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEqualSizeContainingNone) {
  constexpr std::string_view kValue("\x02\x03\x00", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayEqualSizeUnequalElements) {
  constexpr std::string_view kValue("\x02\x05\x18\x81\x40", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, EmptyObject) {
  constexpr std::string_view kValue("\x0a", 1);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, EmptyObjectWithExtra) {
  constexpr std::string_view kValue("\x0a\x02", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayMoreItemsThanIndex) {
  constexpr std::string_view kValue("\x06\x06\x01\xBE\x30\x04", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ArrayNestingCloseToLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openArray();
  }
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 6;

  Validator validator(&options);
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, CompactArrayNestingCloseToLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openArray(true);
  }
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 6;

  Validator validator(&options);
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ArrayNestingBeyondLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openArray();
  }
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 5;

  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(
    validator.validate(b.slice().start(), b.slice().byteSize()),
    Exception::kTooDeepNesting);
}

TEST(ValidatorTest, CompactArrayNestingBeyondLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openArray(true);
  }
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 5;

  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(
    validator.validate(b.slice().start(), b.slice().byteSize()),
    Exception::kTooDeepNesting);
}

TEST(ValidatorTest, ObjectMoreItemsThanIndex) {
  constexpr std::string_view kValue("\x0B\x08\x01\xBE\x41\x61\x31\x04", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectValueLeakIndex) {
  constexpr std::string_view kValue("\x0B\x06\x01\x81\x61\x03", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompact) {
  constexpr std::string_view kValue("\x14\x05\x80\x18\x01", 5);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectCompactEmpty) {
  constexpr std::string_view kValue("\x14\x03\x00", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactWithExtra) {
  constexpr std::string_view kValue("\x14\x06\x80\x18\x01\x41", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactTooShort1) {
  constexpr std::string_view kValue("\x14\x05", 2);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactTooShort2) {
  constexpr std::string_view kValue("\x14\x05\x80", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactTooShort3) {
  constexpr std::string_view kValue("\x14\x05\x80\x18", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactNrItemsWrong1) {
  constexpr std::string_view kValue("\x14\x05\x80\x18\x02", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactNrItemsWrong2) {
  constexpr std::string_view kValue("\x14\x07\x80\x18\x80\x18\x01", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactDifferentAttributes) {
  constexpr std::string_view kValue("\x14\x09\x81\x41\x18\x81\x42\x18\x02", 9);

  Options options;
  options.check_attribute_uniqueness = true;
  Validator validator(&options);
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectCompactOnlyKey) {
  constexpr std::string_view kValue("\x14\x04\x80\x01", 4);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactStopInKey) {
  constexpr std::string_view kValue("\x14\x05\x85\x41\x01", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectCompactManyEntries) {
  Builder b;
  b.openObject(true);
  for (size_t i = 0; i < 2048; ++i) {
    std::string key = "test" + std::to_string(i);
    b.add(key, i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x14');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectNegativeKeySmallInt) {
  constexpr std::string_view kValue("\x0b\x06\x01\x3a\x18\x03", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectKeySignedInt) {
  constexpr std::string_view kValue("\x0b\x07\x01\x20\x01\x18\x03", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByte) {
  constexpr std::string_view kValue("\x0b\x06\x01\x80\x18\x03", 6);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectOneByteSingleByteKey) {
  constexpr std::string_view kValue("\x0b\x07\x01\x81\x41\x18\x03", 7);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectOneBytePadding) {
  constexpr std::string_view kValue(
    "\x0b\x0c\x01\x00\x00\x00\x00\x00\x00\x80\x18\x09", 12);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectOneByteInvalidPaddings) {
  constexpr std::string_view kValue1(
    "\x0b\x0b\x01\x00\x00\x00\x00\x00\x40\x18\x08", 11);
  constexpr std::string_view kValue2("\x0b\x0a\x01\x00\x00\x00\x00\x40\x18\x07",
                                     10);
  constexpr std::string_view kValue3("\x0b\x09\x01\x00\x00\x00\x40\x18\x06", 9);
  constexpr std::string_view kValue4("\x0b\x08\x01\x00\x00\x40\x18\x05", 8);
  constexpr std::string_view kValue5("\x0b\x07\x01\x00\x40\x18\x04", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue4.data(), kValue4.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue5.data(), kValue5.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteEmpty) {
  constexpr std::string_view kValue("\x0b\x03\x00", 3);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteWithExtra) {
  constexpr std::string_view kValue("\x0b\x07\x40\x18\x02\x01\x41", 7);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteTooShort) {
  constexpr std::string_view kValue("\x0b\x06\x01\x40\x18\x03", 6);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ObjectOneByteNrItemsWrong1) {
  constexpr std::string_view kValue("\x0b\x06\x02\x80\x18\x03", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteNrItemsWrong2) {
  constexpr std::string_view kValue("\x0b\x08\x01\x80\x18\x80\x18\x03", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteNrItemsWrong3) {
  constexpr std::string_view kValue("\x0b\x08\x02\x80\x18\x80\x18\x03", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteNrItemsMatch) {
  constexpr std::string_view kValue("\x0b\x09\x02\x80\x18\x80\x18\x03\x05", 9);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectOneByteOnlyKey) {
  constexpr std::string_view kValue("\x0b\x05\x01\x80\x03", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteStopInKey) {
  constexpr std::string_view kValue("\x0b\x06\x01\x85\x41\x03", 6);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectOneByteSomeEntries) {
  Builder b;
  b.openObject();
  for (size_t i = 0; i < 8; ++i) {
    std::string key = "t" + std::to_string(i);
    b.add(key, i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x0b');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectOneByteMoreEntries) {
  Builder b;
  b.openObject();
  for (size_t i = 0; i < 17; ++i) {
    std::string key = "t" + std::to_string(i);
    b.add(key, i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x0b');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectTwoByte) {
  constexpr std::string_view kValue("\x0c\x09\x00\x01\x00\x80\x18\x05\x00", 9);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectTwoBytePadding) {
  constexpr std::string_view kValue(
    "\x0c\x0d\x00\x01\x00\x00\x00\x00\x00\x80\x18\x09\x00", 13);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectTwoByteInvalidPaddings) {
  constexpr std::string_view kValue1(
    "\x0c\x0c\x00\x01\x00\x00\x00\x00\x80\x18\x08\x00", 12);
  constexpr std::string_view kValue2(
    "\x0c\x0b\x00\x01\x00\x00\x00\x80\x18\x07\x00", 11);
  constexpr std::string_view kValue3("\x0c\x0a\x00\x01\x00\x00\x80\x18\x06\x00",
                                     10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue1.data(), kValue1.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue2.data(), kValue2.size()),
                         Exception::kValidatorInvalidLength);
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue3.data(), kValue3.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteEmpty) {
  constexpr std::string_view kValue("\x0c\x05\x00\x00\x00", 5);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteWithExtra) {
  constexpr std::string_view kValue("\x0c\x0a\x00\x01\x00\x80\x18\x05\x00\x41",
                                    10);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteTooShort) {
  constexpr std::string_view kValue("\x0c\x06\x00\x01\x00\x80\x18\x05", 8);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ObjectTwoByteNrItemsWrong1) {
  constexpr std::string_view kValue("\x0c\x09\x00\x02\x00\x80\x18\x05\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteNrItemsWrong2) {
  constexpr std::string_view kValue(
    "\x0c\x0b\x00\x01\x00\x80\x18\x80\x18\x05\x00", 11);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteNrItemsWrong3) {
  constexpr std::string_view kValue(
    "\x0c\x0b\x00\x02\x00\x80\x18\x80\x18\x05\x00", 11);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteNrItemsMatch) {
  constexpr std::string_view kValue(
    "\x0c\x0d\x00\x02\x00\x80\x18\x80\x18\x05\x00\x07\x00", 13);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectTwoByteOnlyKey) {
  constexpr std::string_view kValue("\x0c\x08\x00\x01\x00\x80\x05\x00", 8);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteStopInKey) {
  constexpr std::string_view kValue("\x0c\x09\x00\x01\x00\x85\x41\x05\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectTwoByteSomeEntries) {
  std::string prefix;
  for (size_t i = 0; i < 100; ++i) {
    prefix.push_back('x');
  }
  Builder b;
  b.openObject();
  for (size_t i = 0; i < 8; ++i) {
    std::string key = prefix + std::to_string(i);
    b.add(key, i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x0c');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectTwoByteMoreEntries) {
  Builder b;
  b.openObject();
  for (size_t i = 0; i < 256; ++i) {
    std::string key = "test" + std::to_string(i);
    b.add(key, i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x0c');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectFourByte) {
  constexpr std::string_view kValue(
    "\x0d\x0f\x00\x00\x00\x01\x00\x00\x00\x80\x18\x09\x00\x00\x00", 15);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectFourByteEmpty) {
  constexpr std::string_view kValue("\x0d\x09\x00\x00\x00\x00\x00\x00\x00", 9);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteWithExtra) {
  constexpr std::string_view kValue(
    "\x0d\x10\x00\x00\x00\x01\x00\x00\x00\x80\x18\x09\x00\x00\x00\x41", 16);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteTooShort) {
  constexpr std::string_view kValue(
    "\x0d\x0f\x00\x00\x00\x01\x00\x00\x00\x80\x18\x09\x00\x00\x00", 15);
  std::string temp;

  for (size_t i = 0; i < kValue.size() - 1; ++i) {
    temp.push_back(kValue.at(i));

    Validator validator;
    ASSERT_VPACK_EXCEPTION(validator.validate(temp.data(), temp.size()),
                           Exception::kValidatorInvalidLength);
  }
}

TEST(ValidatorTest, ObjectFourByteNrItemsWrong1) {
  constexpr std::string_view kValue(
    "\x0d\x0f\x00\x00\x00\x02\x00\x00\x00\x80\x18\x09\x00\x00\x00", 15);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteNrItemsWrong2) {
  constexpr std::string_view kValue(
    "\x0d\x11\x00\x00\x00\x01\x00\x00\x00\x80\x18\x80\x18\x09\x00\x00\x00", 17);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteNrItemsWrong3) {
  constexpr std::string_view kValue(
    "\x0d\x11\x00\x00\x00\x02\x00\x00\x00\x80\x18\x80\x18\x09\x00\x00\x00", 17);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteNrItemsMatch) {
  constexpr std::string_view kValue(
    "\x0d\x15\x00\x00\x00\x02\x00\x00\x00\x80\x18\x80\x18\x09\x00\x00\x00\x0b"
    "\x00\x00\x00",
    21);

  Validator validator;
  ASSERT_TRUE(validator.validate(kValue.data(), kValue.size()));
}

TEST(ValidatorTest, ObjectFourByteOnlyKey) {
  constexpr std::string_view kValue(
    "\x0d\x0e\x00\x00\x00\x01\x00\x00\x00\x80\x09\x00\x00\x00", 14);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteStopInKey) {
  constexpr std::string_view kValue(
    "\x0d\x0f\x00\x00\x00\x01\x00\x00\x00\x85\x41\x09\x00\x00\x00", 15);

  Validator validator;
  ASSERT_VPACK_EXCEPTION(validator.validate(kValue.data(), kValue.size()),
                         Exception::kValidatorInvalidLength);
}

TEST(ValidatorTest, ObjectFourByteManyEntries) {
  Builder b;
  b.openObject();
  for (size_t i = 0; i < 65536; ++i) {
    std::string key = "test" + std::to_string(i);
    b.add(key, i);
  }
  b.close();

  ASSERT_EQ(b.slice().head(), '\x0d');

  Validator validator;
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectNestingCloseToLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openObject();
    b.add("key");
  }
  b.add(true);
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 6;

  Validator validator(&options);
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, CompactObjectNestingCloseToLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openObject(true);
    b.add("key");
  }
  b.add(true);
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 6;

  Validator validator(&options);
  ASSERT_TRUE(validator.validate(b.slice().start(), b.slice().byteSize()));
}

TEST(ValidatorTest, ObjectNestingBeyondLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openObject(true);
    b.add("key");
  }
  b.add(true);
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 5;

  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(
    validator.validate(b.slice().start(), b.slice().byteSize()),
    Exception::kTooDeepNesting);
}

TEST(ValidatorTest, CompactObjectNestingBeyondLimit) {
  Builder b;
  for (int i = 0; i < 5; ++i) {
    b.openObject(true);
    b.add("key");
  }
  b.add(true);
  for (int i = 0; i < 5; ++i) {
    b.close();
  }
  Options options;
  options.nesting_limit = 5;

  Validator validator(&options);
  ASSERT_VPACK_EXCEPTION(
    validator.validate(b.slice().start(), b.slice().byteSize()),
    Exception::kTooDeepNesting);
}
