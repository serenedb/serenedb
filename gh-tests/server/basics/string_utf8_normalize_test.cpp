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

#include "basics/common.h"
#include "basics/directories.h"
#include "basics/files.h"
#include "basics/strings.h"
#include "basics/utf8_helper.h"
#include "gtest/gtest.h"
#include "icu-helper.h"

using namespace sdb;

class CNormalizeStringTest : public ::testing::Test {
 protected:
  CNormalizeStringTest() {
    IcuInitializer::setup(
      "./third_party/V8/v8/third_party/icu/common/icudtl.dat");
  }
};

// test NFD to NFC
TEST(CNormalizeStringTest, tst_1) {
  /* "Grüß Gott. Здравствуйте! x=(-b±sqrt(b²-4ac))/(2a)  日本語,中文,한글" */
  static const unsigned char kComposed[] = {
    'G',  'r',  0xC3, 0xBC, 0xC3, 0x9F, ' ',  'G',  'o',  't',  't',  '.',
    ' ',  0xD0, 0x97, 0xD0, 0xB4, 0xD1, 0x80, 0xD0, 0xB0, 0xD0, 0xB2, 0xD1,
    0x81, 0xD1, 0x82, 0xD0, 0xB2, 0xD1, 0x83, 0xD0, 0xB9, 0xD1, 0x82, 0xD0,
    0xB5, '!',  ' ',  'x',  '=',  '(',  '-',  'b',  0xC2, 0xB1, 's',  'q',
    'r',  't',  '(',  'b',  0xC2, 0xB2, '-',  '4',  'a',  'c',  ')',  ')',
    '/',  '(',  '2',  'a',  ')',  ' ',  ' ',  0xE6, 0x97, 0xA5, 0xE6, 0x9C,
    0xAC, 0xE8, 0xAA, 0x9E, ',',  0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87, ',',
    0xED, 0x95, 0x9C, 0xEA, 0xB8, 0x80, 'z',  0};

  static const unsigned char kDecomposed[] = {
    'G',  'r',  0x75, 0xCC, 0x88, 0xC3, 0x9F, ' ',  'G',  'o',  't',  't',
    '.',  ' ',  0xD0, 0x97, 0xD0, 0xB4, 0xD1, 0x80, 0xD0, 0xB0, 0xD0, 0xB2,
    0xD1, 0x81, 0xD1, 0x82, 0xD0, 0xB2, 0xD1, 0x83, 0xD0, 0xB8, 0xCC, 0x86,
    0xD1, 0x82, 0xD0, 0xB5, '!',  ' ',  'x',  '=',  '(',  '-',  'b',  0xC2,
    0xB1, 's',  'q',  'r',  't',  '(',  'b',  0xC2, 0xB2, '-',  '4',  'a',
    'c',  ')',  ')',  '/',  '(',  '2',  'a',  ')',  ' ',  ' ',  0xE6, 0x97,
    0xA5, 0xE6, 0x9C, 0xAC, 0xE8, 0xAA, 0x9E, ',',  0xE4, 0xB8, 0xAD, 0xE6,
    0x96, 0x87, ',',  0xE1, 0x84, 0x92, 0xE1, 0x85, 0xA1, 0xE1, 0x86, 0xAB,
    0xE1, 0x84, 0x80, 0xE1, 0x85, 0xB3, 0xE1, 0x86, 0xAF, 'z',  0};

  size_t len = 0;
  char* result = NormalizeUtf8ToNFC((const char*)kDecomposed,
                                    strlen((const char*)kDecomposed), &len);

  size_t l1 = sizeof(kComposed) - 1;
  size_t l2 = strlen(result);
  EXPECT_TRUE(l1 == l2);
  EXPECT_TRUE(std::string((char*)kComposed, l1) == std::string(result, l2));
  FreeString(result);
}
