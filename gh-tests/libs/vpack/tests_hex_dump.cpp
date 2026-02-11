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

TEST(HexDumpTest, TestPointer) {
  std::shared_ptr<Builder> b = Parser::fromJson("\"foobar\"");
  std::ostringstream out;
  Slice s = b->slice();
  out << HexDump(s);

  EXPECT_EQ("0x86 0x66 0x6f 0x6f 0x62 0x61 0x72", out.str());
  EXPECT_EQ("0x86 0x66 0x6f 0x6f 0x62 0x61 0x72",
            HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestNone) {
  std::ostringstream out;
  out << HexDump(Slice::noneSlice());

  EXPECT_EQ("0x00", out.str());
  EXPECT_EQ("0x00", HexDump(Slice::noneSlice()).toString());
}

TEST(HexDumpTest, TestNull) {
  std::shared_ptr<Builder> b = Parser::fromJson("null");
  std::ostringstream out;
  out << HexDump(b->slice());

  EXPECT_EQ("0x18", out.str());
  EXPECT_EQ("0x18", HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestTrue) {
  std::shared_ptr<Builder> b = Parser::fromJson("true");
  std::ostringstream out;
  out << HexDump(b->slice());

  EXPECT_EQ("0x1a", out.str());
  EXPECT_EQ("0x1a", HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestFalse) {
  std::shared_ptr<Builder> b = Parser::fromJson("false");
  std::ostringstream out;
  out << HexDump(b->slice());

  EXPECT_EQ("0x19", out.str());
  EXPECT_EQ("0x19", HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestNumber) {
  std::shared_ptr<Builder> b = Parser::fromJson("0");
  std::ostringstream out;
  out << HexDump(b->slice());

  EXPECT_EQ("0x36", out.str());
  EXPECT_EQ("0x36", HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestString) {
  std::shared_ptr<Builder> b = Parser::fromJson("\"foobar\"");
  std::ostringstream out;
  out << HexDump(b->slice());

  EXPECT_EQ("0x86 0x66 0x6f 0x6f 0x62 0x61 0x72", out.str());
  EXPECT_EQ("0x86 0x66 0x6f 0x6f 0x62 0x61 0x72",
            HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestArray) {
  std::shared_ptr<Builder> b = Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]");
  std::ostringstream out;
  out << HexDump(b->slice());

  EXPECT_EQ(
    "0x06 0x18 0x0a 0x37 0x38 0x39 0x3a 0x3b 0x3c 0x3d 0x3e 0x3f 0x28 0x0a "
    "0x03 0x04 \n0x05 0x06 0x07 0x08 0x09 0x0a 0x0b 0x0c",
    out.str());
  EXPECT_EQ(
    "0x06 0x18 0x0a 0x37 0x38 0x39 0x3a 0x3b 0x3c 0x3d 0x3e 0x3f 0x28 0x0a "
    "0x03 0x04 \n0x05 0x06 0x07 0x08 0x09 0x0a 0x0b 0x0c",
    HexDump(b->slice()).toString());
}

TEST(HexDumpTest, TestFromBytes) {
  uint8_t data[] = {0x01, 0x02, 0xff, 0x40, 0x00, 0xab, 0xcd, 0xde};
  std::ostringstream out;
  out << HexDump(data, sizeof(data), 4, " ");

  EXPECT_EQ("0x01 0x02 0xff 0x40 \n0x00 0xab 0xcd 0xde", out.str());
  EXPECT_EQ("0x01 0x02 0xff 0x40 \n0x00 0xab 0xcd 0xde",
            HexDump(data, sizeof(data), 4, " ").toString());
}

TEST(HexDumpTest, TestValuesPerLine) {
  std::shared_ptr<Builder> b = Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]");
  std::ostringstream out;
  out << HexDump(b->slice(), 4, " ");

  EXPECT_EQ(
    "0x06 0x18 0x0a 0x37 \n0x38 0x39 0x3a 0x3b \n0x3c 0x3d 0x3e 0x3f \n0x28 "
    "0x0a 0x03 0x04 \n0x05 0x06 0x07 0x08 \n0x09 0x0a 0x0b 0x0c",
    out.str());
  EXPECT_EQ(
    "0x06 0x18 0x0a 0x37 \n0x38 0x39 0x3a 0x3b \n0x3c 0x3d 0x3e 0x3f \n0x28 "
    "0x0a 0x03 0x04 \n0x05 0x06 0x07 0x08 \n0x09 0x0a 0x0b 0x0c",
    HexDump(b->slice(), 4, " ").toString());
}

TEST(HexDumpTest, TestSeparator) {
  std::shared_ptr<Builder> b = Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]");
  std::ostringstream out;
  out << HexDump(b->slice(), 16, ", ");

  EXPECT_EQ(
    "0x06, 0x18, 0x0a, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, "
    "0x28, 0x0a, 0x03, 0x04, \n0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, "
    "0x0c",
    out.str());
  EXPECT_EQ(
    "0x06, 0x18, 0x0a, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, "
    "0x28, 0x0a, 0x03, 0x04, \n0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, "
    "0x0c",
    HexDump(b->slice(), 16, ", ").toString());
}

TEST(HexDumpTest, TestEmptySeparator) {
  std::shared_ptr<Builder> b = Parser::fromJson("[1,2,3,4,5,6,7,8,9,10]");
  std::ostringstream out;
  out << HexDump(b->slice(), 16, "");

  EXPECT_EQ(
    "0x060x180x0a0x370x380x390x3a0x3b0x3c0x3d0x3e0x3f0x280x0a0x030x04\n0x050x"
    "060x070x080x090x0a0x0b0x0c",
    out.str());
  EXPECT_EQ(
    "0x060x180x0a0x370x380x390x3a0x3b0x3c0x3d0x3e0x3f0x280x0a0x030x04\n0x050x"
    "060x070x080x090x0a0x0b0x0c",
    HexDump(b->slice(), 16, "").toString());
}
