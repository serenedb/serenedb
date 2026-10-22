////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "third_party/duckdb_pdf/src/include/podofo_layout.hpp"

namespace duckdb::pdf {
namespace {

TEST(PdfLayoutTest, EmptyAndUnorderedLines) {
  EXPECT_EQ(GetReadingOrderText({}, 612), "");
  const std::vector<TextWord> words = {
    {50, 40, 100, 50, "Second"},
    {120, 10, 160, 20, "world"},
    {50, 10, 100, 20, "Hello"},
  };
  EXPECT_EQ(GetReadingOrderText(words, 612), "Hello world\nSecond");
}

TEST(PdfLayoutTest, ColumnsBelowAHeading) {
  const std::vector<TextWord> words = {
    {330, 100, 510, 110, "Right first"},
    {330, 120, 510, 130, "Right last"},
    {48, 120, 228, 130, "Left last"},
    {48, 100, 228, 110, "Left first"},
    {48, 10, 550, 30, "Heading across both columns"},
  };
  EXPECT_EQ(GetReadingOrderText(words, 612),
            "Heading across both columns\nLeft first\nLeft last\nRight "
            "first\nRight last");
}

TEST(PdfLayoutTest, HyphensDoNotCrossColumnsOrParagraphs) {
  const std::vector<TextWord> words = {
    {48, 10, 228, 20, "hyphen-"}, {48, 30, 228, 40, "ated"},
    {48, 50, 228, 60, "last-"},   {330, 10, 510, 20, "con-"},
    {330, 30, 510, 40, "tinued"}, {48, 160, 228, 170, "next paragraph"},
  };
  EXPECT_EQ(GetReadingOrderText(words, 612),
            "hyphenated\nlast-\ncontinued\nnext paragraph");
}

TEST(PdfLayoutTest, InvalidGeometryPreservesContent) {
  const std::vector<TextWord> words = {
    {0, 0, 0, 10, "zero width"},
    {0, std::numeric_limits<double>::quiet_NaN(), 20, 10, "unknown position"},
  };
  EXPECT_EQ(GetReadingOrderText(words, 612), "zero width\nunknown position");
}

}  // namespace
}  // namespace duckdb::pdf
