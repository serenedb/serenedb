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
#include <podofo/main/PdfMemDocument.h>
#include <podofo/main/PdfObjectStream.h>
#include <podofo/main/PdfPageCollection.h>
#include <podofo/main/PdfPainter.h>
#include <podofo/main/PdfXObjectForm.h>

#include <limits>
#include <vector>

#include "third_party/duckdb_pdf/src/include/podofo_layout.hpp"
#include "third_party/duckdb_pdf/src/include/podofo_tables.hpp"

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

TEST(PdfLayoutTest, RuledTablesAndMergedCells) {
  const std::vector<Rule> rules = {
    {0, 0, 200, 0}, {0, 30, 200, 30},   {0, 60, 200, 60},
    {0, 0, 0, 60},  {100, 30, 100, 60}, {200, 0, 200, 60},
  };
  const std::vector<TextWord> words = {
    {110, 40, 150, 50, "42"},
    {10, 40, 70, 50, "Value"},
    {10, 10, 180, 20, "Merged heading"},
    {0, 80, 100, 90, "Outside"},
  };
  const auto tables = GetRuledTables(rules, words, [] {});
  ASSERT_EQ(tables.size(), 1);
  EXPECT_EQ(tables[0].rows, (std::vector<std::vector<std::string>>{
                              {"Merged heading", ""}, {"Value", "42"}}));
}

TEST(PdfLayoutTest, OpenBordersAndNoText) {
  const std::vector<Rule> rules = {
    {0, 0, 200, 0}, {0, 30, 200, 30},  {0, 60, 200, 60},
    {0, 0, 0, 60},  {100, 0, 100, 60}, {200, 0, 200, 60},
  };
  EXPECT_TRUE(GetRuledTables(rules, {}, [] {}).empty());
  const std::vector<TextWord> words = {{10, 10, 90, 20, "text"}};
  EXPECT_EQ(GetRuledTables(std::span(rules).subspan(1), words, [] {}).size(),
            1);
  EXPECT_TRUE(GetRuledTables({}, words, [] {}).empty());
}

TEST(PdfLayoutTest, ThreeMissingOuterBorders) {
  const std::vector<Rule> rules = {
    {0, 0, 300, 0},
    {0, 30, 300, 30},
    {100, 0, 100, 60},
    {200, 0, 200, 60},
  };
  const std::vector<TextWord> words = {
    {10, 10, 90, 20, "A"}, {110, 10, 190, 20, "B"}, {210, 10, 290, 20, "C"},
    {10, 40, 90, 50, "1"}, {110, 40, 190, 50, "2"}, {210, 40, 290, 50, "3"},
  };
  const auto tables = GetRuledTables(rules, words, [] {});
  ASSERT_EQ(tables.size(), 1);
  EXPECT_EQ(tables[0].rows, (std::vector<std::vector<std::string>>{
                              {"A", "B", "C"}, {"1", "2", "3"}}));
}

TEST(PdfLayoutTest, TableCancellationAndComplexity) {
  const std::vector<TextWord> words = {{10, 10, 90, 20, "text"}};
  const std::vector<Rule> rules = {{0, 0, 200, 0}, {0, 0, 0, 100}};
  EXPECT_THROW(
    GetRuledTables(rules, words, [] { throw std::runtime_error("cancelled"); }),
    std::runtime_error);
  std::vector<Rule> complex(10001, Rule{0, 0, 200, 0});
  EXPECT_THROW(GetRuledTables(complex, words, [] {}), std::length_error);
}

TEST(PdfLayoutTest, RightToLeftWordsKeepLatinAndNumbersInOrder) {
  const std::vector<TextWord> words = {
    {10, 10, 30, 20, "עולם"},
    {100, 10, 130, 20, "שלום"},
    {40, 10, 50, 20, "ABC"},
    {60, 10, 70, 20, "123"},
  };
  EXPECT_EQ(GetReadingOrderText(words, 612), "שלום ABC 123 עולם");
}

TEST(PdfLayoutTest, PaintedTableTransformsAndFormObjects) {
  PoDoFo::PdfMemDocument document;
  auto& page = document.GetPages().CreatePage(PoDoFo::Rect(0, 0, 300, 300));
  auto form = document.CreateXObjectForm(PoDoFo::Rect(0, 0, 100, 30));
  const std::string commands =
    "q 0 0 100 30 re f 50 0 m 50 30 l S 0 15 m 100 15 l S Q";
  static_cast<PoDoFo::PdfCanvas&>(*form)
    .GetOrCreateContentsStream(PoDoFo::PdfStreamAppendFlags::None)
    .SetData(PoDoFo::bufferview(commands));
  PoDoFo::PdfPainter painter;
  painter.SetCanvas(page);
  painter.DrawXObject(*form, 10, 20, 2, 2);
  painter.FinishDrawing();
  const std::vector<TextWord> words = {
    {20, 230, 80, 240, "A"},
    {120, 230, 180, 240, "B"},
    {20, 260, 80, 270, "C"},
    {120, 260, 180, 270, "D"},
  };
  const auto rules = GetPageRules(page, [] {});
  ASSERT_EQ(rules.size(), 6);
  const auto tables = GetRuledTables(rules, words, [] {});
  ASSERT_EQ(tables.size(), 1);
  EXPECT_EQ(tables[0].rows,
            (std::vector<std::vector<std::string>>{{"A", "B"}, {"C", "D"}}));
  EXPECT_THROW(
    GetPageRules(page, [] { throw std::runtime_error("cancelled"); }),
    std::runtime_error);
}

TEST(PdfLayoutTest, TablePageRotationAndDiscardedPaths) {
  PoDoFo::PdfMemDocument document;
  auto& page = document.GetPages().CreatePage(PoDoFo::Rect(0, 0, 300, 300));
  const std::string commands = "10 20 m 110 20 l S 0 0 50 50 re n";
  static_cast<PoDoFo::PdfCanvas&>(page)
    .GetOrCreateContentsStream(PoDoFo::PdfStreamAppendFlags::None)
    .SetData(PoDoFo::bufferview(commands));
  page.SetRotation(90);
  const auto rules = GetPageRules(page, [] {});
  ASSERT_EQ(rules.size(), 1);
  EXPECT_NEAR(rules[0].x1, 20, 1e-8);
  EXPECT_NEAR(rules[0].x2, 20, 1e-8);
  EXPECT_NEAR(rules[0].y1, 10, 1e-8);
  EXPECT_NEAR(rules[0].y2, 110, 1e-8);
}

TEST(PdfLayoutTest, MultipleTablesAndMultilineCells) {
  std::vector<Rule> rules;
  for (double y : {0.0, 100.0}) {
    for (double row : {0.0, 30.0, 60.0}) {
      rules.push_back({0, y + row, 200, y + row});
    }
    for (double x : {0.0, 100.0, 200.0}) {
      rules.push_back({x, y, x, y + 60});
    }
  }
  const std::vector<TextWord> words = {
    {10, 5, 80, 12, "first"},
    {10, 17, 80, 24, "second"},
    {110, 140, 180, 150, "other table"},
  };
  const auto tables = GetRuledTables(rules, words, [] {});
  ASSERT_EQ(tables.size(), 2);
  EXPECT_EQ(tables[0].rows[0][0], "first\nsecond");
  EXPECT_EQ(tables[0].rows[1][1], "");
  EXPECT_EQ(tables[1].rows[1][1], "other table");
}

TEST(PdfLayoutTest, NonrectangularMergedCellsAreRejected) {
  const std::vector<Rule> rules = {
    {0, 0, 200, 0},    {0, 60, 200, 60},   {0, 0, 0, 60},
    {200, 0, 200, 60}, {100, 30, 100, 60}, {100, 30, 200, 30},
  };
  const std::vector<TextWord> words = {{10, 10, 90, 20, "text"}};
  EXPECT_TRUE(GetRuledTables(rules, words, [] {}).empty());
}

TEST(PdfLayoutTest, ScaledTextWordsKeepPageCoordinates) {
  PoDoFo::PdfMemDocument document;
  auto& page = document.GetPages().CreatePage(PoDoFo::Rect(0, 0, 300, 300));
  auto& font =
    document.GetObjects().CreateDictionaryObject(PoDoFo::PdfName("Font"));
  font.GetDictionary().AddKey(PoDoFo::PdfName("Subtype"),
                              PoDoFo::PdfName("Type1"));
  font.GetDictionary().AddKey(PoDoFo::PdfName("BaseFont"),
                              PoDoFo::PdfName("Helvetica"));
  auto& canvas = static_cast<PoDoFo::PdfCanvas&>(page);
  static_cast<PoDoFo::PdfResourceOperations&>(canvas.GetOrCreateResources())
    .AddResource(PoDoFo::PdfResourceType::Font, PoDoFo::PdfName("F1"), font);
  const std::string commands = "BT /F1 1 Tf 10 0 0 10 20 200 Tm (12 34) Tj ET";
  canvas.GetOrCreateContentsStream(PoDoFo::PdfStreamAppendFlags::None)
    .SetData(PoDoFo::bufferview(commands));
  std::vector<PoDoFo::PdfTextEntry> entries;
  PoDoFo::PdfTextExtractParams params;
  params.Flags = PoDoFo::PdfTextExtractFlags::TokenizeWords |
                 PoDoFo::PdfTextExtractFlags::ComputeBoundingBox;
  page.ExtractTextTo(entries, params);
  ASSERT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].Text, "12");
  EXPECT_EQ(entries[1].Text, "34");
  EXPECT_NEAR(entries[0].X, 20, 1e-8);
  EXPECT_NEAR(entries[1].X, 33.9, 1e-8);
  ASSERT_TRUE(entries[0].BoundingBox);
  EXPECT_NEAR(entries[0].BoundingBox->Width, 11.12, 1e-8);
}

}  // namespace
}  // namespace duckdb::pdf
