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

#include <cstring>
#include <sstream>

#include "basics/common.h"
#include "basics/csv.h"
#include "gtest/gtest.h"

using namespace sdb;

#define INIT_PARSER                                                            \
  CsvParser parser;                                                            \
                                                                               \
  InitCsvParser(&parser, &CCsvTest::ProcessCsvBegin, &CCsvTest::ProcessCsvAdd, \
                &CCsvTest::ProcessCsvEnd, nullptr);                            \
  parser.data_add = this;

#define TAB "\x9"
#define CR "\xd"
#define LF "\xa"

class CCsvTest : public ::testing::Test {
 protected:
  CCsvTest() { _column = 0; }

  ~CCsvTest() override = default;

  static void ProcessCsvBegin(CsvParser* parser, size_t row) {
    CCsvTest* me = reinterpret_cast<CCsvTest*>(parser->data_add);

    me->_out << row << ":";
    me->_column = 0;
  }

  static void ProcessCsvAdd(CsvParser* parser, const char* field, size_t,
                            size_t row, size_t column, bool escaped) {
    CCsvTest* me = reinterpret_cast<CCsvTest*>(parser->data_add);

    if (me->_column++ > 0) {
      me->_out << ",";
    }

    me->_out << (escaped ? "ESC" : "") << field << (escaped ? "ESC" : "");
  }

  static void ProcessCsvEnd(CsvParser* parser, const char* field, size_t,
                            size_t row, size_t column, bool escaped) {
    CCsvTest* me = reinterpret_cast<CCsvTest*>(parser->data_add);

    if (me->_column++ > 0) {
      me->_out << ",";
    }

    me->_out << (escaped ? "ESC" : "") << field << (escaped ? "ESC" : "")
             << "\n";
  }

  std::ostringstream _out;
  size_t _column;
};

TEST_F(CCsvTest, tst_csv_simple) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ',');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv = "a,b,c,d,e," LF "f,g,h" LF ",,i,j,," LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0:a,b,c,d,e,\n1:f,g,h\n2:,,i,j,,\n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_crlf) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ',');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv = "a,b,c,d,e" CR LF "f,g,h" CR LF "i,j" CR LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0:a,b,c,d,e\n1:f,g,h\n2:i,j\n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_whitespace) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ',');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv = " a , \"b \" , c , d , e " LF LF LF "   x   x  " LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0: a , \"b \" , c , d , e \n1:\n2:\n3:   x   x  \n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_quotes1) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ',');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv = "\"a\",\"b\"" LF "a,b" LF "\"a,b\",\"c,d\"" LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0:ESCaESC,ESCbESC\n1:a,b\n2:ESCa,bESC,ESCc,dESC\n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_quotes2) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ',');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv =
    "\"x\"\"y\",\"a\"\"\"" LF "\"\",\"\"\"ab\",\"\"\"\"\"ab\"" LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0:ESCx\"yESC,ESCa\"ESC\n1:ESCESC,ESC\"abESC,ESC\"\"abESC\n",
            _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_quotes_whitespace) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ',');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv =
    "\"a \" ,\" \"\" b \",\"\"\"\" ,\" \" " LF " \"\" ix" LF " \"\" " LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_TRUE(
    "0:ESCa ESC,ESC \" b ESC,ESC\"ESC,ESC ESC\n1: \"\" ix\n2: \"\" \n" ==
    _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_tsv_simple) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, '\t');
  SetQuoteCsvParser(&parser, '\0', false);

  const char* tsv = "a" TAB "b" TAB "c" LF "the quick" TAB
                    "brown fox jumped" TAB "over the" TAB "lazy" TAB "dog" LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, tsv, strlen(tsv));
  EXPECT_EQ("0:a,b,c\n1:the quick,brown fox jumped,over the,lazy,dog\n",
            _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_tsv_whitespace) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, '\t');
  SetQuoteCsvParser(&parser, '\0', false);

  const char* tsv =
    "a " TAB " b" TAB " c " LF "  " LF "" LF "something else" LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, tsv, strlen(tsv));
  EXPECT_EQ("0:a , b, c \n1:  \n2:\n3:something else\n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_tsv_quotes) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, '\t');
  SetQuoteCsvParser(&parser, '\0', false);

  const char* tsv = "\"a\"" TAB "\"b\"" TAB "\"c" LF " \"" LF "\" fox " LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, tsv, strlen(tsv));
  EXPECT_EQ("0:\"a\",\"b\",\"c\n1: \"\n2:\" fox \n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_tsv_separator) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, '\t');
  SetQuoteCsvParser(&parser, ',', false);

  const char* tsv =
    "\"a,,\"" TAB "\",,b\"" TAB "\",c," LF " , ,\", " LF ",\", fox,, " LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, tsv, strlen(tsv));
  EXPECT_EQ("0:\"a,,\",\",,b\",\",c,\n1: , ,\", \n2:,\", fox,, \n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_tsv_crlf) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, '\t');
  SetQuoteCsvParser(&parser, '\0', false);

  const char* tsv =
    "a" TAB "b" TAB "c" CR LF "the quick" TAB "brown fox jumped" TAB
    "over the" TAB "lazy" TAB "dog" CR LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, tsv, strlen(tsv));
  EXPECT_EQ("0:a,b,c\n1:the quick,brown fox jumped,over the,lazy,dog\n",
            _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_semicolon) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ';');
  SetQuoteCsvParser(&parser, '"', true);

  const char* csv = "a;b,c;d;e;" LF "f;g;;\"h,;\"" LF ";" LF ";;i; ;j; ;" LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0:a,b,c,d,e,\n1:f,g,,ESCh,;ESC\n2:,\n3:,,i, ,j, ,\n", _out.str());

  DestroyCsvParser(&parser);
}

TEST_F(CCsvTest, tst_csv_semicolon_noquote) {
  INIT_PARSER
  SetSeparatorCsvParser(&parser, ';');
  SetQuoteCsvParser(&parser, '\0', false);

  const char* csv =
    "a; b; c; d  ;" CR LF CR LF " ;" CR LF " " CR LF "\" abc \" " CR LF;

  // TODO(mbkkt) why ignore?
  std::ignore = ParseCsvString(&parser, csv, strlen(csv));
  EXPECT_EQ("0:a, b, c, d  ,\n1:\n2: ,\n3: \n4:\" abc \" \n", _out.str());

  DestroyCsvParser(&parser);
}
