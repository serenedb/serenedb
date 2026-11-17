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

#include <cstddef>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "formats/text_splitter.h"
#include "pg/copy_batch_builder.h"

namespace {

using sdb::formats::TextSplitter;

struct Row {
  std::vector<std::string_view> fields;
  std::vector<bool> escaped;
};

struct Expected {
  std::vector<Row> rows;
  bool marker = false;
  bool error = false;
  size_t pos = 0;
};

std::vector<std::string_view> SplitLine(std::string_view line, char delim,
                                        std::vector<bool>& escaped) {
  std::vector<std::string_view> fields;
  size_t start = 0;
  bool backslash = false;
  for (size_t i = 0; i < line.size(); ++i) {
    if (line[i] == '\\') {
      backslash = true;
      ++i;
      continue;
    }
    if (line[i] == delim) {
      fields.push_back(line.substr(start, i - start));
      escaped.push_back(backslash);
      start = i + 1;
      backslash = false;
    }
  }
  fields.push_back(line.substr(start));
  escaped.push_back(backslash || line.substr(start).contains('\\'));
  return fields;
}

Expected Reference(std::string_view bytes, size_t pos, size_t columns,
                   size_t max_rows, char delim) {
  Expected result;
  while (pos < bytes.size() && result.rows.size() < max_rows) {
    const auto end = sdb::pg::FindRowEnd(bytes, pos);
    const auto next = end == std::string_view::npos ? bytes.size() : end + 1;
    auto line = bytes.substr(
      pos, (end == std::string_view::npos ? bytes.size() : end) - pos);
    if (line.ends_with('\r') &&
        sdb::pg::TrailingBackslashes(line.substr(0, line.size() - 1)) % 2 ==
          0) {
      line.remove_suffix(1);
    }
    Row row;
    row.fields = SplitLine(line, delim, row.escaped);
    if (row.fields.size() == 1 && row.fields[0] == "\\.") {
      result.marker = true;
      result.pos = next;
      return result;
    }
    if (row.fields.size() != columns) {
      result.error = true;
      return result;
    }
    result.rows.push_back(std::move(row));
    pos = next;
  }
  result.pos = pos;
  return result;
}

TEST(TextSplitter, MatchesScalarReference) {
  constexpr char kAlphabet[] = {'a', '\t', '\n', '\\', '\r', '.', 'N', 'b'};
  std::mt19937_64 rng{7};
  for (int iter = 0; iter < 200000; ++iter) {
    const size_t columns = 1 + rng() % 3;
    const size_t len = rng() % 200;
    std::string input;
    for (size_t i = 0; i < len; ++i) {
      input.push_back(kAlphabet[rng() % sizeof(kAlphabet)]);
    }
    const size_t max_rows = 1 + rng() % 8;
    const auto expected = Reference(input, 0, columns, max_rows, '\t');
    TextSplitter splitter{'\t', columns, max_rows};
    size_t pos = 0;
    sdb::formats::TextRows split;
    bool error = false;
    try {
      split = splitter.Split(input, pos, max_rows);
    } catch (...) {
      error = true;
    }
    ASSERT_EQ(error, expected.error) << iter;
    if (error) {
      continue;
    }
    ASSERT_EQ(split.marker, expected.marker) << iter;
    ASSERT_EQ(split.rows, expected.rows.size()) << iter;
    ASSERT_EQ(pos, expected.pos) << iter;
    std::vector<bool> escaped(columns * max_rows);
    for (const auto index : splitter.Escaped()) {
      ASSERT_LT(index % max_rows, split.rows) << iter;
      escaped[index] = true;
    }
    for (size_t row = 0; row < split.rows; ++row) {
      for (size_t column = 0; column < columns; ++column) {
        ASSERT_EQ(splitter.Column(column, split.rows)[row],
                  expected.rows[row].fields[column])
          << iter << " row " << row << " column " << column;
        ASSERT_EQ(escaped[column * max_rows + row],
                  static_cast<bool>(expected.rows[row].escaped[column]))
          << iter << " row " << row << " column " << column;
      }
    }
  }
}

TEST(TextSplitter, ContinuesAtTheRowItStoppedAt) {
  std::string input;
  for (int i = 0; i < 500; ++i) {
    input += std::to_string(i) + "\tv\\\\" + std::to_string(i) + "\n";
  }
  TextSplitter splitter{'\t', 2, 7};
  size_t pos = 0;
  int next = 0;
  while (pos < input.size()) {
    const auto split = splitter.Split(input, pos, 7);
    ASSERT_GT(split.rows, 0);
    for (size_t row = 0; row < split.rows; ++row, ++next) {
      EXPECT_EQ(splitter.Column(0, split.rows)[row], std::to_string(next));
      EXPECT_EQ(splitter.Column(1, split.rows)[row],
                "v\\\\" + std::to_string(next));
    }
  }
  EXPECT_EQ(next, 500);
}

TEST(TextSplitter, UnescapesLikePostgres) {
  std::string out(64, '\0');
  EXPECT_EQ(sdb::formats::UnescapeText("a\\tb\\nc\\\\d", out.data()),
            "a\tb\nc\\d");
  EXPECT_EQ(sdb::formats::UnescapeText("\\x41\\101\\x\\q\\", out.data()),
            "AAxq\\");
  EXPECT_EQ(sdb::formats::UnescapeText("\\777", out.data()), "\xFF");
  EXPECT_THROW(sdb::formats::UnescapeText("a\\.b", out.data()), std::exception);
}

}  // namespace
