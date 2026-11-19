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

#include <algorithm>
#include <cstddef>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "formats/csv_splitter.h"
#include "formats/lexed_unit.h"

namespace {

using sdb::formats::CsvDialect;
using sdb::formats::CsvSplitter;
using sdb::formats::LexedUnit;

struct RefField {
  std::string value;
  bool quoted = false;
};

struct Reference {
  std::vector<std::vector<RefField>> rows;
  bool unterminated = false;
};

std::vector<RefField> SplitLine(std::string_view line, const CsvDialect& d,
                                bool& unterminated) {
  std::vector<RefField> fields;
  size_t i = 0;
  for (;;) {
    RefField field;
    bool delimited = false;
    for (;;) {
      bool quoted = false;
      while (i < line.size()) {
        const char c = line[i++];
        if (c == d.delimiter) {
          delimited = true;
          break;
        }
        if (c == d.quote) {
          field.quoted = quoted = true;
          break;
        }
        field.value.push_back(c);
      }
      if (!quoted) {
        break;
      }
      for (;;) {
        if (i >= line.size()) {
          unterminated = true;
          break;
        }
        const char c = line[i++];
        if (c == d.escape && i < line.size() &&
            (line[i] == d.escape || line[i] == d.quote)) {
          field.value.push_back(line[i++]);
          continue;
        }
        if (c == d.quote) {
          break;
        }
        field.value.push_back(c);
      }
      if (unterminated) {
        break;
      }
    }
    fields.push_back(std::move(field));
    if (!delimited || unterminated) {
      return fields;
    }
  }
}

Reference Parse(std::string_view input, const CsvDialect& d) {
  Reference result;
  const char escape = d.escape == d.quote ? '\0' : d.escape;
  size_t start = 0;
  bool in_quote = false;
  bool last_was_escape = false;
  const auto line = [&](size_t end, bool at_eof) {
    auto text = input.substr(start, end - start);
    if (text.ends_with('\r') && !(at_eof && in_quote)) {
      text.remove_suffix(1);
    }
    bool unterminated = false;
    result.rows.push_back(SplitLine(text, d, unterminated));
    result.unterminated |= unterminated;
  };
  for (size_t i = 0; i < input.size(); ++i) {
    const char c = input[i];
    if (in_quote && c == escape) {
      last_was_escape = !last_was_escape;
    }
    if (c == d.quote && !last_was_escape) {
      in_quote = !in_quote;
    }
    if (c != escape) {
      last_was_escape = false;
    }
    if (!in_quote && c == '\n') {
      line(i, false);
      start = i + 1;
    }
  }
  if (start < input.size()) {
    line(input.size(), true);
  }
  result.unterminated |= in_quote;
  return result;
}

void Check(const std::string& input, const CsvDialect& dialect, size_t columns,
           size_t max_rows, int iter) {
  const auto expected = Parse(input, dialect);
  LexedUnit unit{duckdb::RowDialect::PostgresCsv(dialect.delimiter,
                                                 dialect.quote, dialect.escape),
                 columns};
  auto& reader = unit.Read(input);
  CsvSplitter splitter{dialect, columns, max_rows};
  const auto next_rows = [&] {
    if (reader.Advance(max_rows) == duckdb::RowReadResult::DONE) {
      return size_t{0};
    }
    return std::min<size_t>(reader.Ready(), max_rows);
  };
  size_t next = 0;
  bool unterminated = false;
  std::vector<char> out(input.size() + 1);
  if (expected.unterminated) {
    for (size_t rows; (rows = next_rows()) != 0; reader.Emit(rows)) {
      unterminated |= splitter.FillLexed(reader, 0, rows).unterminated;
    }
    ASSERT_TRUE(unterminated) << iter;
    return;
  }
  for (size_t rows; (rows = next_rows()) != 0; reader.Emit(rows)) {
    const auto split = splitter.FillLexed(reader, 0, rows);
    unterminated |= split.unterminated;
    std::vector<bool> extra(split.rows);
    for (const auto row : splitter.ExtraRows()) {
      extra[row] = true;
    }
    for (size_t row = 0; row < split.rows; ++row, ++next) {
      ASSERT_LT(next, expected.rows.size()) << iter;
      const auto& fields = expected.rows[next];
      ASSERT_EQ(extra[row], fields.size() > columns) << iter << " row " << next;
      for (size_t column = 0; column < columns; ++column) {
        const auto flags = splitter.Flags(column, split.rows)[row];
        if (column >= fields.size()) {
          ASSERT_EQ(flags, sdb::formats::kCsvMissing) << iter;
          continue;
        }
        const auto raw = splitter.Column(column, split.rows)[row];
        ASSERT_EQ((flags & sdb::formats::kCsvQuoted) != 0,
                  fields[column].quoted)
          << iter << " row " << next << " column " << column;
        const auto value =
          (flags & sdb::formats::kCsvComplex) != 0
            ? sdb::formats::UnquoteCsv(raw, dialect, out.data())
          : (flags & sdb::formats::kCsvQuoted) != 0
            ? raw.substr(1, raw.size() - 2)
            : raw;
        ASSERT_EQ(value, fields[column].value)
          << iter << " row " << next << " column " << column;
      }
    }
  }
  ASSERT_EQ(next, expected.rows.size()) << iter;
  ASSERT_EQ(unterminated, expected.unterminated) << iter;
}

TEST(CsvSplitter, MatchesPostgresReference) {
  constexpr char kAlphabet[] = {'a', ',', '"', '\n', '\r', '\\', 'b', ' '};
  std::mt19937_64 rng{17};
  for (int iter = 0; iter < 300000; ++iter) {
    const CsvDialect dialect{',', '"', iter % 4 == 3 ? '\\' : '"'};
    const size_t len = rng() % 220;
    std::string input;
    for (size_t i = 0; i < len; ++i) {
      input.push_back(kAlphabet[rng() % sizeof(kAlphabet)]);
    }
    Check(input, dialect, 1 + rng() % 3, 1 + rng() % 6, iter);
    if (HasFatalFailure()) {
      return;
    }
  }
}

TEST(CsvSplitter, RecordEndFollowsQuotes) {
  const CsvDialect dialect;
  const std::string input = "a,\"x\ny\"\nb,c\n";
  sdb::formats::CsvQuoteState state;
  EXPECT_EQ(sdb::formats::CsvRecordEnd(input, 0, dialect, state), 7);
  EXPECT_FALSE(state.in_quote);
  state.in_quote = true;
  EXPECT_EQ(sdb::formats::CsvRecordEnd(input, 5, dialect, state), 7);
  state.in_quote = false;
  EXPECT_EQ(sdb::formats::CsvRecordEnd(input, 5, dialect, state),
            std::string_view::npos);
  EXPECT_TRUE(state.in_quote);
}

TEST(CsvSplitter, RecordEndKeepsEscapeAcrossChunks) {
  const CsvDialect dialect{.delimiter = ',', .quote = '\'', .escape = '\\'};
  const std::string input = "1,'a\\'\nb'\n2,c\n";
  for (size_t cut = 0; cut <= input.size(); ++cut) {
    sdb::formats::CsvQuoteState state;
    auto end = sdb::formats::CsvRecordEnd(
      std::string_view{input}.substr(0, cut), 0, dialect, state);
    if (end == std::string_view::npos) {
      end = sdb::formats::CsvRecordEnd(input, cut, dialect, state);
    }
    EXPECT_EQ(end, 9) << "cut at " << cut;
  }
}

}  // namespace
