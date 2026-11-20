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

#include "formats/csv_splitter.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <duckdb/formats/csv_simd.hpp>
#include <limits>

namespace sdb::formats {
namespace {

constexpr size_t kProbeBytes = size_t{1} << 12;

duckdb::RowDialect RowDialectOf(const CsvDialect& dialect) {
  return duckdb::RowDialect::PostgresCsv(dialect.delimiter, dialect.quote,
                                         dialect.escape, dialect.line_break);
}

duckdb::RowLexer ProbeLexer(const CsvDialect& dialect) {
  return {RowDialectOf(dialect), 1, std::numeric_limits<duckdb::idx_t>::max(),
          true};
}

duckdb::RowCursor ProbeCursor(const duckdb::RowLexer& lexer, size_t from,
                              const CsvQuoteState& state) {
  duckdb::RowCursor cursor;
  lexer.Start(cursor, from, true, false);
  cursor.in_quotes = state.in_quote;
  cursor.escape_carry = state.escaped;
  return cursor;
}

template<bool kFirst>
size_t UnquotedLineBreak(std::string_view bytes, size_t from,
                         const CsvDialect& dialect, CsvQuoteState& state) {
  uint64_t inside = state.in_quote ? ~uint64_t{0} : 0;
  size_t found = std::string_view::npos;
  char tail[duckdb::CSV_BLOCK_BYTES];
  for (size_t pos = from; pos < bytes.size();) {
    const char* block = bytes.data() + pos;
    const size_t size = std::min(bytes.size() - pos, duckdb::CSV_BLOCK_BYTES);
    if (size < duckdb::CSV_BLOCK_BYTES) {
      std::memset(tail, 0, sizeof(tail));
      std::memcpy(tail, block, size);
      block = tail;
    }
    const duckdb::CsvVector vector{block};
    const uint64_t quotes = vector.Equal(dialect.quote);
    const uint64_t in = (quotes ? duckdb::CsvPrefixXor(quotes) : 0) ^ inside;
    const uint64_t feeds = vector.Equal(dialect.line_break) & ~in;
    if (feeds) {
      if constexpr (kFirst) {
        state = {};
        return pos + std::countr_zero(feeds);
      }
      found = pos + 63 - std::countl_zero(feeds);
    }
    inside = duckdb::CsvSpread((in >> (size - 1)) & 1);
    pos += size;
  }
  state.in_quote = inside != 0;
  state.escaped = false;
  return found;
}

}  // namespace

CsvSplitter::CsvSplitter(CsvDialect dialect, size_t columns, size_t capacity)
  : _dialect{dialect},
    _columns{columns},
    _capacity{capacity},
    _fields(columns * capacity),
    _flags(columns * capacity),
    _offsets(capacity) {}

uint8_t CsvSplitter::FieldFlags(std::string_view field,
                                bool flagged) const noexcept {
  if (!flagged) {
    return 0;
  }
  const auto quote = _dialect.quote;
  const auto first = field.find(quote);
  if (first == std::string_view::npos) {
    return 0;
  }
  const bool simple = _dialect.escape == quote && first == 0 &&
                      field.size() >= 2 &&
                      field.find(quote, 1) == field.size() - 1;
  return simple ? kCsvQuoted : kCsvQuoted | kCsvComplex;
}

void CsvSplitter::AddField(size_t column, size_t row, std::string_view field,
                           uint8_t flags) {
  if (column >= _columns) {
    return;
  }
  const auto index = column * _capacity + row;
  _fields[index] = field;
  _flags[index] = flags;
}

void CsvSplitter::FinishRow(size_t columns_seen, size_t row) {
  for (auto column = columns_seen; column < _columns; ++column) {
    AddField(column, row, {}, kCsvMissing);
  }
  if (columns_seen > _columns) {
    _extra.push_back(static_cast<uint32_t>(row));
  }
}

CsvRows CsvSplitter::FillLexed(const duckdb::RowRangeReader& reader,
                               size_t first, size_t rows) {
  _extra.clear();
  CsvRows result;
  const auto& fields = reader.fields;
  const auto& window = *reader.bytes;
  const char* const data = window.data;
  const size_t shift = reader.cursor.anchor - window.offset;
  const size_t delimiter = reader.dialect.delimiter_size;
  for (size_t row = 0; row < rows; ++row) {
    const size_t lexed = first + row;
    const auto* anomaly =
      fields.anomalies.empty() ? nullptr : fields.Anomaly(lexed);
    const size_t count = anomaly ? anomaly->fields : _columns;
    const size_t row_start = fields.rows[lexed] + shift;
    _offsets[row] = static_cast<uint32_t>(row_start);
    for (size_t column = 0; column < std::min(count, _columns); ++column) {
      const size_t index = lexed * _columns + column;
      const uint32_t end_bits = fields.ends[index];
      const size_t begin =
        column == 0 ? row_start
                    : (fields.ends[index - 1] & duckdb::ROW_FIELD_MASK) +
                        shift + delimiter;
      const std::string_view field{
        data + begin, (end_bits & duckdb::ROW_FIELD_MASK) + shift - begin};
      AddField(column, row, field,
               FieldFlags(field, (end_bits & duckdb::ROW_FIELD_FLAG) != 0));
    }
    FinishRow(count, row);
  }
  result.rows = rows;
  result.unterminated = reader.cursor.unfinished && reader.Finished() &&
                        first + rows == fields.row_count;
  return result;
}

size_t CsvRecordEnd(std::string_view bytes, size_t from,
                    const CsvDialect& dialect, CsvQuoteState& state) {
  if (dialect.escape == dialect.quote) {
    return UnquotedLineBreak<true>(bytes, from, dialect, state);
  }
  const auto lexer = ProbeLexer(dialect);
  auto cursor = ProbeCursor(lexer, from, state);
  while (cursor.pos < bytes.size()) {
    const auto end = std::min(bytes.size(), cursor.pos + kProbeBytes);
    lexer.Feed(cursor, bytes.data(), 0, end, end == bytes.size(), nullptr);
    if (cursor.first_term != duckdb::DConstants::INVALID_INDEX) {
      state = {};
      return cursor.first_term;
    }
  }
  state.in_quote = cursor.in_quotes != 0;
  state.escaped = cursor.escape_carry != 0;
  return std::string_view::npos;
}

size_t CsvLastRecordEnd(std::string_view bytes, const CsvDialect& dialect,
                        CsvQuoteState& state) {
  if (dialect.escape == dialect.quote) {
    return UnquotedLineBreak<false>(bytes, 0, dialect, state);
  }
  const auto lexer = ProbeLexer(dialect);
  auto cursor = ProbeCursor(lexer, 0, state);
  lexer.Feed(cursor, bytes.data(), 0, bytes.size(), true, nullptr);
  state.in_quote = cursor.in_quotes != 0;
  state.escaped = cursor.escape_carry != 0;
  return cursor.last_term == duckdb::DConstants::INVALID_INDEX
           ? std::string_view::npos
           : cursor.last_term;
}

char DetectLineBreak(std::string_view bytes, bool csv,
                     const CsvDialect& dialect, bool complete) noexcept {
  const bool quoted_escape = dialect.escape != dialect.quote;
  bool in_quote = false;
  for (size_t i = 0; i < bytes.size(); ++i) {
    const char c = bytes[i];
    if (csv && in_quote) {
      if (quoted_escape && c == dialect.escape && i + 1 < bytes.size() &&
          (bytes[i + 1] == dialect.quote || bytes[i + 1] == dialect.escape)) {
        ++i;
      } else if (c == dialect.quote) {
        in_quote = false;
      }
      continue;
    }
    if (csv && c == dialect.quote) {
      in_quote = true;
      continue;
    }
    if (!csv && c == '\\') {
      ++i;
      continue;
    }
    if (c == '\n') {
      return '\n';
    }
    if (c == '\r') {
      if (i + 1 < bytes.size()) {
        return bytes[i + 1] == '\n' ? '\n' : '\r';
      }
      return complete ? '\r' : '\0';
    }
  }
  return complete ? '\n' : '\0';
}

std::string_view UnquoteCsv(std::string_view raw, const CsvDialect& dialect,
                            char* out) {
  char* const begin = out;
  size_t i = 0;
  while (i < raw.size()) {
    const char c = raw[i++];
    if (c != dialect.quote) {
      *out++ = c;
      continue;
    }
    while (i < raw.size()) {
      const char q = raw[i++];
      if (q == dialect.escape && i < raw.size() &&
          (raw[i] == dialect.escape || raw[i] == dialect.quote)) {
        *out++ = raw[i++];
        continue;
      }
      if (q == dialect.quote) {
        break;
      }
      *out++ = q;
    }
  }
  return {begin, static_cast<size_t>(out - begin)};
}

}  // namespace sdb::formats
