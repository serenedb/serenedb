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

#include <algorithm>
#include <array>
#include <bit>

#include "formats/structural.h"

namespace sdb::formats {
namespace {

IRS_FORCE_INLINE uint64_t BitsFrom(size_t from) noexcept {
  return from >= 64 ? 0 : ~uint64_t{0} << from;
}

IRS_FORCE_INLINE uint64_t BitsBelow(size_t to) noexcept {
  return to >= 64 ? ~uint64_t{0} : (uint64_t{1} << to) - 1;
}

IRS_FORCE_INLINE uint64_t Replicate(uint64_t bits) noexcept {
  return static_cast<uint64_t>(static_cast<int64_t>(bits) >> 63);
}

uint8_t FieldFlags(std::string_view field, size_t quotes, char quote) {
  if (quotes == 0) {
    return 0;
  }
  const bool simple = quotes == 2 && field.size() >= 2 &&
                      field.front() == quote && field.back() == quote;
  return simple ? kCsvQuoted : kCsvQuoted | kCsvComplex;
}

}  // namespace

CsvSplitter::CsvSplitter(CsvDialect dialect, size_t columns, size_t capacity)
  : _dialect{dialect},
    _columns{columns},
    _capacity{capacity},
    _fields(columns * capacity),
    _flags(columns * capacity),
    _offsets(capacity) {}

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

CsvRows CsvSplitter::Split(std::string_view bytes, size_t& pos,
                           size_t max_rows) {
  _extra.clear();
  return _dialect.escape == _dialect.quote ? SplitSimd(bytes, pos, max_rows)
                                           : SplitScalar(bytes, pos, max_rows);
}

CsvRows CsvSplitter::SplitSimd(std::string_view bytes, size_t& pos,
                               size_t max_rows) {
  CsvRows result;
  const char* const data = bytes.data();
  const size_t size = bytes.size();
  const char quote = _dialect.quote;
  const std::array<char, 3> chars{_dialect.delimiter, '\n', quote};
  uint64_t carry = 0;
  uint64_t prev_inside = 0;
  size_t last_block = pos;
  size_t column = 0;
  size_t field_start = pos;
  size_t row_start = pos;
  size_t quotes = 0;
  for (size_t block = pos; block < size; block += kBlockBytes) {
    const size_t len = std::min(kBlockBytes, size - block);
    const auto masks = len == kBlockBytes ? MatchBlock(data + block, chars)
                                          : MatchTail(data + block, len, chars);
    const auto [delim, lf, quote_bits] = masks.masks;
    const auto inside = PrefixXor(quote_bits) ^ carry;
    carry = Replicate(inside);
    auto structural = (delim | lf) & ~inside;
    while (structural) {
      const auto bit = static_cast<size_t>(std::countr_zero(structural));
      structural &= structural - 1;
      const size_t at = block + bit;
      const size_t from = field_start > block ? field_start - block : 0;
      quotes += static_cast<size_t>(
        std::popcount(quote_bits & BitsFrom(from) & BitsBelow(bit)));
      const bool row_end = ((lf >> bit) & 1) != 0;
      size_t end = at;
      if (row_end && end > field_start && data[end - 1] == '\r') {
        const bool cr_inside = end - 1 >= block
                                 ? ((inside >> (end - 1 - block)) & 1) != 0
                                 : (prev_inside >> 63) != 0;
        end -= cr_inside ? 0 : 1;
      }
      const std::string_view field{data + field_start, end - field_start};
      AddField(column, result.rows, field, FieldFlags(field, quotes, quote));
      ++column;
      field_start = at + 1;
      quotes = 0;
      if (!row_end) {
        continue;
      }
      _offsets[result.rows] = static_cast<uint32_t>(row_start);
      FinishRow(column, result.rows);
      column = 0;
      row_start = at + 1;
      if (++result.rows == max_rows) {
        result.lines +=
          static_cast<size_t>(std::popcount(lf & BitsBelow(bit + 1)));
        pos = at + 1;
        return result;
      }
    }
    if (field_start < block + len) {
      const size_t from = field_start > block ? field_start - block : 0;
      quotes += static_cast<size_t>(
        std::popcount(quote_bits & BitsFrom(from) & BitsBelow(len)));
    }
    result.lines += static_cast<size_t>(std::popcount(lf));
    prev_inside = inside;
    last_block = block;
  }
  if (carry) {
    result.unterminated = true;
  }
  if (field_start < size || column != 0) {
    size_t end = size;
    if (end > field_start && data[end - 1] == '\r' &&
        !((prev_inside >> (size - 1 - last_block)) & 1)) {
      --end;
    }
    const std::string_view field{data + field_start, end - field_start};
    AddField(column, result.rows, field, FieldFlags(field, quotes, quote));
    ++column;
    _offsets[result.rows] = static_cast<uint32_t>(row_start);
    FinishRow(column, result.rows);
    ++result.rows;
  }
  pos = size;
  return result;
}

CsvRows CsvSplitter::SplitScalar(std::string_view bytes, size_t& pos,
                                 size_t max_rows) {
  CsvRows result;
  const auto [delimiter, quote, escape] = _dialect;
  const size_t size = bytes.size();
  size_t column = 0;
  size_t field_start = pos;
  size_t row_start = pos;
  bool in_quote = false;
  bool last_was_escape = false;
  bool saw_quote = false;
  const auto add = [&](size_t end) {
    const auto field = bytes.substr(field_start, end - field_start);
    AddField(column, result.rows, field,
             saw_quote ? kCsvQuoted | kCsvComplex : uint8_t{0});
    ++column;
    saw_quote = false;
  };
  for (size_t i = pos; i < size; ++i) {
    const char c = bytes[i];
    result.lines += c == '\n';
    if (in_quote && c == escape) {
      last_was_escape = !last_was_escape;
    }
    if (c == quote && !last_was_escape) {
      in_quote = !in_quote;
      saw_quote = true;
    }
    if (c != escape) {
      last_was_escape = false;
    }
    if (in_quote) {
      continue;
    }
    if (c == delimiter) {
      add(i);
      field_start = i + 1;
      continue;
    }
    if (c != '\n') {
      continue;
    }
    add(i > field_start && bytes[i - 1] == '\r' ? i - 1 : i);
    _offsets[result.rows] = static_cast<uint32_t>(row_start);
    FinishRow(column, result.rows);
    column = 0;
    field_start = i + 1;
    row_start = i + 1;
    if (++result.rows == max_rows) {
      pos = i + 1;
      return result;
    }
  }
  if (in_quote) {
    result.unterminated = true;
  }
  if (field_start < size || column != 0) {
    add(size > field_start && bytes[size - 1] == '\r' && !in_quote ? size - 1
                                                                   : size);
    _offsets[result.rows] = static_cast<uint32_t>(row_start);
    FinishRow(column, result.rows);
    ++result.rows;
  }
  pos = size;
  return result;
}

bool CsvQuoteParity(std::string_view bytes, char quote) noexcept {
  const std::array<char, 1> chars{quote};
  uint64_t count = 0;
  size_t block = 0;
  for (; block + kBlockBytes <= bytes.size(); block += kBlockBytes) {
    count += static_cast<uint64_t>(
      std::popcount(MatchBlock(bytes.data() + block, chars).masks[0]));
  }
  if (block < bytes.size()) {
    count += static_cast<uint64_t>(std::popcount(
      MatchTail(bytes.data() + block, bytes.size() - block, chars).masks[0]));
  }
  return (count & 1) != 0;
}

size_t CsvRecordEnd(std::string_view bytes, size_t from,
                    const CsvDialect& dialect, CsvQuoteState& state) noexcept {
  if (dialect.escape == dialect.quote) {
    const std::array<char, 2> chars{'\n', dialect.quote};
    uint64_t carry = state.in_quote ? ~uint64_t{0} : 0;
    for (size_t block = from; block < bytes.size(); block += kBlockBytes) {
      const size_t len = std::min(kBlockBytes, bytes.size() - block);
      const auto masks = len == kBlockBytes
                           ? MatchBlock(bytes.data() + block, chars)
                           : MatchTail(bytes.data() + block, len, chars);
      const auto inside = PrefixXor(masks.masks[1]) ^ carry;
      if (const auto ends = masks.masks[0] & ~inside) {
        const auto bit = static_cast<size_t>(std::countr_zero(ends));
        state.in_quote = ((inside >> bit) & 1) != 0;
        return block + bit;
      }
      carry = Replicate(inside);
    }
    state.in_quote = carry != 0;
    return std::string_view::npos;
  }
  for (size_t i = from; i < bytes.size(); ++i) {
    const char c = bytes[i];
    if (state.in_quote && c == dialect.escape) {
      state.escaped = !state.escaped;
    }
    if (c == dialect.quote && !state.escaped) {
      state.in_quote = !state.in_quote;
    }
    if (c != dialect.escape) {
      state.escaped = false;
    }
    if (!state.in_quote && c == '\n') {
      return i;
    }
  }
  return std::string_view::npos;
}

size_t CsvLastRecordEnd(std::string_view bytes, const CsvDialect& dialect,
                        CsvQuoteState& state) noexcept {
  size_t last = std::string_view::npos;
  if (dialect.escape == dialect.quote) {
    const std::array<char, 2> chars{'\n', dialect.quote};
    uint64_t carry = state.in_quote ? ~uint64_t{0} : 0;
    for (size_t block = 0; block < bytes.size(); block += kBlockBytes) {
      const size_t len = std::min(kBlockBytes, bytes.size() - block);
      const auto masks = len == kBlockBytes
                           ? MatchBlock(bytes.data() + block, chars)
                           : MatchTail(bytes.data() + block, len, chars);
      const auto inside = PrefixXor(masks.masks[1]) ^ carry;
      if (const auto ends = masks.masks[0] & ~inside) {
        last = block + 63 - static_cast<size_t>(std::countl_zero(ends));
      }
      carry = Replicate(inside);
    }
    state.in_quote = carry != 0;
    return last;
  }
  for (size_t from = 0;;) {
    const auto end = CsvRecordEnd(bytes, from, dialect, state);
    if (end == std::string_view::npos) {
      return last;
    }
    last = end;
    from = end + 1;
  }
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
