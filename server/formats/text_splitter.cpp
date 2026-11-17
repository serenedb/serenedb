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

#include "formats/text_splitter.h"

#include <fast_float/fast_float.h>

#include <algorithm>
#include <array>
#include <bit>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "formats/structural.h"
#include "pg/copy_batch_builder.h"

namespace sdb::formats {
namespace {

IRS_FORCE_INLINE uint64_t BitsFrom(size_t from) noexcept {
  return from >= 64 ? 0 : ~uint64_t{0} << from;
}

IRS_FORCE_INLINE uint64_t BitsBelow(size_t to) noexcept {
  return to >= 64 ? ~uint64_t{0} : (uint64_t{1} << to) - 1;
}

RowError BadRow(std::string_view bytes, size_t row_start, size_t fields,
                size_t columns) {
  const auto end = pg::FindRowEnd(bytes, row_start);
  const auto line = bytes.substr(
    row_start, end == std::string_view::npos ? end : end - row_start);
  for (size_t i = 0; i < line.size(); ++i) {
    if (line[i] != '\\') {
      continue;
    }
    if (i + 1 < line.size() && line[i + 1] == '.') {
      return RowError::MarkerNotAlone;
    }
    ++i;
  }
  return fields > columns ? RowError::ExtraData : RowError::MissingData;
}

}  // namespace

TextSplitter::TextSplitter(char delimiter, size_t columns, size_t capacity)
  : _delimiter{delimiter},
    _columns{columns},
    _capacity{capacity},
    _fields(columns * capacity) {}

void TextSplitter::AddField(size_t column, size_t row, std::string_view field,
                            bool backslash) {
  const auto index = column * _capacity + row;
  _fields[index] = field;
  if (backslash) {
    _escaped.push_back(static_cast<uint32_t>(index));
  }
}

TextRows TextSplitter::Split(std::string_view bytes, size_t& pos,
                             size_t max_rows) {
  _escaped.clear();
  TextRows result;
  const char* const data = bytes.data();
  const size_t size = bytes.size();
  const std::array<char, 3> chars{_delimiter, '\n', '\\'};
  EscapeScanner scanner;
  size_t column = 0;
  size_t field_start = pos;
  size_t row_start = pos;
  bool field_backslash = false;
  uint64_t prev_escaped = 0;
  size_t last_block = pos;
  const auto escaped_at = [&](size_t block, uint64_t escaped, size_t at) {
    return at >= block ? ((escaped >> (at - block)) & 1) != 0
                       : (prev_escaped >> 63) != 0;
  };
  const auto fail = [&](size_t fields) {
    result.error = BadRow(bytes, row_start, fields, _columns);
    result.fields = fields;
    result.row_start = row_start;
    while (!_escaped.empty() && _escaped.back() % _capacity >= result.rows) {
      _escaped.pop_back();
    }
    return true;
  };
  const auto add_field = [&](size_t end) {
    if (column == _columns) {
      return fail(column + 1);
    }
    AddField(column, result.rows, {data + field_start, end - field_start},
             field_backslash);
    ++column;
    return false;
  };
  const auto finish_row = [&](size_t row_end) {
    if (column == 1 && Column(0, result.rows + 1)[result.rows] == "\\.") {
      if (!_escaped.empty() && _escaped.back() == result.rows) {
        _escaped.pop_back();
      }
      result.marker = true;
      pos = row_end;
      return true;
    }
    if (column != _columns) {
      return fail(column);
    }
    column = 0;
    row_start = row_end;
    ++result.rows;
    if (result.rows == max_rows) {
      pos = row_end;
      return true;
    }
    return false;
  };
  for (size_t block = pos; block < size; block += kBlockBytes) {
    const size_t len = std::min(kBlockBytes, size - block);
    const auto masks = len == kBlockBytes ? MatchBlock(data + block, chars)
                                          : MatchTail(data + block, len, chars);
    const auto [delim, lf, backslash] = masks.masks;
    const auto escaped = scanner.Next(backslash);
    auto structural = (delim | lf) & ~escaped;
    while (structural) {
      const auto bit = static_cast<size_t>(std::countr_zero(structural));
      structural &= structural - 1;
      const size_t at = block + bit;
      if (!field_backslash) {
        const size_t from = field_start > block ? field_start - block : 0;
        field_backslash = (backslash & BitsFrom(from) & BitsBelow(bit)) != 0;
      }
      const bool row_end = ((lf >> bit) & 1) != 0;
      size_t end = at;
      if (row_end && end > field_start && data[end - 1] == '\r' &&
          !escaped_at(block, escaped, end - 1)) {
        --end;
      }
      if (add_field(end)) {
        return result;
      }
      field_start = at + 1;
      field_backslash = false;
      if (row_end && finish_row(at + 1)) {
        return result;
      }
    }
    if (field_start < block + len) {
      const size_t from = field_start > block ? field_start - block : 0;
      field_backslash |= (backslash & BitsFrom(from) & BitsBelow(len)) != 0;
    }
    prev_escaped = escaped;
    last_block = block;
  }
  if (field_start < size || column != 0) {
    size_t end = size;
    if (end > field_start && data[end - 1] == '\r' &&
        !((prev_escaped >> (size - 1 - last_block)) & 1)) {
      --end;
    }
    if (add_field(end) || finish_row(size)) {
      return result;
    }
  }
  pos = size;
  return result;
}

std::string_view UnescapeText(std::string_view field, char* out) {
  char* const begin = out;
  for (size_t i = 0; i < field.size();) {
    const char c = field[i++];
    if (c != '\\') {
      *out++ = c;
      continue;
    }
    if (i == field.size()) {
      *out++ = '\\';
      break;
    }
    const char e = field[i++];
    switch (e) {
      case 'b':
        *out++ = '\b';
        break;
      case 'f':
        *out++ = '\f';
        break;
      case 'n':
        *out++ = '\n';
        break;
      case 'r':
        *out++ = '\r';
        break;
      case 't':
        *out++ = '\t';
        break;
      case 'v':
        *out++ = '\v';
        break;
      case '\\':
        *out++ = '\\';
        break;
      case 'x': {
        const char* hex = field.data() + i;
        const char* hex_end = field.data() + std::min(i + 2, field.size());
        unsigned value = 0;
        const auto res = fast_float::from_chars(hex, hex_end, value, 16);
        if (res.ptr == hex) {
          *out++ = 'x';
        } else {
          *out++ = static_cast<char>(value);
          i += static_cast<size_t>(res.ptr - hex);
        }
        break;
      }
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7': {
        const char* octal = field.data() + (i - 1);
        const char* octal_end = field.data() + std::min(i + 2, field.size());
        unsigned value = 0;
        const auto res = fast_float::from_chars(octal, octal_end, value, 8);
        i = static_cast<size_t>(res.ptr - field.data());
        *out++ = static_cast<char>(value & 0xFF);
        break;
      }
      case '.':
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                        ERR_MSG("end-of-copy marker is not alone on its line"));
      default:
        *out++ = e;
        break;
    }
  }
  return {begin, static_cast<size_t>(out - begin)};
}

}  // namespace sdb::formats
