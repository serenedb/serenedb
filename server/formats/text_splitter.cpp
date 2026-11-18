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
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "pg/copy_batch_builder.h"

namespace sdb::formats {
namespace {

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
  : _columns{columns},
    _capacity{capacity},
    _rows{duckdb::RowDialect::PostgresText(delimiter), columns},
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
  _rows.Prepare(bytes, pos, max_rows);
  size_t row = 0;
  for (; row < _rows.Rows() && result.rows < max_rows; ++row) {
    const auto first = _rows.FirstField(row);
    const auto fields = _rows.LastField(row) - first;
    const auto row_start = _rows.RowStart(row);
    if (fields == 1 &&
        std::string_view{data + row_start, _rows.FieldEnd(first) - row_start} ==
          "\\.") {
      result.marker = true;
      pos = _rows.After(row);
      _rows.Consume(row + 1);
      return result;
    }
    if (fields != _columns) {
      result.error = BadRow(bytes, row_start, fields, _columns);
      result.fields = fields;
      result.row_start = row_start;
      return result;
    }
    for (size_t column = 0; column < fields; ++column) {
      const auto index = first + column;
      const auto begin = _rows.FieldBegin(row, index);
      AddField(column, result.rows,
               {data + begin, _rows.FieldEnd(index) - begin},
               _rows.Flagged(index));
    }
    ++result.rows;
  }
  pos = row != 0 ? _rows.After(row - 1) : bytes.size();
  _rows.Consume(row);
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
