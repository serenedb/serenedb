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

#include "formats/lexed_rows.h"

#include <algorithm>
#include <iresearch/utils/assert.hpp>
#include <limits>

namespace sdb::formats {
namespace {

constexpr size_t kPieceBytes = size_t{1} << 16;

}  // namespace

LexedRows::LexedRows(const duckdb::RowDialect& dialect, size_t columns)
  : _lexer{dialect, columns, std::numeric_limits<duckdb::idx_t>::max(), true} {}

void LexedRows::Prepare(std::string_view bytes, size_t pos, size_t rows) {
  if (bytes.data() != _data || bytes.size() != _size || pos != _next) {
    _data = bytes.data();
    _size = bytes.size();
    _origin = pos;
    _next = pos;
    _lexer.Start(_cursor, pos, true, false);
    _fields.Clear();
    _bounds.clear();
    _dropped = 0;
    _finished = false;
    _unterminated = false;
  }
  while (_bounds.size() <= rows && !_finished) {
    if (_cursor.pos < _size) {
      const size_t end = std::min(_size, _cursor.pos + kPieceBytes);
      [[maybe_unused]] const bool fed =
        _lexer.Feed(_cursor, _data, 0, end, end == _size, &_fields);
      SDB_ASSERT(fed);
    } else {
      if (_lexer.Dialect().backslash) {
        _cursor.escape_carry = 0;
      }
      duckdb::CsvRangeEntry entry;
      if (!_lexer.Finish(_cursor, _data, 0, _size, true, &_fields,
                         duckdb::CSVState::NOT_SET, entry)) {
        _unterminated = true;
        if (_fields.row_count > _cursor.rows - _dropped) {
          _fields.Reserve(1);
          _fields.ends[_fields.end_count++] =
            static_cast<uint32_t>(_size - _origin) | duckdb::ROW_FIELD_FLAG;
          ++_cursor.rows;
        }
      }
      _finished = true;
    }
    Group();
  }
}

void LexedRows::Group() {
  const size_t complete = _cursor.rows - _dropped;
  size_t index = _bounds.empty() ? 0 : _bounds.back();
  for (auto row = _bounds.size(); row < complete; ++row) {
    const uint32_t next = row + 1 < _fields.row_count
                            ? _fields.rows[row + 1]
                            : std::numeric_limits<uint32_t>::max();
    while (index < _fields.end_count &&
           (_fields.ends[index] & duckdb::ROW_FIELD_MASK) < next) {
      ++index;
    }
    _bounds.push_back(static_cast<uint32_t>(index));
  }
}

void LexedRows::Consume(size_t rows) {
  if (rows == 0) {
    return;
  }
  _next = After(rows - 1);
  const auto ends = _bounds[rows - 1];
  _fields.Drop(rows, ends);
  _bounds.erase(_bounds.begin(), _bounds.begin() + rows);
  for (auto& bound : _bounds) {
    bound -= ends;
  }
  _dropped += rows;
}

}  // namespace sdb::formats
