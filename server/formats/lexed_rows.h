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

#pragma once

#include <cstddef>
#include <cstdint>
#include <duckdb/formats/row_lexer.hpp>
#include <string_view>
#include <vector>

namespace sdb::formats {

class LexedRows {
 public:
  LexedRows(const duckdb::RowDialect& dialect, size_t columns);

  void Prepare(std::string_view bytes, size_t pos, size_t rows);
  void Consume(size_t rows);

  size_t Rows() const noexcept { return _bounds.size(); }
  bool Unterminated() const noexcept { return _unterminated; }
  size_t FirstField(size_t row) const noexcept {
    return row == 0 ? 0 : _bounds[row - 1];
  }
  size_t LastField(size_t row) const noexcept { return _bounds[row]; }
  size_t RowStart(size_t row) const noexcept {
    return _origin + _fields.rows[row];
  }
  size_t FieldEnd(size_t index) const noexcept {
    return _origin + (_fields.ends[index] & duckdb::ROW_FIELD_MASK);
  }
  size_t FieldBegin(size_t row, size_t index) const noexcept {
    return index == FirstField(row) ? RowStart(row) : FieldEnd(index - 1) + 1;
  }
  bool Flagged(size_t index) const noexcept {
    return (_fields.ends[index] & duckdb::ROW_FIELD_FLAG) != 0;
  }
  size_t After(size_t row) const noexcept {
    return row + 1 < _fields.row_count ? RowStart(row + 1) : _size;
  }

 private:
  void Group();

  duckdb::RowLexer _lexer;
  duckdb::RowCursor _cursor;
  duckdb::RowFields _fields;
  std::vector<uint32_t> _bounds;
  const char* _data = nullptr;
  size_t _size = 0;
  size_t _origin = 0;
  size_t _next = 0;
  size_t _dropped = 0;
  bool _finished = false;
  bool _unterminated = false;
};

}  // namespace sdb::formats
