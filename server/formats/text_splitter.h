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
#include <span>
#include <string_view>
#include <vector>

#include "formats/field_columns.h"
#include "formats/lexed_rows.h"

namespace sdb::formats {

enum class RowError : uint8_t {
  None,
  ExtraData,
  MissingData,
  MarkerNotAlone,
};

struct TextRows {
  size_t rows = 0;
  bool marker = false;
  RowError error = RowError::None;
  size_t fields = 0;
  size_t row_start = 0;
};

class TextSplitter {
 public:
  TextSplitter(char delimiter, size_t columns, size_t capacity);

  TextRows Split(std::string_view bytes, size_t& pos, size_t max_rows);

  std::span<std::string_view> Column(size_t column, size_t rows) noexcept {
    return {_fields.data() + column * _capacity, rows};
  }
  FieldColumns Fields() noexcept { return {_fields.data(), _capacity}; }
  std::string_view& Field(uint32_t index) noexcept { return _fields[index]; }
  std::span<const uint32_t> Escaped() const noexcept { return _escaped; }
  size_t Capacity() const noexcept { return _capacity; }

 private:
  void AddField(size_t column, size_t row, std::string_view field,
                bool backslash);

  const size_t _columns;
  const size_t _capacity;
  LexedRows _rows;
  std::vector<std::string_view> _fields;
  std::vector<uint32_t> _escaped;
};

std::string_view UnescapeText(std::string_view field, char* out);

}  // namespace sdb::formats
