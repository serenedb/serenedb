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
#include <duckdb/formats/row_ranges.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "formats/field_columns.h"

namespace sdb::formats {

struct CsvDialect {
  char delimiter = ',';
  char quote = '"';
  char escape = '"';
};

inline constexpr uint8_t kCsvQuoted = 1;
inline constexpr uint8_t kCsvComplex = 2;
inline constexpr uint8_t kCsvMissing = 4;

struct CsvRows {
  size_t rows = 0;
  bool unterminated = false;
};

class CsvSplitter {
 public:
  CsvSplitter(CsvDialect dialect, size_t columns, size_t capacity);

  CsvRows FillLexed(const duckdb::RowRangeReader& reader, size_t first,
                    size_t rows);

  std::span<std::string_view> Column(size_t column, size_t rows) noexcept {
    return {_fields.data() + column * _capacity, rows};
  }
  FieldColumns Fields() noexcept { return {_fields.data(), _capacity}; }
  std::span<uint8_t> Flags(size_t column, size_t rows) noexcept {
    return {_flags.data() + column * _capacity, rows};
  }
  std::span<const uint32_t> RowOffsets(size_t rows) const noexcept {
    return {_offsets.data(), rows};
  }
  std::span<const uint32_t> ExtraRows() const noexcept { return _extra; }
  const CsvDialect& Dialect() const noexcept { return _dialect; }

 private:
  void AddField(size_t column, size_t row, std::string_view field,
                uint8_t flags);
  void FinishRow(size_t columns_seen, size_t row);
  uint8_t FieldFlags(std::string_view field, bool flagged) const noexcept;

  const CsvDialect _dialect;
  const size_t _columns;
  const size_t _capacity;
  std::vector<std::string_view> _fields;
  std::vector<uint8_t> _flags;
  std::vector<uint32_t> _offsets;
  std::vector<uint32_t> _extra;
};

struct CsvQuoteState {
  bool in_quote = false;
  bool escaped = false;
};

size_t CsvRecordEnd(std::string_view bytes, size_t from,
                    const CsvDialect& dialect, CsvQuoteState& state);

size_t CsvLastRecordEnd(std::string_view bytes, const CsvDialect& dialect,
                        CsvQuoteState& state);

std::string_view UnquoteCsv(std::string_view raw, const CsvDialect& dialect,
                            char* out);

}  // namespace sdb::formats
