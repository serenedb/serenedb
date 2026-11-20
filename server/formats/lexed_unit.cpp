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

#include "formats/lexed_unit.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace sdb::formats {

LexedUnit::LexedUnit(const duckdb::RowDialect& dialect, size_t columns)
  : _lexer{dialect, columns, std::numeric_limits<duckdb::idx_t>::max(),
           true,    false,   true},
    _ranges{_lexer, _span, 0, 0, duckdb::DConstants::INVALID_INDEX, true},
    _reader{_ranges} {}

duckdb::idx_t LexedUnit::Span::Read(char* data, duckdb::idx_t count,
                                    duckdb::idx_t offset) const {
  const auto available =
    offset < bytes.size() ? std::min<size_t>(count, bytes.size() - offset) : 0;
  std::memcpy(data, bytes.data() + offset, available);
  return available;
}

duckdb::RowRangeReader& LexedUnit::Read(std::string_view rows) {
  _span.bytes = rows;
  _ranges.Reset(rows.size(), duckdb::DConstants::INVALID_INDEX);
  _ranges.SetWholeFile(nullptr, rows.data());
  _reader.Start(0, true, false, false);
  return _reader;
}

}  // namespace sdb::formats
