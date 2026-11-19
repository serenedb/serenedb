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
#include <duckdb/formats/row_ranges.hpp>
#include <string_view>

namespace sdb::formats {

class LexedUnit {
 public:
  LexedUnit(const duckdb::RowDialect& dialect, size_t columns);

  LexedUnit(const LexedUnit&) = delete;
  LexedUnit& operator=(const LexedUnit&) = delete;

  duckdb::RowRangeReader& Read(std::string_view rows);
  duckdb::RowRangeReader& Reader() noexcept { return _reader; }

 private:
  class Span final : public duckdb::RowSource {
   public:
    duckdb::idx_t Read(char* data, duckdb::idx_t count,
                       duckdb::idx_t offset) const final;

    std::string_view bytes;
  };

  duckdb::RowLexer _lexer;
  Span _span;
  duckdb::RowRanges _ranges;
  duckdb::RowRangeReader _reader;
};

}  // namespace sdb::formats
