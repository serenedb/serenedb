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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "formats/csv_splitter.h"
#include "formats/field_columns.h"
#include "formats/text_splitter.h"
#include "pg/copy_in_batches.h"
#include "pg/deserialize.h"

namespace sdb::formats {

struct CopyTextEnd {
  std::atomic<uint64_t> marker{std::numeric_limits<uint64_t>::max()};
  std::atomic<uint64_t> latest{0};
};

inline constexpr size_t kNoColumn = std::numeric_limits<size_t>::max();

struct CopyRowError {
  std::exception_ptr error;
  uint64_t line = 0;
  size_t column = kNoColumn;
  std::string value;
  bool has_value = false;
};

struct CopySkippedRow {
  uint64_t line = 0;
  size_t column = 0;
  std::string value;
};

struct CsvMissing {
  size_t row = 0;
  size_t column = 0;
};

class CopyDecoder {
 public:
  CopyDecoder(duckdb::ClientContext& context, pg::CopyInTarget target,
              CopyTextEnd& end, std::atomic<uint64_t>& rejected);

  void Start(const pg::CopyBatch& batch);
  bool Active() const noexcept { return static_cast<bool>(_batch); }
  bool Exhausted() const noexcept {
    return _segment == _batch->segments.size();
  }
  uint64_t Seq() const noexcept { return _batch->seq; }
  void Stop() noexcept { _batch = nullptr; }
  duckdb::idx_t Parse(duckdb::DataChunk& output, duckdb::idx_t row);

  bool Failed() const noexcept { return static_cast<bool>(_error.error); }
  uint64_t FailedLine() const noexcept { return _error.line; }
  [[noreturn]] void ThrowError(uint64_t line) const;
  uint64_t Lines() const noexcept { return _lines; }
  std::vector<CopySkippedRow> TakeSkipped() noexcept {
    return std::exchange(_skipped, {});
  }

 private:
  duckdb::idx_t ParseText(duckdb::DataChunk& output, duckdb::idx_t row);
  duckdb::idx_t ParseCsv(duckdb::DataChunk& output, duckdb::idx_t row);
  duckdb::idx_t ParseBinary(duckdb::DataChunk& output, duckdb::idx_t row);
  void ResolveText(size_t rows);
  void ResolveCsv(size_t rows);
  CsvMissing FirstMissing(size_t rows);
  bool MarkDefault(size_t column, size_t row, std::string_view& field);
  void FailDefault(size_t column, size_t row, std::string_view row_text);
  void ApplyDefaults(duckdb::DataChunk& output, duckdb::idx_t row, size_t rows);
  duckdb::idx_t Decode(duckdb::DataChunk& output, duckdb::idx_t row,
                       size_t rows, FieldColumns fields);
  size_t WalkBinary(std::string_view data, size_t max_rows);
  void SkipHeader(std::string_view data);
  void SkipCsvHeader(std::string_view data);
  std::exception_ptr MatchHeader(FieldColumns fields, size_t columns) const;
  std::exception_ptr StructureError(RowError error, size_t column) const;
  std::exception_ptr TypeError(size_t column, std::string_view value) const;
  void FailRow(uint64_t line, std::exception_ptr error, size_t column,
               std::string_view value, bool has_value);
  uint64_t UnitLine(size_t row) const;
  std::string_view RowText(size_t row) const;
  void CheckAfterMarker(std::string_view rest) const;
  void EndMarker();
  void CountRejected(uint64_t rejected);

  duckdb::ClientContext& _context;
  pg::CopyInTarget _target;
  std::vector<pg::DeserializationFunction<pg::ColumnSink>> _decoders;
  pg::DeserializeContext _dctx;
  TextSplitter _splitter;
  std::optional<CsvSplitter> _csv;
  std::string _unescaped;
  std::vector<uint64_t> _failed;
  std::vector<uint64_t> _rejected_rows;
  std::vector<uint32_t> _reject_columns;
  std::vector<std::unique_ptr<duckdb::ExpressionExecutor>> _default_executors;
  std::vector<uint64_t> _defaulted;
  duckdb::DataChunk _default_input;
  CopyRowError _error;
  std::vector<CopySkippedRow> _skipped;
  uint64_t _lines = 0;
  std::string_view _split_data;
  size_t _split_start = 0;
  size_t _split_rows = 0;
  bool _split_unterminated = false;
  int32_t _bad_fields = -1;
  CopyTextEnd& _end;
  std::atomic<uint64_t>& _rejected;
  const pg::CopyBatch* _batch = nullptr;
  size_t _segment = 0;
  size_t _pos = 0;
  bool _skip = false;
  bool _ended = false;
  bool _null_has_backslash = true;
};

}  // namespace sdb::formats
