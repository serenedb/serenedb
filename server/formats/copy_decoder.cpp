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

#include "formats/copy_decoder.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <bit>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <exception>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/error_mapping.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <source_location>
#include <string>
#include <utility>

#include "formats/text_encoding.h"
#include "pg/input_error.h"

namespace sdb::formats {
namespace {

constexpr size_t kWords = STANDARD_VECTOR_SIZE / 64;

[[noreturn]] void ThrowDataAfterMarker() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                  ERR_MSG("end-of-copy marker is followed by data"));
}

template<typename... Args>
std::exception_ptr CopyError(int code, Args&&... message) {
  return std::make_exception_ptr(irs::SqlException{
    SQL_ERROR_DATA(ERR_CODE(code), ERR_MSG(std::forward<Args>(message)...)),
    std::source_location::current()});
}

std::string Printout(std::string_view value) {
  constexpr size_t kMaxDisplay = 100;
  if (value.size() <= kMaxDisplay) {
    return std::string{value};
  }
  size_t size = kMaxDisplay;
  while (size != 0 && (static_cast<uint8_t>(value[size]) & 0xC0) == 0x80) {
    --size;
  }
  return absl::StrCat(value.substr(0, size), "...");
}

std::string_view LineText(std::string_view line) {
  if (line.ends_with('\n')) {
    line.remove_suffix(1);
  }
  if (line.ends_with('\r')) {
    line.remove_suffix(1);
  }
  return line;
}

void ResetNulls(duckdb::Vector& vec, duckdb::idx_t row) {
  duckdb::FlatVector::SetNull(vec, row, false);
  switch (vec.GetType().InternalType()) {
    case duckdb::PhysicalType::STRUCT:
      for (auto& child : duckdb::StructVector::GetEntries(vec)) {
        ResetNulls(child, row);
      }
      break;
    case duckdb::PhysicalType::ARRAY: {
      const auto size = duckdb::ArrayType::GetSize(vec.GetType());
      auto& child = duckdb::ArrayVector::GetChildMutable(vec);
      for (auto i = row * size; i != (row + 1) * size; ++i) {
        ResetNulls(child, i);
      }
      break;
    }
    default:
      break;
  }
}

size_t RowAfter(const duckdb::RowRangeReader& reader, size_t row) {
  const auto& window = *reader.bytes;
  const size_t shift = reader.cursor.anchor - window.offset;
  if (row + 1 < reader.fields.row_count) {
    return reader.fields.rows[row + 1] + shift;
  }
  size_t after = reader.RowEnd(row) - window.offset;
  if (after < window.size && window.data[after] == '\r') {
    ++after;
  }
  if (reader.dialect.line_break == '\n' && after < window.size &&
      window.data[after] == '\n') {
    ++after;
  }
  return after;
}

size_t RowStart(const duckdb::RowRangeReader& reader, size_t row) {
  return reader.fields.rows[row] + reader.cursor.anchor - reader.bytes->offset;
}

size_t FirstSet(const uint64_t* words, size_t count) {
  for (size_t word = 0; word < count; ++word) {
    if (words[word] != 0) {
      return word * 64 + static_cast<size_t>(std::countr_zero(words[word]));
    }
  }
  return count * 64;
}

}  // namespace

duckdb::RowDialect RowDialectOf(const pg::CopyInTarget& target) {
  return target.csv
           ? duckdb::RowDialect::PostgresCsv(target.delim, target.quote,
                                             target.escape, target.line_break)
           : duckdb::RowDialect::PostgresText(target.delim, target.line_break);
}

CopyDecoder::CopyDecoder(duckdb::ClientContext& context,
                         pg::CopyInTarget target, CopyTextEnd& end,
                         std::atomic<uint64_t>& rejected)
  : _context{context},
    _target{std::move(target)},
    _splitter{_target.types.size(), STANDARD_VECTOR_SIZE},
    _failed(kWords),
    _rejected_rows(kWords),
    _reject_columns(_target.log_verbosity == pg::CopyLogVerbosity::Verbose
                      ? STANDARD_VECTOR_SIZE
                      : 0),
    _end{end},
    _rejected{rejected},
    _null_has_backslash{_target.null_str.contains('\\')} {
  _decoders.reserve(_target.types.size());
  for (const auto& type : _target.types) {
    _decoders.push_back(pg::GetDeserialization<pg::ColumnSink>(
      type, _target.binary ? pg::VarFormat::Binary : pg::VarFormat::Text));
    if (!_target.binary &&
        _target.rendering != duckdb::ValueRendering::POSTGRES) {
      _rendered.push_back(
        duckdb::MakeValueDecoder(context, type, _target.rendering));
    }
  }
  if (_target.csv) {
    _csv.emplace(CsvDialect{_target.delim, _target.quote, _target.escape},
                 _target.types.size(), STANDARD_VECTOR_SIZE);
  }
  if (!_target.binary) {
    _unit.emplace(RowDialectOf(_target), _target.types.size());
  }
  if (_target.default_str) {
    _defaulted.assign(_target.types.size() * kWords, 0);
    for (const auto& expr : *_target.defaults) {
      _default_executors.push_back(
        expr ? std::make_unique<duckdb::ExpressionExecutor>(context, *expr)
             : nullptr);
    }
  }
  pg::FillDeserializeContext(context, _dctx);
}

bool CopyDecoder::MarkDefault(size_t column, size_t row,
                              std::string_view& field) {
  if (!_default_executors[column]) {
    return false;
  }
  _defaulted[column * kWords + row / 64] |= uint64_t{1} << (row % 64);
  field = {};
  return true;
}

void CopyDecoder::FailDefault(size_t column, size_t row,
                              std::string_view row_text) {
  FailRow(UnitLine(row),
          std::make_exception_ptr(irs::SqlException{
            SQL_ERROR_DATA(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                           ERR_MSG("unexpected default marker in COPY data"),
                           ERR_DETAIL("Column \"", _target.names[column],
                                      "\" has no default value.")),
            std::source_location::current()}),
          kNoColumn, row_text, true);
}

void CopyDecoder::ApplyDefaults(duckdb::DataChunk& output, duckdb::idx_t row,
                                size_t rows) {
  const auto words = (rows + 63) / 64;
  for (size_t column = 0; column < _default_executors.size(); ++column) {
    auto* bits = _defaulted.data() + column * kWords;
    size_t count = 0;
    for (size_t word = 0; word < words; ++word) {
      count += static_cast<size_t>(std::popcount(bits[word]));
    }
    if (count == 0) {
      continue;
    }
    auto& target = output.data[column];
    duckdb::Vector values{target.GetType(), count};
    _default_input.SetCardinality(count);
    _default_executors[column]->ExecuteExpression(_default_input, values);
    size_t next = 0;
    for (size_t word = 0; word < words; ++word) {
      for (auto set = std::exchange(bits[word], 0); set != 0; set &= set - 1) {
        const auto at = word * 64 + static_cast<size_t>(std::countr_zero(set));
        duckdb::VectorOperations::Copy(values, target, next + 1, next,
                                       row + at);
        ++next;
      }
    }
  }
}

void CopyDecoder::CountRejected(uint64_t rejected) {
  const auto total =
    _rejected.fetch_add(rejected, std::memory_order_relaxed) + rejected;
  if (_target.reject_limit != 0 && total > _target.reject_limit) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("skipped more than REJECT_LIMIT (", _target.reject_limit,
              ") rows due to data type incompatibility"));
  }
}

void CopyDecoder::EndMarker() {
  _ended = true;
  const auto seq = _seq;
  for (auto marker = _end.marker.load();
       seq < marker && !_end.marker.compare_exchange_weak(marker, seq);) {
  }
  if (_end.latest.load() > seq) {
    ThrowDataAfterMarker();
  }
}

void CopyDecoder::StartLexed(uint64_t seq) {
  _batch = nullptr;
  _seq = seq;
  _lines = 0;
  _ended = false;
  _skip = _target.header && seq == 0;
  if (_target.csv) {
    return;
  }
  for (auto latest = _end.latest.load();
       latest < seq && !_end.latest.compare_exchange_weak(latest, seq);) {
  }
  if (_end.marker.load() < seq) {
    ThrowDataAfterMarker();
  }
}

void CopyDecoder::Start(const pg::CopyBatch& batch) {
  if (!_target.binary && batch.line_break != _target.line_break) {
    _target.line_break = batch.line_break;
    _unit.emplace(RowDialectOf(_target), _target.types.size());
  }
  _batch = &batch;
  _seq = batch.seq;
  _segment = 0;
  _pos = 0;
  _lines = 0;
  _lexing = false;
  _ended = false;
  _skip = _target.header && !_target.binary && batch.seq == 0;
  if (_target.binary || _target.csv) {
    return;
  }
  for (auto latest = _end.latest.load();
       latest < batch.seq &&
       !_end.latest.compare_exchange_weak(latest, batch.seq);) {
  }
  if (_end.marker.load() < batch.seq) {
    ThrowDataAfterMarker();
  }
}

duckdb::idx_t CopyDecoder::Parse(duckdb::DataChunk& output, duckdb::idx_t row) {
  return _target.binary ? ParseBinary(output, row) : ParseRows(output, row);
}

duckdb::idx_t CopyDecoder::ParseRows(duckdb::DataChunk& output,
                                     duckdb::idx_t row) {
  const auto& segments = _batch->segments;
  while (row < STANDARD_VECTOR_SIZE && _segment != segments.size() &&
         !Failed()) {
    auto& reader = std::exchange(_lexing, true)
                     ? _unit->Reader()
                     : _unit->Read(segments[_segment]);
    if (reader.Advance(STANDARD_VECTOR_SIZE - row) ==
        duckdb::RowReadResult::DONE) {
      _lexing = false;
      ++_segment;
      continue;
    }
    const auto rows =
      std::min<duckdb::idx_t>(reader.Ready(), STANDARD_VECTOR_SIZE - row);
    row = ParseLexed(output, row, reader, rows);
    reader.Emit(rows);
  }
  return row;
}

void CopyDecoder::FailRow(uint64_t line, std::exception_ptr error,
                          size_t column, std::string_view value,
                          bool has_value) {
  if (Failed()) {
    return;
  }
  _error = CopyRowError{
    .error = std::move(error),
    .line = line,
    .column = column,
    .value = Printout(value),
    .has_value = has_value,
  };
}

std::exception_ptr CopyDecoder::TypeError(size_t column,
                                          std::string_view value) const {
  return pg::InputError(_context, _target.types[column], value);
}

void CopyDecoder::ThrowError(uint64_t line) const {
  auto context = absl::StrCat("COPY ", _target.table, ", line ", line);
  if (_error.column != kNoColumn) {
    absl::StrAppend(&context, ", column ", _target.names[_error.column]);
  }
  if (_error.has_value) {
    absl::StrAppend(&context, ": \"", _error.value, "\"");
  }
  pg::ThrowInContext(_error.error, std::move(context));
}

std::exception_ptr CopyDecoder::MatchHeader(FieldColumns fields,
                                            size_t columns) const {
  if (columns != _target.names.size()) {
    return CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                     "wrong number of fields in header line: got ", columns,
                     ", expected ", _target.names.size());
  }
  for (size_t column = 0; column < columns; ++column) {
    const auto name = fields.Column(column, 1)[0];
    if (!name.data()) {
      return CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                       "column name mismatch in header line field ", column + 1,
                       ": got null value (\"", _target.null_str,
                       "\"), expected \"", _target.names[column], "\"");
    }
    if (name != _target.names[column]) {
      return CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                       "column name mismatch in header line field ", column + 1,
                       ": got \"", name, "\", expected \"",
                       _target.names[column], "\"");
    }
  }
  return nullptr;
}

uint64_t CopyDecoder::UnitLine(size_t row) const {
  if (!_csv) {
    return _lines + row + 1;
  }
  const auto offsets = _csv->RowOffsets(_split_rows);
  const size_t start = offsets[row];
  const size_t end = row + 1 < _split_rows ? offsets[row + 1] : _pos;
  const auto record = _split_data.substr(start, end - start);
  const char line_break = _target.line_break;
  const bool terminated = record.ends_with(line_break) &&
                          !(_split_unterminated && row + 1 == _split_rows);
  const auto before = static_cast<uint64_t>(absl::c_count(
    _split_data.substr(_split_start, start - _split_start), line_break));
  const auto inside = static_cast<uint64_t>(absl::c_count(record, line_break));
  return _lines + before + inside - (terminated ? 1 : 0) + 1;
}

std::string_view CopyDecoder::RowText(size_t row) const {
  const auto offsets = _csv->RowOffsets(_split_rows);
  const size_t start = offsets[row];
  const size_t end = row + 1 < _split_rows ? offsets[row + 1] : _pos;
  const auto record = _split_data.substr(start, end - start);
  return _split_unterminated && row + 1 == _split_rows ? record
                                                       : LineText(record);
}

std::exception_ptr CopyDecoder::StructureError(RowError error,
                                               size_t column) const {
  switch (error) {
    case RowError::ExtraData:
      return CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                       "extra data after last expected column");
    case RowError::MissingData:
      return CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                       "missing data for column \"", _target.names[column],
                       "\"");
    case RowError::MarkerNotAlone:
      return CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                       "end-of-copy marker is not alone on its line");
    case RowError::None:
      break;
  }
  SDB_UNREACHABLE();
}

duckdb::idx_t CopyDecoder::ParseLexed(duckdb::DataChunk& output,
                                      duckdb::idx_t row,
                                      const duckdb::RowRangeReader& reader,
                                      size_t rows) {
  size_t first = 0;
  if (std::exchange(_skip, false)) {
    LexedHeader(reader);
    first = 1;
  }
  if (Failed() || first == rows) {
    return row;
  }
  if (_ended) {
    LexedAfterMarker(reader, first, rows - first);
    return row;
  }
  return _csv ? ParseLexedCsv(output, row, reader, first, rows - first)
              : ParseLexedText(output, row, reader, first, rows - first);
}

void CopyDecoder::LexedAfterMarker(const duckdb::RowRangeReader& reader,
                                   size_t first, size_t count) const {
  const auto& window = *reader.bytes;
  for (size_t row = first; row < first + count; ++row) {
    const size_t start = RowStart(reader, row);
    const size_t end = reader.RowEnd(row) - window.offset;
    if (end != start) {
      ThrowDataAfterMarker();
    }
  }
}

void CopyDecoder::LexedHeader(const duckdb::RowRangeReader& reader) {
  const auto& window = *reader.bytes;
  const std::string_view data{window.data, window.size};
  const size_t start = RowStart(reader, 0);
  const auto record = data.substr(start, RowAfter(reader, 0) - start);
  const auto newlines =
    static_cast<uint64_t>(absl::c_count(record, _target.line_break));
  const bool terminated = record.ends_with(_target.line_break);
  const auto header_line =
    _lines + 1 + (_csv ? newlines - (terminated ? 1 : 0) : 0);
  _lines += _csv ? newlines : 1;
  if (!_target.header_match) {
    return;
  }
  const auto line = terminated ? LineText(record) : record;
  if (!_csv) {
    TextSplitter header{_target.names.size(), 1};
    const auto split = header.FillLexed(reader, 0, 1);
    const size_t columns =
      split.error != RowError::None ? split.fields : _target.names.size();
    std::string unescaped(record.size(), '\0');
    char* out = unescaped.data();
    for (const auto index : header.Escaped()) {
      auto& field = header.Field(index);
      if (field == _target.null_str) {
        field = {};
        continue;
      }
      field = UnescapeText(field, out);
      out += field.size();
    }
    if (auto error = MatchHeader(header.Fields(), columns)) {
      FailRow(header_line, std::move(error), kNoColumn, line, true);
    }
    return;
  }
  CsvSplitter header{_csv->Dialect(), _target.names.size(), 1};
  if (header.FillLexed(reader, 0, 1).unterminated) {
    FailRow(
      header_line,
      CopyError(ERRCODE_BAD_COPY_FILE_FORMAT, "unterminated CSV quoted field"),
      kNoColumn, line, true);
    return;
  }
  size_t columns = _target.names.size();
  for (size_t column = 0; column < _target.names.size(); ++column) {
    if (header.Flags(column, 1)[0] & kCsvMissing) {
      columns = column;
      break;
    }
  }
  if (!header.ExtraRows().empty()) {
    ++columns;
  }
  std::string unquoted(record.size(), '\0');
  char* out = unquoted.data();
  const std::string_view null_str = _target.null_str;
  for (size_t column = 0; column < columns && column < _target.names.size();
       ++column) {
    auto& field = header.Column(column, 1)[0];
    const auto flags = header.Flags(column, 1)[0];
    if (!(flags & kCsvQuoted)) {
      if (field == null_str) {
        field = {};
      }
      continue;
    }
    field = (flags & kCsvComplex) ? UnquoteCsv(field, _csv->Dialect(), out)
                                  : field.substr(1, field.size() - 2);
    out += field.size();
  }
  if (auto error = MatchHeader(header.Fields(), columns)) {
    FailRow(header_line, std::move(error), kNoColumn, line, true);
  }
}

duckdb::idx_t CopyDecoder::ParseLexedText(duckdb::DataChunk& output,
                                          duckdb::idx_t row,
                                          const duckdb::RowRangeReader& reader,
                                          size_t first, size_t count) {
  const auto& window = *reader.bytes;
  const std::string_view data{window.data, window.size};
  const auto split = _splitter.FillLexed(reader, first, count);
  const size_t start = RowStart(reader, first);
  const bool stopped = split.error != RowError::None || split.marker;
  const size_t last = first + split.rows + (stopped ? 1 : 0);
  const size_t end = last > first ? RowAfter(reader, last - 1) : start;
  size_t rows = split.rows;
  auto invalid = FindInvalidUtf8(data.substr(start, end - start));
  if (invalid != std::string_view::npos) {
    invalid += start;
    rows = std::min(
      rows, static_cast<size_t>(absl::c_count(
              data.substr(start, invalid - start), _target.line_break)));
  }
  if (rows != 0) {
    ResolveText(rows);
    const auto decoded =
      Failed() ? 0 : Decode(output, row, rows, _splitter.Fields());
    if (Failed()) {
      return row;
    }
    row += decoded;
    _lines += rows;
  }
  if (invalid != std::string_view::npos) {
    FailRow(_lines + 1, InvalidUtf8Error(data, invalid), kNoColumn, {}, false);
    return row;
  }
  if (split.error != RowError::None) {
    const auto bad = data.substr(
      split.row_start, RowAfter(reader, first + split.rows) - split.row_start);
    FailRow(_lines + 1, StructureError(split.error, split.fields), kNoColumn,
            LineText(bad), true);
    return row;
  }
  if (split.marker) {
    EndMarker();
    const size_t after = first + split.rows + 1;
    LexedAfterMarker(reader, after, first + count - after);
  }
  return row;
}

duckdb::idx_t CopyDecoder::ParseLexedCsv(duckdb::DataChunk& output,
                                         duckdb::idx_t row,
                                         const duckdb::RowRangeReader& reader,
                                         size_t first, size_t count) {
  const auto& window = *reader.bytes;
  const std::string_view data{window.data, window.size};
  const auto split = _csv->FillLexed(reader, first, count);
  const size_t start = RowStart(reader, first);
  _split_data = data;
  _split_start = start;
  _split_rows = split.rows;
  _split_unterminated = split.unterminated;
  _pos = RowAfter(reader, first + count - 1);
  size_t good = split.unterminated ? split.rows - 1 : split.rows;
  auto error = RowError::None;
  size_t error_column = 0;
  if (const auto extra = _csv->ExtraRows();
      !extra.empty() && extra.front() < good) {
    good = extra.front();
    error = RowError::ExtraData;
  }
  if (const auto missing = FirstMissing(good); missing.row < good) {
    good = missing.row;
    error = RowError::MissingData;
    error_column = missing.column;
  }
  auto invalid = FindInvalidUtf8(data.substr(start, _pos - start));
  if (invalid != std::string_view::npos) {
    invalid += start;
    const auto offsets = _csv->RowOffsets(split.rows);
    const auto bad = static_cast<size_t>(
      std::upper_bound(offsets.begin(), offsets.end(), invalid) -
      offsets.begin() - 1);
    if (bad <= good) {
      good = bad;
      error = RowError::None;
    } else {
      invalid = std::string_view::npos;
    }
  }
  if (good != 0) {
    ResolveCsv(good);
    const auto decoded =
      Failed() ? 0 : Decode(output, row, good, _csv->Fields());
    if (Failed()) {
      return row;
    }
    row += decoded;
  }
  if (invalid != std::string_view::npos) {
    FailRow(UnitLine(good), InvalidUtf8Error(data, invalid), kNoColumn, {},
            false);
    return row;
  }
  if (good != split.rows) {
    FailRow(UnitLine(good),
            error == RowError::None ? CopyError(ERRCODE_BAD_COPY_FILE_FORMAT,
                                                "unterminated CSV quoted field")
                                    : StructureError(error, error_column),
            kNoColumn, RowText(good), true);
    return row;
  }
  _lines += static_cast<uint64_t>(
    absl::c_count(data.substr(start, _pos - start), _target.line_break));
  return row;
}

CsvMissing CopyDecoder::FirstMissing(size_t rows) {
  CsvMissing first{.row = rows, .column = 0};
  for (size_t column = 0; column < _decoders.size(); ++column) {
    const auto flags = _csv->Flags(column, first.row);
    for (size_t row = 0; row < first.row; ++row) {
      if (flags[row] & kCsvMissing) {
        first = {.row = row, .column = column};
        break;
      }
    }
  }
  return first;
}

void CopyDecoder::ResolveCsv(size_t rows) {
  const std::string_view null_str = _target.null_str;
  size_t total = 0;
  for (size_t column = 0; column < _decoders.size(); ++column) {
    const auto flags = _csv->Flags(column, rows);
    const auto fields = _csv->Column(column, rows);
    for (size_t row = 0; row < rows; ++row) {
      if (flags[row] & kCsvComplex) {
        total += fields[row].size();
      }
    }
  }
  _unescaped.resize(total);
  char* out = _unescaped.data();
  const auto& dialect = _csv->Dialect();
  const auto* default_str =
    _target.default_str ? &*_target.default_str : nullptr;
  for (size_t column = 0; column < _decoders.size(); ++column) {
    const bool force_not_null =
      !_target.force_not_null.empty() && _target.force_not_null[column];
    const bool force_null =
      !_target.force_null.empty() && _target.force_null[column];
    const auto flags = _csv->Flags(column, rows);
    const auto fields = _csv->Column(column, rows);
    for (size_t row = 0; row < rows; ++row) {
      auto& field = fields[row];
      const auto flag = flags[row];
      if (!(flag & kCsvQuoted)) {
        if (field == null_str) {
          field = force_not_null ? null_str : std::string_view{};
        } else if (default_str && field == *default_str &&
                   !MarkDefault(column, row, field)) {
          FailDefault(column, row, RowText(row));
          return;
        }
        continue;
      }
      if (flag & kCsvComplex) {
        field = UnquoteCsv(field, dialect, out);
        out += field.size();
      } else {
        field = field.substr(1, field.size() - 2);
      }
      if (force_null && field == null_str) {
        field = {};
      }
    }
  }
}

void CopyDecoder::ResolveText(size_t rows) {
  const std::string_view null_str = _target.null_str;
  if (_target.default_str) {
    const std::string_view marker = *_target.default_str;
    const auto last = _decoders.size() - 1;
    for (size_t row = 0; row < rows; ++row) {
      const auto first_field = _splitter.Column(0, rows)[row];
      const auto last_field = _splitter.Column(last, rows)[row];
      const std::string_view row_text{
        first_field.data(),
        static_cast<size_t>(last_field.data() + last_field.size() -
                            first_field.data())};
      for (size_t column = 0; column <= last; ++column) {
        auto& field = _splitter.Column(column, rows)[row];
        if (field == marker && !MarkDefault(column, row, field)) {
          FailDefault(column, row, row_text);
          return;
        }
      }
    }
  }
  if (!_null_has_backslash) {
    for (size_t column = 0; column < _decoders.size(); ++column) {
      for (auto& field : _splitter.Column(column, rows)) {
        if (field == null_str) {
          field = {};
        }
      }
    }
  }
  const auto escaped = _splitter.Escaped();
  if (escaped.empty()) {
    return;
  }
  const auto capacity = _splitter.Capacity();
  size_t total = 0;
  for (const auto index : escaped) {
    total += _splitter.Field(index).size();
  }
  _unescaped.resize(total);
  char* out = _unescaped.data();
  for (const auto index : escaped) {
    auto& field = _splitter.Field(index);
    if (!field.data() || index % capacity >= rows) {
      continue;
    }
    if (field == null_str) {
      field = {};
      continue;
    }
    try {
      field = UnescapeText(field, out);
    } catch (const irs::SqlException&) {
      FailRow(UnitLine(index % _splitter.Capacity()), std::current_exception(),
              index / _splitter.Capacity(), field, true);
      return;
    }
    out += field.size();
  }
}

duckdb::idx_t CopyDecoder::Decode(duckdb::DataChunk& output, duckdb::idx_t row,
                                  size_t rows, FieldColumns fields) {
  const auto words = (rows + 63) / 64;
  std::fill_n(_rejected_rows.begin(), words, 0);
  size_t first_row = rows;
  size_t first_column = 0;
  std::exception_ptr first_error;
  bool failed = false;
  const bool verbose = _target.log_verbosity == pg::CopyLogVerbosity::Verbose;
  for (size_t column = 0; column < _decoders.size(); ++column) {
    std::fill_n(_failed.begin(), words, 0);
    _dctx.column_error = nullptr;
    if (DecodeColumn(column, fields.Column(column, rows), output.data[column],
                     row) == 0) {
      continue;
    }
    failed = true;
    for (size_t word = 0; word < words; ++word) {
      if (verbose) {
        for (auto added = _failed[word] & ~_rejected_rows[word]; added != 0;
             added &= added - 1) {
          _reject_columns[word * 64 +
                          static_cast<size_t>(std::countr_zero(added))] =
            static_cast<uint32_t>(column);
        }
      }
      _rejected_rows[word] |= _failed[word];
    }
    if (const auto at = FirstSet(_failed.data(), words); at < first_row) {
      first_row = at;
      first_column = column;
      first_error = std::exchange(_dctx.column_error, nullptr);
    }
  }
  if (!failed) {
    if (!_defaulted.empty()) {
      ApplyDefaults(output, row, rows);
    }
    return rows;
  }
  if (_target.binary || !_target.ignore_errors) {
    const auto value = fields.Column(first_column, rows)[first_row];
    if (!first_error) {
      first_error = _target.binary
                      ? CopyError(ERRCODE_INVALID_BINARY_REPRESENTATION,
                                  "incorrect binary data format")
                      : TypeError(first_column, value);
    }
    FailRow(UnitLine(first_row), std::move(first_error), first_column, value,
            !_target.binary);
    return 0;
  }
  if (verbose) {
    for (size_t r = 0; r < rows; ++r) {
      if ((_rejected_rows[r / 64] >> (r % 64)) & 1) {
        const auto column = _reject_columns[r];
        _skipped.push_back(CopySkippedRow{
          .line = UnitLine(r),
          .column = column,
          .value = Printout(fields.Column(column, rows)[r]),
        });
      }
    }
  }
  size_t kept = 0;
  for (size_t r = 0; r < rows; ++r) {
    if ((_rejected_rows[r / 64] >> (r % 64)) & 1) {
      continue;
    }
    for (size_t column = 0; column < _decoders.size(); ++column) {
      auto values = fields.Column(column, rows);
      values[kept] = values[r];
    }
    ++kept;
  }
  if (!_defaulted.empty()) {
    for (size_t column = 0; column < _decoders.size(); ++column) {
      auto* bits = _defaulted.data() + column * kWords;
      size_t at = 0;
      for (size_t r = 0; r < rows; ++r) {
        if ((_rejected_rows[r / 64] >> (r % 64)) & 1) {
          continue;
        }
        const auto bit = (bits[r / 64] >> (r % 64)) & 1;
        bits[at / 64] =
          (bits[at / 64] & ~(uint64_t{1} << (at % 64))) | (bit << (at % 64));
        ++at;
      }
      for (size_t word = at / 64; word < words; ++word) {
        bits[word] &= word == at / 64 ? (uint64_t{1} << (at % 64)) - 1 : 0;
      }
    }
  }
  for (auto& vec : output.data) {
    for (size_t r = row; r < row + rows; ++r) {
      ResetNulls(vec, r);
    }
  }
  for (size_t column = 0; column < _decoders.size(); ++column) {
    DecodeColumn(column, fields.Column(column, kept), output.data[column], row);
  }
  CountRejected(rows - kept);
  if (!_defaulted.empty()) {
    ApplyDefaults(output, row, kept);
  }
  return kept;
}

duckdb::idx_t CopyDecoder::DecodeColumn(size_t column,
                                        std::span<std::string_view> fields,
                                        duckdb::Vector& out,
                                        duckdb::idx_t row) {
  if (_rendered.empty()) {
    return _decoders[column](_dctx, fields, out, row, _failed.data());
  }
  const auto count = fields.size();
  duckdb::Vector text{duckdb::LogicalType::VARCHAR, count};
  auto* values = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(text);
  auto& valid = duckdb::FlatVector::ValidityMutable(text);
  for (size_t i = 0; i < count; ++i) {
    if (!fields[i].data()) {
      valid.SetInvalid(i);
      continue;
    }
    values[i] = duckdb::string_t{fields[i].data(),
                                 static_cast<uint32_t>(fields[i].size())};
  }
  duckdb::Vector decoded{out.GetType(), count};
  std::string error;
  duckdb::idx_t failures = 0;
  if (!_rendered[column]->Decode(text, decoded, count, error)) {
    const auto& decoded_valid = duckdb::FlatVector::Validity(decoded);
    for (size_t i = 0; i < count; ++i) {
      if (fields[i].data() && !decoded_valid.RowIsValid(i)) {
        _failed[i / 64] |= uint64_t{1} << (i % 64);
        ++failures;
      }
    }
    if (failures != 0 && !_dctx.column_error) {
      _dctx.column_error =
        CopyError(ERRCODE_INVALID_TEXT_REPRESENTATION, error);
    }
  }
  duckdb::VectorOperations::Copy(decoded, out, count, 0, row);
  return failures;
}

size_t CopyDecoder::WalkBinary(std::string_view data, size_t max_rows) {
  constexpr size_t kFieldCountBytes = sizeof(int16_t);
  constexpr size_t kFieldLengthBytes = sizeof(int32_t);
  constexpr int32_t kNullFieldLength = -1;
  const auto columns = _decoders.size();
  const char* p = data.data() + _pos;
  const char* const end = data.data() + data.size();
  size_t rows = 0;
  while (rows < max_rows && p != end) {
    const auto fields = static_cast<int16_t>(absl::big_endian::Load16(p));
    if (static_cast<size_t>(fields) != columns) {
      _bad_fields = fields;
      break;
    }
    p += kFieldCountBytes;
    for (size_t column = 0; column < columns; ++column) {
      const auto length = static_cast<int32_t>(absl::big_endian::Load32(p));
      p += kFieldLengthBytes;
      auto& field = _splitter.Column(column, rows + 1)[rows];
      if (length == kNullFieldLength) {
        field = {};
        continue;
      }
      field = {p, static_cast<size_t>(length)};
      p += length;
    }
    ++rows;
  }
  _pos = static_cast<size_t>(p - data.data());
  return rows;
}

duckdb::idx_t CopyDecoder::ParseBinary(duckdb::DataChunk& output,
                                       duckdb::idx_t row) {
  const auto& segments = _batch->segments;
  while (row < STANDARD_VECTOR_SIZE && _segment != segments.size() &&
         !Failed()) {
    const auto data = segments[_segment];
    if (_pos == data.size()) {
      ++_segment;
      _pos = 0;
      continue;
    }
    _bad_fields = -1;
    auto rows = WalkBinary(data, STANDARD_VECTOR_SIZE - row);
    size_t bad_column = kNoColumn;
    size_t bad_offset = 0;
    for (size_t column = 0; column < _decoders.size(); ++column) {
      if (_target.types[column].id() != duckdb::LogicalTypeId::VARCHAR) {
        continue;
      }
      const auto fields = _splitter.Column(column, rows);
      for (size_t r = 0; r < rows; ++r) {
        if (!fields[r].data()) {
          continue;
        }
        if (const auto at = FindInvalidUtf8(fields[r]);
            at != std::string_view::npos) {
          rows = r;
          bad_column = column;
          bad_offset = at;
          break;
        }
      }
    }
    if (rows != 0) {
      const auto decoded = Decode(output, row, rows, _splitter.Fields());
      if (Failed()) {
        break;
      }
      row += decoded;
      _lines += rows;
    }
    if (bad_column != kNoColumn) {
      FailRow(_lines + 1,
              InvalidUtf8Error(_splitter.Column(bad_column, rows + 1)[rows],
                               bad_offset),
              bad_column, {}, false);
      break;
    }
    if (_bad_fields != -1) {
      FailRow(_lines + 1,
              CopyError(ERRCODE_BAD_COPY_FILE_FORMAT, "row field count is ",
                        _bad_fields, ", expected ", _decoders.size()),
              kNoColumn, {}, false);
    }
  }
  return row;
}

}  // namespace sdb::formats
