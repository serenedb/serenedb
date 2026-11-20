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

#include "formats/binary_copy_decoder.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>
#include <simdutf.h>

#include <algorithm>
#include <bit>
#include <duckdb/formats/copy_fields.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <source_location>
#include <utility>

#include "pg/input_error.h"

namespace sdb::formats {
namespace {

constexpr size_t kWords = STANDARD_VECTOR_SIZE / 64;

template<typename... Args>
std::exception_ptr CopyError(int code, Args&&... message) {
  return std::make_exception_ptr(irs::SqlException{
    SQL_ERROR_DATA(ERR_CODE(code), ERR_MSG(std::forward<Args>(message)...)),
    std::source_location::current()});
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

BinaryCopyDecoder::BinaryCopyDecoder(duckdb::ClientContext& context,
                                     pg::CopyInTarget target)
  : _target{std::move(target)},
    _fields(_target.types.size() * STANDARD_VECTOR_SIZE),
    _failed(kWords) {
  _decoders.reserve(_target.types.size());
  for (const auto& type : _target.types) {
    _decoders.push_back(
      pg::GetDeserialization<pg::ColumnSink>(type, pg::VarFormat::Binary));
  }
  pg::FillDeserializeContext(context, _dctx);
}

void BinaryCopyDecoder::Start(const pg::CopyBatch& batch) {
  _batch = &batch;
  _seq = batch.seq;
  _segment = 0;
  _pos = 0;
  _lines = 0;
}

void BinaryCopyDecoder::FailRow(uint64_t line, std::exception_ptr error,
                                size_t column) {
  if (Failed()) {
    return;
  }
  _error =
    CopyRowError{.error = std::move(error), .line = line, .column = column};
}

void BinaryCopyDecoder::ThrowError(uint64_t line) const {
  auto context = absl::StrCat("COPY ", _target.table, ", line ", line);
  if (_error.column != kNoColumn) {
    absl::StrAppend(&context, ", column ", _target.names[_error.column]);
  }
  pg::ThrowInContext(_error.error, std::move(context));
}

size_t BinaryCopyDecoder::Walk(std::string_view data, size_t max_rows) {
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
      auto& field = Column(column, rows + 1)[rows];
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

duckdb::idx_t BinaryCopyDecoder::Decode(duckdb::DataChunk& output,
                                        duckdb::idx_t row, size_t rows) {
  const auto words = (rows + 63) / 64;
  size_t first_row = rows;
  size_t first_column = 0;
  std::exception_ptr first_error;
  for (size_t column = 0; column < _decoders.size(); ++column) {
    std::fill_n(_failed.begin(), words, 0);
    _dctx.column_error = nullptr;
    if (_decoders[column](_dctx, Column(column, rows), output.data[column], row,
                          _failed.data()) == 0) {
      continue;
    }
    if (const auto at = FirstSet(_failed.data(), words); at < first_row) {
      first_row = at;
      first_column = column;
      first_error = std::exchange(_dctx.column_error, nullptr);
    }
  }
  if (first_row == rows) {
    return rows;
  }
  FailRow(_lines + first_row + 1,
          first_error ? std::move(first_error)
                      : CopyError(ERRCODE_INVALID_BINARY_REPRESENTATION,
                                  "incorrect binary data format"),
          first_column);
  return 0;
}

duckdb::idx_t BinaryCopyDecoder::Parse(duckdb::DataChunk& output,
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
    auto rows = Walk(data, STANDARD_VECTOR_SIZE - row);
    size_t bad_column = kNoColumn;
    std::string_view bad_field;
    for (size_t column = 0; column < _decoders.size(); ++column) {
      if (_target.types[column].id() != duckdb::LogicalTypeId::VARCHAR) {
        continue;
      }
      const auto fields = Column(column, rows);
      for (size_t r = 0; r < rows; ++r) {
        if (fields[r].data() &&
            !simdutf::validate_utf8(fields[r].data(), fields[r].size())) {
          rows = r;
          bad_column = column;
          bad_field = fields[r];
          break;
        }
      }
    }
    if (rows != 0) {
      const auto decoded = Decode(output, row, rows);
      if (Failed()) {
        break;
      }
      row += decoded;
      _lines += rows;
    }
    if (bad_column != kNoColumn) {
      FailRow(_lines + 1,
              CopyError(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
                        duckdb::CopyInvalidUtf8(bad_field)),
              bad_column);
      break;
    }
    if (_bad_fields != -1) {
      FailRow(_lines + 1,
              CopyError(ERRCODE_BAD_COPY_FILE_FORMAT, "row field count is ",
                        _bad_fields, ", expected ", _decoders.size()),
              kNoColumn);
    }
  }
  return row;
}

}  // namespace sdb::formats
