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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <exception>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "pg/copy_batch_builder.h"

namespace sdb::formats {
namespace {

constexpr size_t kWords = STANDARD_VECTOR_SIZE / 64;

[[noreturn]] void ThrowDataAfterMarker() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
                  ERR_MSG("end-of-copy marker is followed by data"));
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

size_t FirstSet(const uint64_t* words, size_t count) {
  for (size_t word = 0; word < count; ++word) {
    if (words[word] != 0) {
      return word * 64 + static_cast<size_t>(std::countr_zero(words[word]));
    }
  }
  return count * 64;
}

}  // namespace

CopyDecoder::CopyDecoder(duckdb::ClientContext& context,
                         pg::CopyInTarget target, CopyTextEnd& end,
                         std::atomic<uint64_t>& rejected)
  : _target{std::move(target)},
    _splitter{_target.delim, _target.types.size(), STANDARD_VECTOR_SIZE},
    _failed(kWords),
    _rejected_rows(kWords),
    _end{end},
    _rejected{rejected},
    _null_has_backslash{_target.null_str.contains('\\')} {
  _decoders.reserve(_target.types.size());
  for (const auto& type : _target.types) {
    _decoders.push_back(pg::GetDeserialization<pg::ColumnSink>(
      type, _target.binary ? pg::VarFormat::Binary : pg::VarFormat::Text));
  }
  pg::FillDeserializeContext(context, _dctx);
}

void CopyDecoder::CountRejected(uint64_t rejected) {
  const auto total =
    _rejected.fetch_add(rejected, std::memory_order_relaxed) + rejected;
  if (total > _target.reject_limit) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("skipped more than REJECT_LIMIT (", _target.reject_limit,
              ") rows due to data type incompatibility"));
  }
}

void CopyDecoder::EndMarker() {
  _ended = true;
  const auto seq = _batch->seq;
  for (auto marker = _end.marker.load();
       seq < marker && !_end.marker.compare_exchange_weak(marker, seq);) {
  }
  if (_end.latest.load() > seq) {
    ThrowDataAfterMarker();
  }
}

void CopyDecoder::Start(const pg::CopyBatch& batch) {
  _batch = &batch;
  _segment = 0;
  _pos = 0;
  _ended = false;
  _skip = _target.header && !_target.binary && batch.seq == 0;
  if (_target.binary) {
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
  return _target.binary ? ParseBinary(output, row) : ParseText(output, row);
}

void CopyDecoder::SkipHeader(std::string_view data) {
  const auto end = pg::FindRowEnd(data, _pos);
  _pos = end == std::string_view::npos ? data.size() : end + 1;
}

void CopyDecoder::CheckAfterMarker(std::string_view rest) const {
  for (size_t i = 0; i < rest.size(); ++i) {
    if (rest[i] == '\n') {
      continue;
    }
    if (rest[i] == '\r' && (i + 1 == rest.size() || rest[i + 1] == '\n')) {
      continue;
    }
    ThrowDataAfterMarker();
  }
}

duckdb::idx_t CopyDecoder::ParseText(duckdb::DataChunk& output,
                                     duckdb::idx_t row) {
  const auto& segments = _batch->segments;
  while (row < STANDARD_VECTOR_SIZE && _segment != segments.size()) {
    const auto data = segments[_segment];
    if (_pos == data.size()) {
      ++_segment;
      _pos = 0;
      continue;
    }
    if (std::exchange(_skip, false)) {
      SkipHeader(data);
      continue;
    }
    if (_ended) {
      CheckAfterMarker(data.substr(_pos));
      _pos = data.size();
      continue;
    }
    const auto split = _splitter.Split(data, _pos, STANDARD_VECTOR_SIZE - row);
    if (split.rows != 0) {
      ResolveText(split.rows);
      row += Decode(output, row, split.rows);
    }
    if (split.marker) {
      EndMarker();
    }
  }
  return row;
}

void CopyDecoder::ResolveText(size_t rows) {
  const std::string_view null_str = _target.null_str;
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
  size_t total = 0;
  for (const auto index : escaped) {
    total += _splitter.Field(index).size();
  }
  _unescaped.resize(total);
  char* out = _unescaped.data();
  for (const auto index : escaped) {
    auto& field = _splitter.Field(index);
    if (field == null_str) {
      field = {};
      continue;
    }
    field = UnescapeText(field, out);
    out += field.size();
  }
}

duckdb::idx_t CopyDecoder::Decode(duckdb::DataChunk& output, duckdb::idx_t row,
                                  size_t rows) {
  const auto words = (rows + 63) / 64;
  std::fill_n(_rejected_rows.begin(), words, 0);
  size_t first_row = rows;
  size_t first_column = 0;
  std::exception_ptr first_error;
  bool failed = false;
  for (size_t column = 0; column < _decoders.size(); ++column) {
    std::fill_n(_failed.begin(), words, 0);
    _dctx.column_error = nullptr;
    if (_decoders[column](_dctx, _splitter.Column(column, rows),
                          output.data[column], row, _failed.data()) == 0) {
      continue;
    }
    failed = true;
    for (size_t word = 0; word < words; ++word) {
      _rejected_rows[word] |= _failed[word];
    }
    if (const auto at = FirstSet(_failed.data(), words); at < first_row) {
      first_row = at;
      first_column = column;
      first_error = std::exchange(_dctx.column_error, nullptr);
    }
  }
  if (!failed) {
    return rows;
  }
  if (first_error && (_target.binary || !_target.ignore_errors)) {
    std::rethrow_exception(first_error);
  }
  if (_target.binary) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
      ERR_MSG("COPY FROM: invalid binary value in column ", first_column + 1));
  }
  if (!_target.ignore_errors) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
      ERR_MSG("COPY FROM: invalid input for column ", first_column + 1));
  }
  size_t kept = 0;
  for (size_t r = 0; r < rows; ++r) {
    if ((_rejected_rows[r / 64] >> (r % 64)) & 1) {
      continue;
    }
    for (size_t column = 0; column < _decoders.size(); ++column) {
      auto fields = _splitter.Column(column, rows);
      fields[kept] = fields[r];
    }
    ++kept;
  }
  for (auto& vec : output.data) {
    for (size_t r = row; r < row + rows; ++r) {
      ResetNulls(vec, r);
    }
  }
  for (size_t column = 0; column < _decoders.size(); ++column) {
    _decoders[column](_dctx, _splitter.Column(column, kept),
                      output.data[column], row, _failed.data());
  }
  if (_target.reject_limit != 0) {
    CountRejected(rows - kept);
  }
  return kept;
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
        ERR_MSG("COPY FROM: row has ", fields, " fields, expected ", columns));
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
  while (row < STANDARD_VECTOR_SIZE && _segment != segments.size()) {
    const auto data = segments[_segment];
    if (_pos == data.size()) {
      ++_segment;
      _pos = 0;
      continue;
    }
    const auto rows = WalkBinary(data, STANDARD_VECTOR_SIZE - row);
    row += Decode(output, row, rows);
  }
  return row;
}

}  // namespace sdb::formats
