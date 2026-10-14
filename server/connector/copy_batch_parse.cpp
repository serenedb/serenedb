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

#include "connector/copy_batch_parse.h"

#include <absl/base/internal/endian.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <duckdb/common/types/vector.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "pg/copy_batch_builder.h"

namespace sdb::connector {
namespace {

constexpr size_t kFieldCountBytes = sizeof(int16_t);
constexpr size_t kFieldLengthBytes = sizeof(int32_t);
constexpr int32_t kNullFieldLength = -1;

// Decode one PG TEXT field body that contains a backslash escape, unescaping
// into `out` and returning a view over it. The no-escape common case never
// calls this: ProcessTextRow hands the raw view straight to the decoder
// (zero-copy). The exact two-byte `\N` SQL NULL sentinel is checked by the
// caller before this runs.
std::string_view DecodeFieldView(std::string_view field, std::string& out) {
  out.clear();
  out.reserve(field.size());
  for (size_t i = 0; i < field.size();) {
    const char c = field[i++];
    if (c != '\\') {
      out.push_back(c);
      continue;
    }
    if (i == field.size()) {
      out.push_back('\\');  // trailing backslash is literal
      break;
    }
    const char e = field[i++];
    switch (e) {
      case 'b':
        out.push_back('\b');
        break;
      case 'f':
        out.push_back('\f');
        break;
      case 'n':
        out.push_back('\n');
        break;
      case 'r':
        out.push_back('\r');
        break;
      case 't':
        out.push_back('\t');
        break;
      case 'v':
        out.push_back('\v');
        break;
      case '\\':
        out.push_back('\\');
        break;
      case 'x': {
        const char* begin = field.data() + i;
        const char* end = field.data() + std::min(i + 2, field.size());
        unsigned value = 0;
        const auto res = fast_float::from_chars(begin, end, value, 16);
        if (res.ptr == begin) {
          out.push_back('x');  // `\x` with no hex digit -> literal x
        } else {
          out.push_back(static_cast<char>(value));
          i += static_cast<size_t>(res.ptr - begin);
        }
        break;
      }
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7': {
        // The switch already consumed the first octal digit, so parse the run
        // of up to three starting one byte back. A byte value wraps (& 0xFF).
        const char* begin = field.data() + (i - 1);
        const char* end = field.data() + std::min(i + 2, field.size());
        unsigned value = 0;
        const auto res = fast_float::from_chars(begin, end, value, 8);
        i = static_cast<size_t>(res.ptr - field.data());
        out.push_back(static_cast<char>(value & 0xFF));
        break;
      }
      default:
        out.push_back(e);  // `\<anyotherchar>` -> drop the backslash
        break;
    }
  }
  return out;
}

// Scan from `pos` to the field's end -- the next unescaped delimiter, or the
// row end if none. A backslash escapes the following byte, so an escaped
// delimiter
// (\<delim>, the form EmitEscaped produces) stays in the body. Sets
// `had_escape` when any backslash is seen, so a field with none skips
// DecodeFieldView and reaches the decoder as a zero-copy view straight into
// `line`.
size_t ScanField(std::string_view line, char delim, size_t pos,
                 bool& had_escape) {
  had_escape = false;
  while (pos < line.size()) {
    const char c = line[pos];
    if (c == '\\') {
      had_escape = true;
      pos += 2;
      continue;
    }
    if (c == delim) {
      return pos;
    }
    ++pos;
  }
  return line.size();
}

std::string_view TrimCarriageReturn(std::string_view line) {
  if (!line.ends_with('\r')) {
    return line;
  }
  const auto body = line.substr(0, line.size() - 1);
  return pg::TrailingBackslashes(body) % 2 == 0 ? body : line;
}

}  // namespace

CopyDeserializers MakeCopyDeserializers(
  const std::vector<duckdb::LogicalType>& types, bool binary) {
  CopyDeserializers result;
  result.reserve(types.size());
  for (const auto& type : types) {
    result.emplace_back(pg::GetDeserialization<pg::VectorSink>(
      type, binary ? pg::VarFormat::Binary : pg::VarFormat::Text));
  }
  return result;
}

// Process one complete row (the bytes between row separators, delimiters still
// raw) into `output` at `row`. One pass per field finds the unescaped delimiter
// and notes whether the field needs unescaping: a field with no backslash
// reaches the decoder as a zero-copy view straight into `line`; only an escaped
// field is materialized through `field_buf`. The field count is validated
// against the column count.
void ProcessTextRow(std::string_view line, char delim,
                    std::string_view null_str,
                    const CopyDeserializers& deserializers,
                    pg::DeserializeContext& dctx, duckdb::DataChunk& output,
                    duckdb::idx_t row, std::string& field_buf) {
  const auto columns = deserializers.size();
  duckdb::idx_t column = 0;
  size_t pos = 0;
  for (;;) {
    bool had_escape = false;
    const auto end = ScanField(line, delim, pos, had_escape);
    if (column >= columns) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
        ERR_MSG("COPY FROM: row has more than ", columns, " fields"));
    }
    const auto raw = line.substr(pos, end - pos);
    auto& vec = output.data[column];
    if (raw == null_str) {
      duckdb::FlatVector::SetNull(vec, row, true);
    } else {
      const auto field = had_escape ? DecodeFieldView(raw, field_buf) : raw;
      pg::VectorSink sink{vec, row};
      if (!deserializers[column](dctx, field, sink)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
          ERR_MSG("COPY FROM: invalid input for column ", column + 1));
      }
    }
    ++column;
    if (end == line.size()) {
      break;
    }
    pos = end + 1;
  }
  if (column != columns) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
      ERR_MSG("COPY FROM: row has ", column, " fields, expected ", columns));
  }
}

CopyBatchParser::CopyBatchParser(duckdb::ClientContext& context,
                                 pg::CopyInTarget target)
  : _target{std::move(target)},
    _deserializers{MakeCopyDeserializers(_target.types, _target.binary)} {
  pg::FillDeserializeContext(context, _dctx);
}

void CopyBatchParser::Start(const pg::CopyBatch& batch) {
  _batch = &batch;
  _segment = 0;
  _pos = 0;
  _skip = _target.header && !_target.binary && batch.seq == 0;
}

duckdb::idx_t CopyBatchParser::Parse(duckdb::DataChunk& output,
                                     duckdb::idx_t row) {
  return _target.binary ? ParseBinary(output, row) : ParseText(output, row);
}

duckdb::idx_t CopyBatchParser::ParseText(duckdb::DataChunk& output,
                                         duckdb::idx_t row) {
  const auto& segments = _batch->segments;
  while (row < STANDARD_VECTOR_SIZE && _segment != segments.size()) {
    const auto data = segments[_segment];
    if (_pos == data.size()) {
      ++_segment;
      _pos = 0;
      continue;
    }
    const auto rest = data.substr(_pos);
    const auto end = pg::FindRowEnd(rest, 0);
    const auto line = TrimCarriageReturn(
      end == std::string_view::npos ? rest : rest.substr(0, end));
    _pos += end == std::string_view::npos ? rest.size() : end + 1;
    if (std::exchange(_skip, false)) {
      continue;
    }
    ProcessTextRow(line, _target.delim, _target.null_str, _deserializers, _dctx,
                   output, row, _field_buf);
    ++row;
  }
  return row;
}

duckdb::idx_t CopyBatchParser::ParseBinary(duckdb::DataChunk& output,
                                           duckdb::idx_t row) {
  const auto& segments = _batch->segments;
  const auto columns = _deserializers.size();
  while (row < STANDARD_VECTOR_SIZE && _segment != segments.size()) {
    const auto data = segments[_segment];
    if (_pos == data.size()) {
      ++_segment;
      _pos = 0;
      continue;
    }
    const auto fields =
      static_cast<int16_t>(absl::big_endian::Load16(data.data() + _pos));
    if (static_cast<size_t>(fields) != columns) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_BAD_COPY_FILE_FORMAT),
        ERR_MSG("COPY FROM: row has ", fields, " fields, expected ", columns));
    }
    const char* p = data.data() + _pos + kFieldCountBytes;
    for (duckdb::idx_t column = 0; column < columns; ++column) {
      const auto len = static_cast<int32_t>(absl::big_endian::Load32(p));
      p += kFieldLengthBytes;
      auto& vec = output.data[column];
      if (len == kNullFieldLength) {
        duckdb::FlatVector::SetNull(vec, row, true);
        continue;
      }
      pg::VectorSink sink{vec, row};
      if (!_deserializers[column](_dctx, {p, static_cast<size_t>(len)}, sink)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
          ERR_MSG("COPY FROM: invalid binary value in column ", column + 1));
      }
      p += len;
    }
    _pos = static_cast<size_t>(p - data.data());
    ++row;
  }
  return row;
}

}  // namespace sdb::connector
