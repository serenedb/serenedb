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

#include "pg/types/tid.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/str_format.h>

#include <array>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

constexpr int kOffsetBits = 16;

bool TidToText(duckdb::Vector& source, duckdb::Vector& result,
               duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    source, result, count, [&](int64_t tid) {
      char buffer[kTidBufferSize];
      return duckdb::StringVector::AddString(result, buffer,
                                             WriteTid(tid, buffer));
    });
  return true;
}

}  // namespace

std::optional<irs::pg::SqlErrorData> TryParseTid(std::string_view text,
                                                 int64_t& tid) {
  const auto error = [&] {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type tid: \"", text, "\""));
  };
  std::array<size_t, 2> coords{};
  size_t found = 0;
  for (size_t i = 0;
       i != text.size() && found != coords.size() && text[i] != ')'; ++i) {
    if (text[i] == ',' || (text[i] == '(' && found == 0)) {
      coords[found++] = i + 1;
    }
  }
  if (found != coords.size()) {
    return error();
  }
  const auto block = ParseNumber(text, coords[0]);
  SDB_ASSERT(block.end < text.size());
  if (block.out_of_range || text[block.end] != ',') {
    return error();
  }
  const auto block_value =
    block.negative ? uint64_t{0} - block.magnitude : block.magnitude;
  const auto block_number = static_cast<uint32_t>(block_value);
  if (block_value != block_number &&
      block_value !=
        static_cast<uint64_t>(static_cast<int32_t>(block_number))) {
    return error();
  }
  const auto offset = ParseNumber(text, coords[1]);
  if (offset.out_of_range || offset.end == text.size() ||
      text[offset.end] != ')' || (offset.negative && offset.magnitude != 0) ||
      offset.magnitude > std::numeric_limits<uint16_t>::max()) {
    return error();
  }
  tid = (static_cast<int64_t>(block_number) << kOffsetBits) |
        static_cast<int64_t>(offset.magnitude);
  return std::nullopt;
}

size_t WriteTid(int64_t tid, char* buffer) {
  return static_cast<size_t>(absl::SNPrintF(
    buffer, kTidBufferSize, "(%u,%u)",
    static_cast<uint32_t>(tid >> kOffsetBits),
    static_cast<uint32_t>(tid & std::numeric_limits<uint16_t>::max())));
}

std::optional<int64_t> TidRecv(std::string_view bytes) {
  if (bytes.size() != kTidBytes) {
    return std::nullopt;
  }
  const auto block = absl::big_endian::Load32(bytes.data());
  const auto offset = absl::big_endian::Load16(bytes.data() + sizeof(block));
  return (static_cast<int64_t>(block) << kOffsetBits) | offset;
}

void TidSend(int64_t tid, uint8_t* buffer) {
  absl::big_endian::Store32(buffer, static_cast<uint32_t>(tid >> kOffsetBits));
  absl::big_endian::Store16(
    buffer + sizeof(uint32_t),
    static_cast<uint16_t>(tid & std::numeric_limits<uint16_t>::max()));
}

void RegisterTid(duckdb::CastFunctionSet& casts) {
  ReplaceTextInput(casts, TID(), TextInputCast<int64_t, TryParseTid>);
  casts.RegisterCastFunction(TID(), duckdb::LogicalType::VARCHAR, TidToText);
}

}  // namespace sdb::pg
