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

#include "pg/types/char.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>

#include "pg/types/cast.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

duckdb::string_t AddChar(duckdb::Vector& result, uint8_t value) {
  char buffer[kCharText];
  return duckdb::StringVector::AddString(result, buffer,
                                         WriteChar(value, buffer));
}

bool TextToChar(duckdb::Vector& source, duckdb::Vector& result,
                duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
    source, result, count, [&](duckdb::string_t text) {
      return AddChar(result, CharIn({text.GetData(), text.GetSize()}));
    });
  return true;
}

bool CharFromInteger(duckdb::Vector& source, duckdb::Vector& result,
                     duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<int32_t, duckdb::string_t>(
    source, result, count, [&](int32_t value) {
      if (value < std::numeric_limits<int8_t>::min() ||
          value > std::numeric_limits<int8_t>::max()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("\"char\" out of range"));
      }
      return AddChar(result, static_cast<uint8_t>(value));
    });
  return true;
}

bool CharToInteger(duckdb::Vector& source, duckdb::Vector& result,
                   duckdb::idx_t count, duckdb::CastParameters&) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
    source, result, count, [](duckdb::string_t stored) {
      return static_cast<int32_t>(
        static_cast<int8_t>(CharIn({stored.GetData(), stored.GetSize()})));
    });
  return true;
}

}  // namespace

uint8_t CharIn(std::string_view text) {
  if (text.size() == kCharText && text[0] == '\\' &&
      absl::c_all_of(text.substr(1),
                     [](char c) { return c >= '0' && c <= '7'; })) {
    return static_cast<uint8_t>(((text[1] - '0') << 6) +
                                ((text[2] - '0') << 3) + (text[3] - '0'));
  }
  return text.empty() ? 0 : static_cast<uint8_t>(text[0]);
}

size_t WriteChar(uint8_t value, char* buffer) {
  if (value == 0) {
    return 0;
  }
  if (value < 0x80) {
    buffer[0] = static_cast<char>(value);
    return 1;
  }
  buffer[0] = '\\';
  buffer[1] = static_cast<char>('0' + (value >> 6));
  buffer[2] = static_cast<char>('0' + ((value >> 3) & 7));
  buffer[3] = static_cast<char>('0' + (value & 7));
  return kCharText;
}

void RegisterChar(duckdb::CastFunctionSet& casts) {
  ReplaceTextInput(casts, CHAR(), TextToChar);
  casts.RegisterCastFunction(duckdb::LogicalType::INTEGER, CHAR(),
                             CharFromInteger);
  casts.RegisterCastFunction(CHAR(), duckdb::LogicalType::INTEGER,
                             CharToInteger);
}

}  // namespace sdb::pg
