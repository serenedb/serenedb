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

#include "network/pg/bind_decoder.h"

#include <absl/base/internal/endian.h>

#include <cstdint>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::network::pg {

std::vector<sdb::pg::VarFormat> ParseBindFormats(std::string_view& cursor) {
  if (cursor.size() < sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  const auto count = absl::big_endian::Load16(cursor.data());
  cursor.remove_prefix(sizeof(uint16_t));
  if (cursor.size() < count * sizeof(uint16_t)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("malformed Bind message"));
  }
  std::vector<sdb::pg::VarFormat> formats;
  formats.reserve(count);
  for (uint16_t i = 0; i < count; ++i) {
    const auto code = absl::big_endian::Load16(cursor.data());
    cursor.remove_prefix(sizeof(uint16_t));
    if (code != 0 && code != 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                      ERR_MSG("invalid format code: ", code));
    }
    formats.push_back(code == 0 ? sdb::pg::VarFormat::Text
                                : sdb::pg::VarFormat::Binary);
  }
  return formats;
}

}  // namespace sdb::network::pg
