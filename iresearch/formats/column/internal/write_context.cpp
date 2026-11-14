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

#include "iresearch/formats/column/internal/write_context.hpp"

#include <absl/base/internal/endian.h>

#include <limits>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

duckdb::block_id_t WriteContext::WriteOverflowString(duckdb::string_t string) {
  const auto size = string.GetSize();
  SDB_ENSURE(size <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  byte_type length[sizeof(uint32_t)];
  absl::little_endian::Store32(length, static_cast<uint32_t>(size));
  const auto block = static_cast<duckdb::block_id_t>(_blocks->Position());
  _blocks->WriteData(length, sizeof(length));
  _blocks->WriteData(reinterpret_cast<const byte_type*>(string.GetData()),
                     size);
  return block;
}

}  // namespace irs
