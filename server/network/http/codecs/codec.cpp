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

#include "network/http/codecs/codec.h"

#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::network::http {

// A codec failure is never expected; it must not be an assert, which compiles
// out in release builds and would leave the drain loops spinning forever on a
// sticky error. The session turns this into a 500 (or drops the connection
// once the head is out).
void ThrowCodecError(std::string_view coding, std::string_view detail) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INTERNAL_ERROR),
    ERR_MSG("HTTP response compression (", coding, ") failed: ", detail));
}

void ThrowCorrupt(std::string_view coding, std::string_view detail) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_EXCEPTION),
                  ERR_MSG("HTTP request body (", coding, "): ", detail));
}

}  // namespace sdb::network::http
