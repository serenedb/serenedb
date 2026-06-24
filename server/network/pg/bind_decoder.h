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

#include <string_view>
#include <vector>

#include "pg/serialize.h"

namespace sdb::network::pg {

// Parse a Bind-message format-code array: an int16 count followed by that many
// int16 codes (0 = Text, 1 = Binary). Advances `cursor` past the array.
// Throws ERRCODE_PROTOCOL_VIOLATION on a truncated array or an out-of-range
// code. A Bind message carries two such arrays (input formats before the
// parameter values, output formats after), so this is called twice.
std::vector<sdb::pg::VarFormat> ParseBindFormats(std::string_view& cursor);

}  // namespace sdb::network::pg
