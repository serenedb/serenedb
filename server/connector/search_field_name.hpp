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

#include <iresearch/types.hpp>
#include <string>

#include "basics/string_utils.h"

namespace sdb::connector {

// Encodes a unified iresearch field id (covers both columns and expressions;
// see `SearchColumnInfo::field_id`) as an 8-byte big-endian string. Caller
// mangles the result for the target value kind.
inline void MakeFieldName(irs::field_id field_id, std::string& out) {
  basics::StrResize(out, sizeof(field_id));
  absl::big_endian::Store(out.data(), field_id);
}

}  // namespace sdb::connector
