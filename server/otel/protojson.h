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

#include <cstddef>
#include <string_view>

#include "otel/model.h"

namespace sdb::otel {

// The JSON parser reads up to this many bytes past the end of its input.
inline constexpr size_t kJsonPadding = 64;

// `padded`: `json` is followed by kJsonPadding readable bytes, so it is parsed
// in place; otherwise it is first copied into a padded buffer.
void ParseLogsRequest(std::string_view json, ExportLogsRequest& out,
                      bool padded = false);
void ParseTracesRequest(std::string_view json, ExportTracesRequest& out,
                        bool padded = false);
void ParseMetricsRequest(std::string_view json, ExportMetricsRequest& out,
                         bool padded = false);

}  // namespace sdb::otel
