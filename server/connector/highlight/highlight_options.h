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

#include <cstdint>
#include <string>
#include <string_view>

namespace sdb::connector::highlight {

struct HighlightOptions {
  std::string start_sel = "<b>";
  std::string stop_sel = "</b>";
  std::string fragment_delim = " ... ";
  uint32_t max_words = 35;
  uint32_t max_fragments = 0;
  // Forwarded into the synthesised ts_offsets() call by sugar forms;
  // 0 lets ts_offsets() apply its own default cap.
  uint32_t max_offsets = 0;
  bool highlight_all = false;

  bool operator==(const HighlightOptions&) const = default;
};

// Parse a comma-separated `Key=Value` option string. Throws on
// unknown keys, type mismatches, or constraint violations.
HighlightOptions ParseHighlightOptions(std::string_view text);

}  // namespace sdb::connector::highlight
