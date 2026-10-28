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

namespace sdb::pg {

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

}  // namespace sdb::pg
