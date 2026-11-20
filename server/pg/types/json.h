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

#include <simdjson.h>

#include <cstring>
#include <string>
#include <string_view>

namespace sdb::pg {

class JsonInput {
 public:
  JsonInput();

  simdjson::error_code Check(std::string_view text);

 private:
  simdjson::ondemand::parser _parser;
  std::string _buffer;
};

#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
inline constexpr bool kZeroJsonPadding = true;
#else
inline constexpr bool kZeroJsonPadding = false;
#endif
#else
inline constexpr bool kZeroJsonPadding = false;
#endif

inline simdjson::padded_string_view PadJson(std::string_view json,
                                            std::string& buffer) {
  buffer.resize_and_overwrite(
    json.size() + simdjson::SIMDJSON_PADDING, [&](char* data, size_t size) {
      std::memcpy(data, json.data(), json.size());
      if constexpr (kZeroJsonPadding) {
        std::memset(data + json.size(), 0, simdjson::SIMDJSON_PADDING);
      }
      return size;
    });
  return simdjson::padded_string_view{buffer.data(), json.size(),
                                      buffer.size()};
}

[[noreturn]] void ThrowInvalidJson(simdjson::error_code error);

}  // namespace sdb::pg
