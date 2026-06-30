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

#include <absl/strings/ascii.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace irs::analysis {

size_t FindFirstAlnum(const char* data, size_t pos, size_t size) noexcept;
size_t FindFirstNonAlnum(const char* data, size_t pos, size_t size) noexcept;

template<typename Emit>
void SplitByNonAlpha(std::string_view data, Emit&& emit) {
  const char* const base = data.data();
  const size_t size = data.size();
  size_t pos = 0;

  while (pos < size) {
    pos = FindFirstAlnum(base, pos, size);
    if (pos >= size) {
      break;
    }
    const size_t start = pos;
    pos = FindFirstNonAlnum(base, pos, size);
    emit(std::string_view{base + start, pos - start});
  }
}

template<typename Emit>
void SplitByNonAlpha(std::string_view data, bool to_lower, std::string& buf,
                     Emit&& emit) {
  SplitByNonAlpha(data, [&](std::string_view token) {
    if (to_lower) {
      buf.resize(token.size());
      absl::ascii_internal::AsciiStrToLower(buf.data(), token.data(),
                                            token.size());
      emit(std::string_view{buf});
    } else {
      emit(token);
    }
  });
}

}  // namespace irs::analysis
