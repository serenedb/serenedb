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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>

#include <array>
#include <magic_enum/magic_enum.hpp>
#include <string>
#include <string_view>

#include "iresearch/utils/assert.hpp"

namespace irs {

template<typename E>
std::string_view SnakeCaseName(E value) {
  static const auto kNames = [] {
    std::array<std::string, magic_enum::enum_count<E>()> names;
    absl::c_transform(magic_enum::enum_names<E>(), names.begin(),
                      [](std::string_view name) {
                        std::string snake;
                        for (const auto c : name) {
                          if (absl::ascii_isupper(c) && !snake.empty()) {
                            snake += '_';
                          }
                          snake += absl::ascii_tolower(c);
                        }
                        return snake;
                      });
    return names;
  }();
  const auto index = magic_enum::enum_index(value);
  SDB_ASSERT(index);
  return kNames[*index];
}

}  // namespace irs
