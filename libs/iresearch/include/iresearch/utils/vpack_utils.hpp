////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Alexey Bakharew
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <vpack/common.h>
#include <vpack/options.h>
#include <vpack/slice.h>

#include "string.hpp"

namespace irs {

// return slice as string
inline std::string slice_to_string(
  const vpack::Slice slice,
  const vpack::Options* options = &vpack::Options::gDefaults) noexcept {
  std::string str;
  try {
    str = slice.toString(options);
  } catch (...) {
    try {
      str = "<non-representable type>";
    } catch (...) {
    }
  }

  return str;
}

inline std::string_view slice_to_string_view(vpack::Slice slice) {
  if (slice.isNull()) {
    return {};
  }
  SDB_ASSERT(slice.isString());
  return slice.stringView();
}

template<typename Chr>
auto slice_to_view(vpack::Slice s) {
  static_assert(sizeof(Chr) == sizeof(uint8_t));
  return irs::basic_string_view{s.startAs<Chr>(), s.byteSize()};
}

template<typename Chr>
auto view_to_slice(irs::basic_string_view<Chr> s) {
  static_assert(sizeof(Chr) == sizeof(uint8_t));
  SDB_ASSERT(s.data());
  return vpack::Slice{reinterpret_cast<const uint8_t*>(s.data())};
}

}  // namespace irs
