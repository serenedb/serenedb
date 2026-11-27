////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/strings/ascii.h>
#include <absl/strings/charconv.h>
#include <absl/strings/internal/resize_uninitialized.h>
#include <absl/strings/match.h>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "iresearch/utils/common.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::utils {

template<typename Str>
IRS_FORCE_INLINE void StrResize(Str& str, size_t len) {
  absl::strings_internal::STLStringResizeUninitialized(&str, len);
}

template<typename Str>
IRS_FORCE_INLINE void StrResizeAmortized(Str& str, size_t len) {
  absl::strings_internal::STLStringResizeUninitializedAmortized(&str, len);
}

template<typename Str>
IRS_FORCE_INLINE void StrAppend(Str& str, size_t len) {
  str.__append_default_init(len);
}

}  // namespace irs::utils
