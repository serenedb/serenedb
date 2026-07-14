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

#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/debugging.h"
#include "basics/shared.hpp"

namespace sdb::basics {

static constexpr size_t kMaxU64B10StringSize = 21;
static constexpr size_t kMaxU64B64StringSize = 11;

template<typename Str>
IRS_FORCE_INLINE void StrReserveAmortized(Str& str, size_t len) {
  const size_t cap = str.capacity();
  if (len > cap) {
    // Make sure to always grow by at least a factor of 2x.
    str.reserve(std::max(len, 2 * cap));
  }
}

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

// Parses a boolean spelled as on/true/yes/1/t/y or off/false/no/0/f/n
// (case-insensitive); nullopt for anything else. The caller decides how an
// unrecognized value is reported.
inline std::optional<bool> ParseBool(std::string_view value) {
  if (value == "1" || absl::EqualsIgnoreCase(value, "on") ||
      absl::EqualsIgnoreCase(value, "true") ||
      absl::EqualsIgnoreCase(value, "yes") ||
      absl::EqualsIgnoreCase(value, "t") ||
      absl::EqualsIgnoreCase(value, "y")) {
    return true;
  }
  if (value == "0" || absl::EqualsIgnoreCase(value, "off") ||
      absl::EqualsIgnoreCase(value, "false") ||
      absl::EqualsIgnoreCase(value, "no") ||
      absl::EqualsIgnoreCase(value, "f") ||
      absl::EqualsIgnoreCase(value, "n")) {
    return false;
  }
  return std::nullopt;
}

/// collection of string utility functions
///
/// This namespace holds function used for string manipulation.
namespace string_utils {

// -----------------------------------------------------------------------------
// ADDITIONAL STRING UTILITIES
// -----------------------------------------------------------------------------

/// returns "an" for words starting with a vowel sound, "a" otherwise
std::string_view GetArticle(std::string_view word) noexcept;

// Returns the English plural form of a word
// doesn't work for all the cases, cause applies basic rules:
// -s, -sh, -ch, -x, -z -> +es;
// consonant + y -> -y +ies;
// otherwise -> +s.
std::string GetPluralFormLowerCase(std::string_view word);

}  // namespace string_utils
}  // namespace sdb::basics
