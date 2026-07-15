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

#include "string_utils.h"

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/internal/memutil.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <fast_float/fast_float.h>
#include <math.h>
#include <stdlib.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <regex>
#include <system_error>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "pg/sql_exception_macro.h"

namespace {

bool IsVowel(char c) noexcept {
  switch (c) {
    case 'a':
    case 'e':
    case 'i':
    case 'o':
    case 'u':
      return true;
    default:
      return false;
  }
}

}  // namespace
namespace sdb::basics::string_utils {

// TODO(gnusi) use more specialized error codes?

std::string_view GetArticle(std::string_view word) noexcept {
  if (word.empty()) {
    return "";
  }

  if (IsVowel(absl::ascii_tolower(word.front()))) {
    return "an";
  }

  return "a";
}

std::string GetPluralFormLowerCase(std::string_view word) {
  if (word.empty()) {
    return {};
  }

  std::string lower;
  lower.reserve(word.size() + 2);
  absl::StrAppend(&lower, absl::AsciiStrToLower(word));

  if (lower.ends_with('s') || lower.ends_with('x') || lower.ends_with('z') ||
      lower.ends_with("sh") || lower.ends_with("ch")) {
    absl::StrAppend(&lower, "es");
    return lower;
  }

  if (lower.size() >= 2 && lower.ends_with('y') &&
      !IsVowel(lower[lower.size() - 2])) {
    lower.back() = 'i';
    absl::StrAppend(&lower, "es");
    return lower;
  }

  lower.push_back('s');
  return lower;
}

}  // namespace sdb::basics::string_utils
