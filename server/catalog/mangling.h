////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/strings/str_cat.h>

#include <string>

#include "basics/assert.h"

namespace sdb::search::mangling {

// Field mangling rules:
//
// Null     |  <field> | \0 |            |
// Bool     |  <field> | \1 |            |
// Numeric  |  <field> | \2 |            |
// String   |  <field> | \3 | <analyzer> |
// Analyzer |  <field> | \4 | <analyzer> |
// Nested   |  <field> | \5 |            |

inline constexpr char kNull = '\0';
inline constexpr char kBool = '\1';
inline constexpr char kNumeric = '\2';
inline constexpr char kString = '\3';
inline constexpr char kAnalyzer = '\4';
inline constexpr char kNested = '\5';

inline void NormalizeExpansion(std::string& name) {
  // remove the last expansion as it could be omitted according to our
  // indicies behaviour
  if (name.ends_with("[*]")) {
    name.erase(name.size() - 3);
  }
}

inline void MangleNested(std::string& name) {
  NormalizeExpansion(name);
  name.push_back(kNested);
}

inline void MangleAnalyzer(std::string& name) {
  NormalizeExpansion(name);
  name.push_back(kAnalyzer);
}

inline void MangleNull(std::string& name) {
  NormalizeExpansion(name);
  name.push_back(kNull);
}

inline void MangleBool(std::string& name) {
  NormalizeExpansion(name);
  name.push_back(kBool);
}

inline void MangleNumeric(std::string& name) {
  NormalizeExpansion(name);
  name.push_back(kNumeric);
}

inline void MangleString(std::string& name) {
  NormalizeExpansion(name);
  name.push_back(kString);
}

[[maybe_unused]] inline bool IsNestedField(std::string_view name) noexcept {
  return !name.empty() && name.back() == kNested;
}

std::string_view DemangleType(std::string_view name) noexcept;
std::string_view DemangleNested(std::string_view name, std::string& buf);
[[maybe_unused]] inline std::string_view Demangle(std::string_view name,
                                                  std::string& buf) {
  return DemangleNested(DemangleType(name), buf);
}
std::string_view ExtractAnalyzerName(std::string_view field_name);

}  // namespace sdb::search::mangling
