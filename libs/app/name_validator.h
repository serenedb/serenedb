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

#include <cstddef>
#include <string>
#include <string_view>

#include "basics/result.h"

namespace sdb {

struct DatabaseNameValidator {
  /// maximal database name length, in bytes
  static constexpr size_t kMaxNameLength = 128;

  static Result validateName(bool allow_system, std::string_view name);

 private:
  /// checks if a database name is allowed in the given context.
  /// returns true if the name is allowed and false otherwise.
  /// does not check for proper UTF-8 NFC normalization.
  [[nodiscard]] static bool isAllowedName(bool allow_system,
                                          std::string_view name) noexcept;
};

struct TableNameValidator {
  /// maximal collection name length, in bytes
  static constexpr size_t kMaxNameLength = 256;

  static Result validateName(std::string_view name);

 private:
  /// checks if a collection name is allowed in the given context.
  /// returns true if the name is allowed and false otherwise
  /// does not check for proper UTF-8 NFC normalization.
  [[nodiscard]] static bool isAllowedName(std::string_view name) noexcept;
};

}  // namespace sdb
