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

#include <string_view>

#include "basics/error-registry.h"
#include "basics/error_code.h"

namespace sdb {

void SetError(ErrorCode);

std::string_view LastError() noexcept;

constexpr std::string_view GetErrorStr(ErrorCode code) noexcept {
  using sdb::error::ErrorMessages;
  auto it = ErrorMessages.find(code);

  if (it == ErrorMessages.end()) {
    return "unknown error";
  }

  return it->second;
}

}  // namespace sdb
