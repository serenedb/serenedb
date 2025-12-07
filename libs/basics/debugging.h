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

#include <cstdlib>
#include <string_view>
#include <vector>

/// macro SDB_IF_FAILURE
/// this macro can be used in maintainer mode to make the server fail at
/// certain locations in the C code. The points at which a failure is actually
/// triggered can be defined at runtime using AddFailurePointDebugging().
#ifdef SDB_FAULT_INJECTION

#define SDB_IF_FAILURE(what) if (sdb::ShouldFailDebugging(what))

#else

#define SDB_IF_FAILURE(what) if constexpr (false)

#endif

namespace sdb {

inline constexpr std::string_view kFailPointPrefix = "sdb_fault";

/// intentionally cause a segmentation violation or other failures
#ifdef SDB_FAULT_INJECTION
void TerminateDebugging(std::string_view value);
#else
inline void TerminateDebugging(std::string_view) {}
#endif

#ifdef SDB_FAULT_INJECTION
bool ShouldFailDebugging(std::string_view value) noexcept;
#else
constexpr bool ShouldFailDebugging(std::string_view) noexcept { return false; }
#endif

#ifdef SDB_FAULT_INJECTION
bool AddFailurePointDebugging(std::string_view value);
#else
constexpr bool AddFailurePointDebugging(std::string_view) noexcept {
  return false;
}
#endif

#ifdef SDB_FAULT_INJECTION
bool RemoveFailurePointDebugging(std::string_view value);
#else
constexpr bool RemoveFailurePointDebugging(std::string_view) noexcept {
  return false;
}
#endif

#ifdef SDB_FAULT_INJECTION
void ClearFailurePointsDebugging() noexcept;
#else
constexpr void ClearFailurePointsDebugging() noexcept {}
#endif

#ifdef SDB_FAULT_INJECTION
std::vector<std::string> GetFailurePointsDebugging();
#else
constexpr std::vector<std::string> GetFailurePointsDebugging() { return {}; }
#endif

constexpr bool CanUseFailurePointsDebugging() {
#ifdef SDB_FAULT_INJECTION
  return true;
#else
  return false;
#endif
}

}  // namespace sdb

#include "basics/assert.h"
