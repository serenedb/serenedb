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

#include <cstdint>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <string>
#include <string_view>

#include "basics/assert.h"

namespace sdb::connector {

// Resolve a UINTEGER option: the explicit WITH value when present, else the
// registered DB-level default. Shared by the search-table and inverted-index
// CREATE paths so both resolve the maintenance intervals identically.
inline uint32_t ResolveUintWithOption(duckdb::ClientContext& context,
                                      std::string_view name,
                                      const duckdb::Value* with_value) {
  if (with_value != nullptr) {
    return with_value->GetValue<uint32_t>();
  }
  duckdb::Value value;
  auto found = context.TryGetCurrentSetting(std::string{name}, value);
  SDB_ASSERT(found, "missing DB-level default for setting: ", name);
  return value.GetValue<uint32_t>();
}

// 64-bit twin of ResolveUintWithOption for byte-sized options.
inline uint64_t ResolveUbigintWithOption(duckdb::ClientContext& context,
                                         std::string_view name,
                                         const duckdb::Value* with_value) {
  if (with_value != nullptr) {
    return with_value->GetValue<uint64_t>();
  }
  duckdb::Value value;
  auto found = context.TryGetCurrentSetting(std::string{name}, value);
  SDB_ASSERT(found, "missing DB-level default for setting: ", name);
  return value.GetValue<uint64_t>();
}

}  // namespace sdb::connector
