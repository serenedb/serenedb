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
#include <duckdb/common/types/string_type.hpp>
#include <span>
#include <string_view>

#include "basics/bit_utils.hpp"
#include "catalog/table_options.h"

namespace sdb::connector {

inline std::string_view AsView(const duckdb::string_t& s) noexcept {
  return {s.GetData(), s.GetSize()};
}

// TODO(Dronplane) unify with key?
inline constexpr std::string_view kStringPrefix{"\0", 1};

}  // namespace sdb::connector
