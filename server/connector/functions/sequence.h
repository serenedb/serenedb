////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <array>
#include <duckdb/main/database.hpp>
#include <string_view>

namespace sdb::connector {

inline constexpr std::string_view kNextval = "nextval";
inline constexpr std::string_view kCurrval = "currval";
inline constexpr std::string_view kSetval = "setval";

inline constexpr std::array<std::string_view, 3> kSequenceFunctionNames = {
  kNextval,
  kCurrval,
  kSetval,
};

void RegisterSequenceFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
