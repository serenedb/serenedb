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

#include <cstdint>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string_view>
#include <vector>

namespace sdb::pg {

std::optional<irs::pg::SqlErrorData> ParseInt2Vector(
  std::string_view text, std::vector<int16_t>& values);
std::optional<irs::pg::SqlErrorData> ParseOidVector(
  std::string_view text, std::vector<int64_t>& values);

void RegisterVectors(duckdb::CastFunctionSet& casts);

}  // namespace sdb::pg
