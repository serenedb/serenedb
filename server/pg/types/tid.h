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

#include <cstddef>
#include <cstdint>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string_view>

namespace sdb::pg {

inline constexpr size_t kTidBufferSize = sizeof("(4294967295,65535)");
inline constexpr size_t kTidBytes = 6;

std::optional<irs::pg::SqlErrorData> TryParseTid(std::string_view text,
                                                 int64_t& tid);
size_t WriteTid(int64_t tid, char* buffer);
std::optional<int64_t> TidRecv(std::string_view bytes);
void TidSend(int64_t tid, uint8_t* buffer);

void RegisterTid(duckdb::CastFunctionSet& casts);

}  // namespace sdb::pg
