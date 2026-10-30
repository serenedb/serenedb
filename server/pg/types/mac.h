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

#include "pg/types/function.h"

namespace sdb::pg {

inline constexpr size_t kMacaddr8Text = sizeof("08:00:2b:ff:fe:01:02:03") - 1;
inline constexpr size_t kMacaddrBytes = 6;
inline constexpr size_t kMacaddr8Bytes = 8;

size_t WriteMacaddr(uint64_t mac, char* buffer);
size_t WriteMacaddr8(uint64_t mac, char* buffer);
std::optional<irs::pg::SqlErrorData> TryParseMacaddr(std::string_view text,
                                                     uint64_t& mac);
std::optional<irs::pg::SqlErrorData> TryParseMacaddr8(std::string_view text,
                                                      uint64_t& mac);
std::optional<uint64_t> MacaddrRecv(std::string_view bytes);
std::optional<uint64_t> Macaddr8Recv(std::string_view bytes);
void MacSend(uint64_t mac, size_t bytes, uint8_t* buffer);

void RegisterMacaddr(duckdb::CastFunctionSet& casts, Functions& functions);

}  // namespace sdb::pg
