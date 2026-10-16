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
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string_view>

#include "pg/types/function.h"

namespace sdb::pg {

inline constexpr size_t kMoneyText = sizeof("-$92,233,720,368,547,758.08") - 1;
inline constexpr int64_t kMoneyThousand = 100'000;

std::optional<irs::pg::SqlErrorData> TryParseMoney(std::string_view text,
                                                   duckdb::hugeint_t& money);
int64_t MoneyCents(duckdb::hugeint_t money);
size_t WriteMoney(duckdb::hugeint_t money, char* buffer);

void RegisterMoney(duckdb::CastFunctionSet& casts, Functions& functions);

}  // namespace sdb::pg
