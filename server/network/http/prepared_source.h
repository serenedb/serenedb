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

#include <duckdb/common/error_data.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "network/http/handler.h"

namespace sdb::network {

struct CachedStatement {
  duckdb::PreparedStatement* statement = nullptr;
  std::optional<duckdb::ErrorData> error;
};

inline CachedStatement PrepareCached(RequestContext& ctx, PreparedSlotId slot,
                                     std::string_view sql) {
  auto& cache = ctx.PreparedSlot(slot);
  if (auto* statement = cache.Find(sql)) {
    return {.statement = statement};
  }
  try {
    auto statement = ctx.Connection().Prepare(sql);
    if (statement->HasError()) {
      return {.error = statement->GetErrorObject()};
    }
    return {.statement = &cache.Insert(std::string{sql}, std::move(statement),
                                       PreparedSlotCapacity(slot))};
  } catch (const std::exception& error) {
    return {.error = duckdb::ErrorData{error}};
  }
}

}  // namespace sdb::network
