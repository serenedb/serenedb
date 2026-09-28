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
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <exception>
#include <optional>
#include <string>
#include <utility>

#include "network/http/handler.h"

namespace sdb::network {

inline std::optional<duckdb::ErrorData> EnsurePrepared(RequestContext& ctx,
                                                       PreparedEntry& entry,
                                                       const std::string& sql) {
  try {
    auto& connection = ctx.Connection();
    auto& context = *connection.context;
    if (entry.statement != nullptr) {
      bool stale = false;
      context.RunFunctionInTransaction([&] {
        stale = entry.statement->data->RequireRebind(context, nullptr);
      });
      if (stale) {
        entry.statement.reset();
      }
    }
    if (entry.statement == nullptr) {
      auto statement = connection.Prepare(sql);
      if (statement->HasError()) {
        return statement->GetErrorObject();
      }
      entry.statement = std::move(statement);
    }
  } catch (const std::exception& error) {
    return duckdb::ErrorData{error};
  }
  return std::nullopt;
}

}  // namespace sdb::network
