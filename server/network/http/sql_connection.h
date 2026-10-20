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
#include <duckdb/main/connection.hpp>
#include <duckdb/parallel/task_scheduler_token.hpp>
#include <memory>
#include <string_view>

#include "network/cancel_registry.h"
#include "pg/connection_context.h"

namespace sdb::network::http {

struct SqlConnection {
  duckdb::unique_ptr<duckdb::Connection> conn;
  std::shared_ptr<ConnectionContext> context;
};

SqlConnection OpenSqlConnection(
  std::string_view user, std::string_view database, uint64_t cancel_key,
  CancelRegistry* cancel,
  const duckdb::shared_ptr<duckdb::SchedulerAccount>& account);

}  // namespace sdb::network::http
