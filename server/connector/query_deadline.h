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

#include <chrono>
#include <duckdb/main/client_context.hpp>

namespace sdb::connector {

inline bool DeadlinePassed(const duckdb::ClientContext& client) noexcept {
  if (!client.query_deadline.IsValid()) {
    return false;
  }
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
  return static_cast<duckdb::idx_t>(now) >= client.query_deadline.GetIndex();
}

inline bool StatementRunning(const duckdb::ClientContext& client) noexcept {
  return !client.IsInterrupted() && !DeadlinePassed(client);
}

}  // namespace sdb::connector
