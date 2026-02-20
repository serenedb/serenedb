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

#include <string>
#include <string_view>

#include "basics/system-compiler.h"
#include "query/config.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "nodes/parsenodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

constexpr std::string_view kTransactionIsolation = "transaction_isolation";
constexpr std::string_view kDefaultTransactionIsolation =
  "default_transaction_isolation";

constexpr std::string_view kReadUncommitted = "read uncommitted";
constexpr std::string_view kReadCommitted = "read committed";
constexpr std::string_view kRepeatableRead = "repeatable read";
constexpr std::string_view kSerializable = "serializable";

constexpr std::string_view IsolationLevelName(IsolationLevel isolation_level) {
  switch (isolation_level) {
    case IsolationLevel::ReadCommitted:
      return kReadCommitted;
    case IsolationLevel::RepeatableRead:
      return kRepeatableRead;
    default:
      SDB_UNREACHABLE();
  }
}

// Extracts the isolation level string from some statements.
std::string GetIsolationLevel(const VariableSetStmt& stmt);
std::string GetIsolationLevel(const TransactionStmt& stmt);

// Validates that isolation_level is a known and supported level.
// Throws a SQL error for unknown or unsupported levels.
void ValidateIsolationLevel(std::string_view isolation_level,
                            std::string_view parameter_name);

bool IsKnownIsolationLevel(std::string_view data);
bool IsSupportedIsolationLevel(std::string_view data);

}  // namespace sdb::pg
