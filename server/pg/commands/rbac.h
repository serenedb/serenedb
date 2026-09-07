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

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace sdb {

class ConnectionContext;

}  // namespace sdb
namespace sdb::pg {

void RegisterRbacFunctions(duckdb::DatabaseInstance& db);

std::string SetRole(ConnectionContext& ctx, std::string_view name);
void ResetRole(ConnectionContext& ctx);
std::string SetSessionAuthorization(ConnectionContext& ctx,
                                    std::string_view name);
void ResetSessionAuthorization(ConnectionContext& ctx);

}  // namespace sdb::pg
