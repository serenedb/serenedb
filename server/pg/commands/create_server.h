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

#include <duckdb/common/named_parameter_map.hpp>
#include <string_view>

#include "pg/connection_context.h"

namespace sdb::pg {

// CREATE SERVER: persist a foreign server (the external-DB connection target)
// and ATTACH it so it is immediately usable and survives restart. `options`
// carries the connection settings (host/port/database/user/password/...).
void CreateForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                         std::string_view fdw_name, bool if_not_exists,
                         const duckdb::named_parameter_map_t& options);

// DROP SERVER: remove the persisted server and DETACH its live attachment.
// `cascade` also drops the server's user mappings (RESTRICT, the default,
// refuses while any exist).
void DropForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                       bool missing_ok, bool cascade);

// CREATE USER MAPPING FOR <user> SERVER <server>: persist credentials for a
// (server, local-user) pair. A PUBLIC mapping re-attaches the server with the
// merged credentials; per-user mappings are stored/visible only (Phase 2a).
void CreateUserMapping(ConnectionContext& conn_ctx, std::string_view user,
                       std::string_view server, bool if_not_exists,
                       const duckdb::named_parameter_map_t& options);

// DROP USER MAPPING FOR <user> SERVER <server>.
void DropUserMapping(ConnectionContext& conn_ctx, std::string_view user,
                     std::string_view server, bool missing_ok);

}  // namespace sdb::pg
