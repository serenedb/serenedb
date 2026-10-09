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
#include <duckdb/common/named_parameter_map.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "pg/connection_context.h"

namespace sdb::pg {

std::optional<uint64_t> ParseLsn(std::string_view text);
std::string FormatLsn(uint64_t lsn);

void CreateSubscription(ConnectionContext& conn_ctx, std::string_view name,
                        std::string_view conninfo,
                        std::vector<std::string> publications,
                        const duckdb::named_parameter_map_t& options);

void DropSubscription(ConnectionContext& conn_ctx, std::string_view name,
                      bool missing_ok, bool cascade);

void AlterSubscription(ConnectionContext& conn_ctx, std::string_view name,
                       std::string_view action, std::string_view argument,
                       std::vector<std::string> publications,
                       const duckdb::named_parameter_map_t& options);

}  // namespace sdb::pg
