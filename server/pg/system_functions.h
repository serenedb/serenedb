////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <array>
#include <string_view>

namespace sdb::pg {

// TODO(mkornaukhov) write queries in separate sql file
// TODO revoke, grant, create rules and other stuff?
inline constexpr auto kSystemFunctionsQueries =
  std::to_array<std::string_view>({
    R"(CREATE FUNCTION pg_show_all_settings()
        RETURNS TABLE(name TEXT, value TEXT, description TEXT)
        LANGUAGE SQL
        BEGIN ATOMIC
            SELECT * FROM sdb_show_all_settings;
        END;)",
  });

}  // namespace sdb::pg
