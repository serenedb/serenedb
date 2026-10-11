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

#include <string_view>

namespace sdb::pg {

std::string_view gPgStatActivity = R"sql(SELECT
        S.datid AS datid,
        S.datname AS datname,
        S.pid,
        NULL::INTEGER AS leader_pid,
        U.oid AS usesysid,
        S.usename AS usename,
        NULL::TEXT AS application_name,
        NULL::TEXT AS client_addr,
        NULL::TEXT AS client_hostname,
        NULL::INTEGER AS client_port,
        to_timestamp(S.backend_start_us / 1000000.0) AS backend_start,
        NULL::TIMESTAMPTZ AS xact_start,
        to_timestamp(S.query_start_us / 1000000.0) AS query_start,
        NULL::TIMESTAMPTZ AS state_change,
        NULL::TEXT AS wait_event_type,
        NULL::TEXT AS wait_event,
        S.state AS state,
        NULL::TEXT AS backend_xid,
        NULL::TEXT AS backend_xmin,
        NULL::BIGINT AS query_id,
        S.query AS query,
        'client backend' AS backend_type
FROM sdb_progress AS S
    LEFT JOIN pg_authid AS U ON (S.usename = U.rolname))sql";

}  // namespace sdb::pg
