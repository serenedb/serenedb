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

// SDB_RBAC_DISABLED. rbac.cpp is out of the build until the RBAC phase. Only
// the four identity switches need a definition: they are reached from the
// `role` and `session_authorization` GUCs, which every session can set, rather
// than from a role DDL statement. The rest of the header's surface is called
// only by the RBAC command paths, which left the build with it.
//
// PLAN.md: an RBAC statement in a non-RBAC test parses and succeeds as a
// no-op, so these echo the requested identity back without switching one.

#include <string>
#include <string_view>

#include "pg/commands/rbac.h"

namespace sdb::pg {

std::string SetRole(ConnectionContext& /*ctx*/, std::string_view name) {
  return std::string{name};
}

void ResetRole(ConnectionContext& /*ctx*/) {}

std::string SetSessionAuthorization(ConnectionContext& /*ctx*/,
                                    std::string_view name) {
  return std::string{name};
}

void ResetSessionAuthorization(ConnectionContext& /*ctx*/) {}

}  // namespace sdb::pg
