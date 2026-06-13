////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "database_context.h"

#include "basics/static_strings.h"
#include "catalog/database.h"
#include "catalog/server_state.h"

namespace sdb {

DatabaseContext::DatabaseContext(ConstructorToken, GeneralRequest& request,
                                 std::shared_ptr<catalog::Database> database,
                                 ExecContext::Type type,
                                 auth::Level system_level, auth::Level db_level,
                                 bool is_admin_user)
  : ExecContext{ConstructorToken{},
                type,
                request.user().empty()
                  ? request.value(StaticStrings::kUserString)
                  : request.user(),
                database->GetName(),
                database->GetId(),
                system_level,
                db_level,
                is_admin_user},
    _database{std::move(database)} {
  SDB_ASSERT(_database);
}

std::shared_ptr<DatabaseContext> DatabaseContext::create(
  GeneralRequest& req, std::shared_ptr<catalog::Database> database) {
  SDB_ASSERT(database);
  // Auth is intentionally absent until post-RBAC. Every request is admin.
  return std::make_shared<DatabaseContext>(
    ConstructorToken{}, req, std::move(database),
    req.user().empty() ? ExecContext::Type::Internal
                       : ExecContext::Type::Default,
    auth::Level::RW, auth::Level::RW, true);
}

void DatabaseContext::forceSuperuser() {
  SDB_ASSERT(_type != ExecContext::Type::Internal || _user.empty());
  if (ServerState::instance()->ReadOnly()) {
    forceReadOnly();
  } else {
    _type = ExecContext::Type::Internal;
    _system_db_auth_level = auth::Level::RW;
    _database_auth_level = auth::Level::RW;
    _is_admin_user = true;
  }
}

void DatabaseContext::forceReadOnly() {
  SDB_ASSERT(_type != ExecContext::Type::Internal || _user.empty());
  _type = ExecContext::Type::Internal;
  _system_db_auth_level = auth::Level::RO;
  _database_auth_level = auth::Level::RO;
}

}  // namespace sdb
