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

#include "auth/role_utils.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "catalog/database.h"
#include "general_server/authentication_feature.h"
#include "general_server/state.h"

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
  // superusers will have an empty username. This MUST be invalid
  // for users authenticating with name / password
  bool is_super_user =
    req.authenticated() && req.user().empty() &&
    req.authenticationMethod() == rest::AuthenticationMethod::Jwt;
  if (is_super_user) {
    return std::make_shared<DatabaseContext>(
      ConstructorToken{}, req, std::move(database), ExecContext::Type::Internal,
      auth::Level::RW, auth::Level::RW, true);
  }

  AuthenticationFeature* auth = AuthenticationFeature::instance();
  SDB_ASSERT(auth != nullptr);
  if (!auth->isActive()) {
    if (ServerState::instance()->ReadOnly()) {
      // special read-only case
      return std::make_shared<DatabaseContext>(
        ConstructorToken{}, req, std::move(database),
        ExecContext::Type::Internal, auth::Level::RO, auth::Level::RO, true);
    }
    return std::make_shared<DatabaseContext>(
      ConstructorToken{}, req, std::move(database),
      req.user().empty() ? ExecContext::Type::Internal
                         : ExecContext::Type::Default,
      auth::Level::RW, auth::Level::RW, true);
  }

  if (!req.authenticated()) {
    return std::make_shared<DatabaseContext>(
      ConstructorToken{}, req, std::move(database), ExecContext::Type::Default,
      auth::Level::None, auth::Level::None, false);
  }

  if (req.user().empty()) {
    std::string msg = "only jwt can be used to authenticate as superuser";
    SDB_WARN(GENERAL, msg);
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, std::move(msg));
  }

  if (!ServerState::instance()->IsClientNode()) {
    SDB_WARN(GENERAL,
             "users are not supported on this server");
    return nullptr;
  }

  auto db_lvl = auth::DatabaseAuthLevel(req.user(), req.databaseName(), false);
  auto sys_lvl = db_lvl;
  if (req.databaseName() != StaticStrings::kDefaultDatabase) {
    sys_lvl = auth::DatabaseAuthLevel(req.user(),
                                      StaticStrings::kDefaultDatabase, false);
  }
  bool is_admin_user = (sys_lvl == auth::Level::RW);
  if (!is_admin_user && ServerState::instance()->ReadOnly()) {
    // in case we are in read-only mode, we need to re-check the original
    // permissions
    is_admin_user =
      auth::DatabaseAuthLevel(req.user(), StaticStrings::kDefaultDatabase,
                              true) == auth::Level::RW;
  }

  return std::make_shared<DatabaseContext>(
    ConstructorToken{}, req, std::move(database), ExecContext::Type::Default,
    sys_lvl, db_lvl, is_admin_user);
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
