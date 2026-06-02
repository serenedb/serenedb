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

#include "exec_context.h"

#include "basics/static_strings.h"
#include "catalog/identifiers/object_id.h"

namespace sdb {

static constexpr Superuser kSuperuser{StaticStrings::kDefaultDatabase,
                                      id::kSystemDB};

ExecContext::ExecContext(std::string_view user, std::string_view dbname,
                         ObjectId database_id)
  : _user{user},
    _database{dbname},
    _database_id{database_id},
    _type{ExecContext::Type::Default} {
  // Auth is intentionally absent until post-RBAC. Every context is admin.
  _database_auth_level = auth::Level::RW;
  _system_db_auth_level = auth::Level::RW;
  _is_admin_user = true;
}

const ExecContext& ExecContext::superuser() { return kSuperuser; }

std::shared_ptr<const ExecContext> ExecContext::superuserAsShared() {
  return {std::shared_ptr<const ExecContext>{}, &kSuperuser};
}

bool ExecContext::isAuthEnabled() { return false; }

bool ExecContext::canUseDatabase(std::string_view /*db*/,
                                 auth::Level requested) const {
  return requested <= _database_auth_level;
}

std::shared_ptr<ExecContext> ExecContext::create(std::string_view user,
                                                 std::string_view dbname,
                                                 ObjectId database_id) {
  return std::make_shared<ExecContext>(user, dbname, database_id);
}

std::shared_ptr<Superuser> Superuser::create(std::string_view database,
                                             ObjectId database_id) {
  return std::make_shared<Superuser>(database, database_id);
}

}  // namespace sdb
