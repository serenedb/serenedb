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

#pragma once

#include <memory>
#include <string>

#include "auth/common.h"
#include "basics/assert.h"
#include "basics/static_strings.h"
#include "catalog/identifiers/object_id.h"
#include "rest/request_context.h"

namespace sdb {
namespace transaction {

class Methods;
}

/// Carries some information about the current
/// context in which this thread is executed.
/// We should strive to have it always accessible
/// from ExecContext::CURRENT. Inherits from request
/// context for convenience
class ExecContext : public RequestContext {
 protected:
  enum class Type {
    Default,
    Internal,
  };

  struct ConstructorToken {
    explicit ConstructorToken() = default;
  };

 public:
  constexpr ExecContext(ConstructorToken, ExecContext::Type type,
                        std::string_view user, std::string_view database,
                        ObjectId database_id, auth::Level system_level,
                        auth::Level db_level, bool is_admin_user)
    : _user{user},
      _database{database},
      _database_id{database_id},
      _type{type},
      _is_admin_user{is_admin_user},
      _system_db_auth_level{system_level},
      _database_auth_level{db_level} {
    SDB_ASSERT(_system_db_auth_level != auth::Level::Undefined);
    SDB_ASSERT(_database_auth_level != auth::Level::Undefined);
  }

  ExecContext(std::string_view user, std::string_view dbname,
              ObjectId database_id);

  ExecContext(const ExecContext&) = delete;
  ExecContext(ExecContext&&) = delete;

  ~ExecContext() override = default;

  /// shortcut helper to check the AuthenticationFeature
  static bool isAuthEnabled();

  static const ExecContext& superuser();
  static std::shared_ptr<const ExecContext> superuserAsShared();

  /// create user context
  static std::shared_ptr<ExecContext> create(std::string_view user,
                                             std::string_view db,
                                             ObjectId database_id);

  /// an internal user is none / ro / rw for all collections / dbs
  /// mainly used to override further permission resolution
  bool isInternal() const noexcept { return _type == Type::Internal; }

  /// any internal operation is a superuser.
  bool isSuperuser() const noexcept {
    return isInternal() && _system_db_auth_level == auth::Level::RW &&
           _database_auth_level == auth::Level::RW;
  }

  /// is allowed to manage users, create databases, ...
  bool isAdminUser() const noexcept { return _is_admin_user; }

  virtual bool isCanceled() const { return false; }
  virtual void cancel() {}

  /// current user, may be empty for internal users
  const auto& user() const { return _user; }

  /// current database
  const auto& GetDatabase() const { return _database; }
  auto GetDatabaseId() const { return _database_id; }

  /// authentication level on _system. Always RW for superuser
  auth::Level systemAuthLevel() const noexcept { return _system_db_auth_level; }

  /// Authentication level on database selected in the current
  ///        request scope. Should almost always contain something,
  ///        if this thread originated from HTTP
  auth::Level databaseAuthLevel() const noexcept {
    return _database_auth_level;
  }

  /// returns true if auth level is above or equal `requested`
  bool canUseDatabase(auth::Level requested) const noexcept {
    return requested <= _database_auth_level;
  }

  /// returns true if auth level is above or equal `requested`
  bool canUseDatabase(std::string_view db, auth::Level requested) const;

 protected:
  /// current user, may be empty for internal users
  const std::string _user;
  /// current database to use, superuser db is empty
  const std::string _database;
  const ObjectId _database_id;

  Type _type;
  /// Flag if admin user access (not regarding cluster RO mode)
  bool _is_admin_user;
  /// level of system database
  auth::Level _system_db_auth_level;
  /// level of current database
  auth::Level _database_auth_level;
};

struct Superuser final : ExecContext {
  constexpr Superuser(std::string_view database, ObjectId database_id)
    : ExecContext{ExecContext::ConstructorToken{},
                  ExecContext::Type::Internal,
                  "",
                  database,
                  database_id,
                  auth::Level::RW,
                  auth::Level::RW,
                  true} {}

  static std::shared_ptr<Superuser> create(std::string_view database,
                                           ObjectId database_id);
  static std::shared_ptr<Superuser> System() {
    return create(StaticStrings::kSystemDatabase, id::kSystemDB);
  }
};

}  // namespace sdb
