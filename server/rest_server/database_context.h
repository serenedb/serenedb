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

#include <atomic>

#include "rest/general_request.h"
#include "utils/exec_context.h"

namespace sdb {
namespace catalog {

class Database;
}

class DatabaseContext final : public ExecContext {
 public:
  static std::shared_ptr<DatabaseContext> create(
    GeneralRequest& req, std::shared_ptr<catalog::Database> database);

  DatabaseContext(const DatabaseContext&) = delete;
  DatabaseContext& operator=(const DatabaseContext&) = delete;

  auto& GetDatabase() const { return *_database; }

  /// upgrade to internal superuser
  void forceSuperuser();

  /// upgrade to internal read-only user
  void forceReadOnly();

  bool isCanceled() const final {
    return _canceled.load(std::memory_order_relaxed);
  }

  void cancel() final { _canceled.store(true, std::memory_order_relaxed); }

 private:
  std::shared_ptr<catalog::Database> _database;
  /// should be used to indicate a canceled request / thread
  std::atomic_bool _canceled = false;

 public:
  explicit DatabaseContext(ConstructorToken, GeneralRequest& req,
                           std::shared_ptr<catalog::Database> database,
                           ExecContext::Type type, auth::Level system_level,
                           auth::Level db_level, bool is_admin_user);
};

}  // namespace sdb
