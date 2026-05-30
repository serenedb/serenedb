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

#include "lockfile_feature.h"

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/exceptions.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "rest_server/database_path_feature.h"

using namespace sdb::basics;

namespace sdb {

LockfileFeature::LockfileFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(false);
}

void LockfileFeature::start() {
  // build lockfile name
  auto& database = server().getFeature<DatabasePathFeature>();
  _lock_filename = database.subdirectoryName("LOCK");

  SDB_ASSERT(!_lock_filename.empty());

  auto res = SdbVerifyLockFile(_lock_filename.c_str());

  if (res != ERROR_OK) {
    std::string other_pid;
    try {
      other_pid = file_utils::Slurp(_lock_filename);
    } catch (...) {
    }
    if (other_pid.empty()) {
      SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
        "failed to read/write lockfile, please check the file permissions "
        "of the lockfile '",
        _lock_filename, "'");
    } else {
      SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
        "database is locked by process ", other_pid,
        "; please stop it first and check that the lockfile '", _lock_filename,
        "' goes away. If you are sure no other serened process is ",
        "running, please remove the lockfile '", _lock_filename,
        "' and try again");
    }
  }

  if (SdbExistsFile(_lock_filename.c_str())) {
    res = SdbUnlinkFile(_lock_filename.c_str());

    if (res != ERROR_OK) {
      SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
        "failed to remove an abandoned lockfile in the database "
        "directory, please check the file permissions of the lockfile '",
        _lock_filename, "': ", GetErrorStr(res));
    }
  }
  res = SdbCreateLockFile(_lock_filename.c_str());

  if (res != ERROR_OK) {
    SDB_FATAL_EXIT_CODE(GENERAL, EXIT_COULD_NOT_LOCK,
                        "failed to lock the database directory using '",
                        _lock_filename, "': ", GetErrorStr(res));
  }

  auto cleanup = std::make_unique<CleanupFunctions::CleanupFunction>(
    [&](int code, void* data) {
      // TODO(mbkkt) why ignore?
      std::ignore = SdbDestroyLockFile(_lock_filename.c_str());
    });
  CleanupFunctions::registerFunction(std::move(cleanup));
}

void LockfileFeature::unprepare() {
  // TODO(mbkkt) why ignore?
  std::ignore = SdbDestroyLockFile(_lock_filename.c_str());
}

}  // namespace sdb
