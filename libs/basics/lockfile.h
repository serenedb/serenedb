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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/error.h"
#include "basics/errors.h"

namespace sdb {

// Pid-locking primitives backed by fcntl(2) F_WRLCK / F_UNLCK on a small file
// whose body is the holder's pid. Used by DatabasePathFeature to gate "one
// serened per datadir". No std::filesystem equivalent -- file locks are
// kernel-level.
//
// CreateLockFile: O_EXCL-create the file, write the pid, take an exclusive
// fcntl lock. ERROR_OK on success.
// VerifyLockFile: existing file? read the pid, check if that process is alive
// (kill(pid, 0)), then try to acquire the lock. Returns
// ERROR_SERVER_DATADIR_LOCKED if held; ERROR_OK if it's a stale leftover.
ErrorCode CreateLockFile(const char* filename);
ErrorCode VerifyLockFile(const char* filename);

}  // namespace sdb
