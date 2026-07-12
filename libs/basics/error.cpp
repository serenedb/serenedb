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

#include <cstring>

#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/error-registry.h"
#include "basics/errors.h"
#include "basics/exitcodes.h"

namespace sdb {

// error number and system error
struct ErrorContainer {
  ErrorCode number = ERROR_OK;

  int sys = 0;
};

// holds the last error that occurred in the current thread
thread_local ErrorContainer gLastError;

std::string_view LastError() noexcept {
  ErrorCode err = gLastError.number;

  if (err == ERROR_SYS_ERROR) {
    return strerror(gLastError.sys);
  }

  return GetErrorStr(err);
}

/// sets the last error
void SetError(ErrorCode error) {
  gLastError.number = error;

  if (error == ERROR_SYS_ERROR) {
    gLastError.sys = errno;
  } else {
    gLastError.sys = 0;
  }
}

}  // namespace sdb
