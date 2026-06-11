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

#include <cstdarg>
#include <cstdio>
#include <source_location>
#include <type_traits>

#include "basics/errors.h"

#ifdef SDB_DEV
#include <cstring>
#endif

#include <yaclib/util/result.hpp>

#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/log.h"
#include "exceptions.h"

namespace sdb::basics {

Exception::Exception(ErrorCode code, std::string&& error_message,
                     std::source_location location) noexcept try
  : _error_message{std::move(error_message)}, _location{location}, _code{code} {
  switch (code.value()) {
    case ERROR_INTERNAL.value():
    case ERROR_OUT_OF_MEMORY.value():
    case ERROR_NOT_IMPLEMENTED.value():
      // TODO(gnusi): always append in maintainer mode
      // TODO(gnusi): avoid potenital double append
      absl::StrAppend(&error_message,
                      " (exception location: ", _location.file_name(), ":",
                      _location.line(), ")");
      break;
    default:
      break;
  }
} catch (...) {
}

[[noreturn]] void helper::DieWithLogMessage(const char* error_message) {
  SDB_FATAL(GENERAL, "Failed to create an error message, giving up. ",
            error_message);
}

[[noreturn]] void helper::LogAndAbort(const char* what) {
  // In SDB_DEV, SDB_ASSERT already routes through
  // CrashHandler::assertionFailure
  // -> LogCrash -> std::abort(). In non-DEV it is a no-op, so emit the crash
  // line directly and abort here.
  SDB_ASSERT(false, what);
  log::LogCrash(what != nullptr ? what : "LogAndAbort");
  std::abort();
}

Result TryToResult(yaclib::Result<Result>&& try_result) noexcept {
  return SafeCall([&] { return std::move(try_result).Ok(); });
}

}  // namespace sdb::basics
