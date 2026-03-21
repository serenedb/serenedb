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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <exception>
#include <new>
#include <source_location>
#include <string>
#include <type_traits>
#include <utility>
#include <yaclib/fwd.hpp>

#include "basics/error-registry.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/result.h"
#include "basics/result_or.h"
#include "basics/system-compiler.h"

#define SDB_THROW(...)                            \
  ::sdb::basics::detail::ThrowError{__VA_ARGS__}; \
  SDB_UNREACHABLE()

#define SDB_ENSURE(expr, ...) \
  if (!(expr)) [[unlikely]] { \
    SDB_ASSERT(false);        \
    SDB_THROW(__VA_ARGS__);   \
  }

#define SDB_THROW_FORMAT(code, ...) \
  SDB_THROW((code), absl::StrFormat(GetErrorStr(code), __VA_ARGS__));

namespace sdb::basics {

class Exception final : public std::exception {
 public:
  Exception(ErrorCode code, std::string&& error_message,
            std::source_location location) noexcept;
  Exception(ErrorCode code, std::source_location location)
    : Exception(code, std::string{GetErrorStr(code)}, location) {}

  const char* what() const noexcept final { return message().c_str(); }
  const std::string& message() const noexcept { return _error_message; }
  ErrorCode code() const noexcept { return _code; }
  std::source_location location() const noexcept { return _location; }

 private:
  std::string _error_message;
  std::source_location _location;
  const ErrorCode _code;
};

namespace detail {

template<typename... Args>
[[noreturn]] void ThrowErrorImpl(std::source_location location, ErrorCode code,
                                 Args&&... args) {
  constexpr auto kSize = sizeof...(args);

  if constexpr (kSize == 0) {
    throw Exception{code, location};
  } else {
    throw Exception{code, absl::StrCat(std::forward<Args>(args)...), location};
  }
}

template<typename... Args>
[[noreturn]] void ThrowErrorImpl(std::source_location location, Result&& r) {
  SDB_ASSERT(!r.ok());
  auto code = r.errorNumber();
  ThrowErrorImpl(location, code, std::move(r).errorMessage());
}

template<typename... Args>
struct ThrowError {
  [[noreturn]] ThrowError(Args&&... args, std::source_location location =
                                            std::source_location::current()) {
    ThrowErrorImpl(location, std::forward<Args>(args)...);
  }
};

template<typename... Args>
ThrowError(Args&&...) -> ThrowError<Args...>;

}  // namespace detail
namespace helper {

// just so we don't have to include logger and application-exit into this
// central header.
[[noreturn]] void DieWithLogMessage(const char*);

}  // namespace helper

template<typename F, typename E>
Result SafeCall(F&& fn, E&& err) noexcept {
  // The outer try/catch catches possible exceptions thrown by result.reset(),
  // due to allocation failure. If we don't have enough memory to allocate an
  // error, let's just give up.
  try {
    try {
      if constexpr (std::is_void_v<std::invoke_result_t<F>>) {
        std::forward<F>(fn)();
        return {};
      } else {
        return std::forward<F>(fn)();
      }
      // TODO check whether there are other specific exceptions we should catch
    } catch (const basics::Exception& e) {
      return err(e.code(), e.message());
    } catch (const std::bad_alloc& e) {
      return err(ERROR_OUT_OF_MEMORY, e.what());
    } catch (const std::exception& e) {
      return err(ERROR_INTERNAL, e.what());
    } catch (...) {
      return err(ERROR_INTERNAL, "");
    }
  } catch (const std::exception& e) {
    helper::DieWithLogMessage(e.what());
  } catch (...) {
    helper::DieWithLogMessage(nullptr);
  }
}

template<typename F>
Result SafeCall(F&& fn) noexcept {
  return SafeCall(
    std::forward<F>(fn),
    [](ErrorCode code, std::string_view msg) { return Result{code, msg}; });
}

template<typename F, typename T = std::invoke_result_t<F>>
ResultOr<T> SafeCallT(F&& fn) noexcept {
  // The outer try/catch catches possible exceptions thrown by result.reset(),
  // due to allocation failure. If we don't have enough memory to allocate an
  // error, let's just give up.
  try {
    try {
      return std::forward<F>(fn)();
      // TODO check whether there are other specific exceptions we should catch
    } catch (const basics::Exception& e) {
      return std::unexpected<Result>(std::in_place, e.code(), e.message());
    } catch (const std::bad_alloc&) {
      return std::unexpected<Result>(std::in_place, ERROR_OUT_OF_MEMORY);
    } catch (const std::exception& e) {
      return std::unexpected<Result>(std::in_place, ERROR_INTERNAL, e.what());
    } catch (...) {
      return std::unexpected<Result>(std::in_place, ERROR_INTERNAL);
    }
  } catch (const std::exception& e) {
    helper::DieWithLogMessage(e.what());
  } catch (...) {
    helper::DieWithLogMessage(nullptr);
  }
}

Result TryToResult(yaclib::Result<Result>&& try_result) noexcept;

namespace helper {

// just so we don't have to include logger into this header
[[noreturn]] void LogAndAbort(const char* what);

}  // namespace helper

// Throws the passed exception, but in maintainer mode, logs the error
// and aborts instead.
template<typename E>
[[noreturn]] void AbortOrThrowException(E&& e) {
#ifndef SDB_DEV
  throw std::forward<E>(e);
#else
  helper::LogAndAbort(e.what());
#endif
}

// Forwards arguments to an Exception constructor and calls
// abortOrThrowException
template<typename... Args>
[[noreturn]] void AbortOrThrow(Args... args) {
  AbortOrThrowException(Exception(std::forward<Args>(args)...));
}

}  // namespace sdb::basics
