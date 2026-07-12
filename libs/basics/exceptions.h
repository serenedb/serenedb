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
#include "basics/system-compiler.h"

#define SDB_THROW(...)                            \
  ::sdb::basics::detail::ThrowError{__VA_ARGS__}; \
  SDB_UNREACHABLE()

#define SDB_ENSURE(expr, error_code, ...) \
  if (!(expr)) [[unlikely]] {             \
    SDB_ASSERT(false, __VA_ARGS__);       \
    SDB_THROW(error_code, __VA_ARGS__);   \
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
