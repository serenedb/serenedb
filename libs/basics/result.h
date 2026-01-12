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

#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

#include "basics/error_code.h"
#include "basics/result_error.h"

namespace sdb {

class [[nodiscard]] Result final {
 public:
  Result() noexcept = default;
  Result(bool /*avoidCastingErrors*/) = delete;
  Result(int /*avoidCastingErrors*/) = delete;
  /* implicit */ Result(ErrorCode error_number);

  Result(ErrorCode error_number, std::string&& error_message);
  Result(ErrorCode error_number, std::string_view error_message);
  Result(ErrorCode error_number, const char* error_message);

  template<typename... Args>
  Result(ErrorCode error_number, Args&&... args)
    : Result{error_number, absl::StrCat(std::forward<Args>(args)...)} {}

  Result(const Result& other) = delete;
  Result(Result&& other) noexcept = default;

  Result& operator=(const Result& other) = delete;
  Result& operator=(Result&& other) noexcept = default;

  Result clone() const;

  bool ok() const noexcept { return _error == nullptr; }

  bool fail() const noexcept { return _error != nullptr; }

  ErrorCode errorNumber() const noexcept;

  bool is(ErrorCode error_number) const noexcept {
    return errorNumber() == error_number;
  }

  bool isNot(ErrorCode error_number) const noexcept {
    return !is(error_number);
  }

  void reset() noexcept { _error.reset(); }

  void reset(ErrorCode error_number);
  void reset(ErrorCode error_number, std::string&& error_message);
  void reset(ErrorCode error_number, std::string_view error_message);
  void reset(ErrorCode error_number, const char* error_message) {
    return reset(error_number, absl::NullSafeStringView(error_message));
  }

  std::string_view errorMessage() const& noexcept;
  std::string errorMessage() && noexcept;

  template<typename F>
    requires std::is_invocable_r_v<void, F, result::Error&>
  void withError(F&& f) {
    if (_error) {
      std::forward<F>(f)(*_error);
    }
  }

  bool operator==(const Result& other) const;

  template<typename Sink>
  friend void AbslStringify(Sink& sink, const Result& value) {
    absl::Format(&sink, "Result(%d, %s)", value.errorNumber().value(),
                 value.errorMessage());
  }

 private:
  std::unique_ptr<result::Error> _error;
};

}  // namespace sdb
