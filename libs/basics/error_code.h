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

#include <iosfwd>
#include <ostream>
#include <string>

namespace vpack {

class Value;
}

// TODO We probably want to put this into a namespace, but this is easy to
//      refactor automatically later.

class [[nodiscard]] ErrorCode {
 public:
  using ValueType = int;

  constexpr ErrorCode() = default;
  constexpr explicit ErrorCode(ValueType value) : _value{value} {}
  constexpr ErrorCode(const ErrorCode&) noexcept = default;
  constexpr ErrorCode(ErrorCode&&) noexcept = default;
  constexpr ErrorCode& operator=(const ErrorCode&) noexcept = default;
  constexpr ErrorCode& operator=(ErrorCode&&) noexcept = default;

  constexpr operator ValueType() const noexcept { return _value; }

  constexpr ValueType value() const noexcept { return _value; }

  constexpr bool operator==(const ErrorCode& other) const noexcept = default;

  template<typename H>
  friend H AbslHashValue(H h, ErrorCode code) {
    return H::combine(std::move(h), code._value);
  }

  template<typename Sink>
  friend void AbslStringify(Sink& sink, ErrorCode value) {
    sink.Append(absl::StrCat(value.value()));
  }

 private:
  ValueType _value{};
};

void VPackRead(auto ctx, ErrorCode& code) {
  code = ErrorCode{static_cast<int>(ctx.vpack().getInt())};
}

void VPackWrite(auto ctx, ErrorCode code) { ctx.vpack().add(code.value()); }
