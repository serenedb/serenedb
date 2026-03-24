////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 YANDEX LLC
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

/// @file userver/utils/string_literal.hpp
/// @brief @copybrief utils::StringLiteral
/// @ingroup userver_universal

#include <fmt/core.h>

#include <string>
#include <string_view>
#include <type_traits>

#include "basics/containers/cstring_view.h"

namespace sdb::containers {

/// @ingroup userver_containers
///
/// @brief Non-empty string view to a compile time known null terminated char
/// array that lives for the lifetime of program, as per [lex.string]; a drop-in
/// replacement for `static const std::string kVar = "value"` and `constexpr
/// std::string_view kVar = "value"`.
class StringLiteral : public cstring_view {
 public:
  StringLiteral() = delete;

  consteval StringLiteral(const char* literal) noexcept
    : cstring_view{literal} {
    // data()[size()] == '\0' is guaranteed by std::string_view that calls
    // std::strlen(literal)
  }

  void swap(cstring_view&) = delete;  // loses guarantee on lifetime because
                                      // cstring_view may refer to non-literal
  void swap(StringLiteral& other) noexcept { cstring_view::swap(other); }

  /// Constructs a StringLiteral from a pointer and size.
  /// @warning `str[len]` should be '\0' and `str` should point to compile time
  /// literal.
  static constexpr StringLiteral UnsafeMake(const char* str,
                                            std::size_t len) noexcept {
    return StringLiteral(str, len);
  }

 private:
  explicit constexpr StringLiteral(const char* str, std::size_t len) noexcept
    : cstring_view{cstring_view::UnsafeMake(str, len)} {}
};

}  // namespace sdb::containers

template<>
struct fmt::formatter<sdb::containers::StringLiteral, char>
  : fmt::formatter<std::string_view> {};
