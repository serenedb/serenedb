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

/// @file userver/utils/zstring_view.hpp
/// @brief @copybrief utils::cstring_view
/// @ingroup userver_universal

#include <fmt/format.h>

#include <string>
#include <string_view>
#include <type_traits>

#include "basics/assert.h"

namespace sdb::containers {

/// @ingroup userver_containers
///
/// @brief Non-empty string view type that guarantees null-termination and has a
/// `c_str()` member function.
class cstring_view
  : public std::string_view {  // NOLINT(readability-identifier-naming)
 public:
  cstring_view() = delete;
  cstring_view(const cstring_view& str) = default;

  constexpr cstring_view(const char* str) noexcept : std::string_view{str} {
    // data()[size()] == '\0' is guaranteed by std::string_view that calls
    // std::strlen(str)
  }

  cstring_view(const std::string& str) noexcept : std::string_view{str} {}

  cstring_view& operator=(std::string_view) = delete;
  cstring_view& operator=(const cstring_view&) = default;

  void remove_suffix(std::size_t) =
    delete;  // cstring_view becomes not null-terminated after that function
             // call
  void swap(std::string_view&) =
    delete;  // cstring_view may become not null-terminated after that function
             // call
  void swap(cstring_view& other) noexcept { std::string_view::swap(other); }

  constexpr const char* c_str() const noexcept {
    return std::string_view::data();
  }

  /// Constructs a cstring_view from a pointer and size.
  /// @warning `str[len]` should be '\0'.
  static constexpr cstring_view UnsafeMake(const char* str,
                                           std::size_t len) noexcept {
    return cstring_view{str, len};
  }

 private:
  constexpr cstring_view(const char* str, std::size_t len) noexcept
    : std::string_view{str, len} {
#ifndef NDEBUG
    SDB_VERIFY(str && str[len] == '\0',
               "`str` should be not null and should be null terminated");
#endif
  }
};

}  // namespace sdb::containers

template<>
struct fmt::formatter<sdb::containers::cstring_view, char>
  : fmt::formatter<std::string_view> {};

namespace fmt {

// Allow fmt::runtime() to work with utils::cstring_view
template<class... NotUsed>
inline auto runtime(sdb::containers::cstring_view s,
                    NotUsed...)  // NOLINT(readability-identifier-naming)
  -> decltype(fmt::runtime(std::string_view{s})) {
  static_assert(sizeof...(NotUsed) == 0);
  return fmt::runtime(std::string_view{s});
}

}  // namespace fmt
