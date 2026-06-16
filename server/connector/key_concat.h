////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <boost/pfr.hpp>
#include <boost/pfr/core.hpp>
#include <boost/pfr/detail/core17_generated.hpp>
#include <boost/pfr/tuple_size.hpp>
#include <tuple>
#include <type_traits>

#include "basics/assert.h"
#include "basics/identifier.h"
#include "basics/string_utils.h"

namespace sdb::keyenc {
namespace detail {

template<typename T>
constexpr size_t GetByteSizeImpl(T v) noexcept {
  if constexpr (std::is_same_v<T, std::string_view>) {
    return v.size();
  } else if constexpr (std::is_integral_v<T> || std::is_enum_v<T> ||
                       std::is_base_of_v<basics::Identifier, T>) {
    return sizeof(T);
  } else {
    static_assert(false);
  }
}

template<typename T>
size_t WriteImpl(char* p, const T& v) noexcept {
  if constexpr (std::is_enum_v<T>) {
    static_assert(sizeof(T) == 1);
    *p = std::to_underlying(v);
    return sizeof(T);
  } else if constexpr (std::is_base_of_v<basics::Identifier, T>) {
    absl::big_endian::Store64(p, v.id());
    return sizeof(T);
  } else if constexpr (std::is_same_v<T, std::string_view>) {
    // TODO(gnusi): std::string_view must be last
    std::memcpy(p, v.data(), v.size());
    return v.size();
  } else {
    absl::big_endian::Store(p, v);
    return sizeof(T);
  }
}

}  // namespace detail

template<typename... Args>
void Concat(std::string& str, const Args&... args) {
  // TODO(mbkkt) maybe amortized?
  basics::StrResize(str, (detail::GetByteSizeImpl(args) + ...));
  auto* p = str.data();
  ((p += detail::WriteImpl(p, args)), ...);
}

template<typename... Args>
void Append(std::string& str, const Args&... args) {
  // TODO(mbkkt) maybe amortized?
  const auto old_size = str.size();
  basics::StrResize(str, (detail::GetByteSizeImpl(args) + ...) + old_size);
  auto* p = str.data() + old_size;
  ((p += detail::WriteImpl(p, args)), ...);
}

}  // namespace sdb::keyenc
