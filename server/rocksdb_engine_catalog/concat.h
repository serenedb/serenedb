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

#include "basics/identifier.h"
#include "catalog/identifiers/revision_id.h"
#include "vpack/slice.h"

namespace sdb::rocksutils {
namespace detail {

template<typename T>
constexpr size_t GetByteSizeImpl(T v) noexcept {
  if constexpr (std::is_same_v<T, std::string_view>) {
    return v.size();
  } else if constexpr (std::is_same_v<T, vpack::Slice>) {
    return v.byteSize();
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
  } else if constexpr (std::is_same_v<T, vpack::Slice>) {
    const auto size = v.byteSize();
    std::memcpy(p, v.start(), size);
    return size;
  } else if constexpr (std::is_base_of_v<basics::Identifier, T>) {
    // TODO(gnusi): unify based on purpose (key, value)
    if constexpr (std::is_same_v<T, RevisionId>) {
      absl::little_endian::Store64(p, v.id());
    } else {
      absl::big_endian::Store64(p, v.id());
    }
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

template<typename T>
const char* ReadImpl(const char* p, const char* end, T& v) noexcept {
  SDB_ASSERT(p < end);
  if constexpr (std::is_enum_v<T>) {
    static_assert(sizeof(T) == 1);
    v = static_cast<T>(*p++);
  } else if constexpr (std::is_same_v<T, vpack::Slice>) {
    v = vpack::Slice{reinterpret_cast<const uint8_t*>(p)};
    p += v.byteSize();
  } else if constexpr (std::is_base_of_v<basics::Identifier, T>) {
    // TODO(gnusi): unify based on purpose (key, value)
    if constexpr (std::is_same_v<T, RevisionId>) {
      v = T{absl::little_endian::Load64(p)};
    } else {
      v = T{absl::big_endian::Load64(p)};
    }
    p += sizeof(T);
  } else if constexpr (std::is_same_v<T, std::string_view>) {
    v = {p, end};
    p = end;
  } else {
    v = absl::big_endian::Load<T>(p);
    p += sizeof(T);
  }
  return p;
}

template<typename T>
struct IsValid {
  static constexpr size_t kCountStringViews = [] {
    size_t count = 0;
    boost::pfr::for_each_field(T{}, [&]<typename U>(const U& field) {
      if constexpr (std::is_same_v<U, std::string_view>) {
        count++;
      }
    });
    return count;
  }();

  static constexpr bool kValue =
    kCountStringViews == 0 ||
    std::is_same_v<
      boost::pfr::tuple_element_t<boost::pfr::tuple_size_v<T> - 1, T>,
      std::string_view>;
};

}  // namespace detail

template<typename... Args>
void Concat(std::string& str, const Args&... args) {
  // TODO(mbkkt) maybe amortized?
  basics::StrResize(str, (detail::GetByteSizeImpl(args) + ...));
  auto* p = str.data();
  ((p += detail::WriteImpl(p, args)), ...);
}

template<typename U>
size_t GetByteSize(const U& in) noexcept {
  size_t size = 0;
  boost::pfr::for_each_field(
    in, [&](const auto& v) { size += detail::GetByteSizeImpl(v); });
  return size;
}

template<typename U>
void Write(const U& in, char* p) {
  static_assert(detail::IsValid<U>::kValue);
  boost::pfr::for_each_field(
    in, [&](const auto& v) { p += detail::WriteImpl(p, v); });
}

template<typename U>
U Read(std::string_view in) {
  static_assert(detail::IsValid<U>::kValue);
  const auto* p = in.data();
  const auto* end = p + in.size();

  U out;
  boost::pfr::for_each_field(out,
                             [&](auto& v) { p = detail::ReadImpl(p, end, v); });
  return out;
}

}  // namespace sdb::rocksutils
