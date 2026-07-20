////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <type_traits>
#include <vector>

#include "basics/shared.hpp"
#include "basics/std.hpp"

namespace irs {

/// @brief compile-time type identifier
template<typename T>
constexpr std::string_view ctti() noexcept {
  return {IRS_FUNC_NAME};
}

///////////////////////////////////////////////////////////////////////////////
/// @returns if 'T' is 'std::shared_ptr<T>' provides the member constant value
///          equal to 'true', or 'false' otherwise
///////////////////////////////////////////////////////////////////////////////
template<typename T>
struct IsSharedPtr : std::false_type {};

template<typename T>
struct IsSharedPtr<std::shared_ptr<T>> : std::true_type {};

template<typename T>
inline constexpr bool kIsSharedPtr = IsSharedPtr<T>::value;

///////////////////////////////////////////////////////////////////////////////
/// @returns if 'T' is 'std::unique_ptr<T, D>' provides the member constant
/// value
///          equal to 'true', or 'false' otherwise
///////////////////////////////////////////////////////////////////////////////
template<typename T>
struct IsUniquePtr : std::false_type {};

template<typename T, typename D>
struct IsUniquePtr<std::unique_ptr<T, D>> : std::true_type {};

template<typename T, typename D>
struct IsUniquePtr<std::unique_ptr<T[], D>> : std::true_type {};

template<typename T>
inline constexpr bool kIsUniquePtr = IsUniquePtr<T>::value;

///////////////////////////////////////////////////////////////////////////////
/// @returns if 'T' is 'std::vector<T, A>' provides the member constant
/// value equal to 'true', or 'false' otherwise
///////////////////////////////////////////////////////////////////////////////
template<typename T>
struct IsVector : std::false_type {};

template<typename T, typename A>
struct IsVector<std::vector<T, A>> : std::true_type {};

template<typename T>
inline constexpr bool kIsVector = IsVector<T>::value;

}  // namespace irs
namespace sdb::type {

template<typename T>
struct Tag {
  using type = T;
};

template<typename U, typename... T>
inline constexpr bool kIsOneOf = (std::is_same_v<U, T> || ...);

template<typename...>
inline constexpr bool kIsUnique = true;

template<typename H, typename... T>
inline constexpr bool kIsUnique<H, T...> =
  kIsUnique<T...> && !kIsOneOf<H, T...>;

template<typename...>
inline constexpr size_t kIndex = 0;

template<typename U, typename H, typename... T>
inline constexpr size_t kIndex<U, H, T...> =
  std::is_same_v<U, H> ? 0 : 1 + kIndex<U, T...>;

template<typename T, typename... List>
inline constexpr bool kContains = (std::is_same_v<T, List> || ...);

template<typename... T>
inline constexpr bool kIsSimple = (std::is_same_v<T, std::decay_t<T>> && ...);

template<typename... T, typename Visitor>
constexpr void Visit(const Visitor& visitor) {
  (visitor(Tag<T>{}), ...);
}

}  // namespace sdb::type
