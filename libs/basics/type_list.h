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

#include <cstddef>

#include "basics/type_utils.hpp"

namespace sdb::type {

template<typename... T>
struct List {
  static_assert(kIsSimple<T...>);
  static_assert(kIsUnique<T...>);

  static consteval size_t size() noexcept { return sizeof...(T); }

  template<typename Visitor>
  static constexpr void visit(Visitor&& visitor) {
    Visit<T...>(std::forward<Visitor>(visitor));
  }

  template<typename U>
  static consteval bool contains() noexcept {
    static_assert(kIsSimple<U>);
    return kContains<U, T...>;
  }

  template<typename U>
  static consteval size_t id() noexcept {
    static_assert(contains<U>());
    return kIndex<U, T...>;
  }
};

}  // namespace sdb::type
