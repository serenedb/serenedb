////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "attribute_provider.hpp"

namespace irs {

template<typename T>
struct AttributePtr {
  T* ptr{};

  constexpr AttributePtr() noexcept = default;

  // Intentionally implicit
  constexpr AttributePtr(T& v) noexcept : ptr{&v} {}

  // Intentionally implicit
  constexpr AttributePtr(T* v) noexcept : ptr{v} {}

  // Intentionally implicit
  constexpr operator AttributePtr<Attribute>() const noexcept {
    return AttributePtr<Attribute>{ptr};
  }
};

template<typename T>
struct Type<AttributePtr<T>> : Type<T> {};

namespace detail {

template<size_t I, typename Tuple>
constexpr AttributePtr<Attribute> get_mutable_helper(
  Tuple& t, TypeInfo::type_id id) noexcept {
  if (Type<std::tuple_element_t<I, Tuple>>::id() == id) {
    return std::get<I>(t);
  }

  if constexpr (I + 1 < std::tuple_size_v<Tuple>) {
    return get_mutable_helper<I + 1>(t, id);
  } else {
    return {};
  }
}

}  // namespace detail

template<typename... T>
constexpr Attribute* GetMutable(std::tuple<T...>& t,
                                TypeInfo::type_id id) noexcept {
  return detail::get_mutable_helper<0>(t, id).ptr;
}

}  // namespace irs
