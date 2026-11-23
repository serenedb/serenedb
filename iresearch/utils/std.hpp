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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <iterator>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace irstd {

template<typename T, template<typename...> typename Template>
inline constexpr bool kIsSpecialization = false;

template<template<typename...> typename Template, typename... Args>
inline constexpr bool kIsSpecialization<Template<Args...>, Template> = true;

template<typename Container, typename Iterator>
void SwapRemove(Container& cont, Iterator it) {
  SDB_ASSERT(!cont.empty());
  std::swap(*it, cont.back());
  cont.pop_back();
}

namespace detail {

template<typename Builder, size_t Size>
struct Initializer {
  using type = typename Builder::Type;

  static constexpr auto kIdx = Size - 1;

  template<typename Array>
  constexpr explicit Initializer(Array& cache) : init(cache) {
    cache[kIdx] = []() -> const type& {
      static const typename Builder::Type kInstance = Builder::Make(kIdx);
      return kInstance;
    };
  }

  Initializer<Builder, Size - 1> init;
};

template<typename Builder>
struct Initializer<Builder, 1> {
  using type = typename Builder::Type;

  static constexpr auto kIdx = 0;

  template<typename Array>
  constexpr explicit Initializer(Array& cache) {
    cache[kIdx] = []() -> const type& {
      static const typename Builder::Type kInstance = Builder::Make(kIdx);
      return kInstance;
    };
  }
};

template<typename Builder>
struct Initializer<Builder, 0>;

}  // namespace detail

template<typename Builder, size_t Size>
class StaticLazyArray {
 public:
  using type = typename Builder::Type;

  static const type& at(size_t i) {
    static const StaticLazyArray kInstance;
    return kInstance._cache[std::min(i, Size)]();
  }

 private:
  constexpr StaticLazyArray() : _init{_cache} {
    _cache[Size] = []() -> const type& {
      static const type kInstance;
      return kInstance;
    };
  }

  using func = const type& (*)();

  func _cache[Size + 1];
  detail::Initializer<Builder, Size> _init;
};

}  // namespace irstd
}  // namespace irs
