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

#include <array>
#include <memory>
#include <type_traits>

#include "basics/assert.h"
#include "basics/shared.hpp"

namespace irs {

// Convenient helper for simulating 'try/catch/finally' semantic
template<typename Func>
class [[nodiscard]] Finally {
 public:
  static_assert(std::is_nothrow_invocable_v<Func>);

  // If you need some of it, please use absl::Cleanup
  Finally(Finally&&) = delete;
  Finally(const Finally&) = delete;
  Finally& operator=(Finally&&) = delete;
  Finally& operator=(const Finally&) = delete;

  Finally(Func&& func) : _func{std::move(func)} {}

  ~Finally() noexcept { _func(); }

 private:
  [[no_unique_address]] Func _func;
};

// Convenient helper for caching function results
template<typename Input, Input Size, typename Func,
         typename = typename std::enable_if_t<std::is_integral_v<Input>>>
class CachedFunc {
 public:
  using input_type = Input;
  using output_type = std::invoke_result_t<Func, Input>;

  constexpr explicit CachedFunc(input_type offset, Func&& func)
    : _func{std::forward<Func>(func)} {
    for (; offset < Size; ++offset) {
      _cache[offset] = _func(offset);
    }
  }

  template<bool Checked>
  constexpr IRS_FORCE_INLINE output_type get(input_type value) const
    noexcept(std::is_nothrow_invocable_v<Func, Input>) {
    if constexpr (Checked) {
      return value < size() ? _cache[value] : _func(value);
    } else {
      SDB_ASSERT(value < _cache.size());
      return _cache[value];
    }
  }

  constexpr size_t size() const noexcept { return _cache.size(); }

 private:
  [[no_unique_address]] Func _func;
  std::array<output_type, Size> _cache{};
};

template<typename Input, size_t Size, typename Func>
constexpr CachedFunc<Input, Size, Func> CacheFunc(Input offset, Func&& func) {
  return CachedFunc<Input, Size, Func>{offset, std::forward<Func>(func)};
}

template<typename Func>
IRS_FORCE_INLINE auto ResolveBool(bool value, Func&& func) {
  if (value) {
    return std::forward<Func>(func).template operator()<true>();
  } else {
    return std::forward<Func>(func).template operator()<false>();
  }
}

}  // namespace irs
