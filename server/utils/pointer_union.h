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
#include <cstdint>
#include <iresearch/utils/assert.hpp>
#include <type_traits>

namespace sdb {

template<typename... Ts>
class PointerUnion {
  static_assert(sizeof(uintptr_t) == 8);
  static_assert(sizeof...(Ts) > 0 && sizeof...(Ts) < 256);
  static_assert((std::is_same_v<Ts, std::remove_cv_t<Ts>> && ...));

  static constexpr int kTagShift = 48;
  static constexpr uintptr_t kTagMask = uintptr_t{0xFF} << kTagShift;

  template<typename T>
  static constexpr size_t kCount =
    (size_t{std::is_same_v<std::remove_cv_t<T>, Ts>} + ...);

  static_assert(((kCount<Ts> == 1) && ...));

  template<typename T>
  static constexpr uintptr_t IndexOf() {
    uintptr_t index = 0;
    uintptr_t position = 0;
    ((++position,
      index = std::is_same_v<std::remove_cv_t<T>, Ts> ? position : index),
     ...);
    return index;
  }

  template<typename T>
  static constexpr uintptr_t kTag = IndexOf<T>() << kTagShift;

 public:
  template<typename T>
  static constexpr bool kHolds = kCount<T> == 1;

  PointerUnion() = default;

  template<typename T>
  void Set(T* pointer) noexcept {
    static_assert(kHolds<T>);
    const auto raw = reinterpret_cast<uintptr_t>(pointer);
    SDB_ASSERT((raw & kTagMask) == 0);
    _value = pointer == nullptr ? 0 : raw | kTag<T>;
  }

  void Reset() noexcept { _value = 0; }

  bool IsNull() const noexcept { return _value == 0; }

  template<typename T>
  bool Is() const noexcept {
    static_assert(kHolds<T>);
    return (_value & kTagMask) == kTag<T>;
  }

  template<typename T>
  T* Get() const noexcept {
    return Is<T>() ? reinterpret_cast<T*>(_value & ~kTagMask) : nullptr;
  }

 private:
  uintptr_t _value = 0;
};

}  // namespace sdb
