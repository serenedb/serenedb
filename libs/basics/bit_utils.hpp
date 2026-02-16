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

#include <cstddef>
#include <utility>

#include "basics/shared.hpp"

namespace irs {

template<typename T>
consteval uint32_t BitsRequired() noexcept {
  return sizeof(T) * 8U;
}

template<typename T>
IRS_FORCE_INLINE constexpr size_t BitsRequired(size_t n) noexcept {
  return BitsRequired<T>() * n;
}

template<typename T>
IRS_FORCE_INLINE constexpr T PopBit(T v) noexcept {
  SDB_ASSERT(v);
  return v & (v - 1);
}

template<typename T>
IRS_FORCE_INLINE constexpr void SetBit(T& value, size_t bit) noexcept {
  static_assert(std::is_unsigned_v<T>);
  value |= (T(1) << bit);
}

template<typename T>
IRS_FORCE_INLINE constexpr void UnsetBit(T& value, size_t bit) noexcept {
  static_assert(std::is_unsigned_v<T>);
  value &= ~(T(1) << bit);
}

template<typename T>
IRS_FORCE_INLINE constexpr void SetBit(T& value, size_t bit,
                                       bool set) noexcept {
  set ? SetBit(value, bit) : UnsetBit(value, bit);
}

template<typename T>
IRS_FORCE_INLINE constexpr void UnsetBit(T& value, size_t bit,
                                         bool unset) noexcept {
  if (unset) {
    UnsetBit(value, bit);
  }
}

template<typename T>
IRS_FORCE_INLINE constexpr bool CheckBit(T value, size_t bit) noexcept {
  static_assert(std::is_unsigned_v<T>);
  return (value & (T(1) << bit)) != 0;
}

template<unsigned Offset, typename T>
IRS_FORCE_INLINE constexpr T Rol(T value) noexcept {
  static_assert(Offset >= 0 && Offset <= sizeof(T) * 8, "Offset out of range");
  return (value << Offset) | (value >> (sizeof(T) * 8 - Offset));
}

template<unsigned Offset, typename T>
IRS_FORCE_INLINE constexpr T Ror(T value) noexcept {
  static_assert(Offset >= 0 && Offset <= sizeof(T) * 8, "Offset out of range");
  return (value >> Offset) | (value << (sizeof(T) * 8 - Offset));
}

// static_assert that signed right shift works as expected
static_assert(static_cast<uint32_t>(INT32_C(-1) >> 31) == UINT32_C(0xFFFFFFFF));
static_assert(static_cast<uint64_t>(INT64_C(-1) >> 63) ==
              UINT64_C(0xFFFFFFFFFFFFFFFF));

template<typename T>
struct EnumBitwiseTraits {
  static_assert(std::is_enum_v<T>);

  static constexpr T Or(T lhs, T rhs) noexcept {
    return static_cast<T>(std::to_underlying(lhs) | std::to_underlying(rhs));
  }

  static constexpr T Xor(T lhs, T rhs) noexcept {
    return static_cast<T>(std::to_underlying(lhs) ^ std::to_underlying(rhs));
  }

  static constexpr T And(T lhs, T rhs) noexcept {
    return static_cast<T>(std::to_underlying(lhs) & std::to_underlying(rhs));
  }

  static constexpr T Not(T v) noexcept {
    return static_cast<T>(~std::to_underlying(v));
  }
};

template<typename T>
inline constexpr T EnumBitwiseOr(T lhs, T rhs) noexcept {
  return EnumBitwiseTraits<T>::Or(lhs, rhs);
}

template<typename T>
inline constexpr T EnumBitwiseXor(T lhs, T rhs) noexcept {
  return EnumBitwiseTraits<T>::Xor(lhs, rhs);
}

template<typename T>
inline constexpr T EnumBitwiseAnd(T lhs, T rhs) noexcept {
  return EnumBitwiseTraits<T>::And(lhs, rhs);
}

template<typename T>
inline constexpr T EnumBitwiseNot(T v) noexcept {
  return EnumBitwiseTraits<T>::Not(v);
}

}  // namespace irs

// TODO(mbkkt) PLEASE DON'T USE IT!
//  For an example, you have enum with 1 and 2:
//  you make OR, it produces 3 which is not part of enum!
// TODO(mbkkt) Remove all usage, then remove it

#define ENABLE_BITMASK_ENUM(x)                                              \
  [[maybe_unused]] inline constexpr x operator&(x lhs, x rhs) noexcept {    \
    return irs::EnumBitwiseAnd(lhs, rhs);                                   \
  }                                                                         \
  [[maybe_unused]] inline constexpr x& operator&=(x& lhs, x rhs) noexcept { \
    return lhs = irs::EnumBitwiseAnd(lhs, rhs);                             \
  }                                                                         \
  [[maybe_unused]] inline constexpr x operator|(x lhs, x rhs) noexcept {    \
    return irs::EnumBitwiseOr(lhs, rhs);                                    \
  }                                                                         \
  [[maybe_unused]] inline constexpr x& operator|=(x& lhs, x rhs) noexcept { \
    return lhs = irs::EnumBitwiseOr(lhs, rhs);                              \
  }                                                                         \
  [[maybe_unused]] inline constexpr x operator^(x lhs, x rhs) noexcept {    \
    return irs::EnumBitwiseXor(lhs, rhs);                                   \
  }                                                                         \
  [[maybe_unused]] inline constexpr x& operator^=(x& lhs, x rhs) noexcept { \
    return lhs = irs::EnumBitwiseXor(lhs, rhs);                             \
  }                                                                         \
  [[maybe_unused]] inline constexpr x operator~(x v) noexcept {             \
    return irs::EnumBitwiseNot(v);                                          \
  }
