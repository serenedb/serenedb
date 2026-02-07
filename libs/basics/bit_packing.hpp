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

#include <cmath>
#include <iterator>
#include <limits>

#include "basics/math_utils.hpp"

namespace irs::packed {

constexpr uint32_t kBlockSize32 =
  sizeof(uint32_t) * 8;  // block size is tied to number of bits in value
constexpr uint32_t kBlockSize64 =
  sizeof(uint64_t) * 8;  // block size is tied to number of bits in value

IRS_FORCE_INLINE constexpr uint32_t Maxbits64(uint64_t val) noexcept {
  return math::MathTraits<uint64_t>::bits_required(val);
}

inline uint32_t Maxbits64(const uint64_t* begin, const uint64_t* end) noexcept {
  uint64_t accum = 0;

  while (begin != end) {
    accum |= *begin++;
  }

  return Maxbits64(accum);
}

IRS_FORCE_INLINE constexpr uint32_t Maxbits32(uint32_t val) noexcept {
  return math::MathTraits<uint32_t>::bits_required(val);
}

inline uint32_t Maxbits32(const uint32_t* begin, const uint32_t* end) noexcept {
  uint32_t accum = 0;

  while (begin != end) {
    accum |= *begin++;
  }

  return Maxbits32(accum);
}

IRS_FORCE_INLINE constexpr uint32_t BytesRequired32(uint32_t count,
                                                    uint32_t bits) noexcept {
  return math::DivCeil32(count * bits, 8);
}

IRS_FORCE_INLINE constexpr uint64_t BytesRequired64(uint64_t count,
                                                    uint64_t bits) noexcept {
  return math::DivCeil64(count * bits, 8);
}

IRS_FORCE_INLINE constexpr uint32_t BlocksRequired32(uint32_t count,
                                                     uint32_t bits) noexcept {
  return math::DivCeil32(count * bits, 8 * sizeof(uint32_t));
}

IRS_FORCE_INLINE constexpr uint64_t BlocksRequired64(uint64_t count,
                                                     uint64_t bits) noexcept {
  return math::DivCeil64(count * bits, 8 * sizeof(uint64_t));
}

//////////////////////////////////////////////////////////////////////////////
/// @brief returns number of elements required to store unpacked data
//////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE constexpr uint64_t ItemsRequired(uint32_t count) noexcept {
  return kBlockSize32 * math::DivCeil32(count, kBlockSize32);
}

IRS_FORCE_INLINE constexpr uint64_t IterationsRequired(
  uint32_t count) noexcept {
  return ItemsRequired(count) / kBlockSize32;
}

template<typename T>
inline T MaxValue(uint32_t bits) noexcept {
  SDB_ASSERT(bits >= 0U && bits <= sizeof(T) * 8U);

  return bits == sizeof(T) * 8U ? (std::numeric_limits<T>::max)()
                                : ~(~T(0) << bits);
}

void PackBlock(const uint32_t* IRS_RESTRICT first, uint32_t* IRS_RESTRICT out,
               const uint32_t bit) noexcept;

void PackBlock(const uint64_t* IRS_RESTRICT first, uint64_t* IRS_RESTRICT out,
               const uint32_t bit) noexcept;

void UnpackBlock(const uint32_t* IRS_RESTRICT in, uint32_t* IRS_RESTRICT out,
                 const uint32_t bit) noexcept;

void UnpackBlock(const uint64_t* IRS_RESTRICT in, uint64_t* IRS_RESTRICT out,
                 const uint32_t bit) noexcept;

uint32_t FastpackAt(const uint32_t* encoded, const size_t i,
                    const uint32_t bit) noexcept;

uint64_t FastpackAt(const uint64_t* encoded, const size_t i,
                    const uint32_t bit) noexcept;

inline uint32_t At(const uint32_t* encoded, const size_t i,
                   const uint32_t bit) noexcept {
  return FastpackAt(encoded + bit * (i / kBlockSize32), i % kBlockSize32, bit);
}

inline uint64_t At(const uint64_t* encoded, const size_t i,
                   const uint32_t bit) noexcept {
  return FastpackAt(encoded + bit * (i / kBlockSize64), i % kBlockSize64, bit);
}

inline uint32_t* Pack(const uint32_t* first, const uint32_t* last,
                      uint32_t* out, const uint32_t bit) noexcept {
  SDB_ASSERT(0 == (last - first) % kBlockSize32);

  for (; first < last; first += kBlockSize32, out += bit) {
    PackBlock(first, out, bit);
  }
  return out;
}

inline uint64_t* Pack(const uint64_t* first, const uint64_t* last,
                      uint64_t* out, const uint32_t bit) noexcept {
  SDB_ASSERT(0 == (last - first) % kBlockSize64);

  for (; first < last; first += kBlockSize64, out += bit) {
    PackBlock(first, out, bit);
  }
  return out;
}

inline void Unpack(uint32_t* first, uint32_t* last, const uint32_t* in,
                   const uint32_t bit) noexcept {
  for (; first < last; first += kBlockSize32, in += bit) {
    UnpackBlock(in, first, bit);
  }
}

inline void Unpack(uint64_t* first, uint64_t* last, const uint64_t* in,
                   const uint32_t bit) noexcept {
  for (; first < last; first += kBlockSize64, in += bit) {
    UnpackBlock(in, first, bit);
  }
}

template<typename T>
class Iterator {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = T;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = ptrdiff_t;
  using const_pointer = const value_type*;

  Iterator(const_pointer packed, uint32_t bits, size_t i = 0) noexcept
    : _packed(packed), _i(i), _bits(bits) {
    SDB_ASSERT(_packed);
    SDB_ASSERT(_bits > 0 && bits <= sizeof(value_type) * 8);
  }

  Iterator(const Iterator&) = default;
  Iterator& operator=(const Iterator&) = default;

  Iterator& operator++() noexcept {
    ++_i;
    return *this;
  }

  Iterator operator++(int) noexcept {
    const auto tmp = *this;
    ++*this;
    return tmp;
  }

  Iterator& operator+=(difference_type v) noexcept {
    _i += v;
    return *this;
  }

  Iterator operator+(difference_type v) const noexcept {
    return Iterator(_packed, _bits, _i + v);
  }

  Iterator& operator--() noexcept {
    --_i;
    return *this;
  }

  Iterator operator--(int) noexcept {
    const auto tmp = *this;
    --*this;
    return tmp;
  }

  Iterator& operator-=(difference_type v) noexcept {
    _i -= v;
    return *this;
  }

  Iterator operator-(difference_type v) const noexcept {
    return Iterator(_packed, _bits, _i - v);
  }

  difference_type operator-(const Iterator& rhs) const noexcept {
    SDB_ASSERT(_packed == rhs._packed);  // compatibility
    return _i - rhs._i;
  }

  value_type operator*() const noexcept { return At(_packed, _i, _bits); }

  bool operator==(const Iterator& rhs) const noexcept {
    SDB_ASSERT(_packed == rhs._packed);  // compatibility
    return _i == rhs._i;
  }

  bool operator<(const Iterator& rhs) const noexcept {
    SDB_ASSERT(_packed == rhs._packed);  // compatibility
    return _i < rhs._i;
  }

  bool operator>=(const Iterator& rhs) const noexcept { return !(*this < rhs); }

  bool operator>(const Iterator& rhs) const noexcept {
    SDB_ASSERT(_packed == rhs._packed);  // compatibility
    return _i > rhs._i;
  }

  bool operator<=(const Iterator& rhs) const noexcept { return !(*this > rhs); }

 private:
  const_pointer _packed;
  size_t _i;
  uint32_t _bits;
};

typedef Iterator<uint32_t> iterator32;
typedef Iterator<uint64_t> iterator64;

}  // namespace irs::packed
