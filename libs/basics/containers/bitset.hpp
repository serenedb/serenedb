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

#include <cstring>
#include <memory>

#include "basics/bit_utils.hpp"
#include "basics/math_utils.hpp"
#include "basics/memory.hpp"
#include "basics/shared.hpp"

namespace irs {

template<typename Alloc>
class DynamicBitset {
 public:
  // NOLINTBEGIN
  using index_t = size_t;
  using word_t = size_t;
  using allocator_type =
    typename std::allocator_traits<Alloc>::template rebind_alloc<word_t>;
  using word_ptr_deleter_t = memory::AllocatorArrayDeallocator<allocator_type>;
  using word_ptr_t = std::unique_ptr<word_t[], word_ptr_deleter_t>;
  // NOLINTEND

  constexpr IRS_FORCE_INLINE static size_t bits_to_words(size_t bits) noexcept {
    return (bits + BitsRequired<word_t>() - 1) / BitsRequired<word_t>();
  }

  // returns corresponding bit index within a word for the
  // specified offset in bits
  constexpr IRS_FORCE_INLINE static size_t bit(size_t i) noexcept {
    return i % BitsRequired<word_t>();
  }

  // returns corresponding word index specified offset in bits
  constexpr IRS_FORCE_INLINE static size_t word(size_t i) noexcept {
    return i / BitsRequired<word_t>();
  }

  // returns corresponding offset in bits for the specified word index
  constexpr IRS_FORCE_INLINE static size_t bit_offset(size_t i) noexcept {
    return i * BitsRequired<word_t>();
  }

  DynamicBitset(const Alloc& alloc = Alloc{})
    : _data{nullptr, word_ptr_deleter_t{alloc, 0}} {}

  explicit DynamicBitset(size_t bits, const Alloc& alloc = Alloc{})
    : DynamicBitset{alloc} {
    reset(bits);
  }

  DynamicBitset(DynamicBitset&& rhs) noexcept
    : _data{std::move(rhs._data)},
      _words{std::exchange(rhs._words, 0)},
      _bits{std::exchange(rhs._bits, 0)} {}

  DynamicBitset(const DynamicBitset& rhs)
    : _data{nullptr, word_ptr_deleter_t{rhs._data.get_deleter().alloc(), 0}},
      _words{rhs._words},
      _bits{rhs._bits} {
    reset(rhs._bits);
    SDB_ASSERT((data() == nullptr) == (rhs.data() == nullptr));
    if (rhs.data()) {
      std::memcpy(_data.get(), rhs.data(), _words * sizeof(word_t));
    }
  }

  DynamicBitset& operator=(DynamicBitset rhs) noexcept {
    if (this != &rhs) {
      _data = std::move(rhs._data);
      _words = std::exchange(rhs._words, 0);
      _bits = std::exchange(rhs._bits, 0);
    }

    return *this;
  }

  void reserve(size_t bits) { reserve_words(bits_to_words(bits)); }

  template<bool Reserve = true>
  void resize(size_t bits) noexcept(!Reserve) {
    const auto new_words = bits_to_words(bits);
    if constexpr (Reserve) {
      reserve_words(new_words);
    } else {
      SDB_ASSERT(bits <= capacity());
      SDB_ASSERT(new_words <= capacity_words());
    }
    const auto old_words = _words;
    _words = new_words;
    clear_offset(old_words);
    _bits = bits;
    sanitize();
  }

  void reset(size_t bits) {
    const auto new_words = bits_to_words(bits);

    if (new_words > capacity_words()) {
      _data = memory::AllocateUnique<word_t[]>(
        _data.get_deleter().alloc(), new_words, memory::kAllocateOnly);
    }

    _words = new_words;
    _bits = bits;
    clear();
  }

  bool operator==(const DynamicBitset& rhs) const noexcept {
    if (_bits != rhs._bits) {
      return false;
    }
    SDB_ASSERT(_words == rhs._words);
    return 0 == std::memcmp(data(), rhs.data(), _words * sizeof(word_t));
  }

  // number of bits in bitset
  size_t size() const noexcept { return _bits; }

  // capacity in bits
  size_t capacity() const noexcept {
    return BitsRequired<word_t>() * capacity_words();
  }

  size_t words() const noexcept { return _words; }

  const word_t* data() const noexcept { return _data.get(); }

  const word_t* begin() const noexcept { return data(); }
  const word_t* end() const noexcept { return data() + _words; }

  word_t operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _words);
    return _data[i];
  }

  template<typename T>
  void memset(const T& value) noexcept {
    memset(&value, sizeof(value));
  }

  void memset(const void* src, size_t byte_size) noexcept {
    byte_size = std::min(byte_size, _words * sizeof(word_t));
    if (byte_size != 0) {
      // passing nullptr to `std::memcpy` is undefined behavior
      SDB_ASSERT(_data != nullptr);
      SDB_ASSERT(src != nullptr);
      std::memcpy(_data.get(), src, byte_size);
      sanitize();
    }
  }

  void set(size_t i) noexcept {
    SDB_ASSERT(i < _bits);
    SetBit(_data[word(i)], bit(i));
  }

  void unset(size_t i) noexcept {
    SDB_ASSERT(i < _bits);
    UnsetBit(_data[word(i)], bit(i));
  }

  void reset(size_t i, bool set) noexcept {
    SDB_ASSERT(i < _bits);
    SetBit(_data[word(i)], bit(i), set);
  }

  bool test(size_t i) const noexcept {
    SDB_ASSERT(i < _bits);
    return CheckBit(_data[word(i)], bit(i));
  }

  bool try_set(size_t i) noexcept {
    SDB_ASSERT(i < _bits);
    auto& data = _data[word(i)];
    const auto mask = word_t{1} << bit(i);
    if ((data & mask) != 0) {
      return false;
    }
    data |= mask;
    return true;
  }

  bool any() const noexcept {
    return std::any_of(begin(), end(), [](word_t w) { return w != 0; });
  }

  bool none() const noexcept { return !any(); }

  bool all() const noexcept {
    const auto* begin = _data.get();
    const auto body = _bits / BitsRequired<word_t>();
    const auto tail = static_cast<int>(_bits % BitsRequired<word_t>());
    for (const auto* end = begin + body; begin != end; ++begin) {
      static_assert(std::is_unsigned_v<word_t>);
      if (*begin != std::numeric_limits<word_t>::max()) {
        return false;
      }
    }
    return tail == 0 || tail == std::popcount(*begin);
  }

  void clear() noexcept { clear_offset(0); }

  // counts bits set
  size_t count() const noexcept { return math::Popcount(begin(), end()); }

  Alloc& get_allocator() { return _data.get_deleter().alloc(); }

 private:
  size_t capacity_words() const noexcept { return _data.get_deleter().size(); }

  void reserve_words(size_t words) {
    if (words > capacity_words()) {
      auto new_data = memory::AllocateUnique<word_t[]>(
        _data.get_deleter().alloc(), words, memory::kAllocateOnly);
      SDB_ASSERT(new_data != nullptr);
      if (_words != 0) {
        SDB_ASSERT(data() != nullptr);
        std::memcpy(new_data.get(), data(), _words * sizeof(word_t));
      }
      _data = std::move(new_data);
    }
  }

  void clear_offset(size_t offset_words) noexcept {
    if (offset_words < _words) {
      SDB_ASSERT(_data != nullptr);
      std::memset(_data.get() + offset_words, 0,
                  (_words - offset_words) * sizeof(word_t));
    }
  }

  void sanitize() noexcept {
    SDB_ASSERT(_bits <= capacity());
    SDB_ASSERT(_words <= capacity_words());
    const auto last_word_bits = bit(_bits);

    if (last_word_bits == 0) {
      return;  // no words or last word has all bits set
    }

    const auto mask = ~(~word_t{0} << last_word_bits);

    _data[_words - 1] &= mask;
  }

  word_ptr_t _data;  // words array: pointer, allocator, capacity in words
  size_t _words{0};  // size of bitset in words
  size_t _bits{0};   // size of bitset in bits
};

// TODO(mbkkt) move to tests?
using bitset = DynamicBitset<std::allocator<size_t>>;
using ManagedBitset = DynamicBitset<ManagedTypedAllocator<size_t>>;

}  // namespace irs
