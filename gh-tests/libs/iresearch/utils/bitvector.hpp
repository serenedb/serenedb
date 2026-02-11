////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <basics/containers/bitset.hpp>

namespace irs {

////////////////////////////////////////////////////////////////////////////////
/// @brief a growable implementation of a bitset
////////////////////////////////////////////////////////////////////////////////
class Bitvector final {
 public:
  using word_t = bitset::word_t;  // NOLINT

  Bitvector() = default;
  explicit Bitvector(size_t bits) : _size{bits} { resize(bits); }
  Bitvector(const Bitvector& other) { *this = other; }
  Bitvector(Bitvector&& other) noexcept
    : _set{std::move(other._set)}, _size{std::exchange(other._size, 0)} {}

  bool operator==(const Bitvector& rhs) const noexcept {
    if (this->size() != rhs.size()) {
      return false;
    }

    return 0 == std::memcmp(this->begin(), rhs.begin(), this->size());
  }

  Bitvector& operator=(const Bitvector& other) {
    if (this != &other) {
      if (_set.words() < other._set.words()) {
        bitset set(other._set.words() * BitsRequired<word_t>());

        set.memset(other._set.begin(), other._set.words() * sizeof(word_t));
        _set = std::move(set);
      } else {  // optimization, reuse existing container
        _set.clear();
        _set.memset(other._set.begin(), other._set.words() * sizeof(word_t));
      }

      _size = other._size;
    }

    return *this;
  }

  Bitvector& operator=(Bitvector&& other) noexcept {
    if (this != &other) {
      _set = std::move(other._set);
      _size = std::exchange(other._size, 0);
    }

    return *this;
  }

  Bitvector& operator&=(const Bitvector& other) {
    if (this == &other || !other.size()) {
      return *this;  // nothing to do
    }

    reserve(other._size);
    _size = std::max(_size, other._size);

    auto* data = const_cast<word_t*>(begin());
    auto last_word = bitset::word(other.size() - 1);  // -1 for bit offset

    for (size_t i = 0; i < last_word; ++i) {
      SDB_ASSERT(i < _set.words() && i < other._set.words());
      *(data + i) &= *(other.data() + i);
    }

    // for the last word consider only those bits included in 'other.size()'
    auto last_word_bits = other.size() % BitsRequired<word_t>();
    const auto mask = (word_t(1) << last_word_bits) -
                      1;  // set all bits that are not part of 'other.size()'

    SDB_ASSERT(last_word < _set.words() && last_word < other._set.words());
    *(data + last_word) &= (*(other.data() + last_word) & mask);
    std::memset(
      data + last_word + 1, 0,
      (_set.words() - last_word - 1) * sizeof(word_t));  // unset tail words

    return *this;
  }

  Bitvector& operator|=(const Bitvector& other) {
    if (this == &other || !other.size()) {
      return *this;  // nothing to do
    }

    reserve(other._size);
    _size = std::max(_size, other._size);

    auto* data = const_cast<word_t*>(begin());
    auto last_word = bitset::word(other.size() - 1);  // -1 for bit offset

    for (size_t i = 0; i <= last_word; ++i) {
      SDB_ASSERT(i < _set.words() && i < other._set.words());
      *(data + i) |= *(other.data() + i);
    }

    return *this;
  }

  Bitvector& operator^=(const Bitvector& other) {
    if (!other.size()) {
      return *this;  // nothing to do
    }

    reserve(other._size);
    _size = std::max(_size, other._size);

    auto* data = const_cast<word_t*>(begin());
    auto last_word = bitset::word(other.size() - 1);  // -1 for bit offset

    for (size_t i = 0; i < last_word; ++i) {
      SDB_ASSERT(i < _set.words() && i < other._set.words());
      *(data + i) ^= *(other.data() + i);
    }

    // for the last word consider only those bits included in 'other.size()'
    auto last_word_bits = other.size() % BitsRequired<word_t>();
    auto mask = ~word_t(0);

    // clear trailing bits
    if (last_word_bits) {
      mask = ~(mask << last_word_bits);  // unset all bits that are not part of
                                         // 'other.size()'
    }

    SDB_ASSERT(last_word < _set.words() && last_word < other._set.words());
    *(data + last_word) ^= (*(other.data() + last_word) & mask);

    return *this;
  }

  Bitvector& operator-=(const Bitvector& other) {
    if (!other.size()) {
      return *this;  // nothing to do
    }

    reserve(other._size);
    _size = std::max(_size, other._size);
    auto* data = const_cast<word_t*>(begin());
    auto last_word = bitset::word(other.size() - 1);  // -1 for bit offset

    for (size_t i = 0; i < last_word; ++i) {
      SDB_ASSERT(i < _set.words() && i < other._set.words());
      *(data + i) &= ~(*(other.data() + i));
    }

    // for the last word consider only those bits included in 'other.size()'
    auto last_word_bits = other.size() % BitsRequired<word_t>();
    auto mask = ~word_t(0);

    // clear trailing bits
    if (last_word_bits) {
      mask = ~(mask << last_word_bits);  // unset all bits that are not part of
                                         // 'other.size()'
    }

    SDB_ASSERT(last_word < _set.words() && last_word < other._set.words());
    *(data + last_word) &= ~(*(other.data() + last_word) & mask);

    return *this;
  }

  bool all() const noexcept { return _set.count() == size(); }
  bool any() const noexcept { return _set.any(); }
  const word_t* begin() const noexcept { return _set.data(); }
  size_t capacity() const noexcept { return _set.capacity(); }
  void clear() noexcept {
    _set.clear();
    _size = 0;
  }
  word_t count() const noexcept { return _set.count(); }
  const word_t* data() const noexcept { return _set.data(); }
  const word_t* end() const noexcept { return _set.end(); }

  template<typename T>
  void memset(const T& value) noexcept {
    memset(&value, sizeof(value));
  }

  void memset(const void* src, size_t size) noexcept {
    auto bits = BitsRequired<uint8_t>() * size;  // size is in bytes

    reserve(bits);
    std::memcpy(const_cast<word_t*>(begin()), src,
                std::min(size, _set.words() * sizeof(word_t)));
    _size = std::max(_size, bits);
  }

  bool none() const noexcept { return _set.none(); }

  // reserve at least this many bits
  void reserve(size_t bits) {
    const auto words = bitset::word(bits - 1) + 1;

    if (!bits || words <= _set.words()) {
      return;  // nothing to do
    }

    auto set = bitset(words * BitsRequired<word_t>());

    if (data()) {
      set.memset(begin(), _set.words() * sizeof(word_t));  // copy original
    }

    _set = std::move(set);
  }

  void resize(size_t bits, bool preserve_capacity = false) {
    const auto words =
      bitset::word(bits) + 1;  // +1 for count instead of offset

    _size = bits;

    if (words > _set.words()) {
      reserve(bits);

      return;
    }

    if (preserve_capacity) {
      std::memset(
        const_cast<word_t*>(begin()) + words, 0,
        (_set.words() - words) * sizeof(word_t));  // clear trailing words
    } else if (words < _set.words()) {
      auto set = bitset(words * BitsRequired<word_t>());

      set.memset(begin(), set.words() * sizeof(word_t));  // copy original
      _set = std::move(set);
    }

    auto last_word_bits = bits % BitsRequired<word_t>();

    // clear trailing bits
    if (last_word_bits) {
      auto* last_word = begin() + words - 1;
      const auto mask =
        ~(~word_t(0)
          << last_word_bits);  // set all bits up to and including max bit

      const_cast<word_t&>(*last_word) &= mask;  // keep only bits up to max bit
    }
  }

  void reset(size_t i, bool set) {
    if (!set && bitset::word(i) >= _set.words()) {
      _size = std::max(size(), i + 1);

      return;  // nothing to do
    }

    resize(std::max(size(), i + 1), true);  // ensure capacity
    _set.reset(i, set);
  }

  void shrink_to_fit() {
    auto words = _set.words();

    if (!words) {
      return;  // nothing to do, no buffer
    }

    while (words && 0 == *(begin() + words - 1)) {
      --words;
    }

    if (!words) {
      auto set = bitset();  // create empty set

      _set = std::move(set);
      _size = 0;

      return;
    }

    if (words != _set.words()) {
      auto set = bitset(words * BitsRequired<word_t>());

      set.memset(begin(), set.words() * sizeof(word_t));  // copy original
      _set = std::move(set);
    }

    _size = (_set.words() * BitsRequired<word_t>()) -
            std::countl_zero(*(begin() + _set.words() - 1));
  }

  void set(size_t i) { reset(i, true); }
  size_t size() const noexcept { return _size; }
  bool empty() const noexcept { return 0 == _size; }

  bool test(size_t i) const noexcept {
    return bitset::word(i) < _set.words() && _set.test(i);
  }

  void unset(size_t i) { reset(i, false); }

  size_t words() const noexcept { return _set.words(); }

  template<typename Visitor>
  bool visit(Visitor visitor) const {
    for (size_t i = 0; i < _size; ++i) {
      if (_set.test(i) && !visitor(i)) {
        return false;
      }
    }
    return true;
  }

  void To(bitset& set) noexcept {
    set = std::move(_set);
    _size = 0;
  }

 private:
  bitset _set;
  size_t _size{0};  // number of bits requested in a bitset
};

static_assert(std::is_nothrow_move_constructible_v<Bitvector>);

}  // namespace irs
