////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "basics/number_utils.h"
#include "vpack/common.h"
#include "vpack/exception.h"
#include "vpack/options.h"
#include "vpack/slice_static_data.h"
#include "vpack/value.h"
#include "vpack/value_type.h"

namespace vpack {

template<typename, typename = void>
struct Extractor;

template<typename DerivedType, typename SliceType = DerivedType>
struct SliceBase {
  friend class Builder;
  friend class ArrayIterator;
  friend class ObjectIterator;
  friend class Iterator;
  friend class ValueSlice;
  template<typename, typename>
  friend struct SliceBase;

  static constexpr uint64_t kDefaultSeed64 = 0xdeadbeef;

  // pointer to the head byte
  constexpr const uint8_t* start() const noexcept {
    return self()->getDataPtr();
  }

  // pointer to the head byte
  template<typename T>
  const T* startAs() const {
    return reinterpret_cast<const T*>(start());
  }

  // value of the head byte
  constexpr uint8_t head() const noexcept { return *start(); }

  constexpr const uint8_t* begin() noexcept { return start(); }

  constexpr const uint8_t* begin() const noexcept { return start(); }

  constexpr const uint8_t* end() const { return start() + byteSize(); }

  // get the type for the slice
  constexpr ValueType type() const noexcept { return type(head()); }

  // get the type name for the slice
  std::string_view typeName() const { return ValueTypeName(type()); }

  // hashes the binary representation of a value. this value is only suitable
  // to be stored in memory, but should not be persisted, as its implementation
  // may change in the future
  uint64_t volatileHash(uint64_t seed = kDefaultSeed64) const {
    return hash(seed);
  }

  // hashes the binary representation of a value
  uint64_t hash(uint64_t seed = kDefaultSeed64) const {
    const auto size = CheckOverflow(byteSize());
    if (seed == kDefaultSeed64 && size == 1) {
      auto h = slice_static_data::kPrecalculatedHashesForDefaultSeed[head()];
      SDB_ASSERT(h != 0);
      return h;
    }
    return VPACK_HASH(start(), size, seed);
  }

  // hashes the binary representation of a value, not using precalculated hash
  // values this is mainly here for testing purposes
  uint64_t hashSlow(uint64_t seed = kDefaultSeed64) const {
    const auto size = CheckOverflow(byteSize());
    return VPACK_HASH(start(), size, seed);
  }

  // hashes the value, normalizing different representations of
  // arrays, objects and numbers. this function may produce different
  // hash values than the binary hash() function
  uint64_t normalizedHash(uint64_t seed = kDefaultSeed64) const;

  // hashes the binary representation of a String slice. No check
  // is done if the Slice value is actually of type String
  uint64_t hashString(uint64_t seed = kDefaultSeed64) const noexcept {
    const auto size = static_cast<size_t>(stringSliceLength());
    return VPACK_HASH(start(), size, seed);
  }

  // check if slice is a None object
  constexpr bool isNone() const noexcept { return head() == 0x00; }

  // check if slice is a Null object
  constexpr bool isNull() const noexcept { return head() == 0x18; }

  // check if slice is the Boolean value false
  constexpr bool isFalse() const noexcept { return head() == 0x19; }

  // check if slice is the Boolean value true
  constexpr bool isTrue() const noexcept { return head() == 0x1a; }

  // check if slice is a Bool object
  constexpr bool isBool() const noexcept { return isFalse() || isTrue(); }

  // check if slice is an Array object
  constexpr bool isArray() const noexcept {
    return head() == 0x13 || headInRange(0x01, 0x09);
  }

  // check if slice is an Object object
  constexpr bool isObject() const noexcept {
    return head() == 0x14 || headInRange(0x0a, 0x0e);
  }

  // check if slice is a Double object
  constexpr bool isDouble() const noexcept { return head() == 0x1f; }

  // check if slice is an Int object
  constexpr bool isInt() const noexcept { return headInRange(0x20, 0x27); }

  // check if slice is a UInt object
  constexpr bool isUInt() const noexcept { return headInRange(0x28, 0x2f); }

  // check if slice is a SmallInt object
  constexpr bool isSmallInt() const noexcept { return headInRange(0x30, 0x3f); }

  // check if slice is a String object
  constexpr bool isString() const noexcept { return head() >= 0x80; }

  // check if a slice is any number type
  constexpr bool isInteger() const noexcept { return headInRange(0x20, 0x3f); }

  // check if slice is any Number-type object
  constexpr bool isNumber() const noexcept { return headInRange(0x1f, 0x3f); }

  template<typename T>
  bool isNumber() const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (isDouble()) {
        const auto v = getDoubleUnchecked();
        return sdb::number_utils::Min<T>() <= v &&
               v < sdb::number_utils::Max<T>();
      } else if (isUInt()) {
        const auto v = getUIntUnchecked();
        return v <= sdb::number_utils::Max<T, decltype(v)>();
      } else if (!isNumber()) {
        return false;
      }
      const auto v = getIntUnchecked();
      using V = decltype(v);
      return sdb::number_utils::Min<T, V>() <= v &&
             v <= sdb::number_utils::Max<T, V>();
    } else {
      return isNumber();
    }
  }

  constexpr bool isSorted() const noexcept { return headInRange(0x0b, 0x0e); }

  // return the value for a Bool object
  bool getBool() const {
    if (!isBool()) {
      throw Exception(Exception::kInvalidValueType, "Expecting type Bool");
    }
    return isTrue();
  }

  // return the value for a Double object
  double getDoubleUnchecked() const {
    SDB_ASSERT(isDouble());
    const auto v = ReadIntegerFixed<uint64_t, sizeof(double)>(start() + 1);
    return std::bit_cast<double>(v);
  }

  double getDouble() const {
    if (!isDouble()) {
      throw Exception(Exception::kInvalidValueType, "Expecting type Double");
    }
    return getDoubleUnchecked();
  }

  // extract the array value at the specified index
  // - 0x02      : array without index table (all subitems have the same
  //               byte length), bytelen 1 byte, no number of subvalues
  // - 0x03      : array without index table (all subitems have the same
  //               byte length), bytelen 2 bytes, no number of subvalues
  // - 0x04      : array without index table (all subitems have the same
  //               byte length), bytelen 4 bytes, no number of subvalues
  // - 0x05      : array without index table (all subitems have the same
  //               byte length), bytelen 8 bytes, no number of subvalues
  // - 0x06      : array with 1-byte index table entries
  // - 0x07      : array with 2-byte index table entries
  // - 0x08      : array with 4-byte index table entries
  // - 0x09      : array with 8-byte index table entries
  SliceType at(ValueLength index) const {
    if (!isArray()) [[unlikely]] {
      throw Exception(Exception::kInvalidValueType, "Expecting type Array");
    }

    return getNth(index);
  }

  // return the number of members for an Array or Object object
  ValueLength length() const;

  // extract a key from an Object at the specified index
  // - 0x0a      : empty object
  // - 0x0b      : object with 1-byte index table entries, sorted by attribute
  // name
  // - 0x0c      : object with 2-byte index table entries, sorted by attribute
  // name
  // - 0x0d      : object with 4-byte index table entries, sorted by attribute
  // name
  // - 0x0e      : object with 8-byte index table entries, sorted by attribute
  // name
  // - 0x0f      : object with 1-byte index table entries, not sorted by
  // attribute name
  // - 0x10      : object with 2-byte index table entries, not sorted by
  // attribute name
  // - 0x11      : object with 4-byte index table entries, not sorted by
  // attribute name
  // - 0x12      : object with 8-byte index table entries, not sorted by
  // attribute name
  SliceType keyAt(ValueLength index) const;
  SliceType valueAt(ValueLength index) const;

  // look for the specified attribute path inside an Object
  // returns a Slice{} if not found
  template<typename Begin, typename End>
  SliceType get(Begin begin, End end) const {
    if (begin == end) [[unlikely]] {
      throw Exception{Exception::kInvalidAttributePath};
    }
    // use ourselves as the starting point
    SliceType last{start()};
    do {
      last = last.get(*begin);
      if (last.isNone()) {
        return SliceType{};
      }
      if (++begin == end) {
        return last;
      }
    } while (last.isObject());
    return SliceType{};
  }

  template<typename Attributes>
  SliceType get(const Attributes& attributes) const
    requires requires {
      std::begin(attributes);
      std::end(attributes);
    } && (!std::is_convertible_v<const Attributes&, std::string_view>)
  {
    return this->get(std::begin(attributes), std::end(attributes));
  }

  SliceType get(std::initializer_list<std::string_view> attributes) const {
    return this->get(attributes.begin(), attributes.end());
  }

  // look for the specified attribute inside an Object
  // returns a Slice(ValueType::None) if not found
  SliceType get(std::string_view attribute) const;

  // whether or not an Object has a specific key
  [[deprecated("Commonly make work twice, use get")]] bool hasKey(
    std::string_view attribute) const {
    return !get(attribute).isNone();
  }

  bool isEmptyString() const noexcept { return head() == 0x80; }

  bool isEmptyArray() const noexcept { return head() == 0x01; }

  bool isEmptyObject() const noexcept { return head() == 0x0a; }

  // return the value for an Int object
  int64_t getInt() const;

  // return the value for a UInt object
  uint64_t getUInt() const;

  template<typename T, bool Strict = false>
  T getNumber() const {
    if constexpr (std::is_integral_v<T>) {
      if (isDouble()) {
        if constexpr (Strict) {
          throw Exception{Exception::kInvalidValueType,
                          "Expecting integer type"};
        }

        const auto v = getDoubleUnchecked();
        if (sdb::number_utils::Min<T>() <= v &&
            v < sdb::number_utils::Max<T>()) {
          return static_cast<T>(v);
        }
        throw Exception(Exception::kNumberOutOfRange);
      }
      if constexpr (std::is_signed_v<T>) {
        const auto v = getInt();
        using V = decltype(v);
        if (sdb::number_utils::Min<T, V>() <= v &&
            v <= sdb::number_utils::Max<T, V>()) {
          return v;
        }
      } else {
        const auto v = getUInt();
        if (v <= sdb::number_utils::Max<T, decltype(v)>()) {
          return v;
        }
      }
      throw Exception(Exception::kNumberOutOfRange);
    } else {
      if (isDouble()) {
        return static_cast<T>(getDoubleUnchecked());
      } else if (isUInt()) {
        return static_cast<T>(getUIntUnchecked());
      } else if (isNumber()) {
        return static_cast<T>(getIntUnchecked());
      }

      throw Exception{Exception::kInvalidValueType, "Expecting numeric type"};
    }
  }

  std::string_view stringViewUnchecked() const noexcept {
    SDB_ASSERT(isString());
    auto* data = start();
    const auto h = *data;
    uint32_t len = 0;
    if (h == 0xff) [[unlikely]] {
      len = ReadIntegerFixed<uint32_t, 4>(data + 1);
      return {reinterpret_cast<const char*>(data + 1 + 4), len};
    }
    len = h - 0x80;
    return {reinterpret_cast<const char*>(data + 1), len};
  }

  std::string_view stringView() const {
    if (!isString()) [[unlikely]] {
      throw Exception{Exception::kInvalidValueType, "Expecting type String"};
    }
    return stringViewUnchecked();
  }

  // return a copy of the value for a String object
  std::string copyString() const { return std::string{stringView()}; }

  ValueLength byteSize(bool precise = true) const {
    auto l = slice_static_data::kFixedTypeLengths[head()];
    if (l != 0) [[likely]] {
      return l;
    }
    if (precise) {
      return byteSizeDynamic(start());
    }
    return std::numeric_limits<ValueLength>::max();
  }

  ValueLength findDataOffset(uint8_t head) const noexcept {
    // Must be called for a non-empty array or object at start():
    SDB_ASSERT(0x01 < head && head != 0x0a && head <= 0x14);
    unsigned int fsm = slice_static_data::kFirstSubMap[head];
    const uint8_t* start = this->start();
    if (fsm == 0) {
      // need to calculate the offset by reading the dynamic length
      SDB_ASSERT(head == 0x13 || head == 0x14);
      return 1 +
             GetVariableValueLength(ReadVariableValueLength<false>(start + 1));
    }
    if (fsm <= 2 && start[2] != 0) {
      return 2;
    }
    if (fsm <= 3 && start[3] != 0) {
      return 3;
    }
    if (fsm <= 5 && start[5] != 0) {
      return 5;
    }
    return 9;
  }

  // get the offset for the nth member from an Array type
  ValueLength getNthOffset(ValueLength index) const;

  int compareString(std::string_view value) const;

  int compareStringUnchecked(std::string_view value) const noexcept;

  bool isEqualString(std::string_view attribute) const;

  bool isEqualStringUnchecked(std::string_view attribute) const noexcept;

  // check if two Slices are equal on the binary level
  // please note that for several values there are multiple possible
  // representations, which differ on the binary level but will still resolve to
  // the same logical values. For example, smallint(1) and int(1) are logically
  // the same, but will resolve to either 0x31 or 0x28 0x01.
  template<typename S, typename T>
  bool binaryEquals(const SliceBase<S, T>& other) const {
    if (start() == other.start()) {
      // same underlying data, so the slices must be identical
      return true;
    }

    if (head() != other.head()) {
      return false;
    }

    const ValueLength size = byteSize();

    if (size != other.byteSize()) {
      return false;
    }

    return (std::memcmp(start(), other.start(), CheckOverflow(size)) == 0);
  }

  static bool binaryEquals(const uint8_t* left, const uint8_t* right) {
    return SliceType(left).binaryEquals(SliceType(right));
  }

  // these operators are now deleted because they didn't do what people expected
  // these operators checked for _binary_ equality of the vpack slice with
  // another. however, for several values there are multiple possible
  // representations, which differ on the binary level but will still resolve to
  // the same logical values. For example, smallint(1) and int(1) are logically
  // the same, but will resolve to either 0x31 or 0x28 0x01.
  bool operator==(const Slice& other) const = delete;
  bool operator!=(const Slice& other) const = delete;

  std::string toJson(const Options* options = &Options::gDefaults) const;
  void toJson(std::string& out,
              const Options* options = &Options::gDefaults) const;
  std::string toString(const Options* options = &Options::gDefaults) const;

  template<typename Sink>
  void toSink(Sink* sink, const Options* options = &Options::gDefaults) const;

  std::string toHex() const;
  std::string hexType() const;

  int64_t getIntUnchecked() const noexcept;

  uint64_t getUIntUnchecked() const noexcept;

  int64_t getSmallIntUnchecked() const noexcept;

 private:
  int64_t intUnchecked() const noexcept;

  // get the type for the slice
  static constexpr ValueType type(uint8_t h) {
    return slice_static_data::kTypeMap[h];
  }

  constexpr bool headInRange(uint8_t low, uint8_t high) const noexcept {
    const auto h = head();
    return low <= h && h <= high;
  }

  // return the number of members for an Array
  // must only be called for Slices that have been validated to be of type Array
  ValueLength arrayLength() const;

  // return the number of members for an Object
  // must only be called for Slices that have been validated to be of type
  // Object
  ValueLength objectLength() const;

  // get the total byte size for a String slice, including the head byte.
  // no check is done if the type of the slice is actually String
  uint32_t stringSliceLength() const noexcept {
    SDB_ASSERT(isString());
    const auto* s = start();
    const auto h = *s;
    if (h == 0xff) [[unlikely]] {
      return 1 + 4 + ReadIntegerFixed<uint32_t, 4>(s + 1);
    }
    return 1 + static_cast<uint32_t>(h - 0x80);
  }

  // translates an integer key into a string, without checks
  SliceType translateUnchecked() const;

  SliceType getFromCompactObject(std::string_view attribute) const;

  // extract the nth member from an Array
  SliceType getNth(ValueLength index) const;

  // extract the nth member from an Object, no translation
  Slice getNthKeyUntranslated(ValueLength index) const;

  // get the offset for the nth member from a compact Array or Object type
  ValueLength getNthOffsetFromCompact(ValueLength index) const;

  // get the offset for the first member from a compact Array or Object type
  // it is only valid to call this method for compact Array or Object values
  // with at least one member!!
  ValueLength getStartOffsetFromCompact() const {
    SDB_ASSERT(head() == 0x13 || head() == 0x14);

    ValueLength end = ReadVariableValueLength<false>(start() + 1);
    return 1 + GetVariableValueLength(end);
  }

  static constexpr ValueLength indexEntrySize(uint8_t h) noexcept {
    SDB_ASSERT(0x01 < h && h != 0x0a && h < 0x0f);
    return slice_static_data::kWidthMap[h];
  }

  // perform a linear search for the specified attribute inside an Object
  SliceType searchObjectKeyLinear(std::string_view attribute,
                                  ValueLength ie_base, ValueLength offset_size,
                                  ValueLength n) const;

  // perform a binary search for the specified attribute inside an Object
  template<ValueLength OffsetSize>
  SliceType searchObjectKeyBinary(std::string_view attribute,
                                  ValueLength ie_base, ValueLength n) const;

  static ValueLength byteSizeDynamic(const uint8_t* start);

  constexpr const DerivedType* self() const noexcept {
    return static_cast<const DerivedType*>(this);
  }

  static SliceType make(const uint8_t* mem) { return SliceType{mem}; }

  static SliceType make() {
    static_assert(std::is_default_constructible_v<SliceType>);
    return SliceType{};
  }
};

}  // namespace vpack
