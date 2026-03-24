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

#include <ostream>

#include "basics/sink.h"
#include "vpack/builder.h"
#include "vpack/common.h"
#include "vpack/dumper.h"
#include "vpack/hex_dump.h"
#include "vpack/iterator.h"
#include "vpack/parser.h"
#include "vpack/slice.h"
#include "vpack/value_type.h"

namespace vpack {

template<typename DerivedType, typename SliceType>
int64_t SliceBase<DerivedType, SliceType>::intUnchecked() const noexcept {
  SDB_ASSERT(isInt());
  return ReadIntegerNonEmpty<int64_t>(start() + 1, head() - 0x1f);
}

template<typename DerivedType, typename SliceType>
uint64_t SliceBase<DerivedType, SliceType>::getUIntUnchecked() const noexcept {
  SDB_ASSERT(isUInt());
  return ReadIntegerNonEmpty<uint64_t>(start() + 1, head() - 0x27);
}

template<typename DerivedType, typename SliceType>
int64_t SliceBase<DerivedType, SliceType>::getSmallIntUnchecked()
  const noexcept {
  SDB_ASSERT(isSmallInt());
  const auto h = static_cast<int64_t>(head());
  return h - 0x36;
}

template<typename DerivedType, typename SliceType>
std::string SliceBase<DerivedType, SliceType>::toHex() const {
  HexDump dump{Slice(start())};
  return dump.toString();
}

template<typename DerivedType, typename SliceType>
std::string SliceBase<DerivedType, SliceType>::toJson(
  const Options* options) const {
  sdb::basics::StrSink sink;
  // TODO(gnusi-vpack) reserve?
  toSink(&sink, options);
  return std::move(sink.Impl());
}

template<typename DerivedType, typename SliceType>
void SliceBase<DerivedType, SliceType>::toJson(std::string& out,
                                               const Options* options) const {
  // TODO(gnusi-vpack) reserve?
  sdb::basics::StrSink sink(std::move(out));
  toSink(&sink, options);
  out = std::move(sink.Impl());
}

template<typename DerivedType, typename SliceType>
template<typename Sink>
void SliceBase<DerivedType, SliceType>::toSink(Sink* sink,
                                               const Options* options) const {
  Dumper dumper(sink, options);
  dumper.Dump(Slice{start()});
}

template<typename DerivedType, typename SliceType>
std::string SliceBase<DerivedType, SliceType>::toString(
  const Options* options) const {
  if (isString()) {
    return copyString();
  }

  // copy options and set prettyPrint in copy
  Options pretty_options = *options;
  pretty_options.pretty_print = true;

  sdb::basics::StrSink sink;
  Dump(Slice(start()), &sink, &pretty_options);
  return std::move(sink.Impl());
}

template<typename DerivedType, typename SliceType>
std::string SliceBase<DerivedType, SliceType>::hexType() const {
  return HexDump::toHex(head());
}

template<typename DerivedType, typename SliceType>
uint64_t SliceBase<DerivedType, SliceType>::normalizedHash(
  uint64_t seed) const {
  uint64_t value;

  if (isNumber()) {
    // upcast integer values to double
    double v = getNumber<double>();
    value = VPACK_HASH(&v, sizeof(v), seed);
  } else if (isArray()) {
    // normalize arrays by hashing array length and iterating
    // over all array members
    ArrayIterator it{Slice(start())};
    const uint64_t n = it.size() ^ 0xba5bedf00d;
    value = VPACK_HASH(&n, sizeof(n), seed);
    while (it.valid()) {
      value ^= it.value().normalizedHash(value);
      it.next();
    }
  } else if (isObject()) {
    // normalize objects by hashing object length and iterating
    // over all object members
    ObjectIterator it(Slice(start()), true);
    const uint64_t n = it.size() ^ 0xf00ba44ba5;
    uint64_t seed2 = VPACK_HASH(&n, sizeof(n), seed);
    value = seed2;
    while (it.valid()) {
      auto current = (*it);
      uint64_t seed3 = current.key.normalizedHash(seed2);
      value ^= seed3;
      value ^= current.value().normalizedHash(seed3);
      it.next();
    }
  } else {
    // fallback to regular hash function
    value = hash(seed);
  }

  return value;
}

// look for the specified attribute inside an Object
// returns a Slice(ValueType::None) if not found
template<typename DerivedType, typename SliceType>
SliceType SliceBase<DerivedType, SliceType>::get(
  std::string_view attribute) const {
  if (!isObject()) [[unlikely]] {
    throw Exception(Exception::kInvalidValueType, "Expecting Object");
  }

  const auto h = head();
  if (h == 0x0a) {
    // special case, empty object
    return SliceType();
  }

  if (h == 0x14) {
    // compact Object
    return getFromCompactObject(attribute);
  }

  const ValueLength offset_size = indexEntrySize(h);
  SDB_ASSERT(offset_size > 0);
  ValueLength end = ReadIntegerNonEmpty<ValueLength>(start() + 1, offset_size);

  // read number of items
  ValueLength n;
  ValueLength ie_base;
  if (offset_size < 8) {
    n =
      ReadIntegerNonEmpty<ValueLength>(start() + 1 + offset_size, offset_size);
    ie_base = end - n * offset_size;
  } else {
    n = ReadIntegerNonEmpty<ValueLength>(start() + end - offset_size,
                                         offset_size);
    ie_base = end - n * offset_size - offset_size;
  }

  if (n == 1) {
    // Just one attribute, there is no index table!
    Slice key(start() + findDataOffset(h));

    if (key.isString()) {
      if (key.isEqualStringUnchecked(attribute)) {
        return make(key.start() + key.byteSize());
      }
    }

    // no match or invalid key type
    return make();
  }

  // only use binary search for attributes if we have at least this many entries
  // otherwise we'll always use the linear search
  constexpr ValueLength kSortedSearchEntriesThreshold = 4;

  if (n >= kSortedSearchEntriesThreshold && (h >= 0x0b && h <= 0x0e)) {
    switch (offset_size) {
      case 1:
        return searchObjectKeyBinary<1>(attribute, ie_base, n);
      case 2:
        return searchObjectKeyBinary<2>(attribute, ie_base, n);
      case 4:
        return searchObjectKeyBinary<4>(attribute, ie_base, n);
      case 8:
        return searchObjectKeyBinary<8>(attribute, ie_base, n);
      default: {
      }
    }
  }

  return searchObjectKeyLinear(attribute, ie_base, offset_size, n);
}

template<typename DerivedType, typename SliceType>
int64_t SliceBase<DerivedType, SliceType>::getIntUnchecked() const noexcept {
  if (isInt()) {
    return intUnchecked();
  }
  return getSmallIntUnchecked();
}

template<typename DerivedType, typename SliceType>
int64_t SliceBase<DerivedType, SliceType>::getInt() const {
  if (isInt()) {
    return intUnchecked();
  } else if (isUInt()) {
    const auto v = getUIntUnchecked();
    if (v <= sdb::number_utils::Max<int64_t, uint64_t>()) {
      return static_cast<int64_t>(v);
    }
    throw Exception(Exception::kNumberOutOfRange);
  } else if (isSmallInt()) {
    return getSmallIntUnchecked();
  }

  throw Exception(Exception::kInvalidValueType, "Expecting type Int");
}

template<typename DerivedType, typename SliceType>
uint64_t SliceBase<DerivedType, SliceType>::getUInt() const {
  if (isUInt()) {
    return getUIntUnchecked();
  } else if (!isInteger()) {
    throw Exception(Exception::kInvalidValueType, "Expecting type UInt");
  }
  const auto v = getIntUnchecked();
  if (v < 0) {
    throw Exception(Exception::kNumberOutOfRange);
  }
  return static_cast<uint64_t>(v);
}

template<typename DerivedType, typename SliceType>
int SliceBase<DerivedType, SliceType>::compareString(
  std::string_view value) const {
  return stringView().compare(value);
}

template<typename DerivedType, typename SliceType>
int SliceBase<DerivedType, SliceType>::compareStringUnchecked(
  std::string_view value) const noexcept {
  return stringViewUnchecked().compare(value);
}

template<typename DerivedType, typename SliceType>
bool SliceBase<DerivedType, SliceType>::isEqualString(
  std::string_view attribute) const {
  return stringView() == attribute;
}

template<typename DerivedType, typename SliceType>
bool SliceBase<DerivedType, SliceType>::isEqualStringUnchecked(
  std::string_view attribute) const noexcept {
  return stringViewUnchecked() == attribute;
}

template<typename DerivedType, typename SliceType>
SliceType SliceBase<DerivedType, SliceType>::getFromCompactObject(
  std::string_view attribute) const {
  ObjectIterator it(Slice(start()), /*useSequentialIteration*/ false);
  while (it.valid()) {
    auto key = (*it).key;
    if (key.isEqualString(attribute)) {
      return SliceType(key.start() + key.byteSize());
    }

    it.next();
  }
  // not found
  return SliceType();
}

// get the offset for the nth member from an Array or Object type
template<typename DerivedType, typename SliceType>
ValueLength SliceBase<DerivedType, SliceType>::getNthOffset(
  ValueLength index) const {
  SDB_ASSERT(isArray() || isObject());

  const auto h = head();

  if (h == 0x13 || h == 0x14) {
    // compact Array or Object
    return getNthOffsetFromCompact(index);
  }

  if (h == 0x01 || h == 0x0a) [[unlikely]] {
    // special case: empty Array or empty Object
    throw Exception(Exception::kIndexOutOfBounds);
  }

  const ValueLength offset_size = indexEntrySize(h);
  ValueLength end = ReadIntegerNonEmpty<ValueLength>(start() + 1, offset_size);

  ValueLength data_offset = 0;

  // find the number of items
  ValueLength n;
  if (h <= 0x05) {  // No offset table or length, need to compute:
    SDB_ASSERT(h != 0x00 && h != 0x01);
    data_offset = findDataOffset(h);
    Slice first(start() + data_offset);
    ValueLength s = first.byteSize();
    if (s == 0) [[unlikely]] {
      throw Exception(Exception::kInternalError,
                      "Invalid data for compact object");
    }
    n = (end - data_offset) / s;
  } else if (offset_size < 8) {
    n =
      ReadIntegerNonEmpty<ValueLength>(start() + 1 + offset_size, offset_size);
  } else {
    n = ReadIntegerNonEmpty<ValueLength>(start() + end - offset_size,
                                         offset_size);
  }

  if (index >= n) {
    throw Exception(Exception::kIndexOutOfBounds);
  }

  // empty array case was already covered
  SDB_ASSERT(n > 0);

  if (h <= 0x05 || n == 1) {
    // no index table, but all array items have the same length
    // now fetch first item and determine its length
    if (data_offset == 0) {
      SDB_ASSERT(h != 0x00 && h != 0x01);
      data_offset = findDataOffset(h);
    }
    return data_offset + index * Slice(start() + data_offset).byteSize();
  }

  const ValueLength ie_base =
    end - n * offset_size + index * offset_size - (offset_size == 8 ? 8 : 0);
  return ReadIntegerNonEmpty<ValueLength>(start() + ie_base, offset_size);
}

// extract the nth member from an Array
template<typename DerivedType, typename SliceType>
SliceType SliceBase<DerivedType, SliceType>::getNth(ValueLength index) const {
  SDB_ASSERT(isArray());

  return make(start() + getNthOffset(index));
}

// get the offset for the nth member from a compact Array or Object type
template<typename DerivedType, typename SliceType>
ValueLength SliceBase<DerivedType, SliceType>::getNthOffsetFromCompact(
  ValueLength index) const {
  const auto h = head();
  SDB_ASSERT(h == 0x13 || h == 0x14);

  ValueLength end = ReadVariableValueLength<false>(start() + 1);
  ValueLength n = ReadVariableValueLength<true>(start() + end - 1);
  if (index >= n) [[unlikely]] {
    throw Exception(Exception::kIndexOutOfBounds);
  }

  ValueLength offset = 1 + GetVariableValueLength(end);
  ValueLength current = 0;
  while (current != index) {
    const uint8_t* s = start() + offset;
    offset += Slice(s).byteSize();
    if (h == 0x14) {
      offset += Slice(start() + offset).byteSize();
    }
    ++current;
  }
  return offset;
}

// perform a linear search for the specified attribute inside an Object
template<typename DerivedType, typename SliceType>
SliceType SliceBase<DerivedType, SliceType>::searchObjectKeyLinear(
  std::string_view attribute, ValueLength ie_base, ValueLength offset_size,
  ValueLength n) const {
  for (ValueLength index = 0; index < n; ++index) {
    ValueLength offset = ie_base + index * offset_size;
    Slice key(start() +
              ReadIntegerNonEmpty<ValueLength>(start() + offset, offset_size));

    if (key.isString()) {
      if (!key.isEqualStringUnchecked(attribute)) {
        continue;
      }
    } else {
      // invalid key type
      return make();
    }

    // key is identical. now return value
    return make(key.start() + key.byteSize());
  }

  // nothing found
  return make();
}

// perform a binary search for the specified attribute inside an Object
template<typename DerivedType, typename SliceType>
template<ValueLength OffsetSize>
SliceType SliceBase<DerivedType, SliceType>::searchObjectKeyBinary(
  std::string_view attribute, ValueLength ie_base, ValueLength n) const {
  SDB_ASSERT(n > 0);

  int64_t l = 0;
  int64_t r = static_cast<int64_t>(n) - 1;
  int64_t index = r / 2;

  do {
    ValueLength offset = ie_base + index * OffsetSize;
    Slice key(start() +
              ReadIntegerFixed<ValueLength, OffsetSize>(start() + offset));

    int res;
    if (key.isString()) {
      res = key.compareStringUnchecked(attribute);
    } else {
      // TODO(gnusi): assert?
      throw Exception(Exception::kInvalidValueType);
    }

    if (res > 0) {
      r = index - 1;
    } else if (res == 0) {
      // found. now return a Slice pointing at the value
      return make(key.start() + key.byteSize());
    } else {
      l = index + 1;
    }

    // determine new midpoint
    index = l + ((r - l) / 2);
  } while (r >= l);

  // not found
  return SliceType();
}

template<typename DerivedType, typename SliceType>
Slice SliceBase<DerivedType, SliceType>::getNthKeyUntranslated(
  ValueLength index) const {
  SDB_ASSERT(type() == ValueType::Object);
  return Slice(start() + getNthOffset(index));
}

template<typename DerivedType, typename SliceType>
SliceType SliceBase<DerivedType, SliceType>::keyAt(ValueLength index) const {
  if (!isObject()) [[unlikely]] {
    throw Exception(Exception::kInvalidValueType, "Expecting type Object");
  }
  auto key = getNthKeyUntranslated(index);
  return make(key.start());
}

template<typename DerivedType, typename SliceType>
SliceType SliceBase<DerivedType, SliceType>::valueAt(ValueLength index) const {
  if (!isObject()) [[unlikely]] {
    throw Exception(Exception::kInvalidValueType, "Expecting type Object");
  }
  auto key = getNthKeyUntranslated(index);
  return make(key.start() + key.byteSize());
}

template<typename DerivedType, typename SliceType>
ValueLength SliceBase<DerivedType, SliceType>::byteSizeDynamic(
  const uint8_t* start) {
  uint8_t h = *start;

  // types with dynamic lengths need special treatment:
  switch (type(h)) {
    case ValueType::Array:
    case ValueType::Object:
      if (h == 0x13 || h == 0x14) {
        // compact Array or Object
        return ReadVariableValueLength<false>(start + 1);
      }
      SDB_ASSERT(0x01 < h && h != 0x0a && h < 0x0f);
      return ReadIntegerNonEmpty<ValueLength>(start + 1, indexEntrySize(h));
    case ValueType::String:
      SDB_ASSERT(h == 0xff);
      // long String
      return 1 + 4 + ReadIntegerFixed<uint32_t, 4>(start + 1);
    default:
      [[fallthrough]];
  }

  throw Exception(Exception::kInternalError, "Invalid type for byteSize()");
}

template<typename DerivedType, typename SliceType>
ValueLength SliceBase<DerivedType, SliceType>::arrayLength() const {
  const auto h = head();
  SDB_ASSERT(isArray());

  if (h == 0x01) {
    // special case: empty!
    return 0;
  }

  if (h == 0x13) {
    // compact Array
    ValueLength end = ReadVariableValueLength<false>(start() + 1);
    return ReadVariableValueLength<true>(start() + end - 1);
  }

  const ValueLength offset_size = indexEntrySize(h);
  SDB_ASSERT(offset_size > 0);

  // find number of items
  if (h <= 0x05) {  // No offset table or length, need to compute:
    SDB_ASSERT(h != 0x00 && h != 0x01);
    ValueLength first_sub_offset = findDataOffset(h);
    Slice first(start() + first_sub_offset);
    ValueLength s = first.byteSize();
    if (s == 0) [[unlikely]] {
      throw Exception(Exception::kInternalError, "Invalid data for Array");
    }
    ValueLength end =
      ReadIntegerNonEmpty<ValueLength>(start() + 1, offset_size);
    return (end - first_sub_offset) / s;
  } else if (offset_size < 8) {
    return ReadIntegerNonEmpty<ValueLength>(start() + offset_size + 1,
                                            offset_size);
  }

  ValueLength end = ReadIntegerNonEmpty<ValueLength>(start() + 1, offset_size);
  return ReadIntegerNonEmpty<ValueLength>(start() + end - offset_size,
                                          offset_size);
}

template<typename DerivedType, typename SliceType>
ValueLength SliceBase<DerivedType, SliceType>::objectLength() const {
  const auto h = head();
  SDB_ASSERT(isObject());

  if (h == 0x0a) {
    // special case: empty!
    return 0;
  }

  if (h == 0x14) {
    // compact Object
    ValueLength end = ReadVariableValueLength<false>(start() + 1);
    return ReadVariableValueLength<true>(start() + end - 1);
  }

  const ValueLength offset_size = indexEntrySize(h);
  SDB_ASSERT(offset_size > 0);

  if (offset_size < 8) {
    return ReadIntegerNonEmpty<ValueLength>(start() + offset_size + 1,
                                            offset_size);
  }

  ValueLength end = ReadIntegerNonEmpty<ValueLength>(start() + 1, offset_size);
  return ReadIntegerNonEmpty<ValueLength>(start() + end - offset_size,
                                          offset_size);
}

template<typename DerivedType, typename SliceType>
ValueLength SliceBase<DerivedType, SliceType>::length() const {
  if (!isArray() && !isObject()) [[unlikely]] {
    throw Exception(Exception::kInvalidValueType,
                    "Expecting type Array or Object");
  }

  const auto h = head();
  if (h == 0x01 || h == 0x0a) {
    // special case: empty!
    return 0;
  }

  if (h == 0x13 || h == 0x14) {
    // compact Array or Object
    ValueLength end = ReadVariableValueLength<false>(start() + 1);
    return ReadVariableValueLength<true>(start() + end - 1);
  }

  const ValueLength offset_size = indexEntrySize(h);
  SDB_ASSERT(offset_size > 0);
  ValueLength end = ReadIntegerNonEmpty<ValueLength>(start() + 1, offset_size);

  // find number of items
  if (h <= 0x05) {  // No offset table or length, need to compute:
    SDB_ASSERT(h != 0x00 && h != 0x01);
    ValueLength first_sub_offset = findDataOffset(h);
    Slice first(start() + first_sub_offset);
    ValueLength s = first.byteSize();
    if (s == 0) [[unlikely]] {
      throw Exception(Exception::kInternalError, "Invalid data for Array");
    }
    return (end - first_sub_offset) / s;
  } else if (offset_size < 8) {
    return ReadIntegerNonEmpty<ValueLength>(start() + offset_size + 1,
                                            offset_size);
  }

  return ReadIntegerNonEmpty<ValueLength>(start() + end - offset_size,
                                          offset_size);
}

}  // namespace vpack
#define INSTANTIATE_TYPE(Derived, SliceType)                                  \
  template struct SliceBase<Derived, SliceType>;                              \
  template SliceType SliceBase<Derived, SliceType>::searchObjectKeyBinary<1>( \
    std::string_view attribute, ValueLength ieBase, ValueLength n) const;     \
  template SliceType SliceBase<Derived, SliceType>::searchObjectKeyBinary<2>( \
    std::string_view attribute, ValueLength ieBase, ValueLength n) const;     \
  template SliceType SliceBase<Derived, SliceType>::searchObjectKeyBinary<4>( \
    std::string_view attribute, ValueLength ieBase, ValueLength n) const;     \
  template SliceType SliceBase<Derived, SliceType>::searchObjectKeyBinary<8>( \
    std::string_view attribute, ValueLength ieBase, ValueLength n) const;
