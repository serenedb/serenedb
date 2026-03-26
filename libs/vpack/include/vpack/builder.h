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

#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "basics/buffer.h"
#include "basics/exceptions.h"
#include "vpack/common.h"
#include "vpack/options.h"
#include "vpack/slice.h"
#include "vpack/value.h"
#include "vpack/value_type.h"

namespace vpack {

class ArrayIterator;
class ObjectIterator;

class Builder {
  friend class Parser;  // The parser needs access to internals.

  // Here are the mechanics of how this building process works:
  // The whole VPack being built starts at where _start points to.
  // The variable _pos keeps the
  // current write position. The method "set" simply writes a new
  // VPack subobject at the current write position and advances
  // it. Whenever one makes an array or object, a ValueLength for
  // the beginning of the value is pushed onto the _stack, which
  // remembers that we are in the process of building an array or
  // object. The _index vectors are used to collect information
  // for the index tables of arrays and objects, which are written
  // behind the subvalues. The add methods are used to keep track
  // of the new subvalue in _index followed by a set, and are
  // what the user from the outside calls. The close method seals
  // the innermost array or object that is currently being built
  // and pops a ValueLength off the _stack. The vectors in _index
  // stay until the next clearTemporary() is called to minimize
  // allocations. In the beginning, the _stack is empty, which
  // allows to build a sequence of unrelated VPack objects in the
  // buffer. Whenever the stack is empty, one can use the start,
  // size and slice methods to get out the ready built VPack
  // object(s).

  // Here we collect the result
  std::shared_ptr<BufferUInt8> _buffer;
  // Always points to the start of _buffer
  uint8_t* _start = nullptr;
  // the append position
  index_t _pos = 0;

  // indicates that in the current object the key has been written
  // but the value not yet
  bool _key_written = false;

  struct CompoundInfo {
    index_t start_pos = 0;
    index_t index_start_pos = 0;
  };

  // Start positions of open objects/arrays
  absl::InlinedVector<CompoundInfo, 3> _stack;

  // Indices for starts of subindex
  std::vector<index_t> _indexes;

 public:
  const Options* options;

  explicit Builder(const Options* options = &Options::gDefaults);

  explicit Builder(std::shared_ptr<BufferUInt8> buffer,
                   const Options* options = &Options::gDefaults);

  explicit Builder(BufferUInt8& buffer,
                   const Options* options = &Options::gDefaults);

  explicit Builder(Slice slice, const Options* options = &Options::gDefaults);

  Builder(const Builder& other);
  Builder& operator=(const Builder& other);
  Builder(Builder&& other) noexcept;
  Builder& operator=(Builder&& other) noexcept;

  // get a reference to the Builder's Buffer object
  // note: this object may be a nullptr if the buffer was already stolen
  // from the Builder, or if the Builder has no ownership for the Buffer
  const auto& buffer() const { return _buffer; }

  BufferUInt8& bufferRef() const {
    if (!_buffer) {
      throw Exception(Exception::kInternalError, "Builder has no Buffer");
    }
    return *_buffer;
  }

  // steal the Builder's Buffer object. afterwards the Builder
  // is unusable - note: this may return a nullptr if the Builder does not
  // own the Buffer!
  auto steal() {
    // After a steal the Builder is broken!
    auto r = std::move(_buffer);
    SDB_ASSERT(!_buffer);
    _start = nullptr;
    clear();
    return r;
  }

  const uint8_t* data() const noexcept {
    SDB_ASSERT(_buffer);
    return _buffer->data();
  }

  std::string toString() const { return slice().toString(); }

  std::string toJson() const { return slice().toJson(); }

  static Builder clone(Slice slice,
                       const Options* options = &Options::gDefaults) {
    if (options == nullptr) {
      throw Exception(Exception::kInternalError, "Options cannot be a nullptr");
    }

    Builder b(options);
    b.add(slice);
    return b;  // Use return value optimization
  }

  void reserve(ValueLength len) {
    SDB_ASSERT(_start == _buffer->data());
    SDB_ASSERT(_start + _pos >= _buffer->data());
    SDB_ASSERT(_start + _pos <= _buffer->data() + _buffer->size());

    // Reserves len bytes at pos of the current state (top of stack)
    // or throws an exception
    if (_pos + len < _buffer->capacity()) {
      return;  // All OK, we can just increase tos->pos by len
    }

#ifndef VPACK_64BIT
    (void)checkOverflow(_pos + len);
#endif

    SDB_ASSERT(_buffer);
    _buffer->reserve(len);
    _start = _buffer->data();
  }

  // Clear and start from scratch:
  void clear() noexcept {
    _pos = 0;
    _key_written = false;
    _stack.clear();
    _indexes.clear();
    if (_buffer) {
      _buffer->reset();
      _start = _buffer->data();
    }
  }

  // Return a pointer to the start of the result:
  uint8_t* start() const {
    if (isClosed()) {
      return _start;
    }
    throw Exception(Exception::kBuilderNotSealed);
  }

  // Return a Slice of the result:
  Slice slice() const {
    if (isEmpty()) {
      return {};
    }
    return Slice(start());
  }

  // Compute the actual size here, but only when sealed
  ValueLength size() const {
    if (!isClosed()) {
      throw Exception(Exception::kBuilderNotSealed);
    }
    return _pos;
  }

  bool isEmpty() const noexcept { return _pos == 0; }

  bool isClosed() const noexcept { return _stack.empty(); }

  bool isOpenArray() const noexcept {
    if (_stack.empty()) {
      return false;
    }
    const ValueLength pos = _stack.back().start_pos;
    return _start[pos] == 0x06 || _start[pos] == 0x13;
  }

  bool isOpenObject() const noexcept {
    if (_stack.empty()) {
      return false;
    }
    const ValueLength pos = _stack.back().start_pos;
    return _start[pos] == 0x0b || _start[pos] == 0x14;
  }

  void openArray(bool unindexed = false) {
    openCompoundValue(unindexed ? 0x13 : 0x06);
  }

  void openObject(bool unindexed = false) {
    openCompoundValue(unindexed ? 0x14 : 0x0b);
  }

  template<typename T>
  uint8_t* addUnchecked(Slice key, T sub) {
    bool need_cleanup = !_stack.empty();
    if (need_cleanup) {
      reportAdd();
    }
    try {
      set(key);
      return writeValue(sub);
    } catch (...) {
      // clean up in case of an exception
      if (need_cleanup) {
        cleanupAdd();
      }
      throw;
    }
  }

  template<typename T>
  uint8_t* addUnchecked(std::string_view key, T sub) {
    bool need_cleanup = !_stack.empty();
    if (need_cleanup) {
      reportAdd();
    }
    try {
      set(key);
      return writeValue(sub);
    } catch (...) {
      // clean up in case of an exception
      if (need_cleanup) {
        cleanupAdd();
      }
      throw;
    }
  }

  // Add a subvalue into an array from a bool:
  uint8_t* add(std::string_view k, bool v) { return addInternal(k, v); }

  // Add a subvalue into an array from a double:
  uint8_t* add(std::string_view k, double v) { return addInternal(k, v); }

  // Add a subvalue into an array from a int64_t:
  uint8_t* add(std::string_view k, signed long long v) {
    return addInternal<int64_t>(k, v);
  }
  uint8_t* add(std::string_view k, signed long v) {
    return addInternal<int64_t>(k, v);
  }
  uint8_t* add(std::string_view k, signed v) {
    return addInternal<int64_t>(k, v);
  }

  // Add a subvalue into an array from a uint64_t:
  uint8_t* add(std::string_view k, unsigned long long v) {
    return addInternal<uint64_t>(k, v);
  }
  uint8_t* add(std::string_view k, unsigned long v) {
    return addInternal<uint64_t>(k, v);
  }
  uint8_t* add(std::string_view k, unsigned v) {
    return addInternal<uint64_t>(k, v);
  }

  // Add a subvalue into an object from a Value:
  uint8_t* add(std::string_view k, Value v) { return addInternal(k, v); }

  // Add a subvalue into an object from a Slice:
  uint8_t* add(std::string_view k, Slice v) { return addInternal(k, v); }

  // Add a subvalue into an object from a std::string_view:
  uint8_t* add(std::string_view k, std::string_view v) {
    return addInternal(k, v);
  }
  uint8_t* add(std::string_view k, const char* v) {
    return addInternal<std::string_view>(k, v);
  }

  // Add a subvalue into an object from a IStringFromParts:
  uint8_t* add(std::string_view k, const IStringFromParts& v) {
    return addInternal<const IStringFromParts&>(k, v);
  }

  // Add a subvalue into an array from a bool:
  uint8_t* add(bool v) { return addInternal(v); }

  // Add a subvalue into an array from a double:
  uint8_t* add(double v) { return addInternal(v); }

  // Add a subvalue into an array from a int64_t:
  uint8_t* add(int64_t v) { return addInternal(v); }
  uint8_t* add(int32_t v) { return addInternal<int64_t>(v); }

  // Add a subvalue into an array from a uint64_t:
  uint8_t* add(uint64_t v) { return addInternal(v); }
  uint8_t* add(uint32_t v) { return addInternal<uint64_t>(v); }

  // Add a subvalue into an array from a Value:
  uint8_t* add(Value v) { return addInternal(v); }

  // Add a slice to an array
  uint8_t* add(Slice v) { return addInternal(v); }

  // Add a subvalue into an array from a std::string_view:
  uint8_t* add(std::string_view v) { return addInternal(v); }
  uint8_t* add(const char* v) { return addInternal<std::string_view>(v); }

  // Add a subvalue into an array from a ValueString2Parts:
  uint8_t* add(const IStringFromParts& v) {
    return addInternal<const IStringFromParts&>(v);
  }

  // Add all subkeys and subvalues into an object from an ObjectIterator
  // and leaves open the object intentionally
  uint8_t* add(ObjectIterator&& v);

  // Add all subvalues into an array from an ArrayIterator
  // and leaves open the array intentionally
  uint8_t* add(ArrayIterator&& v);

  // Seal the innermost array or object:
  Builder& close();

  // return an attribute from an Object value
  Slice getKey(std::string_view key) const;

  void resetTo(size_t value) {
    _pos = value;
    SDB_ASSERT(_buffer);
    _buffer->resetTo(value);
  }

  // move byte position x bytes ahead
  void advance(size_t value) noexcept {
    _pos += value;
    SDB_ASSERT(_buffer);
    _buffer->advance(value);
  }

 private:
  VPACK_FORCE_INLINE void storeUnsigned(uint8_t* to, uint8_t len, auto v) {
    static_assert(std::is_unsigned_v<decltype(v)>);
    if constexpr (sizeof(v) == 1) {
      *to = v;
    } else if constexpr (sizeof(v) == 2) {
      absl::little_endian::Store16(to, v);
    } else if constexpr (sizeof(v) == 4) {
      absl::little_endian::Store32(to, v);
    } else {
      static_assert(sizeof(v) == 8);
      absl::little_endian::Store64(to, v);
    }
    advance(len);
  }
  VPACK_FORCE_INLINE void storeUnsigned(uint8_t* to, uint8_t header,
                                        uint8_t len, auto v) {
    *to = header;
    storeUnsigned(to + 1, len + 1, v);
  }

  void closeLevel() noexcept;

  void sortObjectIndexShort(uint8_t* obj_base,
                            std::vector<index_t>::iterator index_start,
                            std::vector<index_t>::iterator index_end) const;

  void sortObjectIndexLong(uint8_t* obj_base,
                           std::vector<index_t>::iterator index_start,
                           std::vector<index_t>::iterator index_end) const;

  void sortObjectIndex(uint8_t* obj_base,
                       std::vector<index_t>::iterator index_start,
                       std::vector<index_t>::iterator index_end) {
    const size_t n = std::distance(index_start, index_end);

    if (n > 32) {
      sortObjectIndexLong(obj_base, index_start, index_end);
    } else {
      sortObjectIndexShort(obj_base, index_start, index_end);
    }
  }

  // close for the empty case:
  Builder& closeEmptyArrayOrObject(ValueLength pos, bool is_array);

  // close for the compact case:
  bool closeCompactArrayOrObject(ValueLength pos, bool is_array,
                                 std::vector<index_t>::iterator index_start,
                                 std::vector<index_t>::iterator index_end);

  // close for the array case:
  Builder& closeArray(ValueLength pos,
                      std::vector<index_t>::iterator index_start,
                      std::vector<index_t>::iterator index_end);

  void addNull() { appendByte(0x18); }

  void addBool(bool v) { appendByte(v ? 0x1a : 0x19); }

  void addDouble(double v) {
    reserve(1 + sizeof(double));
    const auto u = std::bit_cast<uint64_t>(v);
    storeUnsigned(_start + _pos, 0x1f, sizeof(u), u);
  }

  void addInt(int64_t v) {
    if (-6 <= v && v <= 9) {
      appendByte(static_cast<uint8_t>(0x36 + v));
    } else {
      appendInt(v, 0x1f);
    }
  }

  void addUInt(uint64_t v) {
    if (v <= 9) {
      appendByte(static_cast<uint8_t>(0x36 + v));
    } else {
      appendUInt(v, 0x27);
    }
  }

  void checkKeyHasValidType(bool is_valid) {
    if (!_stack.empty()) {
      const auto pos = _stack.back().start_pos;
      if (_start[pos] == 0x0b || _start[pos] == 0x14) {
        if (!_key_written && !is_valid) [[unlikely]] {
          throw Exception(Exception::kBuilderKeyMustBeString);
        }
        _key_written = !_key_written;
      }
    }
  }

  void checkKeyHasValidType(Slice item) {
    if (!_stack.empty()) {
      return checkKeyHasValidType(item.isString());
    }
  }

  void addArray(bool unindexed = false) {
    addCompoundValue(unindexed ? 0x13 : 0x06);
  }

  void addObject(bool unindexed = false) {
    addCompoundValue(unindexed ? 0x14 : 0x0b);
  }

  template<typename T>
  uint8_t* addInternal(T sub) {
    if (_stack.empty()) {
      return set(sub);
    }

    if (!_key_written) {
      reportAdd();
    }
    try {
      return set(sub);
    } catch (...) {
      // clean up in case of an exception
      cleanupAdd();
      throw;
    }
  }

  template<typename T>
  uint8_t* addInternal(std::string_view attr_name, T sub) {
    bool have_reported = false;
    if (!_stack.empty()) {
      const ValueLength to = _stack.back().start_pos;
      if (_start[to] != 0x0b && _start[to] != 0x14) [[unlikely]] {
        throw Exception(Exception::kBuilderNeedOpenObject);
      }
      if (_key_written) [[unlikely]] {
        throw Exception(Exception::kBuilderKeyAlreadyWritten);
      }
      reportAdd();
      have_reported = true;
    }

    try {
      set(attr_name);
      return writeValue<T>(sub);
    } catch (...) {
      // clean up in case of an exception
      if (have_reported) {
        cleanupAdd();
      }
      throw;
    }
  }

  template<typename T>
  uint8_t* writeValue(T sub) {
    _key_written = true;
    return set(sub);
  }

  void addCompoundValue(uint8_t type) {
    reserve(9);
    // an Array or Object is started:
    _stack.push_back(CompoundInfo{_pos, static_cast<index_t>(_indexes.size())});
    appendByteUnchecked(type);
    std::memset(_start + _pos, 0, 8);
    advance(8);  // Will be filled later with bytelength and nr subs
  }

  void openCompoundValue(uint8_t type) {
    if (_stack.empty()) {
      addCompoundValue(type);
    } else if (_key_written) {
      _key_written = false;
      addCompoundValue(type);
    } else {
      const ValueLength to = _stack.back().start_pos;
      if (_start[to] != 0x06 && _start[to] != 0x13) [[unlikely]] {
        throw Exception(Exception::kBuilderNeedOpenArray);
      }
      reportAdd();
      try {
        addCompoundValue(type);
      } catch (...) {
        cleanupAdd();
        throw;
      }
    }
  }

  uint8_t* set(bool item) {
    checkKeyHasValidType(false);
    const auto old_pos = _pos;
    addBool(item);
    return _start + old_pos;
  }

  uint8_t* set(double item) {
    checkKeyHasValidType(false);
    const auto old_pos = _pos;
    addDouble(item);
    return _start + old_pos;
  }

  uint8_t* set(int64_t item) {
    checkKeyHasValidType(false);
    const auto old_pos = _pos;
    addInt(item);
    return _start + old_pos;
  }

  uint8_t* set(uint64_t item) {
    checkKeyHasValidType(false);
    const auto old_pos = _pos;
    addUInt(item);
    return _start + old_pos;
  }

  uint8_t* set(Value item);

  uint8_t* set(std::string_view item);

  uint8_t* set(const IStringFromParts& parts);

  uint8_t* set(Slice item);

  void cleanupAdd() noexcept;

  void reportAdd();

  void appendUInt(uint64_t v, uint8_t base) {
    reserve(1 + sizeof(uint64_t));
    const auto len = ((std::bit_width(v | 1) - 1) / 8) + 1;
    storeUnsigned(_start + _pos, base + len, len, v);
  }

  void appendInt(int64_t v, uint8_t base) {
    reserve(1 + sizeof(int64_t));
    auto* to = _start + _pos;
    auto store_signed = [&](auto i) {
      const auto u = std::bit_cast<std::make_unsigned_t<decltype(i)>>(i);
      storeUnsigned(to, base + sizeof(u), sizeof(u), u);
    };
    const auto u = sdb::ZigZagEncode64(v);
    switch ((std::bit_width(u | 1) - 1) / 8) {
      case 0:
        return store_signed(static_cast<int8_t>(v));
      case 1:
        return store_signed(static_cast<int16_t>(v));
      case 2:
        return storeUnsigned(to, base + 3, 3, static_cast<uint32_t>(u));
      case 3:
        return store_signed(static_cast<int32_t>(v));
      case 4:
        return storeUnsigned(to, base + 5, 5, u);
      case 5:
        return storeUnsigned(to, base + 6, 6, u);
      case 6:
        return storeUnsigned(to, base + 7, 7, u);
      case 7:
        return store_signed(static_cast<int64_t>(v));
      default:
        SDB_UNREACHABLE();
    }
  }

  void appendByte(uint8_t value) {
    reserve(1);
    appendByteUnchecked(value);
  }

  void appendByteUnchecked(uint8_t value) {
    _start[_pos++] = value;
    SDB_ASSERT(_buffer);
    _buffer->advance();
  }

  // move byte position x bytes back
  void rollback(size_t value) noexcept {
    _pos -= value;
    SDB_ASSERT(_buffer);
    _buffer->rollback(value);
  }

  bool checkAttributeUniqueness(Slice obj) const;
  bool checkAttributeUniquenessSorted(Slice obj) const;
  bool checkAttributeUniquenessUnsorted(Slice obj) const;
};

struct BuilderContainer {
  explicit BuilderContainer(Builder* builder) : builder{builder} {}

  BuilderContainer(const BuilderContainer&) = delete;
  BuilderContainer& operator=(const BuilderContainer&) = delete;

  Builder& operator*() { return *builder; }
  const Builder& operator*() const { return *builder; }
  Builder* operator->() { return builder; }
  const Builder* operator->() const { return builder; }

  ~BuilderContainer() {
    try {
      if (!builder->isClosed()) {
        builder->close();
      }
    } catch (...) {
      // destructors must not throw. however, we can at least
      // signal something is very wrong in debug mode
      SDB_ASSERT(builder->isClosed());
    }
  }

  Builder* builder;
};

struct ObjectBuilder final : BuilderContainer {
  explicit ObjectBuilder(Builder* builder) : ObjectBuilder{builder, false} {}

  // this stunt is only necessary to prevent implicit conversions
  // from char const* to bool for the second call argument
  template<typename T, typename = std::enable_if_t<std::is_same_v<bool, T>>>
  explicit ObjectBuilder(Builder* builder, T allow_unindexed)
    : BuilderContainer{builder} {
    builder->openObject(allow_unindexed);
  }

  explicit ObjectBuilder(Builder* builder, std::string_view attribute_name,
                         bool allow_unindexed = false)
    : BuilderContainer{builder} {
    builder->add(attribute_name, Value{ValueType::Object, allow_unindexed});
  }
};

struct ArrayBuilder final : BuilderContainer {
  explicit ArrayBuilder(Builder* builder) : ArrayBuilder{builder, false} {}

  // this stunt is only necessary to prevent implicit conversions
  // from char const* to bool for the second call argument
  template<typename T, typename = std::enable_if_t<std::is_same_v<bool, T>>>
  explicit ArrayBuilder(Builder* builder, T allow_unindexed)
    : BuilderContainer{builder} {
    builder->openArray(allow_unindexed);
  }

  explicit ArrayBuilder(Builder* builder, std::string_view attribute_name,
                        bool allow_unindexed = false)
    : BuilderContainer{builder} {
    builder->add(attribute_name, Value{ValueType::Array, allow_unindexed});
  }
};

}  // namespace vpack
