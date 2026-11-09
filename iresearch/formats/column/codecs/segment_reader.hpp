////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <cstdint>
#include <cstring>
#include <duckdb/common/array_ptr.hpp>
#include <duckdb/common/helper.hpp>
#include <type_traits>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {

class SegmentReader {
 public:
  SegmentReader(duckdb::const_data_ptr_t data, duckdb::idx_t size,
                const char* context) noexcept
    : _data{data}, _size{size}, _context{context} {}

  duckdb::idx_t Size() const noexcept { return _size; }
  duckdb::idx_t Position() const noexcept { return _position; }
  duckdb::idx_t Remaining() const noexcept { return _size - _position; }
  bool Finished() const noexcept { return _position == _size; }

  template<typename T>
  T Read() {
    static_assert(std::is_trivially_copyable_v<T>);
    CheckForwardRead(sizeof(T));
    const auto result = duckdb::Load<T>(_data + _position);
    _position += sizeof(T);
    return result;
  }

  template<typename T>
  T ReadBackward() {
    static_assert(std::is_trivially_copyable_v<T>);
    CheckBackwardRead(sizeof(T));
    _position -= sizeof(T);
    return duckdb::Load<T>(_data + _position);
  }

  template<typename T>
  T Get(duckdb::idx_t offset) const {
    static_assert(std::is_trivially_copyable_v<T>);
    CheckRange(offset, sizeof(T));
    return duckdb::Load<T>(_data + offset);
  }

  duckdb::unsafe_array_ptr<const uint8_t> ReadBytes(duckdb::idx_t length) {
    CheckForwardRead(length);
    duckdb::unsafe_array_ptr<const uint8_t> result{_data + _position, length};
    _position += length;
    return result;
  }

  duckdb::unsafe_array_ptr<const uint8_t> ReadBytesBackward(
    duckdb::idx_t length) {
    CheckBackwardRead(length);
    _position -= length;
    return {_data + _position, length};
  }

  template<typename T>
  duckdb::unsafe_array_ptr<const T> ReadArrayBackward(duckdb::idx_t count) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (count > _position / sizeof(T)) [[unlikely]] {
      Throw("attempted to read before the start of the segment");
    }
    const auto offset = _position - count * sizeof(T);
    auto result = GetArray<T>(offset, count);
    _position = offset;
    return result;
  }

  void ReadBytesInto(duckdb::data_ptr_t destination, duckdb::idx_t length) {
    const auto source = ReadBytes(length);
    std::memcpy(destination, source.data(), length);
  }

  template<typename T, duckdb::idx_t N>
  void ReadBytesIntoArray(T (&destination)[N], duckdb::idx_t length) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (length > sizeof(destination)) [[unlikely]] {
      Throw("read does not fit the destination buffer");
    }
    ReadBytesInto(reinterpret_cast<duckdb::data_ptr_t>(destination), length);
  }

  template<typename T, duckdb::idx_t N>
  void ReadIntoArray(T (&destination)[N], duckdb::idx_t count) {
    static_assert(std::is_trivially_copyable_v<T>);
    if (count > N) [[unlikely]] {
      Throw("read does not fit the destination buffer");
    }
    ReadBytesInto(reinterpret_cast<duckdb::data_ptr_t>(destination),
                  count * sizeof(T));
  }

  template<typename T>
  duckdb::unsafe_array_ptr<const uint8_t> ReadBytesAligned(
    duckdb::idx_t length) {
    CheckForwardRead(length);
    if (reinterpret_cast<uintptr_t>(_data + _position) % alignof(T) != 0)
      [[unlikely]] {
      Throw("array offset is misaligned");
    }
    duckdb::unsafe_array_ptr<const uint8_t> result{_data + _position, length};
    _position += length;
    return result;
  }

  duckdb::unsafe_array_ptr<const uint8_t> GetBytes(duckdb::idx_t offset,
                                                   duckdb::idx_t length) const {
    CheckRange(offset, length);
    return {_data + offset, length};
  }

  template<typename T>
  void CheckArray(duckdb::idx_t offset, duckdb::idx_t count) const {
    static_assert(std::is_trivially_copyable_v<T>);
    CheckRange(offset, 0);
    if (reinterpret_cast<uintptr_t>(_data + offset) % alignof(T) != 0)
      [[unlikely]] {
      Throw("array offset is misaligned");
    }
    if (count > (_size - offset) / sizeof(T)) [[unlikely]] {
      Throw("array count extends past the end of the segment");
    }
  }

  template<typename T>
  duckdb::unsafe_array_ptr<const T> GetArray(duckdb::idx_t offset,
                                             duckdb::idx_t count) const {
    return GetArraySlice<T>(offset, 0, count);
  }

  template<typename T>
  duckdb::unsafe_array_ptr<const T> GetArraySlice(duckdb::idx_t offset,
                                                  duckdb::idx_t start,
                                                  duckdb::idx_t count) const {
    CheckArray<T>(offset, start);
    const auto capacity = (_size - offset) / sizeof(T);
    if (count > capacity - start) [[unlikely]] {
      Throw("array count extends past the end of the segment");
    }
    return {reinterpret_cast<const T*>(_data + offset) + start, count};
  }

  template<typename T>
  T GetArrayElement(duckdb::idx_t offset, duckdb::idx_t array_count,
                    duckdb::idx_t index) const {
    CheckArray<T>(offset, array_count);
    if (index >= array_count) [[unlikely]] {
      Throw("array count extends past the end of the segment");
    }
    return duckdb::Load<T>(_data + offset + index * sizeof(T));
  }

  SegmentReader ReadSubReader(duckdb::idx_t length, const char* context) {
    CheckForwardRead(length);
    SegmentReader sub{_data + _position, length, context};
    _position += length;
    return sub;
  }

  SegmentReader GetSubReader(duckdb::idx_t offset, duckdb::idx_t length,
                             const char* context) const {
    CheckRange(offset, length);
    return {_data + offset, length, context};
  }

  template<typename T>
  SegmentReader GetArraySubReader(duckdb::idx_t offset, duckdb::idx_t count,
                                  const char* context) const {
    CheckArray<T>(offset, count);
    return GetSubReader(offset, count * sizeof(T), context);
  }

  void Skip(duckdb::idx_t length) {
    CheckForwardRead(length);
    _position += length;
  }

  void SetPosition(duckdb::idx_t position) {
    CheckRange(position, 0);
    _position = position;
  }

  void Align(duckdb::idx_t alignment) {
    SDB_ASSERT(alignment != 0);
    const auto remainder = _position % alignment;
    if (remainder == 0) {
      return;
    }
    const auto padding = alignment - remainder;
    CheckForwardRead(padding);
    _position += padding;
  }

  void AlignBackward(duckdb::idx_t alignment) {
    SDB_ASSERT(alignment != 0);
    _position -= _position % alignment;
  }

 private:
  void CheckForwardRead(duckdb::idx_t length) const {
    if (length > _size - _position) [[unlikely]] {
      Throw("attempted to read past the end of the segment");
    }
  }

  void CheckBackwardRead(duckdb::idx_t length) const {
    if (length > _position) [[unlikely]] {
      Throw("attempted to read before the start of the segment");
    }
  }

  void CheckRange(duckdb::idx_t offset, duckdb::idx_t length) const {
    if (offset > _size) [[unlikely]] {
      Throw("offset is outside the segment");
    }
    if (length > _size - offset) [[unlikely]] {
      Throw("requested range extends past the end of the segment");
    }
  }

  [[noreturn]] IRS_NO_INLINE void Throw(const char* what) const {
    ThrowCorrupted(_context, what);
  }

  duckdb::const_data_ptr_t _data;
  duckdb::idx_t _size;
  duckdb::idx_t _position = 0;
  const char* _context;
};

}  // namespace irs::codecs
