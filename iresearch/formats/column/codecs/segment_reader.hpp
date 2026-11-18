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
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
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

  template<typename T>
  T Read() {
    static_assert(std::is_trivially_copyable_v<T>);
    CheckForwardRead(sizeof(T));
    const auto result = duckdb::Load<T>(_data + _position);
    _position += sizeof(T);
    return result;
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

  duckdb::unsafe_array_ptr<const uint8_t> GetBytes(duckdb::idx_t offset,
                                                   duckdb::idx_t length) const {
    CheckRange(offset, length);
    return {_data + offset, length};
  }

  template<typename T>
  duckdb::unsafe_array_ptr<const T> GetArray(duckdb::idx_t offset,
                                             duckdb::idx_t count) const {
    static_assert(std::is_trivially_copyable_v<T>);
    CheckRange(offset, 0);
    if (reinterpret_cast<uintptr_t>(_data + offset) % alignof(T) != 0)
      [[unlikely]] {
      Throw("array offset is misaligned");
    }
    if (count > (_size - offset) / sizeof(T)) [[unlikely]] {
      Throw("array count extends past the end of the segment");
    }
    return {reinterpret_cast<const T*>(_data + offset), count};
  }

  SegmentReader GetSubReader(duckdb::idx_t offset, duckdb::idx_t length,
                             const char* context) const {
    CheckRange(offset, length);
    return {_data + offset, length, context};
  }

  void Skip(duckdb::idx_t length) {
    CheckForwardRead(length);
    _position += length;
  }

 private:
  void CheckForwardRead(duckdb::idx_t length) const {
    if (length > _size - _position) [[unlikely]] {
      Throw("attempted to read past the end of the segment");
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
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("Corrupted ", _context, ": ", what));
  }

  duckdb::const_data_ptr_t _data;
  duckdb::idx_t _size;
  duckdb::idx_t _position = 0;
  const char* _context;
};

}  // namespace irs::codecs
