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

#include <absl/base/internal/endian.h>

#include <cstring>
#include <memory>
#include <utility>

#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class DataOutput {
 public:
  DataOutput(const DataOutput&) = delete;
  DataOutput& operator=(const DataOutput&) = delete;

  IRS_FORCE_INLINE void WriteByte(byte_type b) {
    if (_pos == _end) [[unlikely]] {
      return Overflow(&b, 1);
    }
    *_pos++ = b;
  }

  IRS_FORCE_INLINE void WriteData(const byte_type* b, size_t size) {
    if (size >= Remain()) [[unlikely]] {
      return Overflow(b, size);
    }
    std::memcpy(_pos, b, size);
    _pos += size;
  }

  IRS_FORCE_INLINE void WriteU16(uint16_t n) { WriteFixed(n); }
  IRS_FORCE_INLINE void WriteU32(uint32_t n) { WriteFixed(n); }
  IRS_FORCE_INLINE void WriteU64(uint64_t n) { WriteFixed(n); }
  IRS_FORCE_INLINE void WriteV32(uint32_t n) { WriteVarint(n); }
  IRS_FORCE_INLINE void WriteV64(uint64_t n) { WriteVarint(n); }

  uint64_t Position() const noexcept {
    return _offset + static_cast<uint64_t>(_pos - _begin);
  }

  byte_type* Reserve(size_t size) noexcept {
    if (Remain() < size) {
      return nullptr;
    }
    auto* reserved = _pos;
    _pos += size;
    return reserved;
  }

 protected:
  DataOutput() noexcept = default;
  ~DataOutput() = default;

  IRS_FORCE_INLINE size_t Remain() const noexcept {
    SDB_ASSERT(_pos <= _end);
    return static_cast<size_t>(_end - _pos);
  }

  byte_type* _begin = nullptr;
  byte_type* _pos = nullptr;
  byte_type* _end = nullptr;
  uint64_t _offset = 0;

 private:
  template<typename N>
  IRS_FORCE_INLINE void WriteFixed(N n) {
    n = absl::little_endian::FromHost(n);
    const auto* b = reinterpret_cast<const byte_type*>(&n);
    if (Remain() < sizeof(n)) [[unlikely]] {
      return Overflow(b, sizeof(n));
    }
    std::memcpy(_pos, b, sizeof(n));
    _pos += sizeof(n);
  }

  template<typename N>
  IRS_FORCE_INLINE void WriteVarint(N n) {
    if (Remain() < bytes_io<N>::kMaxVSize) [[unlikely]] {
      return WriteVarintSlow(n);
    }
    irs::WriteVarint(n, _pos);
  }

  template<typename N>
  IRS_NO_INLINE void WriteVarintSlow(N n) {
    byte_type buf[bytes_io<N>::kMaxVSize];
    auto* end = buf;
    irs::WriteVarint(n, end);
    WriteData(buf, static_cast<size_t>(end - buf));
  }

  virtual void Overflow(const byte_type* data, size_t size) = 0;
};

class IndexOutput : public DataOutput {
 public:
  using ptr = std::unique_ptr<IndexOutput>;

  virtual ~IndexOutput() = default;

  // Flush output
  virtual void Flush() = 0;

  // Flush and compute checksum output
  // TODO(mbkkt) maybe int64_t?
  virtual uint32_t Checksum() = 0;

  // Flush and close output
  void Close() {
    if (std::exchange(_closed, true)) {
      return;
    }
    CloseImpl();
  }

 private:
  virtual void CloseImpl() = 0;

  bool _closed = false;
};

}  // namespace irs
