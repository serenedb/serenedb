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

#include <duckdb/common/serializer/write_stream.hpp>

#include "basics/noncopyable.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/io_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class DataOutput : public duckdb::WriteStream {
 public:
  // According to C++ standard it's safe to convert signed to unsigned
  // But not opposite!
  // So we should read signed but write unsigned or duplicate code.
  // Duplicate code potentially worse for perfomance:
  //  more virtual functions in vtable, symbols, etc -- code colder

  virtual void WriteByte(byte_type b) = 0;

  virtual void WriteU16(uint16_t n) { WriteNumU(n); }
  virtual void WriteU32(uint32_t n) { WriteNumU(n); }
  virtual void WriteU64(uint64_t n) { WriteNumU(n); }
  virtual void WriteV32(uint32_t n) { WriteNumV(n); }
  virtual void WriteV64(uint64_t n) { WriteNumV(n); }

 protected:
  IRS_FORCE_INLINE void WriteNumU(auto n) {
    // TODO(mbkkt) change to little endian!
    n = absl::little_endian::FromHost(n);
    WriteData(reinterpret_cast<byte_type*>(&n), sizeof(n));
  }

  IRS_FORCE_INLINE void WriteNumV(auto n) {
    WriteVarint(n, [&](byte_type b) { WriteByte(b); });
  }
};

class BufferedOutput : public DataOutput {
 public:
  void WriteByte(byte_type b) final {
    // We can make separate flush virtual function
    // But I think better if buffer is not enough append with flush combined
    if (Remain() == 0) [[unlikely]] {
      return WriteDirect(&b, 1);
    }
    *_pos++ = b;
  }

  void WriteData(const byte_type* b, uint64_t len) final {
    // We need to use <= instead of < to avoid double buffering
    if (Remain() <= len) [[unlikely]] {
      return WriteDirect(b, len);
    }
    WriteBuffer(b, len);
  }

  void WriteU16(uint16_t n) final { WriteNumU(n); }
  void WriteU32(uint32_t n) final { WriteNumU(n); }
  void WriteU64(uint64_t n) final { WriteNumU(n); }
  void WriteV32(uint32_t n) final { WriteNumV(n); }
  void WriteV64(uint64_t n) final { WriteNumV(n); }

 protected:
  explicit BufferedOutput(byte_type* pos, byte_type* end) noexcept
    : _buf{pos}, _pos{pos}, _end{end} {}

  IRS_FORCE_INLINE size_t Length() const noexcept {
    SDB_ASSERT(_buf <= _pos);
    SDB_ASSERT(_pos <= _end);
    return _pos - _buf;
  }

  IRS_FORCE_INLINE size_t Remain() const noexcept {
    SDB_ASSERT(_buf <= _pos);
    SDB_ASSERT(_pos <= _end);
    return _end - _pos;
  }

  virtual void WriteDirect(const byte_type* b, size_t len) = 0;

  IRS_FORCE_INLINE void WriteBuffer(const byte_type* b, size_t len) {
    SDB_ASSERT(_pos);
    SDB_ASSERT(b);
    SDB_ASSERT(len <= Remain());
    std::memcpy(_pos, b, len);
    _pos += len;
  }

  template<typename Output>
  static void Write(Output& output, const byte_type* b, size_t len) {
    auto remain = output.Remain();
    SDB_ASSERT(len >= remain);
    while (len != 0) {
      if (remain == 0) {
        output.FlushBuffer();
        SDB_ASSERT(output.Remain() != 0);
        remain = std::min(len, output.Remain());
      }
      output.WriteBuffer(b, remain);
      b += remain;
      len -= remain;
      SDB_ASSERT(len == 0 || output.Remain() == 0);
      remain = 0;
    }
  }

  // TODO(mbkkt) for some implementation _buf is unneeded,
  //  so maybe better to move this field to implementation
  byte_type* _buf;
  byte_type* _pos;
  byte_type* _end;

 private:
  IRS_NO_INLINE void WriteDirectNumV(auto n) { DataOutput::WriteNumV(n); }

  IRS_FORCE_INLINE void WriteNumU(auto n) {
    static constexpr size_t kNeeded = sizeof(n);
    // TODO(mbkkt) change to little endian!
    n = absl::little_endian::FromHost(n);
    const auto* b = reinterpret_cast<const byte_type*>(&n);
    // We assume buffer size > kNeeded, so double buffering is impossible
    // and we can use < instead of <=
    if (Remain() < kNeeded) [[unlikely]] {
      return WriteDirect(b, kNeeded);
    }
    WriteBuffer(b, kNeeded);
  }

  IRS_FORCE_INLINE void WriteNumV(auto n) {
    static constexpr size_t kNeeded = (sizeof(n) * 8 + 6) / 7;
    if (Remain() < kNeeded) [[unlikely]] {
      return WriteDirectNumV(n);
    }
    WriteVarint(n, _pos);
  }
};

class IndexOutput : public BufferedOutput {
 public:
  using OnClose = absl::FunctionRef<void(uint64_t)>;

  using BufferedOutput::BufferedOutput;

  void SetOnClose(OnClose on_close) noexcept { _on_close = on_close; }

  IRS_USING_IO_PTR(IndexOutput, Close);

  uint64_t Position() const noexcept { return _offset + Length(); }

  // Flush output
  virtual void Flush() = 0;

  // Flush and compute checksum output
  // TODO(mbkkt) maybe int64_t?
  virtual uint32_t Checksum() = 0;

  // Flush and close output
  void Close() {
    const auto size = CloseImpl();
    _on_close(size);
  }

 protected:
  uint64_t _offset = 0;

 private:
  virtual uint64_t CloseImpl() = 0;

  OnClose _on_close{absl::FunctionValue{}, [](uint64_t) noexcept {}};
};

}  // namespace irs
