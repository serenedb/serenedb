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

#include <iterator>
#include <memory>
#include <streambuf>

#include "basics/noncopyable.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

class DataInput {
 public:
  virtual ~DataInput() = default;

  enum class Type {
    Generic,
    MMapIndexInput,
  };

  virtual Type GetType() const noexcept { return Type::Generic; }

  // If supported, provides access to an internal buffer containing
  // the requested 'count' of bytes. Stream guarantees that buffer is immutable
  // and will reside in memory while underlying stream is open.
  virtual const byte_type* ReadData(uint64_t count) = 0;

  // If supported, provides access to an internal buffer containing
  // the requested 'count' of bytes. Stream guarantees that buffer is immutable
  // and will reside in memory while underlying stream is open
  // and before the next read operation.
  virtual const byte_type* ReadView(uint64_t count) = 0;

  virtual byte_type ReadByte() = 0;
  virtual size_t ReadBytes(byte_type* b, size_t count) = 0;

  virtual int16_t ReadI16() = 0;
  virtual int32_t ReadI32() = 0;
  virtual int64_t ReadI64() = 0;
  virtual uint32_t ReadV32() = 0;
  virtual uint64_t ReadV64() = 0;

  virtual uint64_t Position() const noexcept = 0;
  virtual uint64_t Length() const noexcept = 0;
  // @note calling "ReadByte()" on a stream in EOF state is undefined behavior
  virtual bool IsEOF() const noexcept = 0;

  virtual void Skip(uint64_t count) = 0;

  // TODO(mbkkt) Remove this
  using iterator_category = std::forward_iterator_tag;
  using value_type = byte_type;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  IRS_FORCE_INLINE byte_type operator*(this auto& self) {
    return self.ReadByte();
  }
  IRS_FORCE_INLINE auto& operator++(this auto& self) noexcept { return self; }
  IRS_FORCE_INLINE auto& operator++(this auto& self, int) noexcept {
    return self;
  }
};

class IndexInput : public DataInput {
 public:
  using ptr = std::unique_ptr<IndexInput>;

  using DataInput::ReadData;
  virtual const byte_type* ReadData(uint64_t offset, uint64_t count) = 0;

  using DataInput::ReadView;
  virtual const byte_type* ReadView(uint64_t offset, uint64_t count) = 0;

  using DataInput::ReadBytes;
  virtual size_t ReadBytes(uint64_t offset, byte_type* b, size_t count) = 0;

  virtual void Seek(uint64_t pos) = 0;

  // TODO(mbkkt) now they're both implemented the same they,
  // also it doesn't look like all users aware. Maybe we should remove dup?
  virtual ptr Dup() const = 0;     // thread-unsafe fd copy (offset preserved)
  virtual ptr Reopen() const = 0;  // thread-safe fd copy (offset preserved)

  // Checksum from the current position to a specified offset
  // without changing current position.
  // TODO(mbkkt) Maybe change offset to count?
  virtual uint32_t Checksum(uint64_t offset) const = 0;

  virtual uint64_t CountMappedMemory() const { return 0; }

  IndexInput& operator=(const IndexInput&) = delete;

 protected:
  IndexInput() = default;
  IndexInput(const IndexInput&) = default;
};

class InputBuf final : public std::streambuf, util::Noncopyable {
 public:
  using char_type = std::streambuf::char_type;
  using int_type = std::streambuf::int_type;

  explicit InputBuf(IndexInput* in);

  std::streamsize showmanyc() final;

  std::streamsize xsgetn(char_type* s, std::streamsize size) final;

  int_type uflow() final;

  int_type underflow() final;

  operator IndexInput&() { return *_in; }
  IndexInput* Internal() const { return _in; }

 private:
  IndexInput* _in;
};

class BufferedIndexInput : public IndexInput {
 public:
  const byte_type* ReadData(uint64_t count) noexcept final { return nullptr; }
  const byte_type* ReadData(uint64_t offset, uint64_t count) noexcept final {
    return nullptr;
  }

  const byte_type* ReadView(uint64_t count) final;
  const byte_type* ReadView(uint64_t offset, uint64_t count) final {
    if (count <= _buf_size && offset + count <= Length()) {
      Seek(offset);
      return ReadView(count);
    }
    return nullptr;
  }

  byte_type ReadByte() final;
  size_t ReadBytes(byte_type* b, size_t count) final;
  size_t ReadBytes(uint64_t offset, byte_type* b, size_t count) final {
    Seek(offset);
    return ReadBytes(b, count);
  }

  int16_t ReadI16() final {
    return Remain() < sizeof(uint16_t) ? irs::read<uint16_t>(*this)
                                       : irs::read<uint16_t>(_begin);
  }
  int32_t ReadI32() final {
    return Remain() < sizeof(uint32_t) ? irs::read<uint32_t>(*this)
                                       : irs::read<uint32_t>(_begin);
  }
  int64_t ReadI64() final {
    return Remain() < sizeof(uint64_t) ? irs::read<uint64_t>(*this)
                                       : irs::read<uint64_t>(_begin);
  }
  uint32_t ReadV32() final {
    return Remain() < bytes_io<uint32_t>::kMaxVSize
             ? irs::vread<uint32_t>(*this)
             : irs::vread<uint32_t>(_begin);
  }
  uint64_t ReadV64() final {
    return Remain() < bytes_io<uint64_t>::kMaxVSize
             ? irs::vread<uint64_t>(*this)
             : irs::vread<uint64_t>(_begin);
  }

  uint64_t Position() const noexcept final { return _start + Offset(); }

  void Skip(uint64_t count) final { Seek(Position() + count); }
  void Seek(uint64_t pos) final;

  BufferedIndexInput(const BufferedIndexInput&) = delete;
  BufferedIndexInput& operator=(const BufferedIndexInput&) = delete;

 protected:
  BufferedIndexInput() = default;

  void reset(byte_type* buf, uint64_t size, uint64_t start) noexcept {
    _buf = buf;
    _begin = buf;
    _end = buf;
    _buf_size = size;
    _start = start;
  }

  virtual void SeekInternal(uint64_t pos) = 0;

  virtual size_t ReadInternal(byte_type* b, size_t count) = 0;

  // returns number of reamining bytes in the buffer
  IRS_FORCE_INLINE uint64_t Remain() const noexcept {
    return std::distance(_begin, _end);
  }

 private:
  // number of bytes between begin_ & end_
  uint64_t Refill();

  // number of elements between current position and beginning of the buffer
  IRS_FORCE_INLINE uint64_t Offset() const noexcept {
    return std::distance(_buf, _begin);
  }

  // number of valid bytes in the buffer
  IRS_FORCE_INLINE uint64_t Size() const noexcept {
    return std::distance(_buf, _end);
  }

  byte_type* _buf = nullptr;  // buffer itself
  byte_type* _begin = _buf;   // current position in the buffer
  byte_type* _end = _buf;     // end of the valid bytes in the buffer
  uint64_t _start = 0;        // position of the buffer in file
  uint64_t _buf_size = 0;     // size of the buffer in bytes
};

}  // namespace irs
