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

#include "basics/bit_utils.hpp"
#include "basics/noncopyable.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/io_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

/// various hints for direct buffer access
enum class BufferHint {
  /// buffer is valid until the next read operation
  NORMAL = 0,

  /// stream guarantees that buffer is immutable and will reside
  /// in memory while underlying stream is open
  PERSISTENT,
};

struct DataInput {
  using iterator_category = std::forward_iterator_tag;
  using value_type = byte_type;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  virtual ~DataInput() = default;

  virtual byte_type ReadByte() = 0;

  virtual size_t ReadBytes(byte_type* b, size_t count) = 0;

  /// if supported, provides access to an internal buffer containing
  /// the requested 'count' of bytes
  virtual const byte_type* ReadBuffer(size_t count, BufferHint hint) = 0;

  virtual uint64_t Position() const = 0;

  virtual uint64_t Length() const = 0;

  /// @note calling "ReadByte()" on a stream in EOF state is undefined behavior
  virtual bool IsEOF() const = 0;

  virtual int16_t ReadI16() {
    // important to read as unsigned
    return irs::read<uint16_t>(*this);
  }

  virtual int32_t ReadI32() {
    // important to read as unsigned
    return irs::read<uint32_t>(*this);
  }

  virtual int64_t ReadI64() {
    // important to read as unsigned
    return irs::read<uint64_t>(*this);
  }

  virtual uint32_t ReadV32() { return irs::vread<uint32_t>(*this); }

  virtual uint64_t ReadV64() { return irs::vread<uint64_t>(*this); }

  byte_type operator*() { return ReadByte(); }
  DataInput& operator++() noexcept { return *this; }
  DataInput& operator++(int) noexcept { return *this; }
};

struct IndexInput : public DataInput {
 public:
  using ptr = std::unique_ptr<IndexInput>;

  // TODO(mbkkt) now they're both implemented the same they,
  // also it doesn't look like all users aware. Maybe we should remove dup?
  virtual ptr Dup() const = 0;     // thread-unsafe fd copy (offset preserved)
  virtual ptr Reopen() const = 0;  // thread-safe fd copy (offset preserved)

  virtual void Seek(size_t pos) = 0;
  virtual void Skip(size_t count) { Seek(Position() + count); }

  using DataInput::ReadBytes;
  virtual size_t ReadBytes(size_t offset, byte_type* b, size_t count) = 0;

  using DataInput::ReadBuffer;

  /// if supported, provides access to an internal buffer at the specified
  /// 'offset' containing the requested 'count' of bytes
  /// @note operation is atomic
  /// @note in case of failure stream state doesn't change
  virtual const byte_type* ReadBuffer(size_t offset, size_t count,
                                      BufferHint hint) = 0;

  /// checksum from the current position to a specified offset without changing
  /// current position
  virtual uint32_t Checksum(size_t offset) const = 0;

  virtual uint64_t CountMappedMemory() const { return 0; }

 protected:
  IndexInput() = default;
  IndexInput(const IndexInput&) = default;

 private:
  IndexInput& operator=(const IndexInput&) = delete;
};

class InputBuf final : public std::streambuf, util::Noncopyable {
 public:
  typedef std::streambuf::char_type char_type;
  typedef std::streambuf::int_type int_type;

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
  byte_type ReadByte() final;

  size_t ReadBytes(byte_type* b, size_t count) final;

  size_t ReadBytes(size_t offset, byte_type* b, size_t count) final {
    Seek(offset);
    return ReadBytes(b, count);
  }

  const byte_type* ReadBuffer(size_t size, BufferHint hint) noexcept final;

  const byte_type* ReadBuffer(size_t offset, size_t size,
                              BufferHint hint) noexcept final;

  uint64_t Position() const noexcept final { return _start + Offset(); }

  bool IsEOF() const final { return Position() >= Length(); }

  void Seek(size_t pos) final;
  void Skip(size_t pos) final;

  int16_t ReadI16() final;

  int32_t ReadI32() final;

  int64_t ReadI64() final;

  uint32_t ReadV32() final;

  uint64_t ReadV64() final;

  byte_type operator*() { return ReadByte(); }
  BufferedIndexInput& operator++() noexcept { return *this; }
  BufferedIndexInput& operator++(int) noexcept { return *this; }

 protected:
  BufferedIndexInput() = default;

  void reset(byte_type* buf, size_t size, size_t start) noexcept {
    _buf = buf;
    _begin = buf;
    _end = buf;
    _buf_size = size;
    _start = start;
  }

  virtual void SeekInternal(size_t pos) = 0;

  virtual size_t ReadInternal(byte_type* b, size_t count) = 0;

  // returns number of reamining bytes in the buffer
  IRS_FORCE_INLINE size_t Remain() const noexcept {
    return std::distance(_begin, _end);
  }

 private:
  BufferedIndexInput(const BufferedIndexInput&) = delete;
  BufferedIndexInput& operator=(const BufferedIndexInput&) = delete;

  /// number of bytes between begin_ & end_
  size_t Refill();

  /// number of elements between current position and beginning of the buffer
  IRS_FORCE_INLINE size_t Offset() const noexcept {
    return std::distance(_buf, _begin);
  }

  /// number of valid bytes in the buffer
  IRS_FORCE_INLINE size_t Size() const noexcept {
    return std::distance(_buf, _end);
  }

  byte_type* _buf{};        // buffer itself
  byte_type* _begin{_buf};  // current position in the buffer
  byte_type* _end{_buf};    // end of the valid bytes in the buffer
  size_t _start{};          // position of the buffer in file
  size_t _buf_size{};       // size of the buffer in bytes
};

}  // namespace irs
