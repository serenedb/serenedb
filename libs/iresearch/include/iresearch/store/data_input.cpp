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

#include "data_input.hpp"

#include <memory>

#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/utils/numeric_utils.hpp"

namespace irs {

InputBuf::InputBuf(IndexInput* in) : _in(in) { SDB_ASSERT(_in); }

std::streamsize InputBuf::xsgetn(InputBuf::char_type* c, std::streamsize size) {
  [[maybe_unused]] const auto read =
    _in->ReadBytes(reinterpret_cast<byte_type*>(c), size);
  SDB_ASSERT(read == size_t(size));
  return size;
}

InputBuf::int_type InputBuf::underflow() {
  // FIXME add 'peek()' function to 'index_input'
  const auto ch = uflow();
  if (EOF != ch) {
    _in->Seek(_in->Position() - 1);
  }
  return ch;
}

InputBuf::int_type InputBuf::uflow() {
  if (_in->IsEOF())
    return EOF;
  return traits_type::to_int_type(_in->ReadByte());
}

std::streamsize InputBuf::showmanyc() {
  return _in->Length() - _in->Position();
}

byte_type BufferedIndexInput::ReadByte() {
  if (_begin >= _end) {
    Refill();
  }

  return *_begin++;
}

const byte_type* BufferedIndexInput::ReadView(uint64_t count) {
  if (count <= Remain()) {
    auto begin = _begin;
    _begin += count;
    return begin;
  }

  if (count <= std::min(_buf_size, Length() - Position())) {
    SeekInternal(Position());
    Refill();

    auto begin = _begin;
    _begin += count;
    return begin;
  }

  return nullptr;
}

size_t BufferedIndexInput::ReadBytes(byte_type* b, size_t count) {
  SDB_ASSERT(_begin <= _end);

  // read remaining data from buffer
  const auto read = std::min(count, Remain());
  if (read) {
    std::memcpy(b, _begin, sizeof(byte_type) * read);
    _begin += read;
  }

  if (read == count) {
    return read;  // it's enough for us
  }

  auto size = count - read;
  b += read;
  if (size < _buf_size) {  // refill buffer & copy
    size = std::min(size, Refill());
    std::memcpy(b, _begin, sizeof(byte_type) * size);
    _begin += size;
  } else {  // read directly to output buffer if possible
    size = ReadInternal(b, size);
    _start += (Offset() + size);
    _begin = _end = _buf;  // will trigger refill on the next read
  }

  return read + size;
}

uint64_t BufferedIndexInput::Refill() {
  const auto data_start = this->Position();
  const auto data_end = std::min(data_start + _buf_size, Length());

  const ptrdiff_t data_size = data_end - data_start;
  if (data_size <= 0) {
    return 0;  // read past eof
  }

  SDB_ASSERT(_buf);
  _begin = _buf;
  _end = _begin + ReadInternal(_buf, data_size);
  _start = data_start;

  return data_size;
}

void BufferedIndexInput::Seek(uint64_t pos) {
  if (pos >= _start && pos < (_start + Size())) {
    _begin = _buf + pos - _start;
  } else {
    SeekInternal(pos);
    _begin = _end = _buf;
    _start = pos;
  }
}

}  // namespace irs
