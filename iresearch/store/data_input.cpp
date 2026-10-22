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

#include "iresearch/utils/assert.hpp"

namespace irs {

InputBuf::InputBuf(IndexInput* in) : _in(in) { SDB_ASSERT(_in); }

std::streamsize InputBuf::xsgetn(InputBuf::char_type* c, std::streamsize size) {
  _in->ReadData(reinterpret_cast<byte_type*>(c), size);
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
  if (_in->IsEOF()) {
    return EOF;
  }
  return traits_type::to_int_type(_in->ReadByte());
}

std::streamsize InputBuf::showmanyc() {
  return _in->Length() - _in->Position();
}

}  // namespace irs
