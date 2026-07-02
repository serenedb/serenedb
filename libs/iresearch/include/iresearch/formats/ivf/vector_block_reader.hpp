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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "basics/assert.h"
#include "iresearch/store/data_input.hpp"
#include "iresearch/types.hpp"

namespace irs {

class VectorBlockReader {
 public:
  VectorBlockReader(IndexInput& in, uint32_t record_size)
    : _in{in}, _record_size{record_size} {}

  void Reset(uint64_t base_offset) {
    _base = base_offset;
    _cur = 0;
    _in.Seek(base_offset);
  }

  const byte_type* Read(size_t index, size_t count) {
    SDB_ASSERT(index >= _cur);
    if (index != _cur) {
      _in.Seek(_base + static_cast<uint64_t>(index) * _record_size);
      _cur = index;
    }
    const size_t bytes = count * size_t{_record_size};
    _buf.resize(bytes);
    _in.ReadData(_buf.data(), bytes);
    _cur += count;
    return _buf.data();
  }

 private:
  IndexInput& _in;
  std::vector<byte_type> _buf;
  uint64_t _base = 0;
  uint32_t _record_size;
  size_t _cur = 0;
};

}  // namespace irs
