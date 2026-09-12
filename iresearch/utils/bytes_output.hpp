////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class BytesOutput : public DataOutput {
 public:
  explicit BytesOutput(bstring& buf) noexcept : _buf{&buf} {}

  void WriteByte(byte_type b) final { _buf->push_back(b); }

  void WriteData(const byte_type* b, uint64_t size) final {
    _buf->append(b, size);
  }

 private:
  bstring* _buf;
};

}  // namespace irs
