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

#include <memory>

#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class BytesOutput final : public DataOutput {
 public:
  BytesOutput() noexcept = default;

  bytes_view Bytes() const noexcept {
    return {_begin, static_cast<size_t>(_pos - _begin)};
  }

  void Clear() noexcept { _pos = _begin; }

 private:
  void Overflow(const byte_type* data, size_t size) final;

  std::unique_ptr<byte_type[]> _buf;
};

}  // namespace irs
