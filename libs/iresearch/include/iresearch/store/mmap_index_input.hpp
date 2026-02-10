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

#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/mmap_utils.hpp"

namespace irs {

class MMapIndexInput final : public BytesViewInput {
 public:
  explicit MMapIndexInput(
    std::shared_ptr<mmap_utils::MMapHandle> handle) noexcept
    : _handle{std::move(handle)} {
    if (_handle && _handle->size() != 0) [[likely]] {
      SDB_ASSERT(_handle->addr() != MAP_FAILED);
      const auto* begin = reinterpret_cast<byte_type*>(_handle->addr());
      BytesViewInput::reset(begin, _handle->size());
    } else {
      _handle.reset();
    }
  }

  Type GetType() const noexcept final { return Type::MMapIndexInput; }

  uint64_t CountMappedMemory() const final;

  ptr Dup() const final { return std::make_unique<MMapIndexInput>(*this); }

 private:
  std::shared_ptr<mmap_utils::MMapHandle> _handle;
};

}  // namespace irs
