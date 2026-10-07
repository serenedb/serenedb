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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/index/docs_mask/chunks.hpp"

namespace irs::docs_mask {

int32_t ArrayChunk::Forward(const uint16_t* values, int32_t pos, int32_t size,
                            uint32_t low) noexcept {
  const auto bound = static_cast<uint16_t>(low);
  for (uint32_t block = 0; block != kScanBlocks && pos + kScanWidth <= size;
       ++block, pos += kScanWidth) {
    int32_t above = 0;
    for (int32_t i = 0; i != kScanWidth; ++i) {
      above += std::max(values[pos + i], bound) == values[pos + i];
    }
    if (above != 0) {
      return pos + kScanWidth - above;
    }
  }
  return static_cast<int32_t>(
    Gallop(values, static_cast<uint32_t>(pos), static_cast<uint32_t>(size),
           [low](uint16_t v) noexcept { return v < low; }));
}

DenseLayout::DenseLayout(const DocumentMask* mask) noexcept
  : _containers{mask->Containers()},
    _first{mask->Keys()[0]},
    _count{mask->ContainerCount()} {
  SDB_ASSERT(_count != 0);
  SDB_ASSERT(uint32_t{mask->Keys()[_count - 1]} - _first == _count - 1);
}

}  // namespace irs::docs_mask
