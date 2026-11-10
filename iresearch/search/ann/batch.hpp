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

#include <cstdint>
#include <span>

#include "iresearch/index/column_info.hpp"

namespace irs::ann {

struct QueryBatch {
  std::span<const float> vectors;
  uint32_t d = 0;

  uint32_t Size() const noexcept {
    return static_cast<uint32_t>(vectors.size() / d);
  }

  std::span<const float> Query(uint32_t i) const noexcept {
    return vectors.subspan(size_t{i} * d, d);
  }
};

uint32_t BatchRows(AnnKind kind, uint32_t d) noexcept;

}  // namespace irs::ann
