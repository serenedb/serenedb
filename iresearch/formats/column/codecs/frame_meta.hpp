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

#include <bit>
#include <cstddef>
#include <cstdint>

namespace irs::codecs {

static_assert(std::endian::native == std::endian::little);

struct FrameMeta {
  uint32_t first_entry = 0;
  uint32_t raw_len = 0;
  uint32_t comp_off = 0;
  uint32_t comp_len = 0;
};

inline constexpr size_t kFrameMetaSize = 16;
static_assert(sizeof(FrameMeta) == kFrameMetaSize);
static_assert(offsetof(FrameMeta, first_entry) == 0);
static_assert(offsetof(FrameMeta, raw_len) == 4);
static_assert(offsetof(FrameMeta, comp_off) == 8);
static_assert(offsetof(FrameMeta, comp_len) == 12);

}  // namespace irs::codecs
