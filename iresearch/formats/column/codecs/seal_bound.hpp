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
#include <duckdb/common/types.hpp>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/index/column_info.hpp"

namespace irs::codecs {

struct SealLeaf {
  duckdb::PhysicalType physical = duckdb::PhysicalType::INVALID;
  uint64_t rows = 0;
  uint64_t heap = 0;
};

uint64_t SealBytes(const SealLeaf& leaf, CodecType method,
                   const ColCodecParams& params) noexcept;

}  // namespace irs::codecs
