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

#include <bitset>
#include <cstdint>
#include <magic_enum/magic_enum.hpp>

namespace irs::codecs {

enum class CodecType : uint8_t {
  Auto,
  Uncompressed,
  Rle,
  Bitpacking,
  Alp,
  AlpRd,
  Roaring,
  Constant,
  Empty,
  Numeric,
  DictLz4,
  Lz4,
  DictZstd,
  Zstd,
  DictZxc,
  Zxc,
  DictFsst,
  Fsst,
  DictFsstPlus,
  FsstPlus,
  DictOnPair,
  OnPair,
  DictFsst12,
  Fsst12,
};

using CodecSet = std::bitset<magic_enum::enum_count<CodecType>()>;

}  // namespace irs::codecs
