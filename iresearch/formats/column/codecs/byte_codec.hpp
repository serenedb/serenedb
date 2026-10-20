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

namespace irs::codecs {

enum class ByteCodec : uint8_t {
  Lz4 = 0,
  Zstd = 1,
  Fsst = 2,
  Zxc = 3,
};

inline constexpr uint8_t kByteCodecCount = 4;

constexpr bool Trainable(ByteCodec leaf) noexcept {
  return leaf == ByteCodec::Lz4 || leaf == ByteCodec::Zstd;
}

template<typename Codec>
constexpr uint8_t EffectiveLevel(uint8_t level) noexcept {
  if (level == 0) {
    return Codec::kDefaultLevel;
  }
  return level < Codec::kMaxLevel ? level : Codec::kMaxLevel;
}

}  // namespace irs::codecs
