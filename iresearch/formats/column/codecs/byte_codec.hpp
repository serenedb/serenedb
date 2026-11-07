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

#include <algorithm>
#include <cstdint>

namespace irs::codecs {

enum class ByteCodec : uint8_t {
  Lz4 = 0,
  Zstd = 1,
  Fsst = 2,
  Zxc = 3,
  FsstPlus = 4,
  OnPair = 5,
};

inline constexpr uint8_t kByteCodecCount = 6;

constexpr bool Sorted(ByteCodec leaf) noexcept {
  return leaf == ByteCodec::Fsst || leaf == ByteCodec::FsstPlus ||
         leaf == ByteCodec::OnPair;
}

template<typename Codec>
constexpr uint8_t EffectiveLevel(uint8_t level) noexcept {
  return level ? std::min(level, Codec::kMaxLevel) : Codec::kDefaultLevel;
}

enum class Shape : uint8_t {
  Dedup = 0,
  Plain = 1,
};

struct StringChoice {
  Shape shape;
  ByteCodec leaf;

  bool operator==(const StringChoice&) const = default;
};

}  // namespace irs::codecs
