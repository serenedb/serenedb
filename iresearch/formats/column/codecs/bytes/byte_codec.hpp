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
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "iresearch/formats/column/codecs/codec_type.hpp"

namespace irs::codecs {

enum class FrameDictionary : uint8_t {
  None,
  FirstFrame,
  Local,
};

constexpr bool Sorted(ByteCodec leaf) noexcept {
  return leaf == ByteCodec::Fsst || leaf == ByteCodec::FsstPlus ||
         leaf == ByteCodec::OnPair || leaf == ByteCodec::Fsst12;
}

template<typename Codec>
constexpr uint8_t EffectiveLevel(uint8_t level) noexcept {
  return level ? std::min(level, Codec::kMaxLevel) : Codec::kDefaultLevel;
}

}  // namespace irs::codecs
