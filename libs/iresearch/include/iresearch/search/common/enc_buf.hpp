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

#include <type_traits>

#include "basics/empty.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

struct EncBuf {
  ABSL_CACHELINE_ALIGNED uint32_t data[doc_limits::kBlockSize];
};

struct FreqBuf {
  ABSL_CACHELINE_ALIGNED uint32_t data[doc_limits::kBlockSize];
};

struct GatherBuf {
  ABSL_CACHELINE_ALIGNED uint32_t data[doc_limits::kBlockSize]{};
};

struct SlackBuf {
  doc_id_t data[doc_limits::kRunSlack];
};

template<typename InputType>
using NeedEnc = utils::Need<!InputType::kVolatileAlways, EncBuf>;

template<typename InputType, typename Enc>
IRS_FORCE_INLINE uint32_t* EncOf(Enc& enc) noexcept {
  if constexpr (InputType::kVolatileAlways) {
    return nullptr;
  } else {
    return enc.data;
  }
}

}  // namespace irs::search
