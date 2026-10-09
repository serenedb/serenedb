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

#include "iresearch/formats/posting/block_codec.hpp"

namespace irs::block_codec {

const DeltaDecoders kDeltaDecoders = [] {
#ifdef __AVX2__
  __builtin_cpu_init();
  if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl") &&
      __builtin_cpu_supports("avx512bw") &&
      __builtin_cpu_supports("avx512dq")) {
    if (__builtin_cpu_supports("avx512vbmi2")) {
      return kDeltaDecodersOf<true, true>;
    }
    return kDeltaDecodersOf<true>;
  }
#endif
  return kDeltaDecodersOf<false>;
}();

}  // namespace irs::block_codec
