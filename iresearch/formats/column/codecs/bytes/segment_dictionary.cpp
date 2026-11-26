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

#include "iresearch/formats/column/codecs/bytes/segment_dictionary.hpp"

#define ZDICT_STATIC_LINKING_ONLY
#define ZSTD_STATIC_LINKING_ONLY

#include <zdict.h>
#include <zstd.h>

#include <utility>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

constexpr ZDICT_fastCover_params_t kTrainParams{
  .k = 256,
  .d = 8,
  .f = 16,
  .nbThreads = 1,
  .splitPoint = 1.0,
  .accel = 10,
  .zParams = {.compressionLevel = 1},
};

size_t TrainScratchBytes(size_t samples, size_t dictionary) noexcept {
  constexpr size_t kTable = size_t{1} << kTrainParams.f;
  constexpr int kLevel = kTrainParams.zParams.compressionLevel;
  return (samples + 1) * sizeof(size_t) +
         kTable * (sizeof(uint32_t) + sizeof(uint16_t)) +
         ZSTD_estimateCCtxSize(kLevel) +
         ZSTD_estimateCDictSize(dictionary, kLevel);
}

}  // namespace

void TrainDictionary(std::string_view frames, std::span<const size_t> sizes,
                     size_t cap, TrackedString& dictionary) {
  SDB_ASSERT(std::in_range<unsigned>(sizes.size()));
  const auto bytes = SegmentDictionaryBytes(frames.size(), cap);
  const MemoryCharge scratch{*dictionary.get_allocator().budget,
                             TrainScratchBytes(sizes.size(), bytes)};
  dictionary.resize_and_overwrite(bytes, [&](char* buf, size_t size) {
    const auto n = ZDICT_trainFromBuffer_fastCover(
      buf, size, frames.data(), sizes.data(),
      static_cast<unsigned>(sizes.size()), kTrainParams);
    return ZDICT_isError(n) ? 0 : n;
  });
  if (dictionary.empty()) {
    return;
  }
  const auto header =
    ZDICT_getDictHeaderSize(dictionary.data(), dictionary.size());
  SDB_ASSERT(!ZDICT_isError(header) && header < dictionary.size());
  dictionary.erase(0, header);
}

}  // namespace irs::codecs
