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

#include "iresearch/formats/column/codecs/segment_dictionary.hpp"

#define ZDICT_STATIC_LINKING_ONLY

#include <zdict.h>

#include <utility>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

constexpr ZDICT_fastCover_params_t kTrainParams{
  .k = 256,
  .d = 8,
  .f = 18,
  .nbThreads = 1,
  .splitPoint = 1.0,
  .accel = 10,
  .zParams = {.compressionLevel = 1},
};

}  // namespace

void TrainDictionary(std::string_view frames, std::span<const size_t> sizes,
                     size_t cap, std::string& dictionary) {
  SDB_ASSERT(std::in_range<unsigned>(sizes.size()));
  dictionary.resize_and_overwrite(
    SegmentDictionaryBytes(frames.size(), cap), [&](char* buf, size_t size) {
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
  if (ZDICT_isError(header) || header >= dictionary.size()) {
    dictionary.clear();
    return;
  }
  dictionary.erase(0, header);
}

}  // namespace irs::codecs
