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

#include "iresearch/formats/column/codecs/trained_dictionary.hpp"

#define ZSTD_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY
#include <zdict.h>
#include <zstd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

std::atomic<uint64_t> gNextDictionaryId{1};

constexpr ZDICT_fastCover_params_t kTrainParams{
  .k = 256,
  .d = 8,
  .f = 20,
  .nbThreads = 1,
  .splitPoint = 1.0,
  .accel = 4,
  .zParams = {.compressionLevel = 3},
};

ZSTD_DDict* LoadDictionary(std::string_view bytes) {
  auto* ddict =
    ZSTD_createDDict_advanced(bytes.data(), bytes.size(), ZSTD_dlm_byRef,
                              ZSTD_dct_auto, ZSTD_defaultCMem);
  SDB_ENSURE(ddict, "zstd: cannot load a trained dictionary");
  return ddict;
}

}  // namespace

TrainedDictionary::TrainedDictionary(std::string bytes)
  : _owned{std::move(bytes)},
    _bytes{_owned},
    _ddict{LoadDictionary(_bytes)},
    _id{gNextDictionaryId.fetch_add(1, std::memory_order_relaxed)} {}

TrainedDictionary::TrainedDictionary(std::string_view mapped,
                                     IndexInput::ptr mapping)
  : _mapping{std::move(mapping)},
    _bytes{mapped},
    _ddict{LoadDictionary(_bytes)},
    _id{gNextDictionaryId.fetch_add(1, std::memory_order_relaxed)} {}

void DictionarySampler::Add(std::span<const std::string_view> entries) {
  uint64_t total = 0;
  for (const auto e : entries) {
    total += e.size();
  }
  if (total < kSamplePieceBytes) {
    return;
  }
  const uint64_t every = std::max<uint64_t>(1, total / kRowGroupSampleBytes);
  uint64_t piece = 0;
  size_t fill = 0;
  for (auto e : entries) {
    while (!e.empty()) {
      const auto take = std::min(e.size(), kSamplePieceBytes - fill);
      if (piece % every == 0) {
        _samples.append(e.substr(0, take));
      }
      fill += take;
      e.remove_prefix(take);
      if (fill == kSamplePieceBytes) {
        fill = 0;
        ++piece;
      }
    }
  }
  if (fill != 0 && piece % every == 0) {
    _samples.resize(_samples.size() - fill);
  }
}

std::shared_ptr<const TrainedDictionary> DictionarySampler::Train() {
  if (!Ready()) {
    return nullptr;
  }
  const std::vector<size_t> sizes(_samples.size() / kSamplePieceBytes,
                                  kSamplePieceBytes);
  size_t n = 0;
  std::string dictionary;
  dictionary.resize_and_overwrite(
    kTrainedDictionaryBytes, [&](char* buf, size_t size) {
      n = ZDICT_trainFromBuffer_fastCover(
        buf, size, _samples.data(), sizes.data(),
        static_cast<unsigned>(sizes.size()), kTrainParams);
      return ZDICT_isError(n) ? 0 : n;
    });
  std::string{}.swap(_samples);
  if (ZDICT_isError(n)) {
    return nullptr;
  }
  return std::make_shared<const TrainedDictionary>(std::move(dictionary));
}

}  // namespace irs::codecs
