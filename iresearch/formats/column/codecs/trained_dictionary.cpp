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

}  // namespace

TrainedDictionary::TrainedDictionary(std::string bytes)
  : _bytes{std::move(bytes)},
    _ddict{ZSTD_createDDict_advanced(_bytes.data(), _bytes.size(),
                                     ZSTD_dlm_byRef, ZSTD_dct_auto,
                                     ZSTD_defaultCMem)},
    _id{gNextDictionaryId.fetch_add(1, std::memory_order_relaxed)} {
  SDB_ENSURE(_ddict, "zstd: cannot load a trained dictionary");
}

TrainedDictionary::~TrainedDictionary() { ZSTD_freeDDict(_ddict); }

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
      const bool keep = piece % every == 0;
      if (keep) {
        _samples.append(e.substr(0, take));
      }
      fill += take;
      e.remove_prefix(take);
      if (fill == kSamplePieceBytes) {
        if (keep) {
          _sizes.push_back(kSamplePieceBytes);
        }
        fill = 0;
        ++piece;
      }
    }
  }
  if (fill != 0 && piece % every == 0) {
    _samples.resize(_samples.size() - fill);
  }
}

std::optional<std::string> DictionarySampler::Train() {
  std::string dictionary(kTrainedDictionaryBytes, '\0');
  ZDICT_fastCover_params_t params{};
  params.k = 256;
  params.d = 8;
  params.f = 20;
  params.accel = 4;
  params.nbThreads = 1;
  params.splitPoint = 1.0;
  params.zParams.compressionLevel = 3;
  const auto n = ZDICT_trainFromBuffer_fastCover(
    dictionary.data(), dictionary.size(), _samples.data(), _sizes.data(),
    static_cast<unsigned>(_sizes.size()), params);
  std::string{}.swap(_samples);
  std::vector<size_t>{}.swap(_sizes);
  if (ZDICT_isError(n)) {
    return std::nullopt;
  }
  dictionary.resize(n);
  return dictionary;
}

}  // namespace irs::codecs
