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
#include <zstd.h>

#include <atomic>
#include <cstdint>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

std::atomic<uint64_t> gNextDictionaryId{1};

}  // namespace

TrainedDictionary::TrainedDictionary(std::string bytes)
  : _owned{std::move(bytes)},
    _bytes{_owned},
    _id{gNextDictionaryId.fetch_add(1, std::memory_order_relaxed)} {}

TrainedDictionary::TrainedDictionary(std::string_view mapped,
                                     IndexInput::ptr mapping)
  : _mapping{std::move(mapping)},
    _bytes{mapped},
    _id{gNextDictionaryId.fetch_add(1, std::memory_order_relaxed)} {}

const ZSTD_DDict* TrainedDictionary::ZstdDictionary() const {
  absl::call_once(_ddict_once, [&] {
    _ddict.reset(ZSTD_createDDict_advanced(_bytes.data(), _bytes.size(),
                                           ZSTD_dlm_byRef, ZSTD_dct_auto,
                                           ZSTD_defaultCMem));
  });
  SDB_ENSURE(_ddict, "zstd: cannot load a trained dictionary");
  return _ddict.get();
}

}  // namespace irs::codecs
