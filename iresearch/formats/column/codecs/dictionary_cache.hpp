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

#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/optional_idx.hpp>
#include <duckdb/common/vector/dictionary_vector.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <string>
#include <utility>

namespace irs::codecs {

class DecodedDictionary final : public duckdb::ObjectCacheEntry {
 public:
  static std::string ObjectType() { return "col-dict"; }

  DecodedDictionary(duckdb::buffer_ptr<duckdb::DictionaryEntry> dictionary,
                    uint64_t bytes) noexcept
    : _dictionary{std::move(dictionary)}, _bytes{bytes} {}

  std::string GetObjectType() final { return ObjectType(); }

  duckdb::optional_idx GetEstimatedCacheMemory() const final { return _bytes; }

  const duckdb::buffer_ptr<duckdb::DictionaryEntry>& Dictionary()
    const noexcept {
    return _dictionary;
  }

 private:
  duckdb::buffer_ptr<duckdb::DictionaryEntry> _dictionary;
  uint64_t _bytes;
};

inline std::string DictionaryCacheKey(uint64_t scope, size_t block) {
  return absl::StrCat("col-dict:", scope, ":", block);
}

}  // namespace irs::codecs
