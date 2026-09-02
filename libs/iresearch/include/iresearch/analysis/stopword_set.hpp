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

#include <duckdb/storage/shared_object_cache.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/dict/string_table.hpp"

namespace irs::analysis {

struct StopwordSet final : duckdb::ObjectCacheEntry,
                           dict::StringSet<std::string> {
  static constexpr std::string_view ObjectType() { return "stopword_set"; }

  std::string GetObjectType() final { return std::string{ObjectType()}; }

  static duckdb::shared_ptr<const StopwordSet> GetOrBuild(
    duckdb::SharedObjectCache& cache, duckdb::unique_ptr<StopwordSet> set) {
    char hex[duckdb::MD5Context::MD5_HASH_LENGTH_TEXT];
    set->Hash(hex);
    return cache.GetOrBuild<StopwordSet>(std::string_view{hex, sizeof(hex)},
                                         [&] { return std::move(set); });
  }

  explicit StopwordSet(std::vector<std::string> mask) {
    for (auto& word : mask) {
      Insert(std::move(word));
    }
  }

  duckdb::optional_idx GetEstimatedCacheMemory() const final {
    return MemoryBytes();
  }
};

}  // namespace irs::analysis
