////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <memory>
#include <string>
#include <string_view>

#include "iresearch/utils/fasttext_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace sdb::fast_text {

class Model final : public fasttext::ImmutableFastText,
                    public duckdb::ObjectCacheEntry {
 public:
  static constexpr std::string_view ObjectType() {
    return "sdb_fasttext_model";
  }

  std::string GetObjectType() final { return std::string{ObjectType()}; }

  explicit Model(std::string_view location) {
    loadModel(std::string{location});
  }

  duckdb::optional_idx GetEstimatedCacheMemory() const final;
};

template<typename T>
duckdb::shared_ptr<const T> GetOrBuildModel(duckdb::SharedObjectCache& cache,
                                            std::string_view location) {
  try {
    auto model = cache.GetOrBuild<Model>(
      location, [&] { return duckdb::make_uniq<Model>(location); });
    const T* raw = model.get();
    return duckdb::shared_ptr<const T>{std::move(model), raw};
  } catch (const std::exception& e) {
    THROW_SQL_ERROR(ERR_MSG("failed to load fasttext model from: ", location,
                            ", error: ", e.what()));
  } catch (...) {
    THROW_SQL_ERROR(ERR_MSG("failed to load fasttext model from: ", location));
  }
}

}  // namespace sdb::fast_text
